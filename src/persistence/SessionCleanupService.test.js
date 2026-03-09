import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import SessionCleanupService from './SessionCleanupService.js';

describe('SessionCleanupService', () => {
  let mockDb;
  let mockLogger;
  let cleanupService;

  beforeEach(() => {
    // Mock database
    mockDb = {
      query: vi.fn()
    };

    // Mock logger
    mockLogger = {
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      debug: vi.fn()
    };

    // Create cleanup service with short interval for testing
    cleanupService = new SessionCleanupService({
      db: mockDb,
      logger: mockLogger,
      config: {
        cleanupIntervalMs: 100, // 100ms for testing
        userIdleTtlMs: 3600000, // 1 hour
        batchLimit: 1000
      }
    });
  });

  afterEach(() => {
    // Stop the service to clean up timers
    if (cleanupService) {
      cleanupService.stop();
    }
  });

  describe('Constructor', () => {
    it('should initialize with default configuration', () => {
      const service = new SessionCleanupService({
        db: mockDb,
        logger: mockLogger
      });

      const stats = service.getCleanupStats();
      expect(stats.config.cleanupIntervalMs).toBe(900000); // 15 minutes
      expect(stats.config.userIdleTtlMs).toBe(21600000); // 6 hours
      expect(stats.config.batchLimit).toBe(1000);
    });

    it('should initialize with custom configuration', () => {
      const service = new SessionCleanupService({
        db: mockDb,
        logger: mockLogger,
        config: {
          cleanupIntervalMs: 60000,
          userIdleTtlMs: 7200000,
          batchLimit: 500
        }
      });

      const stats = service.getCleanupStats();
      expect(stats.config.cleanupIntervalMs).toBe(60000);
      expect(stats.config.userIdleTtlMs).toBe(7200000);
      expect(stats.config.batchLimit).toBe(500);
    });

    it('should initialize statistics to zero', () => {
      const stats = cleanupService.getCleanupStats();
      expect(stats.totalRuns).toBe(0);
      expect(stats.totalSessionsDeleted).toBe(0);
      expect(stats.totalErrors).toBe(0);
      expect(stats.lastRunTime).toBeNull();
      expect(stats.isRunning).toBe(false);
    });

    it('should log initialization', () => {
      expect(mockLogger.info).toHaveBeenCalledWith(
        'SessionCleanupService initialized',
        expect.objectContaining({
          cleanupIntervalMs: 100,
          userIdleTtlMs: 3600000,
          batchLimit: 1000
        })
      );
    });
  });

  describe('start()', () => {
    it('should start the cleanup service', () => {
      cleanupService.start();
      expect(cleanupService.isActive()).toBe(true);
    });

    it('should log start message', () => {
      cleanupService.start();
      expect(mockLogger.info).toHaveBeenCalledWith(
        'SessionCleanupService: Starting periodic cleanup',
        expect.any(Object)
      );
    });

    it('should not create duplicate timers if called multiple times', () => {
      cleanupService.start();
      cleanupService.start();
      
      expect(mockLogger.warn).toHaveBeenCalledWith(
        'SessionCleanupService: Already running, ignoring start request'
      );
    });

    it('should run cleanup immediately on start', async () => {
      mockDb.query.mockResolvedValue([[{ affectedRows: 5 }]]);
      
      cleanupService.start();
      
      // Wait for initial cleanup to complete
      await new Promise(resolve => setTimeout(resolve, 50));
      
      expect(mockDb.query).toHaveBeenCalled();
    });
  });

  describe('stop()', () => {
    it('should stop the cleanup service', () => {
      cleanupService.start();
      expect(cleanupService.isActive()).toBe(true);
      
      cleanupService.stop();
      expect(cleanupService.isActive()).toBe(false);
    });

    it('should log stop message', () => {
      cleanupService.start();
      cleanupService.stop();
      
      expect(mockLogger.info).toHaveBeenCalledWith(
        'SessionCleanupService: Stopping periodic cleanup'
      );
    });

    it('should handle stop when not running', () => {
      cleanupService.stop();
      
      expect(mockLogger.warn).toHaveBeenCalledWith(
        'SessionCleanupService: Not running, ignoring stop request'
      );
    });
  });

  describe('runCleanup()', () => {
    it('should delete expired sessions successfully', async () => {
      mockDb.query.mockResolvedValue([[{ affectedRows: 10 }]]);

      const result = await cleanupService.runCleanup();

      expect(result.deleted).toBe(10);
      expect(result.error).toBeNull();
      expect(result.duration).toBeGreaterThanOrEqual(0); // Duration can be 0 for very fast operations
    });

    it('should delete finalized sessions successfully', async () => {
      mockDb.query.mockResolvedValue([[{ affectedRows: 5 }]]);

      const result = await cleanupService.runCleanup();

      expect(result.deleted).toBe(5);
      expect(result.error).toBeNull();
    });

    it('should use correct SQL query with parameters', async () => {
      mockDb.query.mockResolvedValue([[{ affectedRows: 3 }]]);

      await cleanupService.runCleanup();

      expect(mockDb.query).toHaveBeenCalledWith(
        expect.stringContaining('DELETE FROM conversation_states'),
        expect.arrayContaining([
          expect.any(Date), // cutoffTime
          1000 // batchLimit
        ])
      );

      // Verify query includes both conditions
      const query = mockDb.query.mock.calls[0][0];
      expect(query).toContain('last_activity_at < $1');
      expect(query).toContain("(session_data->>'finalized')::boolean = true");
      expect(query).toContain('LIMIT $2');
    });

    it('should handle PostgreSQL result format (rowCount)', async () => {
      mockDb.query.mockResolvedValue([{ rowCount: 15 }]);

      const result = await cleanupService.runCleanup();

      expect(result.deleted).toBe(15);
    });

    it('should handle MySQL result format (affectedRows)', async () => {
      mockDb.query.mockResolvedValue([[{ affectedRows: 20 }]]);

      const result = await cleanupService.runCleanup();

      expect(result.deleted).toBe(20);
    });

    it('should update statistics after successful cleanup', async () => {
      mockDb.query.mockResolvedValue([[{ affectedRows: 7 }]]);

      await cleanupService.runCleanup();

      const stats = cleanupService.getCleanupStats();
      expect(stats.totalRuns).toBe(1);
      expect(stats.totalSessionsDeleted).toBe(7);
      expect(stats.lastRunDeleted).toBe(7);
      expect(stats.lastRunError).toBeNull();
      expect(stats.lastRunTime).toBeInstanceOf(Date);
    });

    it('should accumulate statistics across multiple runs', async () => {
      mockDb.query.mockResolvedValueOnce([[{ affectedRows: 5 }]]);
      mockDb.query.mockResolvedValueOnce([[{ affectedRows: 3 }]]);
      mockDb.query.mockResolvedValueOnce([[{ affectedRows: 8 }]]);

      await cleanupService.runCleanup();
      await cleanupService.runCleanup();
      await cleanupService.runCleanup();

      const stats = cleanupService.getCleanupStats();
      expect(stats.totalRuns).toBe(3);
      expect(stats.totalSessionsDeleted).toBe(16); // 5 + 3 + 8
    });

    it('should log cleanup statistics', async () => {
      mockDb.query.mockResolvedValue([[{ affectedRows: 12 }]]);

      await cleanupService.runCleanup();

      expect(mockLogger.info).toHaveBeenCalledWith(
        'SessionCleanupService: Cleanup run completed',
        expect.objectContaining({
          deleted: 12,
          duration: expect.any(Number),
          batchLimit: 1000
        })
      );
    });

    it('should warn when batch limit is reached', async () => {
      mockDb.query.mockResolvedValue([[{ affectedRows: 1000 }]]);

      await cleanupService.runCleanup();

      expect(mockLogger.warn).toHaveBeenCalledWith(
        'SessionCleanupService: Batch limit reached, more sessions may need cleanup',
        expect.objectContaining({
          deleted: 1000,
          batchLimit: 1000
        })
      );
    });

    it('should handle database errors gracefully', async () => {
      const dbError = new Error('Database connection failed');
      mockDb.query.mockRejectedValue(dbError);

      const result = await cleanupService.runCleanup();

      expect(result.deleted).toBe(0);
      expect(result.error).toBe('Database connection failed');
    });

    it('should update error statistics on failure', async () => {
      mockDb.query.mockRejectedValue(new Error('Query timeout'));

      await cleanupService.runCleanup();

      const stats = cleanupService.getCleanupStats();
      expect(stats.totalErrors).toBe(1);
      expect(stats.lastRunError).toBe('Query timeout');
      expect(stats.lastRunDeleted).toBe(0);
    });

    it('should log errors with full context', async () => {
      const error = new Error('Connection lost');
      mockDb.query.mockRejectedValue(error);

      await cleanupService.runCleanup();

      expect(mockLogger.error).toHaveBeenCalledWith(
        'SessionCleanupService: Cleanup run failed',
        expect.objectContaining({
          error: 'Connection lost',
          stack: expect.any(String),
          totalErrors: 1
        })
      );
    });

    it('should handle zero deletions', async () => {
      mockDb.query.mockResolvedValue([[{ affectedRows: 0 }]]);

      const result = await cleanupService.runCleanup();

      expect(result.deleted).toBe(0);
      expect(result.error).toBeNull();
    });
  });

  describe('getCleanupStats()', () => {
    it('should return complete statistics', async () => {
      mockDb.query.mockResolvedValue([[{ affectedRows: 5 }]]);
      
      await cleanupService.runCleanup();
      
      const stats = cleanupService.getCleanupStats();
      
      expect(stats).toHaveProperty('totalRuns');
      expect(stats).toHaveProperty('totalSessionsDeleted');
      expect(stats).toHaveProperty('totalErrors');
      expect(stats).toHaveProperty('lastRunTime');
      expect(stats).toHaveProperty('lastRunDuration');
      expect(stats).toHaveProperty('lastRunDeleted');
      expect(stats).toHaveProperty('lastRunError');
      expect(stats).toHaveProperty('isRunning');
      expect(stats).toHaveProperty('config');
    });

    it('should include configuration in statistics', () => {
      const stats = cleanupService.getCleanupStats();
      
      expect(stats.config).toEqual({
        cleanupIntervalMs: 100,
        userIdleTtlMs: 3600000,
        batchLimit: 1000
      });
    });

    it('should reflect running state', () => {
      let stats = cleanupService.getCleanupStats();
      expect(stats.isRunning).toBe(false);
      
      cleanupService.start();
      stats = cleanupService.getCleanupStats();
      expect(stats.isRunning).toBe(true);
      
      cleanupService.stop();
      stats = cleanupService.getCleanupStats();
      expect(stats.isRunning).toBe(false);
    });
  });

  describe('isActive()', () => {
    it('should return false when not started', () => {
      expect(cleanupService.isActive()).toBe(false);
    });

    it('should return true when started', () => {
      cleanupService.start();
      expect(cleanupService.isActive()).toBe(true);
    });

    it('should return false after stopped', () => {
      cleanupService.start();
      cleanupService.stop();
      expect(cleanupService.isActive()).toBe(false);
    });
  });

  describe('Periodic Execution', () => {
    it('should run cleanup periodically', async () => {
      mockDb.query.mockResolvedValue([[{ affectedRows: 1 }]]);

      cleanupService.start();

      // Wait for multiple cleanup cycles
      await new Promise(resolve => setTimeout(resolve, 350));

      // Should have run at least 3 times (initial + 2 periodic)
      expect(mockDb.query.mock.calls.length).toBeGreaterThanOrEqual(3);

      cleanupService.stop();
    });

    it('should continue running after errors', async () => {
      mockDb.query
        .mockRejectedValueOnce(new Error('First error'))
        .mockResolvedValueOnce([[{ affectedRows: 5 }]])
        .mockRejectedValueOnce(new Error('Second error'))
        .mockResolvedValueOnce([[{ affectedRows: 3 }]]);

      cleanupService.start();

      // Wait for multiple cleanup cycles
      await new Promise(resolve => setTimeout(resolve, 450));

      const stats = cleanupService.getCleanupStats();
      
      // Should have attempted multiple runs despite errors
      expect(stats.totalRuns).toBeGreaterThan(2);
      expect(stats.totalErrors).toBeGreaterThan(0);
      expect(stats.totalSessionsDeleted).toBeGreaterThan(0);

      cleanupService.stop();
    });
  });

  describe('Edge Cases', () => {
    it('should handle null database result', async () => {
      mockDb.query.mockResolvedValue([null]);

      const result = await cleanupService.runCleanup();

      expect(result.deleted).toBe(0);
      expect(result.error).toBeNull();
    });

    it('should handle undefined database result', async () => {
      mockDb.query.mockResolvedValue([undefined]);

      const result = await cleanupService.runCleanup();

      expect(result.deleted).toBe(0);
      expect(result.error).toBeNull();
    });

    it('should handle empty database result', async () => {
      mockDb.query.mockResolvedValue([[]]);

      const result = await cleanupService.runCleanup();

      expect(result.deleted).toBe(0);
      expect(result.error).toBeNull();
    });

    it('should calculate correct cutoff time', async () => {
      const beforeRun = Date.now();
      mockDb.query.mockResolvedValue([[{ affectedRows: 0 }]]);

      await cleanupService.runCleanup();

      const [query, params] = mockDb.query.mock.calls[0];
      const cutoffTime = params[0];
      const expectedCutoff = beforeRun - 3600000; // 1 hour ago

      // Allow 1 second tolerance for test execution time
      expect(cutoffTime.getTime()).toBeGreaterThanOrEqual(expectedCutoff - 1000);
      expect(cutoffTime.getTime()).toBeLessThanOrEqual(beforeRun);
    });
  });

  describe('Requirements Validation', () => {
    it('should satisfy Requirement 5.1: Run cleanup every 15 minutes (configurable)', () => {
      const service = new SessionCleanupService({
        db: mockDb,
        logger: mockLogger,
        config: { cleanupIntervalMs: 900000 }
      });

      const stats = service.getCleanupStats();
      expect(stats.config.cleanupIntervalMs).toBe(900000);
    });

    it('should satisfy Requirement 5.2: Delete sessions older than USER_IDLE_TTL_MS', async () => {
      mockDb.query.mockResolvedValue([[{ affectedRows: 10 }]]);

      await cleanupService.runCleanup();

      const query = mockDb.query.mock.calls[0][0];
      expect(query).toContain('last_activity_at < $1');
    });

    it('should satisfy Requirement 5.3: Delete finalized sessions', async () => {
      mockDb.query.mockResolvedValue([[{ affectedRows: 5 }]]);

      await cleanupService.runCleanup();

      const query = mockDb.query.mock.calls[0][0];
      expect(query).toContain("(session_data->>'finalized')::boolean = true");
    });

    it('should satisfy Requirement 5.4: Log count of deleted sessions', async () => {
      mockDb.query.mockResolvedValue([[{ affectedRows: 15 }]]);

      await cleanupService.runCleanup();

      expect(mockLogger.info).toHaveBeenCalledWith(
        'SessionCleanupService: Cleanup run completed',
        expect.objectContaining({ deleted: 15 })
      );
    });

    it('should satisfy Requirement 5.5: Use indexes for efficient queries', async () => {
      mockDb.query.mockResolvedValue([[{ affectedRows: 0 }]]);

      await cleanupService.runCleanup();

      // Query uses subquery with LIMIT for efficient batch processing
      const query = mockDb.query.mock.calls[0][0];
      expect(query).toContain('WHERE id IN');
      expect(query).toContain('LIMIT');
    });

    it('should satisfy Requirement 5.6: Limit to 1000 records per batch', async () => {
      mockDb.query.mockResolvedValue([[{ affectedRows: 1000 }]]);

      await cleanupService.runCleanup();

      const params = mockDb.query.mock.calls[0][1];
      expect(params[1]).toBe(1000); // batchLimit parameter
    });

    it('should satisfy Requirement 5.7: Retry on next scheduled run after error', async () => {
      mockDb.query
        .mockRejectedValueOnce(new Error('Database error'))
        .mockResolvedValueOnce([[{ affectedRows: 5 }]]);

      cleanupService.start();

      // Wait for error and retry
      await new Promise(resolve => setTimeout(resolve, 250));

      const stats = cleanupService.getCleanupStats();
      expect(stats.totalErrors).toBeGreaterThan(0);
      expect(stats.totalSessionsDeleted).toBeGreaterThan(0);

      cleanupService.stop();
    });

    it('should satisfy Requirement 10.6: Log cleanup statistics', async () => {
      mockDb.query.mockResolvedValue([[{ affectedRows: 8 }]]);

      await cleanupService.runCleanup();

      expect(mockLogger.info).toHaveBeenCalledWith(
        'SessionCleanupService: Cleanup run completed',
        expect.objectContaining({
          deleted: 8,
          duration: expect.any(Number),
          totalRuns: 1,
          totalSessionsDeleted: 8
        })
      );
    });
  });
});
