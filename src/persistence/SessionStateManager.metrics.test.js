/**
 * Tests for SessionStateManager Metrics and Observability
 * 
 * Feature: session-state-persistence
 * Task: 4.6 Add metrics and observability
 * 
 * Tests the getMetrics() method and observability features added in task 4.6
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import SessionStateManager from './SessionStateManager.js';

describe('SessionStateManager - Metrics (Task 4.6)', () => {
  let mockDb;
  let mockLogger;
  let mockSentry;
  let manager;

  beforeEach(() => {
    mockDb = {
      query: vi.fn()
    };

    mockLogger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn()
    };

    mockSentry = {
      captureException: vi.fn()
    };

    manager = new SessionStateManager({
      db: mockDb,
      logger: mockLogger,
      sentry: mockSentry,
      config: {
        enabled: true,
        retryAttempts: 3,
        retryDelayMs: 10,
        batchWindowMs: 50
      }
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('getMetrics() method', () => {
    it('should return metrics object with all required fields', () => {
      const metrics = manager.getMetrics();

      // Verify all required metrics fields exist
      expect(metrics).toHaveProperty('state_save_duration');
      expect(metrics).toHaveProperty('state_load_duration');
      expect(metrics).toHaveProperty('persistence_errors');
      expect(metrics).toHaveProperty('cache_hit_rate');
      expect(metrics).toHaveProperty('cache_hits');
      expect(metrics).toHaveProperty('cache_misses');
      expect(metrics).toHaveProperty('total_saves');
      expect(metrics).toHaveProperty('total_loads');
      expect(metrics).toHaveProperty('slow_operations');
      expect(metrics).toHaveProperty('circuit_breaker_state');
      expect(metrics).toHaveProperty('circuit_breaker_failures');
      expect(metrics).toHaveProperty('pending_batch_count');
    });

    it('should return zero metrics initially', () => {
      const metrics = manager.getMetrics();

      expect(metrics.persistence_errors).toBe(0);
      expect(metrics.cache_hits).toBe(0);
      expect(metrics.cache_misses).toBe(0);
      expect(metrics.total_saves).toBe(0);
      expect(metrics.total_loads).toBe(0);
      expect(metrics.slow_operations).toBe(0);
      expect(metrics.cache_hit_rate).toBe(0);
      expect(metrics.circuit_breaker_state).toBe('CLOSED');
      expect(metrics.circuit_breaker_failures).toBe(0);
    });

    it('should track state_save_duration with percentiles after flush', async () => {
      const userState = { step: 'test', data: {}, lastUserMessageAt: new Date() };
      mockDb.query.mockResolvedValue([{ rows: [{ id: 1 }] }]);

      // Schedule multiple saves
      for (let i = 0; i < 5; i++) {
        await manager.persistState(1, `+123456789${i}`, userState);
      }

      // Flush batch to complete operations
      await manager.flushBatch();

      const metrics = manager.getMetrics();

      expect(metrics.state_save_duration.samples).toBe(5);
      expect(metrics.state_save_duration.avg).toBeGreaterThanOrEqual(0);
      expect(metrics.state_save_duration.p50).toBeGreaterThanOrEqual(0);
      expect(metrics.state_save_duration.p95).toBeGreaterThanOrEqual(0);
      expect(metrics.state_save_duration.p99).toBeGreaterThanOrEqual(0);
      expect(metrics.total_saves).toBe(5);
    });

    it('should track state_load_duration with percentiles', async () => {
      const mockRows = [
        { session_data: { step: 'test', data: {} } }
      ];
      mockDb.query.mockResolvedValue([mockRows]);

      // Perform multiple loads
      for (let i = 0; i < 5; i++) {
        await manager.loadState(1, `+123456789${i}`);
      }

      const metrics = manager.getMetrics();

      expect(metrics.state_load_duration.samples).toBe(5);
      expect(metrics.state_load_duration.avg).toBeGreaterThanOrEqual(0);
      expect(metrics.state_load_duration.p50).toBeGreaterThanOrEqual(0);
      expect(metrics.state_load_duration.p95).toBeGreaterThanOrEqual(0);
      expect(metrics.state_load_duration.p99).toBeGreaterThanOrEqual(0);
      expect(metrics.total_loads).toBe(5);
    });

    it('should calculate cache_hit_rate correctly', async () => {
      const mockCacheLayer = {
        isAvailable: vi.fn().mockReturnValue(true),
        get: vi.fn()
          .mockResolvedValueOnce({ step: 'test', data: {} }) // Hit
          .mockResolvedValueOnce(null) // Miss
          .mockResolvedValueOnce({ step: 'test', data: {} }) // Hit
          .mockResolvedValueOnce(null), // Miss
        set: vi.fn().mockResolvedValue(true)
      };

      manager.cacheLayer = mockCacheLayer;
      mockDb.query.mockResolvedValue([[{ session_data: { step: 'test', data: {} } }]]);

      // Perform loads: 2 hits, 2 misses
      await manager.loadState(1, '+1234567890');
      await manager.loadState(1, '+0987654321');
      await manager.loadState(1, '+1111111111');
      await manager.loadState(1, '+2222222222');

      const metrics = manager.getMetrics();

      expect(metrics.cache_hits).toBe(2);
      expect(metrics.cache_misses).toBe(2);
      expect(metrics.cache_hit_rate).toBe(50); // 2/4 = 50%
    });

    it('should limit duration samples to 1000 to prevent memory growth', async () => {
      const userState = { step: 'test', data: {}, lastUserMessageAt: new Date() };
      mockDb.query.mockResolvedValue([{ rows: [{ id: 1 }] }]);

      // Perform 1500 saves
      for (let i = 0; i < 1500; i++) {
        await manager.persistState(1, `+${i}`, userState);
      }

      await manager.flushBatch();

      const metrics = manager.getMetrics();

      // Should keep only last 1000 samples
      expect(metrics.state_save_duration.samples).toBeLessThanOrEqual(1000);
    });
  });

  describe('Logging for slow operations (>100ms)', () => {
    it('should log warning when loadState exceeds 100ms', async () => {
      // Mock slow database query
      mockDb.query.mockImplementation(() => {
        return new Promise(resolve => {
          setTimeout(() => {
            resolve([[{ session_data: { step: 'test', data: {} } }]]);
          }, 110);
        });
      });

      await manager.loadState(1, '+1234567890');

      // Check for slow operation warning
      const slowWarnings = mockLogger.warn.mock.calls.filter(call =>
        call[0] === 'SessionStateManager: Slow load operation detected'
      );

      expect(slowWarnings.length).toBeGreaterThan(0);
      expect(slowWarnings[0][1]).toMatchObject({
        adminId: 1,
        phone: '****7890',
        threshold: 100,
        operation: 'loadState'
      });
      expect(slowWarnings[0][1].duration).toBeGreaterThan(100);
    });

    it('should track slow_operations metric', async () => {
      // Mock slow database query
      mockDb.query.mockImplementation(() => {
        return new Promise(resolve => {
          setTimeout(() => {
            resolve([[{ session_data: { step: 'test', data: {} } }]]);
          }, 110);
        });
      });

      await manager.loadState(1, '+1234567890');

      const metrics = manager.getMetrics();
      expect(metrics.slow_operations).toBeGreaterThan(0);
    });
  });

  describe('Error logging with full context', () => {
    it('should log errors with full context for loadState failures', async () => {
      mockDb.query.mockRejectedValue(new Error('Database connection failed'));

      await manager.loadState(1, '+1234567890');

      // Verify error log includes full context
      const errorCalls = mockLogger.error.mock.calls.filter(call =>
        call[0] === 'SessionStateManager: Failed to load state'
      );

      expect(errorCalls.length).toBeGreaterThan(0);
      expect(errorCalls[0][1]).toMatchObject({
        adminId: 1,
        phone: '****7890',
        error: 'Database connection failed',
        totalErrors: expect.any(Number)
      });
      expect(errorCalls[0][1]).toHaveProperty('stack');
      expect(errorCalls[0][1]).toHaveProperty('duration');
    });

    it('should track persistence_errors metric', async () => {
      mockDb.query.mockRejectedValue(new Error('Database error'));

      // Trigger errors
      await manager.loadState(1, '+1234567890');
      await manager.loadState(1, '+0987654321');
      await manager.deleteState(1, '+1111111111');

      const metrics = manager.getMetrics();
      expect(metrics.persistence_errors).toBe(3);
    });
  });

  describe('Sentry alerts for persistence failures', () => {
    it('should emit Sentry alert when loadState fails', async () => {
      const error = new Error('Database query failed');
      mockDb.query.mockRejectedValue(error);

      await manager.loadState(1, '+1234567890');

      expect(mockSentry.captureException).toHaveBeenCalledWith(
        error,
        expect.objectContaining({
          tags: expect.objectContaining({
            component: 'SessionStateManager',
            operation: 'loadState'
          }),
          extra: expect.objectContaining({
            adminId: 1,
            phone: '****7890',
            totalErrors: expect.any(Number)
          })
        })
      );
    });

    it('should emit Sentry alert when deleteState fails', async () => {
      const error = new Error('Database delete failed');
      mockDb.query.mockRejectedValue(error);

      await manager.deleteState(1, '+1234567890');

      expect(mockSentry.captureException).toHaveBeenCalledWith(
        error,
        expect.objectContaining({
          tags: expect.objectContaining({
            component: 'SessionStateManager',
            operation: 'deleteState'
          }),
          extra: expect.objectContaining({
            adminId: 1,
            phone: '****7890',
            totalErrors: expect.any(Number)
          })
        })
      );
    });
  });

  describe('Sensitive data masking', () => {
    it('should mask phone numbers in logs (show only last 4 digits)', async () => {
      const userState = { step: 'test', data: {}, lastUserMessageAt: new Date() };
      mockDb.query.mockResolvedValue([{ rows: [{ id: 1 }] }]);

      await manager.persistState(1, '+1234567890', userState);

      // Check debug logs for masked phone
      const debugCalls = mockLogger.debug.mock.calls;
      const maskedPhones = debugCalls
        .filter(call => call[1] && call[1].phone)
        .map(call => call[1].phone);

      maskedPhones.forEach(phone => {
        expect(phone).toMatch(/^\*\*\*\*\d{4}$/);
        expect(phone).toBe('****7890');
      });
    });

    it('should mask phone numbers in error logs', async () => {
      mockDb.query.mockRejectedValue(new Error('Database error'));

      await manager.loadState(1, '+1234567890');

      const errorCalls = mockLogger.error.mock.calls;
      const maskedPhones = errorCalls
        .filter(call => call[1] && call[1].phone)
        .map(call => call[1].phone);

      maskedPhones.forEach(phone => {
        expect(phone).toMatch(/^\*\*\*\*\d{4}$/);
        expect(phone).toBe('****7890');
      });
    });

    it('should mask phone numbers in Sentry reports', async () => {
      mockDb.query.mockRejectedValue(new Error('Database error'));

      await manager.loadState(1, '+1234567890');

      expect(mockSentry.captureException).toHaveBeenCalledWith(
        expect.any(Error),
        expect.objectContaining({
          extra: expect.objectContaining({
            phone: '****7890'
          })
        })
      );
    });

    it('should handle short phone numbers gracefully', () => {
      expect(manager._maskPhone('123')).toBe('****');
      expect(manager._maskPhone('1234')).toBe('****');
      expect(manager._maskPhone('')).toBe('****');
      expect(manager._maskPhone(null)).toBe('****');
    });

    it('should mask email addresses (show only first char and domain)', () => {
      expect(manager._maskEmail('john@example.com')).toBe('j***@example.com');
      expect(manager._maskEmail('a@test.org')).toBe('a***@test.org');
      expect(manager._maskEmail('invalid')).toBe('****@****.***');
      expect(manager._maskEmail('')).toBe('****@****.***');
      expect(manager._maskEmail(null)).toBe('****@****.***');
    });

    it('should mask sensitive data in objects', () => {
      const data = {
        phone: '+1234567890',
        email: 'john@example.com',
        name: 'John Doe'
      };

      const masked = manager._maskSensitiveData(data);

      expect(masked.phone).toBe('****7890');
      expect(masked.email).toBe('j***@example.com');
      expect(masked.name).toBe('John Doe'); // Name not masked
    });

    it('should handle null or non-object data gracefully', () => {
      expect(manager._maskSensitiveData(null)).toBe(null);
      expect(manager._maskSensitiveData(undefined)).toBe(undefined);
      expect(manager._maskSensitiveData('string')).toBe('string');
      expect(manager._maskSensitiveData(123)).toBe(123);
    });
  });

  describe('Cache metrics integration', () => {
    it('should track cache hits and misses', async () => {
      const mockCacheLayer = {
        isAvailable: vi.fn().mockReturnValue(true),
        get: vi.fn()
          .mockResolvedValueOnce({ step: 'test', data: {} }) // Hit
          .mockResolvedValueOnce(null) // Miss
          .mockResolvedValueOnce({ step: 'test', data: {} }), // Hit
        set: vi.fn().mockResolvedValue(true)
      };

      manager.cacheLayer = mockCacheLayer;
      mockDb.query.mockResolvedValue([[{ session_data: { step: 'test', data: {} } }]]);

      await manager.loadState(1, '+1234567890'); // Hit
      await manager.loadState(1, '+0987654321'); // Miss
      await manager.loadState(1, '+1111111111'); // Hit

      const metrics = manager.getMetrics();

      expect(metrics.cache_hits).toBe(2);
      expect(metrics.cache_misses).toBe(1);
      expect(metrics.cache_hit_rate).toBe(66.67); // 2/3 = 66.67%
    });

    it('should track cache miss on cache error', async () => {
      const mockCacheLayer = {
        isAvailable: vi.fn().mockReturnValue(true),
        get: vi.fn().mockRejectedValue(new Error('Cache error')),
        set: vi.fn().mockResolvedValue(true)
      };

      manager.cacheLayer = mockCacheLayer;
      mockDb.query.mockResolvedValue([[{ session_data: { step: 'test', data: {} } }]]);

      await manager.loadState(1, '+1234567890');

      const metrics = manager.getMetrics();

      expect(metrics.cache_misses).toBe(1);
      expect(metrics.cache_hits).toBe(0);
    });

    it('should not track cache metrics when cache is unavailable', async () => {
      const mockCacheLayer = {
        isAvailable: vi.fn().mockReturnValue(false),
        get: vi.fn(),
        set: vi.fn()
      };

      manager.cacheLayer = mockCacheLayer;
      mockDb.query.mockResolvedValue([[{ session_data: { step: 'test', data: {} } }]]);

      await manager.loadState(1, '+1234567890');

      const metrics = manager.getMetrics();

      expect(metrics.cache_hits).toBe(0);
      expect(metrics.cache_misses).toBe(0);
      expect(mockCacheLayer.get).not.toHaveBeenCalled();
    });
  });
});
