import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import fc from 'fast-check';
import SessionCleanupService from './SessionCleanupService.js';
import db from '../../config/database.js';

/**
 * Property-Based Tests for SessionCleanupService
 * 
 * Feature: session-state-persistence
 * Testing Framework: fast-check
 * 
 * These tests validate universal properties that should hold true across
 * all valid inputs and execution scenarios for the cleanup service.
 */

describe('SessionCleanupService - Property-Based Tests', () => {
  let cleanupService;
  let mockLogger;
  let testAdminIds = [];

  beforeEach(async () => {
    // Mock logger
    mockLogger = {
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      debug: vi.fn()
    };

    // Create test admins for foreign key constraints
    // Using admin IDs 9000-9002 for testing
    testAdminIds = [9000, 9001, 9002];
    
    for (const adminId of testAdminIds) {
      try {
        await db.query(
          `INSERT INTO admins (id, email, password_hash, name, created_at)
           VALUES ($1, $2, $3, $4, NOW())
           ON CONFLICT (id) DO NOTHING`,
          [
            adminId,
            `test${adminId}@test.com`,
            'test_password_hash',
            `Test Admin ${adminId}`
          ]
        );
      } catch (err) {
        // Ignore if admin already exists
      }
    }

    // Create cleanup service with test configuration
    cleanupService = new SessionCleanupService({
      db,
      logger: mockLogger,
      config: {
        cleanupIntervalMs: 60000, // 1 minute for testing
        userIdleTtlMs: 3600000, // 1 hour
        batchLimit: 1000
      }
    });

    // Clean up test conversation states before each test
    await db.query('DELETE FROM conversation_states WHERE admin_id >= 9000');
  });

  afterEach(async () => {
    // Stop the service
    if (cleanupService) {
      cleanupService.stop();
    }

    // Clean up test conversation states
    await db.query('DELETE FROM conversation_states WHERE admin_id >= 9000');
    
    // Clean up test admins
    await db.query('DELETE FROM admins WHERE id >= 9000');
  });

  /**
   * Property 9: Cleanup deletes expired and finalized sessions
   * **Validates: Requirements 5.2, 5.3, 5.4, 5.6**
   * 
   * For any cleanup operation, the SessionCleanupService should delete all conversation states
   * where either (a) last_activity_at is older than USER_IDLE_TTL_MS, or (b) the session_data
   * indicates finalized=true, limiting deletions to 1000 records per batch, and logging the
   * count of deleted sessions.
   */
  it('property: cleanup deletes expired and finalized sessions while preserving active ones', async () => {
    // Arbitrary generator for session configurations
    const sessionConfigArbitrary = fc.record({
      // Number of expired sessions (older than TTL)
      expiredCount: fc.integer({ min: 0, max: 50 }),
      // Number of finalized sessions (within TTL but finalized=true)
      finalizedCount: fc.integer({ min: 0, max: 50 }),
      // Number of active sessions (within TTL and not finalized)
      activeCount: fc.integer({ min: 0, max: 50 }),
      // Admin ID for isolation
      adminId: fc.integer({ min: 9000, max: 9999 })
    });

    await fc.assert(
      fc.asyncProperty(sessionConfigArbitrary, async (config) => {
        const { expiredCount, finalizedCount, activeCount, adminId } = config;
        const totalToDelete = expiredCount + finalizedCount;

        // Insert expired sessions (older than 1 hour)
        const expiredTime = new Date(Date.now() - 7200000); // 2 hours ago
        for (let i = 0; i < expiredCount; i++) {
          await db.query(
            `INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
             VALUES ($1, $2, $3, $4)`,
            [
              adminId,
              `+1555000${i.toString().padStart(4, '0')}`,
              JSON.stringify({ step: 'expired', finalized: false }),
              expiredTime
            ]
          );
        }

        // Insert finalized sessions (within TTL but finalized=true)
        const recentTime = new Date(Date.now() - 1800000); // 30 minutes ago
        for (let i = 0; i < finalizedCount; i++) {
          await db.query(
            `INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
             VALUES ($1, $2, $3, $4)`,
            [
              adminId,
              `+1555100${i.toString().padStart(4, '0')}`,
              JSON.stringify({ step: 'completed', finalized: true }),
              recentTime
            ]
          );
        }

        // Insert active sessions (within TTL and not finalized)
        for (let i = 0; i < activeCount; i++) {
          await db.query(
            `INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
             VALUES ($1, $2, $3, $4)`,
            [
              adminId,
              `+1555200${i.toString().padStart(4, '0')}`,
              JSON.stringify({ step: 'active', finalized: false }),
              recentTime
            ]
          );
        }

        // Run cleanup
        const result = await cleanupService.runCleanup();

        // Verify cleanup results
        expect(result.error).toBeNull();
        expect(result.deleted).toBeGreaterThanOrEqual(0);
        expect(result.deleted).toBeLessThanOrEqual(Math.min(totalToDelete, 1000)); // Batch limit

        // Query remaining sessions for this admin
        const [remainingSessions] = await db.query(
          'SELECT * FROM conversation_states WHERE admin_id = $1',
          [adminId]
        );

        // If total to delete was within batch limit, all should be deleted
        if (totalToDelete <= 1000) {
          expect(result.deleted).toBe(totalToDelete);
          
          // Only active sessions should remain
          expect(remainingSessions.length).toBe(activeCount);
          
          // Verify all remaining sessions are active (not finalized and recent)
          for (const session of remainingSessions) {
            const sessionData = session.session_data;
            expect(sessionData.finalized).toBe(false);
            
            const lastActivity = new Date(session.last_activity_at);
            const ageMs = Date.now() - lastActivity.getTime();
            expect(ageMs).toBeLessThan(3600000); // Within 1 hour TTL
          }
        } else {
          // Batch limit was reached
          expect(result.deleted).toBe(1000);
          
          // Some sessions should remain (could be expired/finalized or active)
          expect(remainingSessions.length).toBeGreaterThan(0);
        }

        // Verify statistics were updated
        const stats = cleanupService.getCleanupStats();
        expect(stats.lastRunDeleted).toBe(result.deleted);
        expect(stats.lastRunError).toBeNull();
        expect(stats.totalSessionsDeleted).toBeGreaterThanOrEqual(result.deleted);

        // Clean up test data for this iteration
        await db.query('DELETE FROM conversation_states WHERE admin_id = $1', [adminId]);
      }),
      { numRuns: 20 } // Run 20 iterations with different configurations
    );
  });

  /**
   * Property 9 (Batch Limit Variant): Cleanup respects batch limit
   * **Validates: Requirements 5.6**
   * 
   * For any cleanup operation where more than 1000 sessions need deletion,
   * the cleanup should delete exactly 1000 sessions and log a warning.
   */
  it('property: cleanup respects batch limit of 1000 records', async () => {
    // Test with counts that exceed batch limit
    const largeCountArbitrary = fc.record({
      expiredCount: fc.integer({ min: 500, max: 1500 }),
      finalizedCount: fc.integer({ min: 500, max: 1500 }),
      adminId: fc.integer({ min: 9000, max: 9999 })
    });

    await fc.assert(
      fc.asyncProperty(largeCountArbitrary, async (config) => {
        const { expiredCount, finalizedCount, adminId } = config;
        const totalToDelete = expiredCount + finalizedCount;

        // Skip if total is within batch limit (not testing batch limit scenario)
        if (totalToDelete <= 1000) {
          return;
        }

        // Insert expired sessions
        const expiredTime = new Date(Date.now() - 7200000);
        for (let i = 0; i < expiredCount; i++) {
          await db.query(
            `INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
             VALUES ($1, $2, $3, $4)`,
            [
              adminId,
              `+1555300${i.toString().padStart(4, '0')}`,
              JSON.stringify({ step: 'expired', finalized: false }),
              expiredTime
            ]
          );
        }

        // Insert finalized sessions
        const recentTime = new Date(Date.now() - 1800000);
        for (let i = 0; i < finalizedCount; i++) {
          await db.query(
            `INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
             VALUES ($1, $2, $3, $4)`,
            [
              adminId,
              `+1555400${i.toString().padStart(4, '0')}`,
              JSON.stringify({ step: 'completed', finalized: true }),
              recentTime
            ]
          );
        }

        // Run cleanup
        const result = await cleanupService.runCleanup();

        // Verify batch limit was respected
        expect(result.deleted).toBe(1000);
        expect(result.error).toBeNull();

        // Verify warning was logged about batch limit
        expect(mockLogger.warn).toHaveBeenCalledWith(
          'SessionCleanupService: Batch limit reached, more sessions may need cleanup',
          expect.objectContaining({
            deleted: 1000,
            batchLimit: 1000
          })
        );

        // Verify some sessions remain
        const [remainingSessions] = await db.query(
          'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
          [adminId]
        );
        expect(parseInt(remainingSessions[0].count)).toBe(totalToDelete - 1000);

        // Clean up test data
        await db.query('DELETE FROM conversation_states WHERE admin_id = $1', [adminId]);
      }),
      { numRuns: 10 } // Run 10 iterations
    );
  });

  /**
   * Property 10: Cleanup failures are logged and retried
   * **Validates: Requirements 5.7**
   * 
   * For any cleanup operation that fails due to database errors, the SessionCleanupService
   * should log the error without crashing and automatically retry on the next scheduled run.
   */
  it('property: cleanup handles database errors gracefully and continues operation', async () => {
    // Arbitrary generator for error scenarios
    const errorScenarioArbitrary = fc.record({
      errorMessage: fc.constantFrom(
        'Connection timeout',
        'Query timeout',
        'Database unavailable',
        'Lock timeout',
        'Deadlock detected'
      ),
      shouldRecover: fc.boolean()
    });

    await fc.assert(
      fc.asyncProperty(errorScenarioArbitrary, async (scenario) => {
        const { errorMessage, shouldRecover } = scenario;

        // Create a service with a mock db that fails
        const mockDb = {
          query: vi.fn()
        };

        const testService = new SessionCleanupService({
          db: mockDb,
          logger: mockLogger,
          config: {
            cleanupIntervalMs: 100,
            userIdleTtlMs: 3600000,
            batchLimit: 1000
          }
        });

        // First call fails
        mockDb.query.mockRejectedValueOnce(new Error(errorMessage));

        // Run cleanup (should fail)
        const result1 = await testService.runCleanup();

        // Verify error was handled gracefully
        expect(result1.deleted).toBe(0);
        expect(result1.error).toBe(errorMessage);
        expect(result1.duration).toBeGreaterThanOrEqual(0);

        // Verify error was logged
        expect(mockLogger.error).toHaveBeenCalledWith(
          'SessionCleanupService: Cleanup run failed',
          expect.objectContaining({
            error: errorMessage,
            totalErrors: 1
          })
        );

        // Verify statistics were updated
        let stats = testService.getCleanupStats();
        expect(stats.totalErrors).toBe(1);
        expect(stats.lastRunError).toBe(errorMessage);
        expect(stats.lastRunDeleted).toBe(0);

        // Second call succeeds (recovery scenario)
        if (shouldRecover) {
          mockDb.query.mockResolvedValueOnce([[{ affectedRows: 5 }]]);

          const result2 = await testService.runCleanup();

          // Verify recovery
          expect(result2.deleted).toBe(5);
          expect(result2.error).toBeNull();

          // Verify statistics show recovery
          stats = testService.getCleanupStats();
          expect(stats.totalErrors).toBe(1); // Still 1 error total
          expect(stats.lastRunError).toBeNull(); // But last run succeeded
          expect(stats.lastRunDeleted).toBe(5);
          expect(stats.totalSessionsDeleted).toBe(5);
        }

        testService.stop();
      }),
      { numRuns: 20 }
    );
  });

  /**
   * Property 10 (Variant): Service continues running after errors
   * **Validates: Requirements 5.7**
   * 
   * The cleanup service should continue its periodic execution even after
   * encountering errors, retrying on each scheduled interval.
   */
  it('property: cleanup service continues periodic execution after errors', async () => {
    // Create a service with a mock db
    const mockDb = {
      query: vi.fn()
    };

    const testService = new SessionCleanupService({
      db: mockDb,
      logger: mockLogger,
      config: {
        cleanupIntervalMs: 50, // Very short interval for testing
        userIdleTtlMs: 3600000,
        batchLimit: 1000
      }
    });

    // Simulate alternating failures and successes
    mockDb.query
      .mockRejectedValueOnce(new Error('Error 1'))
      .mockResolvedValueOnce([[{ affectedRows: 3 }]])
      .mockRejectedValueOnce(new Error('Error 2'))
      .mockResolvedValueOnce([[{ affectedRows: 5 }]])
      .mockResolvedValueOnce([[{ affectedRows: 2 }]]);

    // Start the service
    testService.start();

    // Wait for multiple cleanup cycles
    await new Promise(resolve => setTimeout(resolve, 300));

    // Stop the service
    testService.stop();

    // Verify multiple runs occurred
    const stats = testService.getCleanupStats();
    expect(stats.totalRuns).toBeGreaterThan(3);
    expect(stats.totalErrors).toBeGreaterThan(0);
    expect(stats.totalSessionsDeleted).toBeGreaterThan(0);

    // Verify service continued despite errors
    expect(mockDb.query.mock.calls.length).toBeGreaterThan(3);
  });

  /**
   * Property: Cleanup statistics are accurate across multiple runs
   * **Validates: Requirements 5.4, 10.6**
   * 
   * For any sequence of cleanup operations, the accumulated statistics should
   * accurately reflect the total number of runs, deletions, and errors.
   */
  it('property: cleanup statistics accumulate correctly across multiple runs', async () => {
    // Arbitrary generator for cleanup sequences
    const cleanupSequenceArbitrary = fc.array(
      fc.record({
        deletedCount: fc.integer({ min: 0, max: 100 }),
        shouldFail: fc.boolean()
      }),
      { minLength: 3, maxLength: 10 }
    );

    await fc.assert(
      fc.asyncProperty(cleanupSequenceArbitrary, async (sequence) => {
        // Create a service with a mock db
        const mockDb = {
          query: vi.fn()
        };

        const testService = new SessionCleanupService({
          db: mockDb,
          logger: mockLogger,
          config: {
            cleanupIntervalMs: 60000,
            userIdleTtlMs: 3600000,
            batchLimit: 1000
          }
        });

        let expectedTotalDeleted = 0;
        let expectedTotalErrors = 0;
        let expectedTotalRuns = 0; // Only successful runs increment totalRuns

        // Execute the sequence
        for (const step of sequence) {
          if (step.shouldFail) {
            mockDb.query.mockRejectedValueOnce(new Error('Test error'));
            expectedTotalErrors++;
            // Note: totalRuns is NOT incremented on error
          } else {
            mockDb.query.mockResolvedValueOnce([[{ affectedRows: step.deletedCount }]]);
            expectedTotalDeleted += step.deletedCount;
            expectedTotalRuns++; // Only successful runs increment totalRuns
          }

          await testService.runCleanup();
        }

        // Verify statistics
        const stats = testService.getCleanupStats();
        expect(stats.totalRuns).toBe(expectedTotalRuns);
        expect(stats.totalSessionsDeleted).toBe(expectedTotalDeleted);
        expect(stats.totalErrors).toBe(expectedTotalErrors);
        expect(stats.lastRunTime).toBeInstanceOf(Date);

        testService.stop();
      }),
      { numRuns: 20 }
    );
  });

  /**
   * Property: Cleanup preserves sessions from different admins
   * **Validates: Requirements 5.2, 5.3, 13.4 (Admin isolation)**
   * 
   * For any cleanup operation, sessions belonging to different admins should
   * be handled independently, with expired/finalized sessions deleted regardless
   * of admin_id, but admin isolation maintained.
   */
  it('property: cleanup handles multiple admins independently', async () => {
    const multiAdminArbitrary = fc.record({
      admin1Id: fc.constant(9001),
      admin2Id: fc.constant(9002),
      admin1Expired: fc.integer({ min: 1, max: 20 }),
      admin1Active: fc.integer({ min: 1, max: 20 }),
      admin2Expired: fc.integer({ min: 1, max: 20 }),
      admin2Active: fc.integer({ min: 1, max: 20 })
    });

    await fc.assert(
      fc.asyncProperty(multiAdminArbitrary, async (config) => {
        const { admin1Id, admin2Id, admin1Expired, admin1Active, admin2Expired, admin2Active } = config;

        const expiredTime = new Date(Date.now() - 7200000);
        const recentTime = new Date(Date.now() - 1800000);

        // Insert sessions for admin 1
        for (let i = 0; i < admin1Expired; i++) {
          await db.query(
            `INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
             VALUES ($1, $2, $3, $4)`,
            [admin1Id, `+1555500${i.toString().padStart(4, '0')}`, JSON.stringify({ finalized: false }), expiredTime]
          );
        }
        for (let i = 0; i < admin1Active; i++) {
          await db.query(
            `INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
             VALUES ($1, $2, $3, $4)`,
            [admin1Id, `+1555600${i.toString().padStart(4, '0')}`, JSON.stringify({ finalized: false }), recentTime]
          );
        }

        // Insert sessions for admin 2
        for (let i = 0; i < admin2Expired; i++) {
          await db.query(
            `INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
             VALUES ($1, $2, $3, $4)`,
            [admin2Id, `+1555700${i.toString().padStart(4, '0')}`, JSON.stringify({ finalized: false }), expiredTime]
          );
        }
        for (let i = 0; i < admin2Active; i++) {
          await db.query(
            `INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
             VALUES ($1, $2, $3, $4)`,
            [admin2Id, `+1555800${i.toString().padStart(4, '0')}`, JSON.stringify({ finalized: false }), recentTime]
          );
        }

        // Run cleanup
        const result = await cleanupService.runCleanup();

        const totalExpired = admin1Expired + admin2Expired;
        expect(result.deleted).toBe(Math.min(totalExpired, 1000));

        // Verify remaining sessions for each admin
        const [admin1Remaining] = await db.query(
          'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
          [admin1Id]
        );
        const [admin2Remaining] = await db.query(
          'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
          [admin2Id]
        );

        // Active sessions should remain for both admins
        if (totalExpired <= 1000) {
          expect(parseInt(admin1Remaining[0].count)).toBe(admin1Active);
          expect(parseInt(admin2Remaining[0].count)).toBe(admin2Active);
        }

        // Clean up
        await db.query('DELETE FROM conversation_states WHERE admin_id IN ($1, $2)', [admin1Id, admin2Id]);
      }),
      { numRuns: 15 }
    );
  });
});
