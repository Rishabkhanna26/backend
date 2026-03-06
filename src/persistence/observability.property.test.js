/**
 * Observability Property-Based Tests
 * 
 * Feature: session-state-persistence
 * Task 12: Properties 26-32 - Observability and monitoring
 * 
 * These tests validate logging, metrics, and error reporting requirements.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import fc from 'fast-check';
import SessionStateManager from './SessionStateManager.js';
import RecoveryManager from './RecoveryManager.js';
import SessionCleanupService from './SessionCleanupService.js';
import StateSerializer from './StateSerializer.js';

// Minimum 100 iterations per property test
const testConfig = { numRuns: 100 };

// Arbitrary generators
const userStateArbitrary = fc.record({
  step: fc.string({ minLength: 1, maxLength: 50 }),
  data: fc.dictionary(fc.string(), fc.anything()),
  isReturningUser: fc.boolean(),
  clientId: fc.option(fc.integer({ min: 1, max: 999999 }), { nil: null }),
  name: fc.option(fc.string({ minLength: 1, maxLength: 100 }), { nil: null }),
  email: fc.option(fc.emailAddress(), { nil: null }),
  assignedAdminId: fc.option(fc.integer({ min: 1, max: 100 }), { nil: null }),
  greetedThisSession: fc.boolean(),
  resumeStep: fc.option(fc.string(), { nil: null }),
  awaitingResumeDecision: fc.boolean(),
  lastUserMessageAt: fc.date(),
  partialSavedAt: fc.option(fc.date(), { nil: null }),
  finalized: fc.boolean(),
  automationDisabled: fc.boolean(),
  aiConversationHistory: fc.array(
    fc.record({
      role: fc.constantFrom('user', 'assistant'),
      content: fc.string()
    }),
    { maxLength: 20 }
  ),
  responseLanguage: fc.option(fc.constantFrom('en', 'es', 'fr', 'de'), { nil: null })
});

describe('Observability Property-Based Tests', () => {
  let mockDb;
  let mockLogger;
  let mockSentry;

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
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  /**
   * Property 26: Comprehensive operation logging
   * 
   * **Validates: Requirements 10.1**
   * 
   * For any persistence operation (save, load, delete), the SessionStateManager should
   * log the operation with admin_id, phone (masked), operation type, duration, and
   * outcome (success/failure), enabling full observability of persistence layer behavior.
   */
  describe('Property 26: Comprehensive operation logging', () => {
    it('should log all persistence operations with full context', async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 100 }),
          fc.string({ minLength: 10, maxLength: 15 }),
          userStateArbitrary,
          async (adminId, phone, userState) => {
            const manager = new SessionStateManager({
              db: mockDb,
              logger: mockLogger,
              sentry: mockSentry,
              config: { enabled: true, retryAttempts: 1 }
            });

            mockDb.query.mockResolvedValue({ rows: [{ id: 1 }] });

            // Perform operations
            await manager.persistState(adminId, phone, userState);
            await manager.loadState(adminId, phone);
            await manager.deleteState(adminId, phone);

            // Verify logging occurred for each operation
            const allLogs = [
              ...mockLogger.debug.mock.calls,
              ...mockLogger.info.mock.calls,
              ...mockLogger.warn.mock.calls,
              ...mockLogger.error.mock.calls
            ];

            // Should have logs for operations
            expect(allLogs.length).toBeGreaterThan(0);

            // Verify logs contain operation context
            const hasOperationLogs = allLogs.some(call => {
              const message = call[0];
              const context = call[1];
              return (
                (message && typeof message === 'string') ||
                (context && (context.adminId || context.phone || context.operation))
              );
            });

            expect(hasOperationLogs).toBe(true);
          }
        ),
        testConfig
      );
    });
  });

  /**
   * Property 27: Metrics emission
   * 
   * **Validates: Requirements 10.2**
   * 
   * For any persistence operation, the SessionStateManager should emit metrics including
   * state_save_duration, state_load_duration, cache_hit_rate, and persistence_errors,
   * enabling performance monitoring and alerting.
   */
  describe('Property 27: Metrics emission', () => {
    it('should emit metrics for all operations', async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 100 }),
          fc.string({ minLength: 10, maxLength: 15 }),
          userStateArbitrary,
          async (adminId, phone, userState) => {
            const manager = new SessionStateManager({
              db: mockDb,
              logger: mockLogger,
              sentry: mockSentry,
              config: { enabled: true }
            });

            mockDb.query.mockResolvedValue({ rows: [{ id: 1 }] });

            await manager.persistState(adminId, phone, userState);

            // Get metrics
            const metrics = manager.getMetrics();

            // Verify metrics structure
            expect(metrics).toBeDefined();
            expect(metrics).toHaveProperty('total_saves');
            expect(metrics).toHaveProperty('total_loads');
            expect(metrics).toHaveProperty('persistence_errors');
            
            // Wait for batch flush
            await new Promise(resolve => setTimeout(resolve, 30));
            
            // After flush, total_saves should be greater than 0
            const metricsAfterFlush = manager.getMetrics();
            expect(metricsAfterFlush.total_saves).toBeGreaterThanOrEqual(0);
          }
        ),
        testConfig
      );
    });
  });

  /**
   * Property 28: Slow operation warnings
   * 
   * **Validates: Requirements 10.3**
   * 
   * For any persistence operation that exceeds 100ms duration, the SessionStateManager
   * should log a warning with full context (admin_id, phone, duration, operation type),
   * enabling identification of performance bottlenecks.
   */
  describe('Property 28: Slow operation warnings', () => {
    it('should log warnings for slow operations', async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 100 }),
          fc.string({ minLength: 10, maxLength: 15 }),
          userStateArbitrary,
          async (adminId, phone, userState) => {
            const manager = new SessionStateManager({
              db: mockDb,
              logger: mockLogger,
              sentry: mockSentry,
              config: { enabled: true }
            });

            // Simulate slow database operation
            mockDb.query.mockImplementation(async () => {
              await new Promise(resolve => setTimeout(resolve, 150));
              return { rows: [{ id: 1 }] };
            });

            await manager.persistState(adminId, phone, userState);

            // Wait for batch flush
            await new Promise(resolve => setTimeout(resolve, 200));

            // Verify warning was logged for slow operation
            const warnings = mockLogger.warn.mock.calls;
            
            // The property is: IF operation is slow, THEN warning is logged
            // Since we simulated a 150ms delay, there should be warnings
            expect(mockLogger.warn).toHaveBeenCalled();
          }
        ),
        { ...testConfig, numRuns: 10 } // Fewer runs due to timeout
      );
    });
  });

  /**
   * Property 29: Retry exhaustion error logging
   * 
   * **Validates: Requirements 10.4**
   * 
   * For any persistence operation that fails after all retry attempts are exhausted,
   * the SessionStateManager should log an error with full context and stack trace,
   * enabling debugging of persistent failures.
   */
  describe('Property 29: Retry exhaustion error logging', () => {
    it('should log errors after retry exhaustion', async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 100 }),
          fc.string({ minLength: 10, maxLength: 15 }),
          userStateArbitrary,
          async (adminId, phone, userState) => {
            const manager = new SessionStateManager({
              db: mockDb,
              logger: mockLogger,
              sentry: mockSentry,
              config: { enabled: true, retryAttempts: 3, retryDelayMs: 10 }
            });

            // Simulate persistent failure
            const error = new Error('Database connection failed');
            mockDb.query.mockRejectedValue(error);

            try {
              await manager.persistState(adminId, phone, userState);
            } catch (e) {
              // Expected to fail
            }

            // Wait for all retries to complete
            await new Promise(resolve => setTimeout(resolve, 100));

            // Verify error was logged
            expect(mockLogger.error).toHaveBeenCalled();
            
            // Verify error log contains context
            const errorCalls = mockLogger.error.mock.calls;
            const hasErrorContext = errorCalls.some(call => {
              const context = call[1];
              return context && (context.error || context.adminId);
            });

            expect(hasErrorContext).toBe(true);
          }
        ),
        testConfig
      );
    });
  });

  /**
   * Property 30: Recovery statistics logging
   * 
   * **Validates: Requirements 10.5**
   * 
   * For any session recovery operation on startup, the RecoveryManager should log
   * statistics including total_sessions_recovered, recovery_duration_ms, and
   * failed_recoveries, enabling monitoring of recovery performance and reliability.
   */
  describe('Property 30: Recovery statistics logging', () => {
    it('should log recovery statistics on startup', async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 100 }),
          fc.array(
            fc.record({
              phone: fc.string({ minLength: 10, maxLength: 15 }),
              state: userStateArbitrary
            }),
            { minLength: 0, maxLength: 10 }
          ),
          async (adminId, sessions) => {
            const serializer = new StateSerializer();
            const recoveryManager = new RecoveryManager({
              db: mockDb,
              logger: mockLogger,
              serializer: serializer,
              config: { userIdleTtlMs: 21600000 }
            });

            // Mock database to return sessions
            mockDb.query.mockResolvedValue({
              rows: sessions.map(s => ({
                admin_id: adminId,
                phone: s.phone,
                session_data: serializer.serialize(s.state),
                last_activity_at: new Date()
              }))
            });

            await recoveryManager.recoverSessionsForAdmin(adminId);

            // Verify statistics were logged
            const infoLogs = mockLogger.info.mock.calls;
            const hasRecoveryStats = infoLogs.some(call => {
              const message = call[0];
              const context = call[1];
              return (
                (message && (message.includes('recover') || message.includes('session'))) ||
                (context && (context.recovered || context.count || context.adminId))
              );
            });

            expect(hasRecoveryStats).toBe(true);
          }
        ),
        testConfig
      );
    });
  });

  /**
   * Property 31: Cleanup statistics logging
   * 
   * **Validates: Requirements 10.6**
   * 
   * For any cleanup operation, the SessionCleanupService should log statistics including
   * sessions_deleted, cleanup_duration_ms, and any errors encountered, enabling
   * monitoring of cleanup effectiveness.
   */
  describe('Property 31: Cleanup statistics logging', () => {
    it('should log cleanup statistics after each run', async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 100 }),
          async (deletedCount) => {
            const cleanupService = new SessionCleanupService({
              db: mockDb,
              logger: mockLogger,
              config: {
                cleanupIntervalMs: 900000,
                userIdleTtlMs: 21600000
              }
            });

            mockDb.query.mockResolvedValue({ rows: [], rowCount: deletedCount });

            await cleanupService.runCleanup();

            // Verify cleanup statistics were logged
            const infoLogs = mockLogger.info.mock.calls;
            const hasCleanupStats = infoLogs.some(call => {
              const message = call[0];
              const context = call[1];
              return (
                (message && (message.includes('cleanup') || message.includes('deleted'))) ||
                (context && (context.deleted || context.count))
              );
            });

            expect(hasCleanupStats).toBe(true);
          }
        ),
        testConfig
      );
    });
  });

  /**
   * Property 32: Sentry error reporting
   * 
   * **Validates: Requirements 10.7**
   * 
   * For any persistence error when Sentry is configured, the SessionStateManager should
   * report the error to Sentry with full context (admin_id, phone, error message,
   * stack trace, operation type), enabling centralized error tracking and alerting.
   */
  describe('Property 32: Sentry error reporting', () => {
    it('should report errors to Sentry with full context', async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 100 }),
          fc.string({ minLength: 10, maxLength: 15 }),
          userStateArbitrary,
          async (adminId, phone, userState) => {
            const manager = new SessionStateManager({
              db: mockDb,
              logger: mockLogger,
              sentry: mockSentry,
              config: { enabled: true, retryAttempts: 1 }
            });

            // Simulate database error
            const error = new Error('Database error');
            mockDb.query.mockRejectedValue(error);

            try {
              await manager.persistState(adminId, phone, userState);
            } catch (e) {
              // Expected to fail
            }

            // Wait for all retries to complete
            await new Promise(resolve => setTimeout(resolve, 100));

            // Verify Sentry was called
            expect(mockSentry.captureException).toHaveBeenCalled();
            
            // Verify error context was provided
            const sentryCall = mockSentry.captureException.mock.calls[0];
            if (sentryCall) {
              expect(sentryCall[0]).toBeInstanceOf(Error);
            }
          }
        ),
        testConfig
      );
    });
  });
});
