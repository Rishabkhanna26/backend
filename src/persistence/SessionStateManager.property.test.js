/**
 * Property-Based Tests for SessionStateManager
 * 
 * Feature: session-state-persistence
 * 
 * These tests use fast-check to validate universal properties that should hold
 * across all valid inputs, ensuring correctness guarantees beyond specific examples.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import fc from 'fast-check';
import SessionStateManager from './SessionStateManager.js';

// Minimum 100 iterations per property test as per design spec
const testConfig = { numRuns: 100 };

// Arbitrary generators for user state objects
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

describe('SessionStateManager - Property-Based Tests', () => {
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
   * Property 4: State persistence after message processing
   * 
   * **Validates: Requirements 3.1, 3.2, 3.3**
   * 
   * For any user message that updates conversation state, the SessionStateManager
   * should persist the updated state to the database asynchronously (non-blocking)
   * with the last_activity_at timestamp updated to the current time.
   */
  describe('Property 4: State persistence after message processing', () => {
    it('should persist any valid user state asynchronously', async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 100 }), // adminId
          fc.string({ minLength: 10, maxLength: 15 }), // phone
          userStateArbitrary, // userState
          async (adminId, phone, userState) => {
            // Setup
            const manager = new SessionStateManager({
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

            mockDb.query.mockResolvedValue([{ rows: [{ id: 1 }] }]);

            // Execute
            const startTime = Date.now();
            const result = await manager.persistState(adminId, phone, userState);
            const duration = Date.now() - startTime;

            // Verify async non-blocking behavior (should return quickly)
            // Batching means the actual DB write is scheduled, not immediate
            expect(duration).toBeLessThan(100); // Should be very fast (just scheduling)
            expect(result).toBe(true);

            // Flush the batch to complete the write
            await manager.flushBatch();

            // Verify database was called with correct structure
            expect(mockDb.query).toHaveBeenCalled();
            const queryCall = mockDb.query.mock.calls[0];
            expect(queryCall[0]).toContain('INSERT INTO conversation_states');
            expect(queryCall[0]).toContain('ON CONFLICT');
            expect(queryCall[1][0]).toBe(adminId);
            expect(queryCall[1][1]).toBe(phone);
            expect(queryCall[1][2]).toBeDefined(); // serialized state

            // Verify last_activity_at is updated (query uses NOW())
            expect(queryCall[0]).toContain('last_activity_at');
            expect(queryCall[0]).toContain('NOW()');

            // Cleanup
            await manager.shutdown();
            vi.clearAllMocks();
          }
        ),
        testConfig
      );
    });

    it('should update last_activity_at timestamp on every persist', async () => {
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
              config: { enabled: true, batchWindowMs: 50 }
            });

            mockDb.query.mockResolvedValue([{ rows: [{ id: 1 }] }]);

            await manager.persistState(adminId, phone, userState);
            await manager.flushBatch();

            // Verify query includes last_activity_at update
            const queryCall = mockDb.query.mock.calls[0];
            expect(queryCall[0]).toContain('last_activity_at');
            expect(queryCall[0]).toContain('NOW()');

            await manager.shutdown();
            vi.clearAllMocks();
          }
        ),
        testConfig
      );
    });

    it('should perform persistence asynchronously without blocking', async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 100 }),
          fc.string({ minLength: 10, maxLength: 15 }),
          fc.array(userStateArbitrary, { minLength: 1, maxLength: 10 }),
          async (adminId, phone, userStates) => {
            const manager = new SessionStateManager({
              db: mockDb,
              logger: mockLogger,
              sentry: mockSentry,
              config: { enabled: true, batchWindowMs: 50 }
            });

            mockDb.query.mockResolvedValue([{ rows: [{ id: 1 }] }]);

            // Persist multiple states rapidly
            const startTime = Date.now();
            const results = await Promise.all(
              userStates.map(state => manager.persistState(adminId, phone, state))
            );
            const duration = Date.now() - startTime;

            // All should succeed quickly (non-blocking)
            expect(results.every(r => r === true)).toBe(true);
            expect(duration).toBeLessThan(100 * userStates.length); // Should be fast

            await manager.shutdown();
            vi.clearAllMocks();
          }
        ),
        testConfig
      );
    });
  });

  /**
   * Property 5: Persistence retry with exponential backoff
   * 
   * **Validates: Requirements 3.4, 3.6**
   * 
   * For any database persistence failure, the SessionStateManager should retry
   * up to 3 times with exponential backoff (100ms, 200ms, 400ms), and if all
   * retries fail, should log the error and emit a monitoring alert without crashing.
   */
  describe('Property 5: Persistence retry with exponential backoff', () => {
    it('should retry up to 3 times with exponential backoff on failures', async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 100 }),
          fc.string({ minLength: 10, maxLength: 15 }),
          userStateArbitrary,
          fc.integer({ min: 1, max: 2 }), // Number of failures before success (max 2, so total 3 attempts)
          async (adminId, phone, userState, failuresBeforeSuccess) => {
            // Create fresh mocks for each property test iteration
            const localMockDb = { query: vi.fn() };
            const localMockLogger = {
              debug: vi.fn(),
              info: vi.fn(),
              warn: vi.fn(),
              error: vi.fn()
            };
            const localMockSentry = { captureException: vi.fn() };

            const manager = new SessionStateManager({
              db: localMockDb,
              logger: localMockLogger,
              sentry: localMockSentry,
              config: {
                enabled: true,
                retryAttempts: 3,
                retryDelayMs: 10, // Faster for tests
                batchWindowMs: 50
              }
            });

            // Mock to fail N times then succeed
            let callCount = 0;
            localMockDb.query.mockImplementation(() => {
              callCount++;
              if (callCount <= failuresBeforeSuccess) {
                return Promise.reject(new Error('Database timeout'));
              }
              return Promise.resolve([{ rows: [{ id: 1 }] }]);
            });

            const startTime = Date.now();
            await manager.persistState(adminId, phone, userState);
            await manager.flushBatch();
            const duration = Date.now() - startTime;

            // Verify retry attempts
            expect(localMockDb.query).toHaveBeenCalledTimes(failuresBeforeSuccess + 1);

            // Verify exponential backoff delays were applied (with 10ms base)
            // Expected delays: 10ms * (2^0), 10ms * (2^1)
            const expectedMinDelay = failuresBeforeSuccess === 1 ? 10 : 30; // 10 or 10 + 20
            expect(duration).toBeGreaterThanOrEqual(expectedMinDelay - 10); // Allow 10ms tolerance

            // Verify retry warnings were logged
            const retryWarnings = localMockLogger.warn.mock.calls.filter(call =>
              call[0] === 'SessionStateManager: Retry attempt'
            );
            expect(retryWarnings).toHaveLength(failuresBeforeSuccess);

            await manager.shutdown();
          }
        ),
        testConfig
      );
    }, 10000); // 10 second timeout for property test

    it('should log error and emit alert after exhausting all retries', async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 100 }),
          fc.string({ minLength: 10, maxLength: 15 }),
          userStateArbitrary,
          async (adminId, phone, userState) => {
            // Create fresh mocks for each property test iteration
            const localMockDb = { query: vi.fn() };
            const localMockLogger = {
              debug: vi.fn(),
              info: vi.fn(),
              warn: vi.fn(),
              error: vi.fn()
            };
            const localMockSentry = { captureException: vi.fn() };

            const manager = new SessionStateManager({
              db: localMockDb,
              logger: localMockLogger,
              sentry: localMockSentry,
              config: {
                enabled: true,
                retryAttempts: 3,
                retryDelayMs: 10, // Faster for tests
                batchWindowMs: 50
              }
            });

            // Mock to always fail
            localMockDb.query.mockRejectedValue(new Error('Persistent database failure'));

            const result = await manager.persistState(adminId, phone, userState);
            
            // Should return true (scheduled) 
            expect(result).toBe(true);

            // Flush will trigger the retries and failures
            const flushResult = await manager.flushBatch();

            // Verify 3 retry attempts were made
            expect(localMockDb.query).toHaveBeenCalledTimes(3);

            // Verify flush reported failure
            expect(flushResult.failed).toBeGreaterThan(0);

            // Verify retry warnings were logged (2 warnings for attempts 1 and 2)
            const retryWarnings = localMockLogger.warn.mock.calls.filter(call =>
              call[0] === 'SessionStateManager: Retry attempt'
            );
            expect(retryWarnings.length).toBeGreaterThanOrEqual(2);

            await manager.shutdown();
          }
        ),
        testConfig
      );
    }, 10000); // 10 second timeout

    it('should not crash on retry failures', async () => {
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
              config: {
                enabled: true,
                retryAttempts: 3,
                retryDelayMs: 10,
                batchWindowMs: 50
              }
            });

            mockDb.query.mockRejectedValue(new Error('Database error'));

            // Should not throw
            await expect(async () => {
              await manager.persistState(adminId, phone, userState);
              await manager.flushBatch();
            }).not.toThrow();

            await manager.shutdown();
            vi.clearAllMocks();
          }
        ),
        testConfig
      );
    });
  });

  /**
   * Property 6: Batching rapid state updates
   * 
   * **Validates: Requirements 3.7**
   * 
   * For any sequence of state updates for the same (admin_id, phone) occurring
   * within a 500ms window, the SessionStateManager should coalesce them into a
   * single database write operation containing the most recent state.
   */
  describe('Property 6: Batching rapid state updates', () => {
    it('should coalesce rapid updates within batch window into single write', async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 100 }),
          fc.string({ minLength: 10, maxLength: 15 }),
          fc.array(userStateArbitrary, { minLength: 2, maxLength: 10 }),
          async (adminId, phone, userStates) => {
            const manager = new SessionStateManager({
              db: mockDb,
              logger: mockLogger,
              sentry: mockSentry,
              config: {
                enabled: true,
                retryAttempts: 3,
                retryDelayMs: 10,
                batchWindowMs: 500 // 500ms batch window
              }
            });

            mockDb.query.mockResolvedValue([{ rows: [{ id: 1 }] }]);

            // Persist multiple states rapidly (within batch window)
            for (const state of userStates) {
              await manager.persistState(adminId, phone, state);
            }

            // Before flush, verify pending batch count
            const pendingCount = manager.getPendingBatchCount();
            expect(pendingCount).toBe(1); // All updates for same (adminId, phone) coalesced

            // Flush the batch
            await manager.flushBatch();

            // Verify only ONE database write occurred (coalesced)
            expect(mockDb.query).toHaveBeenCalledTimes(1);

            // Verify the most recent state was persisted
            const queryCall = mockDb.query.mock.calls[0];
            const serializedState = JSON.parse(queryCall[1][2]);
            const lastState = userStates[userStates.length - 1];
            expect(serializedState.step).toBe(lastState.step);

            await manager.shutdown();
            vi.clearAllMocks();
          }
        ),
        testConfig
      );
    });

    it('should persist most recent state when batching', async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 100 }),
          fc.string({ minLength: 10, maxLength: 15 }),
          fc.array(
            fc.record({
              step: fc.string({ minLength: 1, maxLength: 50 }),
              data: fc.dictionary(fc.string(), fc.anything()),
              lastUserMessageAt: fc.date()
            }),
            { minLength: 3, maxLength: 10 }
          ),
          async (adminId, phone, states) => {
            const manager = new SessionStateManager({
              db: mockDb,
              logger: mockLogger,
              sentry: mockSentry,
              config: {
                enabled: true,
                batchWindowMs: 500
              }
            });

            mockDb.query.mockResolvedValue([{ rows: [{ id: 1 }] }]);

            // Persist states rapidly
            for (const state of states) {
              await manager.persistState(adminId, phone, state);
            }

            await manager.flushBatch();

            // Verify only one write
            expect(mockDb.query).toHaveBeenCalledTimes(1);

            // Verify the last state was persisted
            const queryCall = mockDb.query.mock.calls[0];
            const serializedState = JSON.parse(queryCall[1][2]);
            const lastState = states[states.length - 1];
            expect(serializedState.step).toBe(lastState.step);

            await manager.shutdown();
            vi.clearAllMocks();
          }
        ),
        testConfig
      );
    });

    it('should batch updates for different phones separately', async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 100 }),
          fc.array(fc.string({ minLength: 10, maxLength: 15 }), { minLength: 2, maxLength: 5 }),
          userStateArbitrary,
          async (adminId, phones, userState) => {
            const manager = new SessionStateManager({
              db: mockDb,
              logger: mockLogger,
              sentry: mockSentry,
              config: {
                enabled: true,
                batchWindowMs: 500
              }
            });

            mockDb.query.mockResolvedValue([{ rows: [{ id: 1 }] }]);

            // Persist states for different phones
            for (const phone of phones) {
              await manager.persistState(adminId, phone, userState);
            }

            // Verify pending batch count equals number of unique phones
            const pendingCount = manager.getPendingBatchCount();
            const uniquePhones = new Set(phones);
            expect(pendingCount).toBe(uniquePhones.size);

            await manager.flushBatch();

            // Verify one write per unique phone
            expect(mockDb.query).toHaveBeenCalledTimes(uniquePhones.size);

            await manager.shutdown();
            vi.clearAllMocks();
          }
        ),
        testConfig
      );
    });
  });
});
