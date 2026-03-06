/**
 * Tests for RecoveryManager
 * 
 * Feature: session-state-persistence
 * Testing Framework: vitest with fast-check for property-based tests
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import fc from 'fast-check';
import RecoveryManager from './RecoveryManager.js';
import StateSerializer from './StateSerializer.js';

describe('RecoveryManager - Property-Based Tests', () => {
  let mockDb;
  let mockLogger;
  let serializer;
  let recoveryManager;

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

    serializer = new StateSerializer();

    recoveryManager = new RecoveryManager({
      db: mockDb,
      logger: mockLogger,
      serializer,
      config: {
        userIdleTtlMs: 21600000 // 6 hours
      }
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  /**
   * Property 7: Session recovery loads active states
   * **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**
   * 
   * For any server startup, the RecoveryManager should load all conversation states
   * from the database where last_activity_at is within the USER_IDLE_TTL_MS window,
   * restore them to the in-memory sessions Map grouped by admin_id, and log recovery
   * statistics (total recovered, failed recoveries, duration).
   */
  describe('Property 7: Session recovery loads active states', () => {
    it('should recover only active sessions within TTL window', async () => {
      const userIdleTtlMs = 21600000; // 6 hours
      const now = Date.now();

      // Arbitrary generator for session data
      const sessionArbitrary = fc.record({
        admin_id: fc.integer({ min: 1, max: 10 }),
        phone: fc.string({ minLength: 10, maxLength: 15 }),
        step: fc.string(),
        data: fc.dictionary(fc.string(), fc.oneof(fc.string(), fc.integer())),
        isReturningUser: fc.boolean(),
        clientId: fc.integer({ min: 1, max: 10000 }),
        name: fc.string(),
        finalized: fc.boolean(),
        // Generate last_activity_at within TTL window
        lastActivityOffset: fc.integer({ min: 0, max: userIdleTtlMs - 1000 })
      });

      await fc.assert(
        fc.asyncProperty(
          fc.array(sessionArbitrary, { minLength: 1, maxLength: 50 }),
          async (sessions) => {
            // Reset mocks
            vi.clearAllMocks();

            // Create mock database rows
            const rows = sessions.map(s => {
              const lastActivityAt = new Date(now - s.lastActivityOffset);
              const sessionData = {
                step: s.step,
                data: s.data,
                isReturningUser: s.isReturningUser,
                clientId: s.clientId,
                name: s.name,
                finalized: s.finalized,
                lastUserMessageAt: lastActivityAt,
                greetedThisSession: true,
                automationDisabled: false
              };

              return {
                admin_id: s.admin_id,
                phone: s.phone,
                session_data: sessionData,
                last_activity_at: lastActivityAt
              };
            });

            mockDb.query.mockResolvedValue([rows]);

            // Recover sessions
            const result = await recoveryManager.recoverSessions();

            // Verify all sessions were recovered
            expect(result.stats.totalRecovered).toBe(sessions.length);
            expect(result.stats.failedRecoveries).toBe(0);

            // Verify sessions are grouped by admin_id
            const adminIds = [...new Set(sessions.map(s => s.admin_id))];
            expect(Object.keys(result.sessionsByAdmin).length).toBe(adminIds.length);

            // Verify each admin has correct sessions
            for (const adminId of adminIds) {
              const adminSessions = sessions.filter(s => s.admin_id === adminId);
              const recoveredSessions = result.sessionsByAdmin[adminId];
              
              expect(Object.keys(recoveredSessions).length).toBe(adminSessions.length);

              // Verify each session is correctly restored
              for (const session of adminSessions) {
                expect(recoveredSessions[session.phone]).toBeDefined();
                expect(recoveredSessions[session.phone].step).toBe(session.step);
                expect(recoveredSessions[session.phone].clientId).toBe(session.clientId);
              }
            }

            // Verify statistics are logged
            expect(mockLogger.info).toHaveBeenCalledWith(
              'RecoveryManager: Session recovery completed',
              expect.objectContaining({
                totalRecovered: sessions.length,
                failedRecoveries: 0,
                adminCount: adminIds.length
              })
            );
          }
        ),
        { numRuns: 20 } // Run 20 iterations for property test
      );
    });

    it('should filter out expired sessions outside TTL window', async () => {
      const userIdleTtlMs = 21600000; // 6 hours
      const now = Date.now();

      // Generator for sessions with mixed active/expired states
      const sessionWithAgeArbitrary = fc.record({
        admin_id: fc.integer({ min: 1, max: 5 }),
        phone: fc.string({ minLength: 10, maxLength: 15 }),
        step: fc.string(),
        isActive: fc.boolean(), // Determines if within TTL
        ageMs: fc.integer({ min: 0, max: userIdleTtlMs * 2 })
      });

      await fc.assert(
        fc.asyncProperty(
          fc.array(sessionWithAgeArbitrary, { minLength: 5, maxLength: 30 }),
          async (sessions) => {
            vi.clearAllMocks();

            // Calculate cutoff time
            const cutoffTime = new Date(now - userIdleTtlMs);

            // Filter to only active sessions (within TTL)
            const activeSessions = sessions.filter(s => {
              const lastActivityAt = new Date(now - s.ageMs);
              return lastActivityAt > cutoffTime;
            });

            // Create mock database rows (database already filters)
            const rows = activeSessions.map(s => ({
              admin_id: s.admin_id,
              phone: s.phone,
              session_data: {
                step: s.step,
                isReturningUser: false,
                greetedThisSession: true
              },
              last_activity_at: new Date(now - s.ageMs)
            }));

            mockDb.query.mockResolvedValue([rows]);

            // Recover sessions
            const result = await recoveryManager.recoverSessions();

            // Verify only active sessions were recovered
            expect(result.stats.totalRecovered).toBe(activeSessions.length);
            expect(result.stats.failedRecoveries).toBe(0);

            // Verify database query used correct cutoff time
            expect(mockDb.query).toHaveBeenCalledWith(
              expect.stringContaining('WHERE last_activity_at > $1'),
              expect.arrayContaining([expect.any(Date)])
            );
          }
        ),
        { numRuns: 20 }
      );
    });

    it('should track recovery duration and meet performance target', async () => {
      // Generate up to 100 sessions to test performance
      const sessionArbitrary = fc.record({
        admin_id: fc.integer({ min: 1, max: 10 }),
        phone: fc.string({ minLength: 10, maxLength: 15 }),
        step: fc.string()
      });

      await fc.assert(
        fc.asyncProperty(
          fc.array(sessionArbitrary, { minLength: 10, maxLength: 100 }),
          async (sessions) => {
            vi.clearAllMocks();

            const rows = sessions.map(s => ({
              admin_id: s.admin_id,
              phone: s.phone,
              session_data: { step: s.step, greetedThisSession: true },
              last_activity_at: new Date()
            }));

            mockDb.query.mockResolvedValue([rows]);

            const startTime = Date.now();
            const result = await recoveryManager.recoverSessions();
            const actualDuration = Date.now() - startTime;

            // Verify duration is tracked
            expect(result.stats.recoveryDurationMs).toBeGreaterThanOrEqual(0);
            expect(result.stats.recoveryDurationMs).toBeLessThanOrEqual(actualDuration + 50);

            // For 100 sessions, should complete quickly (well under 5 seconds)
            if (sessions.length <= 100) {
              expect(result.stats.recoveryDurationMs).toBeLessThan(1000);
            }

            // Verify statistics include duration
            expect(mockLogger.info).toHaveBeenCalledWith(
              'RecoveryManager: Session recovery completed',
              expect.objectContaining({
                duration: expect.any(Number)
              })
            );
          }
        ),
        { numRuns: 15 }
      );
    });
  });

  /**
   * Property 8: Recovery skips corrupted sessions
   * **Validates: Requirements 4.6, 14.2**
   * 
   * For any session with corrupted or invalid session_data that fails deserialization
   * during recovery, the RecoveryManager should log the error, skip that specific session,
   * and continue loading other sessions without failing the startup process.
   */
  describe('Property 8: Recovery skips corrupted sessions', () => {
    it('should skip sessions with invalid JSON and continue recovery', async () => {
      // Generator for mix of valid and corrupted sessions
      const sessionArbitrary = fc.record({
        admin_id: fc.integer({ min: 1, max: 5 }),
        phone: fc.string({ minLength: 10, maxLength: 15 }),
        isCorrupted: fc.boolean(),
        step: fc.string(),
        name: fc.string()
      });

      await fc.assert(
        fc.asyncProperty(
          fc.array(sessionArbitrary, { minLength: 5, maxLength: 30 }),
          async (sessions) => {
            vi.clearAllMocks();

            // Create mock database rows with some corrupted data
            const rows = sessions.map(s => {
              if (s.isCorrupted) {
                // Create invalid session data that will fail deserialization
                return {
                  admin_id: s.admin_id,
                  phone: s.phone,
                  session_data: { __invalid__: true, circular: {} }, // Will cause issues
                  last_activity_at: new Date()
                };
              } else {
                return {
                  admin_id: s.admin_id,
                  phone: s.phone,
                  session_data: {
                    step: s.step,
                    name: s.name,
                    greetedThisSession: true
                  },
                  last_activity_at: new Date()
                };
              }
            });

            // Add circular reference to corrupted sessions
            rows.forEach(row => {
              if (row.session_data.__invalid__) {
                row.session_data.circular.self = row.session_data.circular;
              }
            });

            mockDb.query.mockResolvedValue([rows]);

            // Mock serializer to fail on corrupted data
            const mockSerializer = {
              deserialize: vi.fn((jsonStr) => {
                const data = JSON.parse(jsonStr);
                if (data.__invalid__) {
                  return null; // Simulate deserialization failure
                }
                return data;
              })
            };

            const manager = new RecoveryManager({
              db: mockDb,
              logger: mockLogger,
              serializer: mockSerializer,
              config: { userIdleTtlMs: 21600000 }
            });

            // Recover sessions
            const result = await manager.recoverSessions();

            const validSessions = sessions.filter(s => !s.isCorrupted);
            const corruptedSessions = sessions.filter(s => s.isCorrupted);

            // Verify valid sessions were recovered
            expect(result.stats.totalRecovered).toBe(validSessions.length);
            
            // Verify corrupted sessions were counted as failures
            expect(result.stats.failedRecoveries).toBe(corruptedSessions.length);

            // Verify errors were logged for corrupted sessions
            if (corruptedSessions.length > 0) {
              expect(mockLogger.error).toHaveBeenCalled();
              expect(mockLogger.warn).toHaveBeenCalledWith(
                'RecoveryManager: Some sessions failed to recover',
                expect.objectContaining({
                  failedCount: corruptedSessions.length
                })
              );
            }

            // Verify recovery did not fail (returned result)
            expect(result).toBeDefined();
            expect(result.sessionsByAdmin).toBeDefined();
          }
        ),
        { numRuns: 20 }
      );
    });

    it('should handle complete recovery failure gracefully', async () => {
      // Simulate database error
      mockDb.query.mockRejectedValue(new Error('Database connection failed'));

      const result = await recoveryManager.recoverSessions();

      // Verify recovery returns empty result instead of throwing
      expect(result).toBeDefined();
      expect(result.sessionsByAdmin).toEqual({});
      expect(result.stats.totalRecovered).toBe(0);
      expect(result.stats.failedRecoveries).toBe(0);

      // Verify error was logged
      expect(mockLogger.error).toHaveBeenCalledWith(
        'RecoveryManager: Session recovery failed',
        expect.objectContaining({
          error: 'Database connection failed'
        })
      );
    });
  });
});

describe('RecoveryManager - Unit Tests', () => {
  let mockDb;
  let mockLogger;
  let serializer;
  let recoveryManager;

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

    serializer = new StateSerializer();

    recoveryManager = new RecoveryManager({
      db: mockDb,
      logger: mockLogger,
      serializer,
      config: {
        userIdleTtlMs: 21600000 // 6 hours
      }
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('recoverSessions', () => {
    it('should return empty result when no active sessions exist', async () => {
      mockDb.query.mockResolvedValue([[]]);

      const result = await recoveryManager.recoverSessions();

      expect(result.sessionsByAdmin).toEqual({});
      expect(result.stats.totalRecovered).toBe(0);
      expect(result.stats.failedRecoveries).toBe(0);
      expect(result.stats.recoveryDurationMs).toBeGreaterThanOrEqual(0);

      expect(mockLogger.info).toHaveBeenCalledWith(
        'RecoveryManager: No active sessions found to recover',
        expect.any(Object)
      );
    });

    it('should recover sessions grouped by admin_id', async () => {
      const rows = [
        {
          admin_id: 1,
          phone: '+1234567890',
          session_data: {
            step: 'awaiting_name',
            data: {},
            greetedThisSession: true,
            isReturningUser: false
          },
          last_activity_at: new Date()
        },
        {
          admin_id: 1,
          phone: '+1234567891',
          session_data: {
            step: 'awaiting_email',
            data: { name: 'John' },
            greetedThisSession: true,
            isReturningUser: false
          },
          last_activity_at: new Date()
        },
        {
          admin_id: 2,
          phone: '+1234567892',
          session_data: {
            step: 'awaiting_confirmation',
            data: {},
            greetedThisSession: true,
            isReturningUser: true
          },
          last_activity_at: new Date()
        }
      ];

      mockDb.query.mockResolvedValue([rows]);

      const result = await recoveryManager.recoverSessions();

      expect(result.stats.totalRecovered).toBe(3);
      expect(result.stats.failedRecoveries).toBe(0);

      // Verify admin 1 has 2 sessions
      expect(Object.keys(result.sessionsByAdmin[1])).toHaveLength(2);
      expect(result.sessionsByAdmin[1]['+1234567890']).toBeDefined();
      expect(result.sessionsByAdmin[1]['+1234567890'].step).toBe('awaiting_name');
      expect(result.sessionsByAdmin[1]['+1234567891']).toBeDefined();
      expect(result.sessionsByAdmin[1]['+1234567891'].step).toBe('awaiting_email');

      // Verify admin 2 has 1 session
      expect(Object.keys(result.sessionsByAdmin[2])).toHaveLength(1);
      expect(result.sessionsByAdmin[2]['+1234567892']).toBeDefined();
      expect(result.sessionsByAdmin[2]['+1234567892'].step).toBe('awaiting_confirmation');
    });

    it('should skip corrupted sessions and log errors', async () => {
      const rows = [
        {
          admin_id: 1,
          phone: '+1234567890',
          session_data: {
            step: 'valid_step',
            greetedThisSession: true
          },
          last_activity_at: new Date()
        },
        {
          admin_id: 1,
          phone: '+1234567891',
          session_data: null, // Corrupted data
          last_activity_at: new Date()
        }
      ];

      mockDb.query.mockResolvedValue([rows]);

      const result = await recoveryManager.recoverSessions();

      expect(result.stats.totalRecovered).toBe(1);
      expect(result.stats.failedRecoveries).toBe(1);

      // Verify error was logged
      expect(mockLogger.error).toHaveBeenCalledWith(
        'RecoveryManager: Failed to recover session',
        expect.objectContaining({
          adminId: 1,
          phone: '****7891'
        })
      );

      // Verify warning about failed recoveries
      expect(mockLogger.warn).toHaveBeenCalledWith(
        'RecoveryManager: Some sessions failed to recover',
        expect.objectContaining({
          failedCount: 1
        })
      );
    });

    it('should log per-admin statistics', async () => {
      const rows = [
        {
          admin_id: 1,
          phone: '+1234567890',
          session_data: { step: 'step1', greetedThisSession: true },
          last_activity_at: new Date()
        },
        {
          admin_id: 1,
          phone: '+1234567891',
          session_data: { step: 'step2', greetedThisSession: true },
          last_activity_at: new Date()
        },
        {
          admin_id: 2,
          phone: '+1234567892',
          session_data: { step: 'step3', greetedThisSession: true },
          last_activity_at: new Date()
        }
      ];

      mockDb.query.mockResolvedValue([rows]);

      await recoveryManager.recoverSessions();

      // Verify per-admin logging (adminId is converted to string in logs)
      const infoLogs = mockLogger.info.mock.calls;
      const adminLogs = infoLogs.filter(call => 
        call[0] === 'RecoveryManager: Sessions recovered for admin'
      );
      
      expect(adminLogs).toHaveLength(2);
      expect(adminLogs.some(call => call[1].adminId === '1' && call[1].sessionCount === 2)).toBe(true);
      expect(adminLogs.some(call => call[1].adminId === '2' && call[1].sessionCount === 1)).toBe(true);
    });
  });

  describe('recoverSessionsForAdmin', () => {
    it('should recover sessions for specific admin only', async () => {
      const rows = [
        {
          phone: '+1234567890',
          session_data: {
            step: 'awaiting_name',
            greetedThisSession: true
          },
          last_activity_at: new Date()
        },
        {
          phone: '+1234567891',
          session_data: {
            step: 'awaiting_email',
            greetedThisSession: true
          },
          last_activity_at: new Date()
        }
      ];

      mockDb.query.mockResolvedValue([rows]);

      const result = await recoveryManager.recoverSessionsForAdmin(1);

      expect(result.stats.totalRecovered).toBe(2);
      expect(result.stats.failedRecoveries).toBe(0);
      expect(Object.keys(result.users)).toHaveLength(2);
      expect(result.users['+1234567890']).toBeDefined();
      expect(result.users['+1234567891']).toBeDefined();

      // Verify query included admin_id filter
      expect(mockDb.query).toHaveBeenCalledWith(
        expect.stringContaining('WHERE admin_id = $1'),
        expect.arrayContaining([1, expect.any(Date)])
      );
    });

    it('should return empty result for admin with no sessions', async () => {
      mockDb.query.mockResolvedValue([[]]);

      const result = await recoveryManager.recoverSessionsForAdmin(1);

      expect(result.users).toEqual({});
      expect(result.stats.totalRecovered).toBe(0);
      expect(result.stats.failedRecoveries).toBe(0);

      expect(mockLogger.info).toHaveBeenCalledWith(
        'RecoveryManager: No active sessions found for admin',
        expect.objectContaining({ adminId: 1 })
      );
    });

    it('should handle invalid adminId gracefully', async () => {
      const result = await recoveryManager.recoverSessionsForAdmin(null);

      expect(result.users).toEqual({});
      expect(result.stats.totalRecovered).toBe(0);

      expect(mockLogger.warn).toHaveBeenCalledWith(
        'RecoveryManager: Invalid adminId provided for recovery',
        expect.any(Object)
      );
    });

    it('should handle database errors gracefully', async () => {
      mockDb.query.mockRejectedValue(new Error('Connection timeout'));

      const result = await recoveryManager.recoverSessionsForAdmin(1);

      expect(result.users).toEqual({});
      expect(result.stats.totalRecovered).toBe(0);

      expect(mockLogger.error).toHaveBeenCalledWith(
        'RecoveryManager: Session recovery failed for admin',
        expect.objectContaining({
          adminId: 1,
          error: 'Connection timeout'
        })
      );
    });
  });

  describe('getRecoveryStats', () => {
    it('should return recovery statistics', async () => {
      const rows = [
        {
          admin_id: 1,
          phone: '+1234567890',
          session_data: { step: 'step1', greetedThisSession: true },
          last_activity_at: new Date()
        }
      ];

      mockDb.query.mockResolvedValue([rows]);

      await recoveryManager.recoverSessions();

      const stats = recoveryManager.getRecoveryStats();

      expect(stats.totalRecovered).toBe(1);
      expect(stats.failedRecoveries).toBe(0);
      expect(stats.recoveryDurationMs).toBeGreaterThanOrEqual(0);
      expect(stats.lastRecoveryTime).toBeInstanceOf(Date);
    });

    it('should return initial stats before any recovery', () => {
      const stats = recoveryManager.getRecoveryStats();

      expect(stats.totalRecovered).toBe(0);
      expect(stats.failedRecoveries).toBe(0);
      expect(stats.recoveryDurationMs).toBe(0);
      expect(stats.lastRecoveryTime).toBeNull();
    });
  });

  describe('_maskPhone', () => {
    it('should mask phone numbers correctly', () => {
      expect(recoveryManager._maskPhone('+1234567890')).toBe('****7890');
      expect(recoveryManager._maskPhone('1234567890')).toBe('****7890');
      expect(recoveryManager._maskPhone('123')).toBe('****');
      expect(recoveryManager._maskPhone('')).toBe('****');
      expect(recoveryManager._maskPhone(null)).toBe('****');
    });
  });

  describe('Performance - 1000 sessions in < 5 seconds', () => {
    it('should recover 1000 sessions within performance target', async () => {
      // Generate 1000 mock sessions
      const rows = Array.from({ length: 1000 }, (_, i) => ({
        admin_id: Math.floor(i / 100) + 1, // 10 admins with 100 sessions each
        phone: `+123456${String(i).padStart(4, '0')}`,
        session_data: {
          step: 'awaiting_name',
          data: { index: i },
          greetedThisSession: true,
          isReturningUser: false
        },
        last_activity_at: new Date()
      }));

      mockDb.query.mockResolvedValue([rows]);

      const startTime = Date.now();
      const result = await recoveryManager.recoverSessions();
      const duration = Date.now() - startTime;

      // Verify all sessions recovered
      expect(result.stats.totalRecovered).toBe(1000);
      expect(result.stats.failedRecoveries).toBe(0);

      // Verify performance target met (< 5 seconds)
      expect(duration).toBeLessThan(5000);

      // Verify sessions grouped correctly (10 admins)
      expect(Object.keys(result.sessionsByAdmin)).toHaveLength(10);

      console.log(`Performance test: Recovered 1000 sessions in ${duration}ms`);
    });
  });
});
