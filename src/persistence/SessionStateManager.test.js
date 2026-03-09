/**
 * Tests for SessionStateManager
 * 
 * Feature: session-state-persistence
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import SessionStateManager from './SessionStateManager.js';

describe('SessionStateManager - Retry Logic', () => {
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
        retryDelayMs: 100
      }
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('_retryWithBackoff', () => {
    it('should succeed on first attempt if operation succeeds', async () => {
      const mockFn = vi.fn().mockResolvedValue('success');

      const result = await manager._retryWithBackoff(mockFn);

      expect(result).toBe('success');
      expect(mockFn).toHaveBeenCalledTimes(1);
      expect(mockLogger.warn).not.toHaveBeenCalled();
    });

    it('should retry with exponential backoff on failures', async () => {
      const mockFn = vi.fn()
        .mockRejectedValueOnce(new Error('Attempt 1 failed'))
        .mockRejectedValueOnce(new Error('Attempt 2 failed'))
        .mockResolvedValueOnce('success');

      const startTime = Date.now();
      const result = await manager._retryWithBackoff(mockFn);
      const duration = Date.now() - startTime;

      expect(result).toBe('success');
      expect(mockFn).toHaveBeenCalledTimes(3);
      
      // Verify exponential backoff delays: 100ms + 200ms = 300ms minimum
      expect(duration).toBeGreaterThanOrEqual(300);
      
      // Verify retry attempts were logged
      expect(mockLogger.warn).toHaveBeenCalledTimes(2);
      
      // Verify first retry log
      expect(mockLogger.warn).toHaveBeenNthCalledWith(1, 'SessionStateManager: Retry attempt', {
        attempt: 1,
        maxAttempts: 3,
        backoffDelay: 100,
        error: 'Attempt 1 failed'
      });
      
      // Verify second retry log
      expect(mockLogger.warn).toHaveBeenNthCalledWith(2, 'SessionStateManager: Retry attempt', {
        attempt: 2,
        maxAttempts: 3,
        backoffDelay: 200,
        error: 'Attempt 2 failed'
      });
    });

    it('should use correct exponential backoff delays: 100ms, 200ms, 400ms', async () => {
      const delays = [];
      const mockFn = vi.fn().mockImplementation(async () => {
        throw new Error('Failed');
      });

      // Mock setTimeout to capture delays
      const originalSetTimeout = global.setTimeout;
      global.setTimeout = vi.fn((fn, delay) => {
        delays.push(delay);
        return originalSetTimeout(fn, 0); // Execute immediately for test speed
      });

      try {
        await manager._retryWithBackoff(mockFn);
      } catch (error) {
        // Expected to throw after all retries
      }

      // Restore setTimeout
      global.setTimeout = originalSetTimeout;

      // Verify delays: 100ms (2^0), 200ms (2^1), no delay after last attempt
      expect(delays).toEqual([100, 200]);
      expect(mockFn).toHaveBeenCalledTimes(3);
    });

    it('should throw error after exhausting all 3 retry attempts', async () => {
      const mockError = new Error('Persistent failure');
      const mockFn = vi.fn().mockRejectedValue(mockError);

      await expect(manager._retryWithBackoff(mockFn)).rejects.toThrow('Persistent failure');
      
      expect(mockFn).toHaveBeenCalledTimes(3);
      expect(mockLogger.warn).toHaveBeenCalledTimes(2); // Only 2 warnings (not after last attempt)
    });

    it('should log retry context including attempt number and backoff delay', async () => {
      const mockFn = vi.fn()
        .mockRejectedValueOnce(new Error('Database timeout'))
        .mockResolvedValueOnce('success');

      await manager._retryWithBackoff(mockFn);

      expect(mockLogger.warn).toHaveBeenCalledWith('SessionStateManager: Retry attempt', {
        attempt: 1,
        maxAttempts: 3,
        backoffDelay: 100,
        error: 'Database timeout'
      });
    });

    it('should not log warning on final attempt before throwing', async () => {
      const mockFn = vi.fn().mockRejectedValue(new Error('Failed'));

      try {
        await manager._retryWithBackoff(mockFn);
      } catch (error) {
        // Expected
      }

      // Should log warnings for attempts 1 and 2, but not for attempt 3
      expect(mockLogger.warn).toHaveBeenCalledTimes(2);
    });
  });

  describe('persistState with retry logic', () => {
    it('should retry database operations on failure', async () => {
      const userState = {
        step: 'awaiting_name',
        data: {},
        lastUserMessageAt: new Date()
      };

      // Mock database to fail twice then succeed
      mockDb.query
        .mockRejectedValueOnce(new Error('Connection timeout'))
        .mockRejectedValueOnce(new Error('Connection timeout'))
        .mockResolvedValueOnce({ rows: [{ id: 1 }] });

      await manager.persistState(1, '+1234567890', userState);

      // Verify retry logic was triggered
      expect(mockDb.query).toHaveBeenCalledTimes(3);
      
      // Verify retry warnings were logged (circuit breaker also logs warnings)
      const retryWarnings = mockLogger.warn.mock.calls.filter(call => 
        call[0] === 'SessionStateManager: Retry attempt'
      );
      expect(retryWarnings).toHaveLength(2);
    });

    it('should return false after exhausting retries during persist', async () => {
      const userState = {
        step: 'awaiting_name',
        data: {},
        lastUserMessageAt: new Date()
      };

      // Mock database to always fail
      mockDb.query.mockRejectedValue(new Error('Database unavailable'));

      const result = await manager.persistState(1, '+1234567890', userState);

      expect(result).toBe(false);
      expect(mockDb.query).toHaveBeenCalledTimes(3);
      expect(mockLogger.error).toHaveBeenCalled();
    });
  });
});

describe('SessionStateManager - Circuit Breaker', () => {
  let mockDb;
  let mockLogger;
  let mockSentry;
  let manager;

  beforeEach(() => {
    mockDb = {
      query: vi.fn()
    };

    mockLogger = {
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      debug: vi.fn()
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
        retryDelayMs: 10, // Faster for tests
        circuitBreakerThreshold: 10,
        circuitBreakerResetMs: 1000 // 1 second for tests
      }
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('Circuit breaker initialization', () => {
    it('should initialize circuit breaker with correct configuration', () => {
      expect(manager.circuitBreaker).toBeDefined();
      expect(manager.circuitBreaker.threshold).toBe(10);
      expect(manager.circuitBreaker.resetMs).toBe(1000);
      expect(manager.circuitBreaker.getState()).toBe('CLOSED');
    });

    it('should expose isCircuitOpen method', () => {
      expect(manager.isCircuitOpen()).toBe(false);
    });

    it('should expose resetCircuit method', () => {
      expect(typeof manager.resetCircuit).toBe('function');
    });
  });

  describe('Circuit breaker tracks consecutive failures', () => {
    it('should track consecutive failures', async () => {
      const userState = { step: 'test', data: {} };
      mockDb.query.mockRejectedValue(new Error('Database error'));

      // Trigger 5 failures
      for (let i = 0; i < 5; i++) {
        await manager.persistState(1, '+1234567890', userState);
      }

      expect(manager.circuitBreaker.getFailures()).toBe(5);
      expect(manager.circuitBreaker.getState()).toBe('CLOSED'); // Not yet at threshold
    });

    it('should open circuit after 10 consecutive failures', async () => {
      const userState = { step: 'test', data: {} };
      mockDb.query.mockRejectedValue(new Error('Database error'));

      // Trigger 10 failures to reach threshold
      for (let i = 0; i < 10; i++) {
        await manager.persistState(1, '+1234567890', userState);
      }

      expect(manager.circuitBreaker.getState()).toBe('OPEN');
      expect(manager.isCircuitOpen()).toBe(true);
      
      // Verify circuit open was logged
      const openLogs = mockLogger.error.mock.calls.filter(call => 
        call[0].includes('Circuit opened')
      );
      expect(openLogs.length).toBeGreaterThan(0);
    });

    it('should reset failure count on successful operation', async () => {
      const userState = { step: 'test', data: {} };
      
      // Trigger 5 failures
      mockDb.query.mockRejectedValue(new Error('Database error'));
      for (let i = 0; i < 5; i++) {
        await manager.persistState(1, '+1234567890', userState);
      }
      
      expect(manager.circuitBreaker.getFailures()).toBe(5);

      // Now succeed
      mockDb.query.mockResolvedValue({ rows: [{ id: 1 }] });
      await manager.persistState(1, '+1234567890', userState);

      // Failure count should reset
      expect(manager.circuitBreaker.getFailures()).toBe(0);
      expect(manager.circuitBreaker.getState()).toBe('CLOSED');
    });
  });

  describe('Circuit breaker open behavior', () => {
    beforeEach(async () => {
      // Open the circuit by triggering 10 failures
      const userState = { step: 'test', data: {} };
      mockDb.query.mockRejectedValue(new Error('Database error'));
      
      for (let i = 0; i < 10; i++) {
        await manager.persistState(1, '+1234567890', userState);
      }
      
      expect(manager.isCircuitOpen()).toBe(true);
      vi.clearAllMocks(); // Clear logs from setup
    });

    it('should fail fast when circuit is open', async () => {
      const userState = { step: 'test', data: {} };
      
      const result = await manager.persistState(1, '+1234567890', userState);

      expect(result).toBe(false);
      expect(mockDb.query).not.toHaveBeenCalled(); // Should not attempt database call
      
      // Should log that circuit is open
      expect(mockLogger.debug).toHaveBeenCalledWith(
        'SessionStateManager: Circuit breaker open, skipping persistence',
        expect.objectContaining({
          adminId: 1,
          circuitState: 'OPEN'
        })
      );
    });

    it('should not increment failure count when circuit is open', async () => {
      const userState = { step: 'test', data: {} };
      const failuresBefore = manager.circuitBreaker.getFailures();
      
      await manager.persistState(1, '+1234567890', userState);

      // Failures should not increase (circuit prevents attempts)
      expect(manager.circuitBreaker.getFailures()).toBe(failuresBefore);
    });
  });

  describe('Circuit breaker auto-reset', () => {
    it('should auto-reset to HALF_OPEN after timeout', async () => {
      const userState = { step: 'test', data: {} };
      mockDb.query.mockRejectedValue(new Error('Database error'));
      
      // Open the circuit
      for (let i = 0; i < 10; i++) {
        await manager.persistState(1, '+1234567890', userState);
      }
      
      expect(manager.circuitBreaker.getState()).toBe('OPEN');

      // Wait for auto-reset (1 second in test config)
      await new Promise(resolve => setTimeout(resolve, 1100));

      expect(manager.circuitBreaker.getState()).toBe('HALF_OPEN');
      
      // Verify reset was logged
      const resetLogs = mockLogger.info.mock.calls.filter(call => 
        call[0].includes('Attempting recovery')
      );
      expect(resetLogs.length).toBeGreaterThan(0);
    });

    it('should close circuit on successful operation in HALF_OPEN state', async () => {
      const userState = { step: 'test', data: {} };
      mockDb.query.mockRejectedValue(new Error('Database error'));
      
      // Open the circuit
      for (let i = 0; i < 10; i++) {
        await manager.persistState(1, '+1234567890', userState);
      }

      // Wait for auto-reset to HALF_OPEN
      await new Promise(resolve => setTimeout(resolve, 1100));
      expect(manager.circuitBreaker.getState()).toBe('HALF_OPEN');

      // Succeed on next attempt
      mockDb.query.mockResolvedValue({ rows: [{ id: 1 }] });
      await manager.persistState(1, '+1234567890', userState);

      expect(manager.circuitBreaker.getState()).toBe('CLOSED');
      expect(manager.circuitBreaker.getFailures()).toBe(0);
    });

    it('should reopen circuit on failure in HALF_OPEN state', async () => {
      const userState = { step: 'test', data: {} };
      mockDb.query.mockRejectedValue(new Error('Database error'));
      
      // Open the circuit
      for (let i = 0; i < 10; i++) {
        await manager.persistState(1, '+1234567890', userState);
      }

      // Wait for auto-reset to HALF_OPEN
      await new Promise(resolve => setTimeout(resolve, 1100));
      expect(manager.circuitBreaker.getState()).toBe('HALF_OPEN');

      // Fail on next attempt
      await manager.persistState(1, '+1234567890', userState);

      expect(manager.circuitBreaker.getState()).toBe('OPEN');
    });
  });

  describe('Circuit breaker logging', () => {
    it('should log circuit state changes', async () => {
      const userState = { step: 'test', data: {} };
      mockDb.query.mockRejectedValue(new Error('Database error'));
      
      // Trigger failures to open circuit
      for (let i = 0; i < 10; i++) {
        await manager.persistState(1, '+1234567890', userState);
      }

      // Verify circuit open was logged with critical level
      const openLogs = mockLogger.error.mock.calls.filter(call => 
        call[0] && call[0].includes('Circuit opened')
      );
      expect(openLogs.length).toBeGreaterThan(0);
      expect(openLogs[0][1]).toMatchObject({
        failures: 10,
        threshold: 10
      });
    });

    it('should include circuit state in error logs', async () => {
      const userState = { step: 'test', data: {} };
      mockDb.query.mockRejectedValue(new Error('Database error'));
      
      await manager.persistState(1, '+1234567890', userState);

      // Verify error log includes circuit state
      expect(mockLogger.error).toHaveBeenCalledWith(
        'SessionStateManager: Failed to persist state',
        expect.objectContaining({
          circuitState: expect.any(String),
          failures: expect.any(Number)
        })
      );
    });

    it('should include circuit state in Sentry reports', async () => {
      const userState = { step: 'test', data: {} };
      mockDb.query.mockRejectedValue(new Error('Database error'));
      
      await manager.persistState(1, '+1234567890', userState);

      expect(mockSentry.captureException).toHaveBeenCalledWith(
        expect.any(Error),
        expect.objectContaining({
          tags: expect.objectContaining({
            circuitState: expect.any(String)
          }),
          extra: expect.objectContaining({
            failures: expect.any(Number)
          })
        })
      );
    });
  });

  describe('Manual circuit reset', () => {
    it('should allow manual circuit reset', async () => {
      const userState = { step: 'test', data: {} };
      mockDb.query.mockRejectedValue(new Error('Database error'));
      
      // Open the circuit
      for (let i = 0; i < 10; i++) {
        await manager.persistState(1, '+1234567890', userState);
      }
      
      expect(manager.isCircuitOpen()).toBe(true);

      // Manually reset
      manager.resetCircuit();

      expect(manager.isCircuitOpen()).toBe(false);
      expect(manager.circuitBreaker.getState()).toBe('CLOSED');
      expect(manager.circuitBreaker.getFailures()).toBe(0);
    });
  });

  describe('Fallback to in-memory when circuit open', () => {
    it('should indicate fallback mode when circuit is open', async () => {
      const userState = { step: 'test', data: {} };
      mockDb.query.mockRejectedValue(new Error('Database error'));
      
      // Open the circuit
      for (let i = 0; i < 10; i++) {
        await manager.persistState(1, '+1234567890', userState);
      }

      // When circuit is open, persistState returns false
      // This signals to the caller to use in-memory storage
      const result = await manager.persistState(1, '+1234567890', userState);
      expect(result).toBe(false);
      expect(manager.isCircuitOpen()).toBe(true);
    });
  });
});

describe('SessionStateManager - Bulk Operations', () => {
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

  describe('persistAllStates', () => {
    it('should persist all states in parallel using Promise.allSettled', async () => {
      const usersMap = {
        '+1234567890': { step: 'awaiting_name', data: {}, lastUserMessageAt: new Date() },
        '+0987654321': { step: 'awaiting_email', data: {}, lastUserMessageAt: new Date() },
        '+1111111111': { step: 'complete', data: {}, lastUserMessageAt: new Date() }
      };

      mockDb.query.mockResolvedValue([{ rows: [{ id: 1 }] }]);

      const result = await manager.persistAllStates(1, usersMap);

      expect(result.total).toBe(3);
      expect(result.succeeded).toBe(3);
      expect(result.failed).toBe(0);
      expect(result.duration).toBeGreaterThanOrEqual(0);

      // Verify info log for starting bulk operation
      expect(mockLogger.info).toHaveBeenCalledWith(
        'SessionStateManager: Starting bulk persist operation',
        expect.objectContaining({
          adminId: 1,
          total: 3
        })
      );

      // Verify info log for completion
      expect(mockLogger.info).toHaveBeenCalledWith(
        'SessionStateManager: Bulk persist operation completed',
        expect.objectContaining({
          adminId: 1,
          total: 3,
          succeeded: 3,
          failed: 0,
          successRate: '100.00%'
        })
      );
    });

    it('should handle partial failures gracefully', async () => {
      const usersMap = {
        '+1234567890': { step: 'awaiting_name', data: {}, lastUserMessageAt: new Date() },
        '+0987654321': { step: 'awaiting_email', data: {}, lastUserMessageAt: new Date() },
        '+1111111111': { step: 'complete', data: {}, lastUserMessageAt: new Date() }
      };

      // Mock to always fail for one specific phone number
      mockDb.query.mockImplementation((query, params) => {
        const phone = params[1]; // phone is second parameter
        if (phone === '+0987654321') {
          return Promise.reject(new Error('Database error'));
        }
        return Promise.resolve([{ rows: [{ id: 1 }] }]);
      });

      const result = await manager.persistAllStates(1, usersMap);

      expect(result.total).toBe(3);
      expect(result.succeeded).toBe(2);
      expect(result.failed).toBe(1);

      // Verify warning log for failures
      expect(mockLogger.warn).toHaveBeenCalledWith(
        'SessionStateManager: Some states failed to persist in bulk operation',
        expect.objectContaining({
          adminId: 1,
          failed: 1,
          failedPhones: expect.arrayContaining([
            expect.objectContaining({
              phone: expect.any(String),
              error: expect.any(String)
            })
          ])
        })
      );
    });

    it('should log statistics with success rate', async () => {
      const usersMap = {
        '+1234567890': { step: 'awaiting_name', data: {}, lastUserMessageAt: new Date() },
        '+0987654321': { step: 'awaiting_email', data: {}, lastUserMessageAt: new Date() },
        '+1111111111': { step: 'complete', data: {}, lastUserMessageAt: new Date() },
        '+2222222222': { step: 'test', data: {}, lastUserMessageAt: new Date() }
      };

      // Mock to always fail for one specific phone number
      mockDb.query.mockImplementation((query, params) => {
        const phone = params[1]; // phone is second parameter
        if (phone === '+0987654321') {
          return Promise.reject(new Error('Database error'));
        }
        return Promise.resolve([{ rows: [{ id: 1 }] }]);
      });

      const result = await manager.persistAllStates(1, usersMap);

      expect(result.total).toBe(4);
      expect(result.succeeded).toBe(3);
      expect(result.failed).toBe(1);

      // Verify success rate calculation
      expect(mockLogger.info).toHaveBeenCalledWith(
        'SessionStateManager: Bulk persist operation completed',
        expect.objectContaining({
          successRate: '75.00%'
        })
      );
    });

    it('should return zero stats when persistence is disabled', async () => {
      manager.config.enabled = false;

      const usersMap = {
        '+1234567890': { step: 'awaiting_name', data: {}, lastUserMessageAt: new Date() }
      };

      const result = await manager.persistAllStates(1, usersMap);

      expect(result).toEqual({ total: 0, succeeded: 0, failed: 0, duration: 0 });
      expect(mockDb.query).not.toHaveBeenCalled();
    });

    it('should handle empty usersMap gracefully', async () => {
      const result = await manager.persistAllStates(1, {});

      expect(result).toEqual({ total: 0, succeeded: 0, failed: 0, duration: 0 });
      expect(mockDb.query).not.toHaveBeenCalled();

      expect(mockLogger.debug).toHaveBeenCalledWith(
        'SessionStateManager: No states to persist for bulk operation',
        expect.objectContaining({
          adminId: 1
        })
      );
    });

    it('should handle invalid parameters', async () => {
      const result = await manager.persistAllStates(null, null);

      expect(result).toEqual({ total: 0, succeeded: 0, failed: 0, duration: 0 });
      expect(mockLogger.warn).toHaveBeenCalledWith(
        'SessionStateManager: Invalid parameters for persistAllStates',
        expect.any(Object)
      );
    });

    it('should mask phone numbers in failure logs', async () => {
      const usersMap = {
        '+1234567890': { step: 'awaiting_name', data: {}, lastUserMessageAt: new Date() }
      };

      mockDb.query.mockRejectedValue(new Error('Database error'));

      await manager.persistAllStates(1, usersMap);

      // Verify phone is masked in logs
      const warnCalls = mockLogger.warn.mock.calls.filter(call =>
        call[0] === 'SessionStateManager: Some states failed to persist in bulk operation'
      );

      expect(warnCalls.length).toBeGreaterThan(0);
      expect(warnCalls[0][1].failedPhones[0].phone).toMatch(/\*\*\*\*\d{4}/);
    });

    it('should report errors to Sentry on bulk operation failure', async () => {
      const usersMap = {
        '+1234567890': { step: 'awaiting_name', data: {}, lastUserMessageAt: new Date() }
      };

      // Mock Promise.allSettled to throw an error
      const originalAllSettled = Promise.allSettled;
      Promise.allSettled = vi.fn().mockRejectedValue(new Error('Critical Promise.allSettled error'));

      await manager.persistAllStates(1, usersMap);

      // Restore Promise.allSettled
      Promise.allSettled = originalAllSettled;

      expect(mockSentry.captureException).toHaveBeenCalledWith(
        expect.any(Error),
        expect.objectContaining({
          tags: expect.objectContaining({
            component: 'SessionStateManager',
            operation: 'persistAllStates'
          })
        })
      );
    });
  });

  describe('loadAllStates', () => {
    it('should load all states for an admin in parallel', async () => {
      const mockRows = [
        {
          phone: '+1234567890',
          session_data: { step: 'awaiting_name', data: {}, lastUserMessageAt: new Date().toISOString() }
        },
        {
          phone: '+0987654321',
          session_data: { step: 'awaiting_email', data: {}, lastUserMessageAt: new Date().toISOString() }
        },
        {
          phone: '+1111111111',
          session_data: { step: 'complete', data: {}, lastUserMessageAt: new Date().toISOString() }
        }
      ];

      mockDb.query.mockResolvedValue([mockRows]);

      const result = await manager.loadAllStates(1);

      expect(result.stats.total).toBe(3);
      expect(result.stats.succeeded).toBe(3);
      expect(result.stats.failed).toBe(0);
      expect(Object.keys(result.users)).toHaveLength(3);
      expect(result.users['+1234567890']).toBeDefined();
      expect(result.users['+0987654321']).toBeDefined();
      expect(result.users['+1111111111']).toBeDefined();

      // Verify query was called with correct parameters
      expect(mockDb.query).toHaveBeenCalledWith(
        expect.stringContaining('SELECT phone, session_data'),
        [1]
      );

      // Verify info logs
      expect(mockLogger.info).toHaveBeenCalledWith(
        'SessionStateManager: Starting bulk load operation',
        expect.objectContaining({
          adminId: 1
        })
      );

      expect(mockLogger.info).toHaveBeenCalledWith(
        'SessionStateManager: Bulk load operation completed',
        expect.objectContaining({
          adminId: 1,
          total: 3,
          succeeded: 3,
          failed: 0,
          successRate: '100.00%'
        })
      );
    });

    it('should handle deserialization failures gracefully', async () => {
      const mockRows = [
        {
          phone: '+1234567890',
          session_data: { step: 'awaiting_name', data: {} }
        },
        {
          phone: '+0987654321',
          session_data: 'invalid json' // This will cause deserialization to fail
        },
        {
          phone: '+1111111111',
          session_data: { step: 'complete', data: {} }
        }
      ];

      mockDb.query.mockResolvedValue([mockRows]);

      // Mock serializer to fail on invalid data
      const originalDeserialize = manager.serializer.deserialize;
      manager.serializer.deserialize = vi.fn((data) => {
        if (data.includes('invalid json')) {
          return null;
        }
        return originalDeserialize.call(manager.serializer, data);
      });

      const result = await manager.loadAllStates(1);

      expect(result.stats.total).toBe(3);
      expect(result.stats.succeeded).toBe(2);
      expect(result.stats.failed).toBe(1);
      expect(Object.keys(result.users)).toHaveLength(2);

      // Verify warning log for failures
      expect(mockLogger.warn).toHaveBeenCalledWith(
        'SessionStateManager: Some states failed to load in bulk operation',
        expect.objectContaining({
          adminId: 1,
          failed: 1,
          failedPhones: expect.arrayContaining([
            expect.objectContaining({
              phone: expect.any(String),
              error: expect.any(String)
            })
          ])
        })
      );
    });

    it('should return empty result when no states found', async () => {
      mockDb.query.mockResolvedValue([[]]);

      const result = await manager.loadAllStates(1);

      expect(result.users).toEqual({});
      expect(result.stats).toEqual({ total: 0, succeeded: 0, failed: 0, duration: expect.any(Number) });

      expect(mockLogger.info).toHaveBeenCalledWith(
        'SessionStateManager: No states found for bulk load',
        expect.objectContaining({
          adminId: 1
        })
      );
    });

    it('should return zero stats when persistence is disabled', async () => {
      manager.config.enabled = false;

      const result = await manager.loadAllStates(1);

      expect(result).toEqual({
        users: {},
        stats: { total: 0, succeeded: 0, failed: 0, duration: 0 }
      });
      expect(mockDb.query).not.toHaveBeenCalled();
    });

    it('should handle invalid adminId parameter', async () => {
      const result = await manager.loadAllStates(null);

      expect(result).toEqual({
        users: {},
        stats: { total: 0, succeeded: 0, failed: 0, duration: 0 }
      });
      expect(mockLogger.warn).toHaveBeenCalledWith(
        'SessionStateManager: Invalid parameters for loadAllStates',
        expect.any(Object)
      );
    });

    it('should mask phone numbers in failure logs', async () => {
      const mockRows = [
        {
          phone: '+1234567890',
          session_data: 'invalid'
        }
      ];

      mockDb.query.mockResolvedValue([mockRows]);
      manager.serializer.deserialize = vi.fn().mockReturnValue(null);

      await manager.loadAllStates(1);

      const warnCalls = mockLogger.warn.mock.calls.filter(call =>
        call[0] === 'SessionStateManager: Some states failed to load in bulk operation'
      );

      expect(warnCalls.length).toBeGreaterThan(0);
      expect(warnCalls[0][1].failedPhones[0].phone).toMatch(/\*\*\*\*\d{4}/);
    });

    it('should report errors to Sentry on bulk load failure', async () => {
      mockDb.query.mockRejectedValue(new Error('Database connection failed'));

      await manager.loadAllStates(1);

      expect(mockSentry.captureException).toHaveBeenCalledWith(
        expect.any(Error),
        expect.objectContaining({
          tags: expect.objectContaining({
            component: 'SessionStateManager',
            operation: 'loadAllStates'
          })
        })
      );
    });

    it('should order results by last_activity_at DESC', async () => {
      const mockRows = [
        { phone: '+1111111111', session_data: { step: 'complete', data: {} } },
        { phone: '+0987654321', session_data: { step: 'awaiting_email', data: {} } },
        { phone: '+1234567890', session_data: { step: 'awaiting_name', data: {} } }
      ];

      mockDb.query.mockResolvedValue([mockRows]);

      await manager.loadAllStates(1);

      // Verify query includes ORDER BY clause
      expect(mockDb.query).toHaveBeenCalledWith(
        expect.stringContaining('ORDER BY last_activity_at DESC'),
        [1]
      );
    });

    it('should calculate success rate correctly', async () => {
      const mockRows = [
        { phone: '+1111111111', session_data: { step: 'complete', data: {} } },
        { phone: '+0987654321', session_data: { step: 'awaiting_email', data: {} } },
        { phone: '+1234567890', session_data: { step: 'awaiting_name', data: {} } },
        { phone: '+2222222222', session_data: { step: 'test', data: {} } }
      ];

      mockDb.query.mockResolvedValue([mockRows]);

      // Mock one deserialization failure
      let callCount = 0;
      const originalDeserialize = manager.serializer.deserialize;
      manager.serializer.deserialize = vi.fn((data) => {
        callCount++;
        if (callCount === 2) {
          return null;
        }
        return originalDeserialize.call(manager.serializer, data);
      });

      const result = await manager.loadAllStates(1);

      expect(result.stats.total).toBe(4);
      expect(result.stats.succeeded).toBe(3);
      expect(result.stats.failed).toBe(1);

      // Verify success rate calculation
      expect(mockLogger.info).toHaveBeenCalledWith(
        'SessionStateManager: Bulk load operation completed',
        expect.objectContaining({
          successRate: '75.00%'
        })
      );
    });
  });
});

describe('SessionStateManager - Metrics and Observability', () => {
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

  describe('getMetrics', () => {
    it('should return metrics object with all required fields', () => {
      const metrics = manager.getMetrics();

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

    it('should track state_save_duration with percentiles', async () => {
      const userState = { step: 'test', data: {}, lastUserMessageAt: new Date() };
      mockDb.query.mockResolvedValue([{ rows: [{ id: 1 }] }]);

      // Perform multiple saves
      for (let i = 0; i < 5; i++) {
        await manager.persistState(1, `+123456789${i}`, userState);
      }

      // Flush batch to complete operations
      await manager.flushBatch();

      const metrics = manager.getMetrics();

      expect(metrics.state_save_duration.samples).toBeGreaterThan(0);
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

      expect(metrics.state_load_duration.samples).toBeGreaterThan(0);
      expect(metrics.state_load_duration.avg).toBeGreaterThanOrEqual(0);
      expect(metrics.state_load_duration.p50).toBeGreaterThanOrEqual(0);
      expect(metrics.state_load_duration.p95).toBeGreaterThanOrEqual(0);
      expect(metrics.state_load_duration.p99).toBeGreaterThanOrEqual(0);
      expect(metrics.total_loads).toBe(5);
    });

    it('should track persistence_errors', async () => {
      const userState = { step: 'test', data: {}, lastUserMessageAt: new Date() };
      mockDb.query.mockRejectedValue(new Error('Database error'));

      // Trigger errors
      await manager.persistState(1, '+1234567890', userState);
      await manager.persistState(1, '+0987654321', userState);

      const metrics = manager.getMetrics();

      expect(metrics.persistence_errors).toBe(2);
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

    it('should track slow_operations when duration exceeds 100ms', async () => {
      const userState = { step: 'test', data: {}, lastUserMessageAt: new Date() };
      
      // Mock slow database operation
      mockDb.query.mockImplementation(() => {
        return new Promise(resolve => {
          setTimeout(() => resolve([{ rows: [{ id: 1 }] }]), 110);
        });
      });

      await manager.persistState(1, '+1234567890', userState);
      await manager.flushBatch();

      const metrics = manager.getMetrics();

      expect(metrics.slow_operations).toBeGreaterThan(0);
    });

    it('should include circuit_breaker_state in metrics', async () => {
      const userState = { step: 'test', data: {}, lastUserMessageAt: new Date() };
      mockDb.query.mockRejectedValue(new Error('Database error'));

      // Open circuit by triggering failures
      for (let i = 0; i < 10; i++) {
        await manager.persistState(1, `+123456789${i}`, userState);
      }

      const metrics = manager.getMetrics();

      expect(metrics.circuit_breaker_state).toBe('OPEN');
      expect(metrics.circuit_breaker_failures).toBe(10);
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

  describe('Logging for operations exceeding 100ms', () => {
    it('should log warning when persistState exceeds 100ms', async () => {
      const userState = { step: 'test', data: {}, lastUserMessageAt: new Date() };
      
      // Mock slow operation
      mockDb.query.mockImplementation(() => {
        return new Promise(resolve => {
          setTimeout(() => resolve([{ rows: [{ id: 1 }] }]), 110);
        });
      });

      await manager.persistState(1, '+1234567890', userState);
      await manager.flushBatch();

      // Check for slow operation warning
      const slowWarnings = mockLogger.warn.mock.calls.filter(call =>
        call[0] === 'SessionStateManager: Slow persistence operation detected'
      );

      expect(slowWarnings.length).toBeGreaterThan(0);
      expect(slowWarnings[0][1]).toMatchObject({
        adminId: 1,
        phone: '****7890',
        threshold: 100,
        operation: 'persistState'
      });
      expect(slowWarnings[0][1].duration).toBeGreaterThan(100);
    });

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
  });

  describe('Error logging with full context', () => {
    it('should log errors with full context for failed operations', async () => {
      const userState = { step: 'test', data: {}, lastUserMessageAt: new Date() };
      mockDb.query.mockRejectedValue(new Error('Database connection failed'));

      await manager.persistState(1, '+1234567890', userState);

      // Verify error log includes full context
      const errorCalls = mockLogger.error.mock.calls.filter(call =>
        call[0] === 'SessionStateManager: Failed to persist state'
      );

      expect(errorCalls.length).toBeGreaterThan(0);
      expect(errorCalls[0][1]).toMatchObject({
        adminId: 1,
        phone: '****7890',
        error: 'Database connection failed',
        circuitState: expect.any(String),
        failures: expect.any(Number),
        totalErrors: expect.any(Number)
      });
      expect(errorCalls[0][1]).toHaveProperty('stack');
      expect(errorCalls[0][1]).toHaveProperty('duration');
    });

    it('should include totalErrors count in error logs', async () => {
      const userState = { step: 'test', data: {}, lastUserMessageAt: new Date() };
      mockDb.query.mockRejectedValue(new Error('Database error'));

      // Trigger multiple errors
      await manager.persistState(1, '+1234567890', userState);
      await manager.persistState(1, '+0987654321', userState);
      await manager.persistState(1, '+1111111111', userState);

      const errorCalls = mockLogger.error.mock.calls.filter(call =>
        call[0] === 'SessionStateManager: Failed to persist state'
      );

      // Last error should show totalErrors = 3
      expect(errorCalls[errorCalls.length - 1][1].totalErrors).toBe(3);
    });
  });

  describe('Sentry alerts for persistence failures', () => {
    it('should emit Sentry alert when persistState fails', async () => {
      const userState = { step: 'test', data: {}, lastUserMessageAt: new Date() };
      const error = new Error('Database connection failed');
      mockDb.query.mockRejectedValue(error);

      await manager.persistState(1, '+1234567890', userState);

      expect(mockSentry.captureException).toHaveBeenCalledWith(
        error,
        expect.objectContaining({
          tags: expect.objectContaining({
            component: 'SessionStateManager',
            operation: 'persistState',
            circuitState: expect.any(String)
          }),
          extra: expect.objectContaining({
            adminId: 1,
            phone: '****7890',
            duration: expect.any(Number),
            failures: expect.any(Number),
            totalErrors: expect.any(Number)
          })
        })
      );
    });

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
      const userState = { step: 'test', data: {}, lastUserMessageAt: new Date() };
      mockDb.query.mockRejectedValue(new Error('Database error'));

      await manager.persistState(1, '+1234567890', userState);

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
      const userState = { step: 'test', data: {}, lastUserMessageAt: new Date() };
      mockDb.query.mockRejectedValue(new Error('Database error'));

      await manager.persistState(1, '+1234567890', userState);

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

  describe('Metrics integration with cache layer', () => {
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
