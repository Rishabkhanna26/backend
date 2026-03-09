import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import pg from 'pg';
import { SessionStateManager } from '../../src/persistence/SessionStateManager.js';
import { StateSerializer } from '../../src/persistence/StateSerializer.js';
import { CircuitBreaker } from '../../src/persistence/CircuitBreaker.js';

const { Pool } = pg;

/**
 * Chaos Test: Network Partition
 * 
 * This test simulates network partitions between application and database
 * and validates that:
 * 1. Circuit breaker activates after consecutive failures
 * 2. System falls back to in-memory storage
 * 3. System recovers when partition heals
 * 4. No data corruption occurs during partition
 * 
 * Requirements tested: 14.4, 14.5, 14.6, 15.7
 * Validates: Property 39 - Circuit breaker activation
 */
describe('Chaos: Network Partition', () => {
  let pool;
  let sessionStateManager;
  let serializer;
  
  const testAdminId = 999;
  const testPhone = '+1234567890';
  
  // Mock logger that captures log messages
  const logMessages = [];
  const mockLogger = {
    info: (msg, meta) => logMessages.push({ level: 'info', msg, meta }),
    warn: (msg, meta) => logMessages.push({ level: 'warn', msg, meta }),
    error: (msg, meta) => logMessages.push({ level: 'error', msg, meta }),
    debug: (msg, meta) => logMessages.push({ level: 'debug', msg, meta })
  };
  
  // Mock config with lower thresholds for testing
  const mockConfig = {
    enabled: true,
    retryAttempts: 3,
    retryDelayMs: 50, // Shorter for testing
    batchWindowMs: 500,
    circuitBreakerThreshold: 5, // Lower threshold for testing
    circuitBreakerResetMs: 2000, // Shorter for testing
    userIdleTtlMs: 21600000
  };
  
  beforeAll(async () => {
    // Create database connection pool
    pool = new Pool({
      host: process.env.DB_HOST || 'localhost',
      port: process.env.DB_PORT || 5432,
      database: process.env.DB_NAME || 'mex_end_test',
      user: process.env.DB_USER || 'postgres',
      password: process.env.DB_PASSWORD || 'postgres'
    });
    
    // Ensure conversation_states table exists
    await pool.query(`
      CREATE TABLE IF NOT EXISTS conversation_states (
        id SERIAL PRIMARY KEY,
        admin_id INTEGER NOT NULL,
        phone VARCHAR(20) NOT NULL,
        session_data JSONB NOT NULL,
        last_activity_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        CONSTRAINT unique_admin_phone UNIQUE (admin_id, phone)
      )
    `);
  });
  
  afterAll(async () => {
    await pool.end();
  });
  
  beforeEach(async () => {
    // Clean up test data and reset log messages
    await pool.query('DELETE FROM conversation_states WHERE admin_id = $1', [testAdminId]);
    logMessages.length = 0;
  });
  
  it('should activate circuit breaker after consecutive failures', async () => {
    // Arrange: Create a pool that always fails (simulating network partition)
    let failureCount = 0;
    const partitionedPool = {
      query: async () => {
        failureCount++;
        throw new Error('ETIMEDOUT: Network partition - connection timeout');
      },
      end: async () => {}
    };
    
    serializer = new StateSerializer({ logger: mockLogger });
    
    sessionStateManager = new SessionStateManager({
      db: partitionedPool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    const userState = {
      step: 'awaiting_name',
      data: { test: 'partition' },
      isReturningUser: false,
      clientId: 12345,
      name: 'Partition Test User',
      email: 'partition@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: new Date(),
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [],
      responseLanguage: 'en'
    };
    
    // Act: Trigger multiple failures to open circuit breaker
    // Need to exceed circuitBreakerThreshold (5 in test config)
    for (let i = 0; i < 6; i++) {
      await sessionStateManager.persistState(testAdminId, `+111111111${i}`, userState);
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    // Wait for all retries and circuit breaker logic
    await new Promise(resolve => setTimeout(resolve, 500));
    
    // Assert: Verify circuit breaker opened
    expect(sessionStateManager.isCircuitOpen()).toBe(true);
    
    // Verify critical alert was logged
    const criticalLogs = logMessages.filter(log => 
      (log.level === 'error' || log.level === 'warn') && 
      log.msg && (
        log.msg.includes('Circuit breaker') || 
        log.msg.includes('circuit') ||
        log.msg.includes('opened')
      )
    );
    expect(criticalLogs.length).toBeGreaterThan(0);
    
    // Verify multiple failures occurred
    expect(failureCount).toBeGreaterThanOrEqual(mockConfig.circuitBreakerThreshold);
  });
  
  it('should fall back to in-memory storage when circuit is open', async () => {
    // Arrange: Create a pool that fails to trigger circuit breaker
    let queryCallCount = 0;
    const failingPool = {
      query: async () => {
        queryCallCount++;
        throw new Error('Network partition');
      },
      end: async () => {}
    };
    
    serializer = new StateSerializer({ logger: mockLogger });
    
    sessionStateManager = new SessionStateManager({
      db: failingPool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    const userState = {
      step: 'awaiting_email',
      data: { fallback: true },
      isReturningUser: false,
      clientId: 67890,
      name: 'Fallback User',
      email: 'fallback@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: new Date(),
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [],
      responseLanguage: 'en'
    };
    
    // Act: Trigger circuit breaker
    for (let i = 0; i < 6; i++) {
      await sessionStateManager.persistState(testAdminId, `+222222222${i}`, userState);
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    await new Promise(resolve => setTimeout(resolve, 300));
    
    // Circuit should be open now
    expect(sessionStateManager.isCircuitOpen()).toBe(true);
    
    // Try to persist another state - should use in-memory fallback
    const initialQueryCount = queryCallCount;
    await sessionStateManager.persistState(testAdminId, testPhone, userState);
    await new Promise(resolve => setTimeout(resolve, 100));
    
    // Assert: No additional database queries should be attempted (circuit is open)
    // Query count should not increase significantly (maybe 1-2 from retries before circuit opened)
    expect(queryCallCount).toBeLessThanOrEqual(initialQueryCount + 3);
    
    // System should log that it's using in-memory fallback
    const fallbackLogs = logMessages.filter(log => 
      log.msg && (
        log.msg.includes('in-memory') || 
        log.msg.includes('fallback') ||
        log.msg.includes('Circuit breaker open')
      )
    );
    expect(fallbackLogs.length).toBeGreaterThan(0);
  });
  
  it('should automatically recover when partition heals', async () => {
    // Arrange: Create a pool that fails initially then recovers
    let failureCount = 0;
    const healingPool = {
      query: async (...args) => {
        failureCount++;
        // Fail for first 10 attempts, then succeed (partition heals)
        if (failureCount <= 10) {
          throw new Error('Network partition');
        }
        // Partition healed - use real pool
        return pool.query(...args);
      },
      end: async () => {}
    };
    
    serializer = new StateSerializer({ logger: mockLogger });
    
    sessionStateManager = new SessionStateManager({
      db: healingPool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    const userState = {
      step: 'awaiting_confirmation',
      data: { healing: true },
      isReturningUser: false,
      clientId: 11111,
      name: 'Healing Test User',
      email: 'healing@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: new Date(),
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [],
      responseLanguage: 'en'
    };
    
    // Act: Trigger failures to open circuit
    for (let i = 0; i < 6; i++) {
      await sessionStateManager.persistState(testAdminId, `+333333333${i}`, userState);
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    await new Promise(resolve => setTimeout(resolve, 300));
    expect(sessionStateManager.isCircuitOpen()).toBe(true);
    
    // Wait for circuit breaker reset timeout
    await new Promise(resolve => setTimeout(resolve, mockConfig.circuitBreakerResetMs + 500));
    
    // Try to persist again - circuit should attempt to close
    await sessionStateManager.persistState(testAdminId, testPhone, userState);
    await new Promise(resolve => setTimeout(resolve, 300));
    
    // Assert: Verify state was persisted after recovery
    const result = await pool.query(
      'SELECT * FROM conversation_states WHERE admin_id = $1 AND phone = $2',
      [testAdminId, testPhone]
    );
    
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].session_data.name).toBe('Healing Test User');
    
    // Circuit should be closed or half-open (attempting recovery)
    // After successful write, it should be closed
    expect(sessionStateManager.isCircuitOpen()).toBe(false);
  });
  
  it('should handle intermittent network issues without opening circuit', async () => {
    // Arrange: Create a pool with intermittent failures (not consecutive)
    let callCount = 0;
    const intermittentPool = {
      query: async (...args) => {
        callCount++;
        // Fail every 3rd call (not consecutive)
        if (callCount % 3 === 0) {
          throw new Error('Intermittent network issue');
        }
        return pool.query(...args);
      },
      end: async () => {}
    };
    
    serializer = new StateSerializer({ logger: mockLogger });
    
    sessionStateManager = new SessionStateManager({
      db: intermittentPool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    const userState = {
      step: 'awaiting_name',
      data: { intermittent: true },
      isReturningUser: false,
      clientId: 22222,
      name: 'Intermittent User',
      email: 'intermittent@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: new Date(),
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [],
      responseLanguage: 'en'
    };
    
    // Act: Perform multiple operations with intermittent failures
    for (let i = 0; i < 10; i++) {
      await sessionStateManager.persistState(testAdminId, `+444444444${i}`, userState);
      await new Promise(resolve => setTimeout(resolve, 150));
    }
    
    await new Promise(resolve => setTimeout(resolve, 300));
    
    // Assert: Circuit should remain closed (failures not consecutive)
    expect(sessionStateManager.isCircuitOpen()).toBe(false);
    
    // Some states should have been persisted successfully
    const result = await pool.query(
      'SELECT COUNT(*) FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    const count = parseInt(result.rows[0].count);
    expect(count).toBeGreaterThan(0);
  });
  
  it('should log circuit breaker state transitions', async () => {
    // Arrange: Create a pool that fails then recovers
    let shouldFail = true;
    const controlledPool = {
      query: async (...args) => {
        if (shouldFail) {
          throw new Error('Controlled partition');
        }
        return pool.query(...args);
      },
      end: async () => {}
    };
    
    serializer = new StateSerializer({ logger: mockLogger });
    
    sessionStateManager = new SessionStateManager({
      db: controlledPool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    const userState = {
      step: 'awaiting_payment',
      data: {},
      isReturningUser: false,
      clientId: 33333,
      name: 'State Transition User',
      email: 'transition@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: new Date(),
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [],
      responseLanguage: 'en'
    };
    
    // Act: Trigger circuit breaker open
    for (let i = 0; i < 6; i++) {
      await sessionStateManager.persistState(testAdminId, `+555555555${i}`, userState);
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    await new Promise(resolve => setTimeout(resolve, 300));
    
    // Verify circuit opened
    expect(sessionStateManager.isCircuitOpen()).toBe(true);
    
    // Clear logs to track recovery
    logMessages.length = 0;
    
    // Heal the partition
    shouldFail = false;
    
    // Wait for circuit breaker reset
    await new Promise(resolve => setTimeout(resolve, mockConfig.circuitBreakerResetMs + 500));
    
    // Trigger recovery attempt
    await sessionStateManager.persistState(testAdminId, testPhone, userState);
    await new Promise(resolve => setTimeout(resolve, 300));
    
    // Assert: Verify state transition logs
    const transitionLogs = logMessages.filter(log => 
      log.msg && (
        log.msg.includes('half-open') || 
        log.msg.includes('closed') ||
        log.msg.includes('recovery') ||
        log.msg.includes('Circuit breaker')
      )
    );
    
    expect(transitionLogs.length).toBeGreaterThan(0);
    
    // Circuit should be closed after successful operation
    expect(sessionStateManager.isCircuitOpen()).toBe(false);
  });
  
  it('should maintain data consistency during partition', async () => {
    // Arrange: Create a pool that partitions mid-operation
    let operationCount = 0;
    const partitioningPool = {
      query: async (...args) => {
        operationCount++;
        // Partition occurs after 3 successful operations
        if (operationCount > 3) {
          throw new Error('Network partition occurred');
        }
        return pool.query(...args);
      },
      end: async () => {}
    };
    
    serializer = new StateSerializer({ logger: mockLogger });
    
    sessionStateManager = new SessionStateManager({
      db: partitioningPool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    // Act: Persist multiple states
    const phones = ['+6666666661', '+6666666662', '+6666666663', '+6666666664', '+6666666665'];
    
    for (let i = 0; i < phones.length; i++) {
      const userState = {
        step: 'awaiting_name',
        data: { index: i },
        isReturningUser: false,
        clientId: 40000 + i,
        name: `Consistency User ${i}`,
        email: `consistency${i}@example.com`,
        assignedAdminId: testAdminId,
        greetedThisSession: true,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: new Date(),
        partialSavedAt: null,
        finalized: false,
        automationDisabled: false,
        aiConversationHistory: [],
        responseLanguage: 'en'
      };
      
      await sessionStateManager.persistState(testAdminId, phones[i], userState);
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    await new Promise(resolve => setTimeout(resolve, 500));
    
    // Assert: Verify data consistency - states before partition should be intact
    const result = await pool.query(
      'SELECT * FROM conversation_states WHERE admin_id = $1 ORDER BY phone',
      [testAdminId]
    );
    
    // Should have at least the first 3 states (before partition)
    expect(result.rows.length).toBeGreaterThanOrEqual(3);
    
    // Verify no data corruption in persisted states
    for (const row of result.rows) {
      expect(row.session_data.name).toMatch(/^Consistency User \d$/);
      expect(row.session_data.email).toMatch(/^consistency\d@example\.com$/);
      expect(row.session_data.data.index).toBeGreaterThanOrEqual(0);
    }
  });
  
  it('should handle circuit breaker with custom thresholds', async () => {
    // Arrange: Test with different circuit breaker threshold
    const customConfig = {
      ...mockConfig,
      circuitBreakerThreshold: 3 // Very low threshold
    };
    
    let failureCount = 0;
    const failingPool = {
      query: async () => {
        failureCount++;
        throw new Error('Network failure');
      },
      end: async () => {}
    };
    
    serializer = new StateSerializer({ logger: mockLogger });
    
    sessionStateManager = new SessionStateManager({
      db: failingPool,
      logger: mockLogger,
      serializer,
      config: customConfig
    });
    
    const userState = {
      step: 'awaiting_name',
      data: {},
      isReturningUser: false,
      clientId: 55555,
      name: 'Threshold Test User',
      email: 'threshold@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: new Date(),
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [],
      responseLanguage: 'en'
    };
    
    // Act: Trigger failures
    for (let i = 0; i < 4; i++) {
      await sessionStateManager.persistState(testAdminId, `+777777777${i}`, userState);
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    await new Promise(resolve => setTimeout(resolve, 300));
    
    // Assert: Circuit should open with lower threshold
    expect(sessionStateManager.isCircuitOpen()).toBe(true);
    
    // Verify it opened after fewer failures
    expect(failureCount).toBeLessThan(20); // Should open quickly with threshold of 3
  });
});
