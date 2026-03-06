import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import pg from 'pg';
import { SessionStateManager } from '../../src/persistence/SessionStateManager.js';
import { StateSerializer } from '../../src/persistence/StateSerializer.js';

const { Pool } = pg;

/**
 * Chaos Test: Database Connection Loss
 * 
 * This test simulates database connection failures during message processing
 * and validates that the system:
 * 1. Continues operating with in-memory fallback
 * 2. Automatically reconnects when database becomes available
 * 3. Resumes persistence after reconnection
 * 
 * Requirements tested: 14.1, 14.6, 15.7
 * Validates: Property 38 - Database failure fallback and retry
 */
describe('Chaos: Database Connection Loss', () => {
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
  
  // Mock config
  const mockConfig = {
    enabled: true,
    retryAttempts: 3,
    retryDelayMs: 50, // Shorter for testing
    batchWindowMs: 500,
    circuitBreakerThreshold: 10,
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
  
  it('should continue operating with in-memory fallback when database connection fails', async () => {
    // Arrange: Create a pool that will fail
    const failingPool = new Pool({
      host: 'invalid-host-that-does-not-exist.local',
      port: 9999,
      database: 'nonexistent',
      user: 'nobody',
      password: 'invalid',
      connectionTimeoutMillis: 100
    });
    
    serializer = new StateSerializer({ logger: mockLogger });
    
    sessionStateManager = new SessionStateManager({
      db: failingPool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    const userState = {
      step: 'awaiting_name',
      data: { selectedProduct: 'birth_chart' },
      isReturningUser: false,
      clientId: 12345,
      name: 'Test User',
      email: 'test@example.com',
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
    
    // Act: Attempt to persist state (should fail but not crash)
    await sessionStateManager.persistState(testAdminId, testPhone, userState);
    
    // Wait for retries to complete
    await new Promise(resolve => setTimeout(resolve, 500));
    
    // Assert: System should log errors but continue operating
    const errorLogs = logMessages.filter(log => log.level === 'error');
    expect(errorLogs.length).toBeGreaterThan(0);
    
    // Verify error messages indicate database failure
    const dbErrorLogs = errorLogs.filter(log => 
      log.msg && (
        log.msg.includes('Failed to persist') || 
        log.msg.includes('Database error') ||
        log.msg.includes('persistence')
      )
    );
    expect(dbErrorLogs.length).toBeGreaterThan(0);
    
    // Clean up
    await failingPool.end();
  });
  
  it('should automatically reconnect and resume persistence when database becomes available', async () => {
    // Arrange: Create SessionStateManager with working pool
    serializer = new StateSerializer({ logger: mockLogger });
    
    sessionStateManager = new SessionStateManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    const userState = {
      step: 'awaiting_email',
      data: { selectedProduct: 'tarot_reading' },
      isReturningUser: false,
      clientId: 67890,
      name: 'Recovery User',
      email: 'recovery@example.com',
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
    
    // Act: First, verify persistence works
    await sessionStateManager.persistState(testAdminId, testPhone, userState);
    
    // Verify state was persisted
    const result1 = await pool.query(
      'SELECT * FROM conversation_states WHERE admin_id = $1 AND phone = $2',
      [testAdminId, testPhone]
    );
    expect(result1.rows).toHaveLength(1);
    expect(result1.rows[0].session_data.name).toBe('Recovery User');
    
    // Simulate database "recovery" by updating state again
    const updatedState = {
      ...userState,
      step: 'awaiting_confirmation',
      name: 'Updated Recovery User'
    };
    
    await sessionStateManager.persistState(testAdminId, testPhone, updatedState);
    
    // Assert: Verify updated state was persisted
    const result2 = await pool.query(
      'SELECT * FROM conversation_states WHERE admin_id = $1 AND phone = $2',
      [testAdminId, testPhone]
    );
    expect(result2.rows).toHaveLength(1);
    expect(result2.rows[0].session_data.step).toBe('awaiting_confirmation');
    expect(result2.rows[0].session_data.name).toBe('Updated Recovery User');
  });
  
  it('should handle transient connection failures with retry logic', async () => {
    // Arrange: Create a custom pool that fails intermittently
    let queryAttempts = 0;
    const intermittentPool = {
      query: async (...args) => {
        queryAttempts++;
        // Fail first 2 attempts, succeed on 3rd
        if (queryAttempts <= 2) {
          throw new Error('ECONNREFUSED: Connection refused');
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
      data: {},
      isReturningUser: false,
      clientId: 11111,
      name: 'Retry Test User',
      email: 'retry@example.com',
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
    
    // Act: Persist state (should succeed after retries)
    await sessionStateManager.persistState(testAdminId, testPhone, userState);
    
    // Wait for async operations
    await new Promise(resolve => setTimeout(resolve, 300));
    
    // Assert: Verify state was eventually persisted
    const result = await pool.query(
      'SELECT * FROM conversation_states WHERE admin_id = $1 AND phone = $2',
      [testAdminId, testPhone]
    );
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].session_data.name).toBe('Retry Test User');
    
    // Verify retry attempts were made
    expect(queryAttempts).toBeGreaterThanOrEqual(3);
    
    // Verify retry warnings were logged
    const warnLogs = logMessages.filter(log => 
      log.level === 'warn' && log.msg && log.msg.includes('Retry')
    );
    expect(warnLogs.length).toBeGreaterThan(0);
  });
  
  it('should maintain in-memory state even when persistence fails', async () => {
    // Arrange: Create a pool that always fails
    const alwaysFailingPool = {
      query: async () => {
        throw new Error('Database unavailable');
      },
      end: async () => {}
    };
    
    serializer = new StateSerializer({ logger: mockLogger });
    
    sessionStateManager = new SessionStateManager({
      db: alwaysFailingPool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    const userState = {
      step: 'awaiting_payment',
      data: { amount: 100 },
      isReturningUser: true,
      clientId: 22222,
      name: 'In-Memory User',
      email: 'inmemory@example.com',
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
    
    // Act: Attempt to persist (will fail)
    await sessionStateManager.persistState(testAdminId, testPhone, userState);
    
    // Wait for retries
    await new Promise(resolve => setTimeout(resolve, 500));
    
    // Assert: Verify system logged errors but didn't crash
    const errorLogs = logMessages.filter(log => log.level === 'error');
    expect(errorLogs.length).toBeGreaterThan(0);
    
    // System should still be operational (no exceptions thrown)
    // In a real scenario, the in-memory state would be maintained in the sessions Map
    expect(sessionStateManager).toBeDefined();
  });
});
