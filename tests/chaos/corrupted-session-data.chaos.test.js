import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import pg from 'pg';
import { RecoveryManager } from '../../src/persistence/RecoveryManager.js';
import { StateSerializer } from '../../src/persistence/StateSerializer.js';

const { Pool } = pg;

/**
 * Chaos Test: Corrupted Session Data
 * 
 * This test simulates corrupted session data in the database
 * and validates that:
 * 1. Recovery skips corrupted sessions without crashing
 * 2. Valid sessions are still recovered
 * 3. Errors are logged with appropriate context
 * 4. System continues operating despite data corruption
 * 
 * Requirements tested: 4.6, 14.2, 15.7
 * Validates: Property 8 - Recovery skips corrupted sessions
 */
describe('Chaos: Corrupted Session Data', () => {
  let pool;
  let recoveryManager;
  let serializer;
  
  const testAdminId = 999;
  
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
    retryDelayMs: 100,
    batchWindowMs: 500,
    circuitBreakerThreshold: 10,
    circuitBreakerResetMs: 60000,
    userIdleTtlMs: 21600000 // 6 hours
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
  
  it('should skip corrupted JSON and continue recovery', async () => {
    // Arrange: Insert sessions with corrupted JSON
    serializer = new StateSerializer({ logger: mockLogger });
    
    recoveryManager = new RecoveryManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    // Insert valid session
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3, NOW())
    `, [testAdminId, '+1111111111', JSON.stringify({
      step: 'awaiting_name',
      data: {},
      isReturningUser: false,
      clientId: 12345,
      name: 'Valid User',
      email: 'valid@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: { __type: 'Date', value: new Date().toISOString() },
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [],
      responseLanguage: 'en'
    })]);
    
    // Insert corrupted session (invalid JSON structure)
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3::jsonb, NOW())
    `, [testAdminId, '+2222222222', '{"invalid": "missing required fields"}']);
    
    // Insert another valid session
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3, NOW())
    `, [testAdminId, '+3333333333', JSON.stringify({
      step: 'awaiting_email',
      data: { product: 'tarot' },
      isReturningUser: true,
      clientId: 67890,
      name: 'Another Valid User',
      email: 'valid2@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: { __type: 'Date', value: new Date().toISOString() },
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [],
      responseLanguage: 'en'
    })]);
    
    // Act: Attempt recovery
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    const stats = recoveryManager.getRecoveryStats();
    
    // Assert: Valid sessions should be recovered, corrupted one skipped
    expect(Object.keys(recovered.users)).toHaveLength(2);
    expect(recovered.users['+1111111111']).toBeDefined();
    expect(recovered.users['+1111111111'].name).toBe('Valid User');
    expect(recovered.users['+3333333333']).toBeDefined();
    expect(recovered.users['+3333333333'].name).toBe('Another Valid User');
    expect(recovered.users['+2222222222']).toBeUndefined();
    
    // Verify stats show failed recovery
    expect(stats.totalRecovered).toBe(2);
    expect(stats.failedRecoveries).toBe(1);
    
    // Verify error was logged
    const errorLogs = logMessages.filter(log => 
      log.level === 'error' && log.msg && (
        log.msg.includes('Failed to recover') || 
        log.msg.includes('corrupted') ||
        log.msg.includes('deserialization')
      )
    );
    expect(errorLogs.length).toBeGreaterThan(0);
  });
  
  it('should handle malformed JSONB data', async () => {
    // Arrange: Insert session with malformed nested data
    serializer = new StateSerializer({ logger: mockLogger });
    
    recoveryManager = new RecoveryManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    // Insert session with deeply nested circular-like structure (simulated)
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3::jsonb, NOW())
    `, [testAdminId, '+4444444444', JSON.stringify({
      step: 'awaiting_name',
      data: {
        nested: {
          level1: {
            level2: {
              level3: {
                level4: {
                  level5: 'too deep'
                }
              }
            }
          }
        }
      },
      // Missing required fields
      isReturningUser: false
    })]);
    
    // Insert valid session for comparison
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3, NOW())
    `, [testAdminId, '+5555555555', JSON.stringify({
      step: 'awaiting_confirmation',
      data: {},
      isReturningUser: false,
      clientId: 11111,
      name: 'Good User',
      email: 'good@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: { __type: 'Date', value: new Date().toISOString() },
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [],
      responseLanguage: 'en'
    })]);
    
    // Act: Attempt recovery
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    
    // Assert: Valid session recovered, malformed one skipped
    expect(recovered.users['+5555555555']).toBeDefined();
    expect(recovered.users['+5555555555'].name).toBe('Good User');
    
    // Malformed session may or may not be recovered depending on validation
    // But system should not crash
    expect(recovered).toBeDefined();
  });
  
  it('should handle sessions with invalid Date objects', async () => {
    // Arrange: Insert session with invalid date strings
    serializer = new StateSerializer({ logger: mockLogger });
    
    recoveryManager = new RecoveryManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    // Insert session with invalid date
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3::jsonb, NOW())
    `, [testAdminId, '+6666666666', JSON.stringify({
      step: 'awaiting_name',
      data: {},
      isReturningUser: false,
      clientId: 22222,
      name: 'Invalid Date User',
      email: 'invaliddate@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: { __type: 'Date', value: 'not-a-valid-date' },
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [],
      responseLanguage: 'en'
    })]);
    
    // Insert valid session
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3, NOW())
    `, [testAdminId, '+7777777777', JSON.stringify({
      step: 'awaiting_payment',
      data: {},
      isReturningUser: false,
      clientId: 33333,
      name: 'Valid Date User',
      email: 'validdate@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: { __type: 'Date', value: new Date().toISOString() },
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [],
      responseLanguage: 'en'
    })]);
    
    // Act: Attempt recovery
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    
    // Assert: System should handle gracefully
    expect(recovered).toBeDefined();
    expect(recovered.users['+7777777777']).toBeDefined();
    expect(recovered.users['+7777777777'].name).toBe('Valid Date User');
    
    // Invalid date session may be skipped or recovered with null date
    // Either way, system should not crash
  });
  
  it('should handle sessions with missing required fields', async () => {
    // Arrange: Insert sessions missing various required fields
    serializer = new StateSerializer({ logger: mockLogger });
    
    recoveryManager = new RecoveryManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    // Missing 'step' field
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3::jsonb, NOW())
    `, [testAdminId, '+8888888881', JSON.stringify({
      data: {},
      isReturningUser: false,
      name: 'Missing Step'
    })]);
    
    // Missing 'data' field
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3::jsonb, NOW())
    `, [testAdminId, '+8888888882', JSON.stringify({
      step: 'awaiting_name',
      isReturningUser: false,
      name: 'Missing Data'
    })]);
    
    // Complete valid session
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3, NOW())
    `, [testAdminId, '+8888888883', JSON.stringify({
      step: 'awaiting_email',
      data: { complete: true },
      isReturningUser: false,
      clientId: 44444,
      name: 'Complete User',
      email: 'complete@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: { __type: 'Date', value: new Date().toISOString() },
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [],
      responseLanguage: 'en'
    })]);
    
    // Act: Attempt recovery
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    const stats = recoveryManager.getRecoveryStats();
    
    // Assert: Complete session should be recovered
    expect(recovered.users['+8888888883']).toBeDefined();
    expect(recovered.users['+8888888883'].name).toBe('Complete User');
    
    // Incomplete sessions should be skipped
    expect(stats.failedRecoveries).toBeGreaterThan(0);
    
    // Errors should be logged
    const errorLogs = logMessages.filter(log => log.level === 'error');
    expect(errorLogs.length).toBeGreaterThan(0);
  });
  
  it('should handle sessions with type mismatches', async () => {
    // Arrange: Insert sessions with wrong data types
    serializer = new StateSerializer({ logger: mockLogger });
    
    recoveryManager = new RecoveryManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    // Insert session with wrong types
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3::jsonb, NOW())
    `, [testAdminId, '+9999999991', JSON.stringify({
      step: 123, // Should be string
      data: 'not an object', // Should be object
      isReturningUser: 'yes', // Should be boolean
      clientId: 'not-a-number', // Should be number
      name: ['array', 'not', 'string'], // Should be string
      email: null,
      assignedAdminId: testAdminId,
      greetedThisSession: 'true', // Should be boolean
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: { __type: 'Date', value: new Date().toISOString() },
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: 'not an array', // Should be array
      responseLanguage: 'en'
    })]);
    
    // Insert valid session
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3, NOW())
    `, [testAdminId, '+9999999992', JSON.stringify({
      step: 'awaiting_name',
      data: { correct: 'types' },
      isReturningUser: false,
      clientId: 55555,
      name: 'Type Safe User',
      email: 'typesafe@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: { __type: 'Date', value: new Date().toISOString() },
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [],
      responseLanguage: 'en'
    })]);
    
    // Act: Attempt recovery
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    
    // Assert: Valid session should be recovered
    expect(recovered.users['+9999999992']).toBeDefined();
    expect(recovered.users['+9999999992'].name).toBe('Type Safe User');
    
    // Type mismatch session may be skipped or recovered with coercion
    // System should not crash
    expect(recovered).toBeDefined();
  });
  
  it('should continue operating after encountering multiple corrupted sessions', async () => {
    // Arrange: Insert mix of valid and corrupted sessions
    serializer = new StateSerializer({ logger: mockLogger });
    
    recoveryManager = new RecoveryManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    // Insert 10 sessions: 5 valid, 5 corrupted
    for (let i = 0; i < 10; i++) {
      const phone = `+100000000${i}`;
      
      if (i % 2 === 0) {
        // Valid session
        await pool.query(`
          INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
          VALUES ($1, $2, $3, NOW())
        `, [testAdminId, phone, JSON.stringify({
          step: 'awaiting_name',
          data: { index: i },
          isReturningUser: false,
          clientId: 60000 + i,
          name: `Valid User ${i}`,
          email: `valid${i}@example.com`,
          assignedAdminId: testAdminId,
          greetedThisSession: true,
          resumeStep: null,
          awaitingResumeDecision: false,
          lastUserMessageAt: { __type: 'Date', value: new Date().toISOString() },
          partialSavedAt: null,
          finalized: false,
          automationDisabled: false,
          aiConversationHistory: [],
          responseLanguage: 'en'
        })]);
      } else {
        // Corrupted session (missing required fields)
        await pool.query(`
          INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
          VALUES ($1, $2, $3::jsonb, NOW())
        `, [testAdminId, phone, JSON.stringify({
          corrupted: true,
          index: i
        })]);
      }
    }
    
    // Act: Attempt recovery
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    const stats = recoveryManager.getRecoveryStats();
    
    // Assert: All valid sessions should be recovered
    expect(stats.totalRecovered).toBe(5);
    expect(stats.failedRecoveries).toBe(5);
    
    // Verify valid sessions are present
    expect(recovered.users['+1000000000']).toBeDefined();
    expect(recovered.users['+1000000002']).toBeDefined();
    expect(recovered.users['+1000000004']).toBeDefined();
    expect(recovered.users['+1000000006']).toBeDefined();
    expect(recovered.users['+1000000008']).toBeDefined();
    
    // Verify corrupted sessions are not present
    expect(recovered.users['+1000000001']).toBeUndefined();
    expect(recovered.users['+1000000003']).toBeUndefined();
    
    // System should still be operational
    expect(recovered).toBeDefined();
  });
  
  it('should log detailed error context for corrupted sessions', async () => {
    // Arrange: Insert corrupted session
    serializer = new StateSerializer({ logger: mockLogger });
    
    recoveryManager = new RecoveryManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    const corruptedPhone = '+1234567890';
    
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3::jsonb, NOW())
    `, [testAdminId, corruptedPhone, '{"broken": "data"}']);
    
    // Act: Attempt recovery
    await recoveryManager.recoverSessionsForAdmin(testAdminId);
    
    // Assert: Error log should contain context
    const errorLogs = logMessages.filter(log => log.level === 'error');
    expect(errorLogs.length).toBeGreaterThan(0);
    
    // Verify error log contains relevant information
    const relevantError = errorLogs.find(log => 
      log.meta && (
        log.meta.phone === corruptedPhone ||
        log.meta.adminId === testAdminId
      )
    );
    
    expect(relevantError).toBeDefined();
  });
  
  it('should handle empty session_data gracefully', async () => {
    // Arrange: Insert session with empty object
    serializer = new StateSerializer({ logger: mockLogger });
    
    recoveryManager = new RecoveryManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3::jsonb, NOW())
    `, [testAdminId, '+0000000000', '{}']);
    
    // Act: Attempt recovery
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    
    // Assert: Should handle gracefully (skip or recover with defaults)
    expect(recovered).toBeDefined();
    
    // Empty session should be skipped
    const stats = recoveryManager.getRecoveryStats();
    expect(stats.failedRecoveries).toBeGreaterThan(0);
  });
});
