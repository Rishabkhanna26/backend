import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import pg from 'pg';
import { SessionStateManager } from '../../src/persistence/SessionStateManager.js';
import { StateSerializer } from '../../src/persistence/StateSerializer.js';
import { RecoveryManager } from '../../src/persistence/RecoveryManager.js';

const { Pool } = pg;

/**
 * Load Test: Session Recovery Performance
 * 
 * This test validates session recovery performance on server startup:
 * - Create 1000 active sessions in database
 * - Measure recovery time on startup
 * - Verify recovery completes in < 5 seconds
 * 
 * Requirements tested: 4.7, 11.3, 15.4
 */
describe('Load Test: Session Recovery Performance', () => {
  let pool;
  let sessionStateManager;
  let recoveryManager;
  let serializer;
  
  const testAdminId = 2000;
  const numSessions = 1000;
  
  // Mock logger
  const mockLogger = {
    info: () => {},
    warn: () => {},
    error: () => {},
    debug: () => {}
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
      password: process.env.DB_PASSWORD || 'postgres',
      max: 50,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000
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
    
    // Create indexes
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_conversation_states_admin_id 
        ON conversation_states(admin_id)
    `);
    
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_conversation_states_last_activity 
        ON conversation_states(last_activity_at)
    `);
    
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_conversation_states_admin_activity 
        ON conversation_states(admin_id, last_activity_at)
    `);
    
    // Initialize components
    serializer = new StateSerializer({ logger: mockLogger });
    
    sessionStateManager = new SessionStateManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    recoveryManager = new RecoveryManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
  });
  
  afterAll(async () => {
    await pool.end();
  });
  
  beforeEach(async () => {
    // Clean up test data
    await pool.query('DELETE FROM conversation_states WHERE admin_id = $1', [testAdminId]);
  });
  
  /**
   * Helper function to generate a realistic user state
   */
  function generateUserState(sessionId) {
    const steps = ['awaiting_name', 'awaiting_email', 'awaiting_product', 'awaiting_payment', 'awaiting_confirmation'];
    const products = ['birth_chart', 'tarot_reading', 'numerology', 'astrology_consultation'];
    const languages = ['en', 'es', 'fr', 'de'];
    
    return {
      step: steps[sessionId % steps.length],
      data: {
        sessionId,
        selectedProduct: products[sessionId % products.length],
        quantity: (sessionId % 5) + 1,
        totalAmount: ((sessionId % 10) + 1) * 25,
        appointmentDate: new Date(Date.now() + sessionId * 86400000).toISOString().split('T')[0]
      },
      isReturningUser: sessionId % 3 === 0,
      clientId: 30000 + sessionId,
      name: `User ${sessionId}`,
      email: `user${sessionId}@example.com`,
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: sessionId % 4 === 0 ? steps[sessionId % steps.length] : null,
      awaitingResumeDecision: sessionId % 5 === 0,
      lastUserMessageAt: new Date(Date.now() - (sessionId % 3600) * 1000), // Within last hour
      partialSavedAt: sessionId % 2 === 0 ? new Date(Date.now() - (sessionId % 7200) * 1000) : null,
      finalized: false,
      automationDisabled: sessionId % 10 === 0,
      aiConversationHistory: Array(Math.min(sessionId % 20, 10)).fill(null).map((_, i) => ({
        role: i % 2 === 0 ? 'user' : 'assistant',
        content: `Message ${i + 1} for session ${sessionId}`
      })),
      responseLanguage: languages[sessionId % languages.length]
    };
  }
  
  it('should recover 1000 sessions in less than 5 seconds', async () => {
    // Arrange: Create 1000 active sessions in database
    console.log('\n=== Session Recovery Performance Test ===');
    console.log(`Creating ${numSessions} sessions...`);
    
    const createStart = Date.now();
    
    // Batch insert sessions for faster setup
    const batchSize = 100;
    for (let batch = 0; batch < numSessions / batchSize; batch++) {
      const promises = [];
      
      for (let i = 0; i < batchSize; i++) {
        const sessionId = batch * batchSize + i;
        const phone = `+1${String(sessionId).padStart(9, '0')}`;
        const userState = generateUserState(sessionId);
        
        promises.push(
          sessionStateManager.persistState(testAdminId, phone, userState)
        );
      }
      
      await Promise.all(promises);
    }
    
    const createEnd = Date.now();
    console.log(`Created ${numSessions} sessions in ${createEnd - createStart}ms`);
    
    // Verify sessions were created
    const countResult = await pool.query(
      'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    expect(parseInt(countResult.rows[0].count)).toBe(numSessions);
    
    // Act: Recover all sessions (simulating server startup)
    console.log(`\nRecovering ${numSessions} sessions...`);
    const recoveryStart = Date.now();
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    const recoveryEnd = Date.now();
    
    const recoveryDuration = recoveryEnd - recoveryStart;
    const recoveredCount = Object.keys(recovered.users).length;
    const throughput = (recoveredCount / recoveryDuration) * 1000; // sessions per second
    
    // Log results
    console.log(`\nRecovery completed in ${recoveryDuration}ms`);
    console.log(`Sessions recovered: ${recoveredCount}`);
    console.log(`Throughput: ${throughput.toFixed(2)} sessions/second`);
    console.log(`Average time per session: ${(recoveryDuration / recoveredCount).toFixed(2)}ms`);
    console.log('=========================================\n');
    
    // Assert: Verify all sessions were recovered
    expect(recoveredCount).toBe(numSessions);
    
    // Assert: Verify recovery completed within 5 seconds (Requirement 4.7, 11.3)
    expect(recoveryDuration).toBeLessThan(5000);
    
    // Verify data integrity of recovered sessions
    const sampleSession = recovered.users['+1000000000'];
    expect(sampleSession).toBeDefined();
    expect(sampleSession.name).toBe('User 0');
    expect(sampleSession.email).toBe('user0@example.com');
    expect(sampleSession.clientId).toBe(30000);
    expect(sampleSession.lastUserMessageAt).toBeInstanceOf(Date);
  }, 30000); // 30 second timeout
  
  it('should handle recovery with mixed session ages', async () => {
    // Create sessions with varying last_activity_at timestamps
    const now = Date.now();
    const sessions = [];
    
    // 500 very recent sessions (within last 5 minutes)
    for (let i = 0; i < 500; i++) {
      const phone = `+2${String(i).padStart(9, '0')}`;
      const userState = generateUserState(i);
      userState.lastUserMessageAt = new Date(now - Math.random() * 300000); // 0-5 minutes ago
      sessions.push({ phone, userState });
    }
    
    // 300 recent sessions (5-30 minutes ago)
    for (let i = 500; i < 800; i++) {
      const phone = `+2${String(i).padStart(9, '0')}`;
      const userState = generateUserState(i);
      userState.lastUserMessageAt = new Date(now - (300000 + Math.random() * 1500000)); // 5-30 minutes ago
      sessions.push({ phone, userState });
    }
    
    // 200 older sessions (30-60 minutes ago)
    for (let i = 800; i < 1000; i++) {
      const phone = `+2${String(i).padStart(9, '0')}`;
      const userState = generateUserState(i);
      userState.lastUserMessageAt = new Date(now - (1800000 + Math.random() * 1800000)); // 30-60 minutes ago
      sessions.push({ phone, userState });
    }
    
    // Persist all sessions
    console.log('\n=== Mixed Session Ages Test ===');
    console.log('Creating 1000 sessions with varying ages...');
    
    const batchSize = 100;
    for (let batch = 0; batch < sessions.length / batchSize; batch++) {
      const promises = [];
      
      for (let i = 0; i < batchSize; i++) {
        const idx = batch * batchSize + i;
        const { phone, userState } = sessions[idx];
        promises.push(
          sessionStateManager.persistState(testAdminId, phone, userState)
        );
      }
      
      await Promise.all(promises);
    }
    
    // Recover sessions
    const recoveryStart = Date.now();
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    const recoveryEnd = Date.now();
    
    const recoveryDuration = recoveryEnd - recoveryStart;
    const recoveredCount = Object.keys(recovered.users).length;
    
    console.log(`Recovery completed in ${recoveryDuration}ms`);
    console.log(`Sessions recovered: ${recoveredCount}`);
    console.log('================================\n');
    
    // All sessions should be recovered (all within 6 hour TTL)
    expect(recoveredCount).toBe(1000);
    expect(recoveryDuration).toBeLessThan(5000);
  }, 30000);
  
  it('should efficiently filter expired sessions during recovery', async () => {
    // Create mix of active and expired sessions
    const now = Date.now();
    const activeCount = 500;
    const expiredCount = 500;
    
    console.log('\n=== Expired Session Filtering Test ===');
    console.log(`Creating ${activeCount} active and ${expiredCount} expired sessions...`);
    
    // Create active sessions (within TTL)
    for (let i = 0; i < activeCount; i++) {
      const phone = `+3${String(i).padStart(9, '0')}`;
      const userState = generateUserState(i);
      userState.lastUserMessageAt = new Date(now - Math.random() * 3600000); // Within last hour
      await sessionStateManager.persistState(testAdminId, phone, userState);
    }
    
    // Create expired sessions (beyond TTL) by directly inserting into database
    const expiredTimestamp = new Date(now - mockConfig.userIdleTtlMs - 3600000); // 7 hours ago
    for (let i = activeCount; i < activeCount + expiredCount; i++) {
      const phone = `+3${String(i).padStart(9, '0')}`;
      const userState = generateUserState(i);
      const serialized = serializer.serialize(userState);
      
      await pool.query(
        `INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (admin_id, phone) DO UPDATE SET
           session_data = EXCLUDED.session_data,
           last_activity_at = EXCLUDED.last_activity_at`,
        [testAdminId, phone, serialized, expiredTimestamp]
      );
    }
    
    // Verify total sessions in database
    const totalResult = await pool.query(
      'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    expect(parseInt(totalResult.rows[0].count)).toBe(activeCount + expiredCount);
    
    // Recover sessions - should only recover active ones
    const recoveryStart = Date.now();
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    const recoveryEnd = Date.now();
    
    const recoveryDuration = recoveryEnd - recoveryStart;
    const recoveredCount = Object.keys(recovered.users).length;
    
    console.log(`Recovery completed in ${recoveryDuration}ms`);
    console.log(`Total sessions in DB: ${activeCount + expiredCount}`);
    console.log(`Active sessions recovered: ${recoveredCount}`);
    console.log(`Expired sessions filtered: ${expiredCount}`);
    console.log('=======================================\n');
    
    // Should only recover active sessions
    expect(recoveredCount).toBe(activeCount);
    
    // Recovery should still be fast even with expired sessions
    expect(recoveryDuration).toBeLessThan(5000);
  }, 30000);
  
  it('should handle recovery with large conversation histories', async () => {
    // Create sessions with large AI conversation histories
    const numSessions = 500;
    
    console.log('\n=== Large Conversation Histories Test ===');
    console.log(`Creating ${numSessions} sessions with large histories...`);
    
    const promises = [];
    for (let i = 0; i < numSessions; i++) {
      const phone = `+4${String(i).padStart(9, '0')}`;
      const userState = generateUserState(i);
      
      // Add large conversation history (50-100 messages)
      const historySize = 50 + Math.floor(Math.random() * 50);
      userState.aiConversationHistory = Array(historySize).fill(null).map((_, idx) => ({
        role: idx % 2 === 0 ? 'user' : 'assistant',
        content: `Message ${idx + 1}: ${Math.random().toString(36).substring(2, 100)}`
      }));
      
      promises.push(
        sessionStateManager.persistState(testAdminId, phone, userState)
      );
    }
    
    await Promise.all(promises);
    
    // Recover sessions
    const recoveryStart = Date.now();
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    const recoveryEnd = Date.now();
    
    const recoveryDuration = recoveryEnd - recoveryStart;
    const recoveredCount = Object.keys(recovered.users).length;
    
    console.log(`Recovery completed in ${recoveryDuration}ms`);
    console.log(`Sessions recovered: ${recoveredCount}`);
    console.log(`Average time per session: ${(recoveryDuration / recoveredCount).toFixed(2)}ms`);
    console.log('==========================================\n');
    
    expect(recoveredCount).toBe(numSessions);
    expect(recoveryDuration).toBeLessThan(5000);
    
    // Verify conversation history was preserved
    const sampleSession = recovered.users['+4000000000'];
    expect(sampleSession.aiConversationHistory.length).toBeGreaterThan(50);
  }, 30000);
});
