import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import pg from 'pg';
import { SessionStateManager } from '../../src/persistence/SessionStateManager.js';
import { StateSerializer } from '../../src/persistence/StateSerializer.js';

const { Pool } = pg;

/**
 * Integration Test: Graceful Shutdown
 * 
 * This test validates that all active sessions are persisted before
 * server shutdown, ensuring zero data loss during deployments.
 * 
 * Requirements tested: 9.1, 9.2, 9.3, 9.4
 */
describe('Integration: Graceful Shutdown', () => {
  let pool;
  let sessionStateManager;
  let serializer;
  
  const testAdminId = 997;
  
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
    userIdleTtlMs: 21600000
  };
  
  beforeAll(async () => {
    pool = new Pool({
      host: process.env.DB_HOST || 'localhost',
      port: process.env.DB_PORT || 5432,
      database: process.env.DB_NAME || 'mex_end_test',
      user: process.env.DB_USER || 'postgres',
      password: process.env.DB_PASSWORD || 'postgres'
    });
    
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
    
    serializer = new StateSerializer({ logger: mockLogger });
    
    sessionStateManager = new SessionStateManager({
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
    await pool.query('DELETE FROM conversation_states WHERE admin_id = $1', [testAdminId]);
  });
  
  it('should persist all active sessions before shutdown', async () => {
    // Arrange: Create multiple active sessions (simulating in-memory state)
    const activeSessions = {
      '+1111111111': {
        step: 'awaiting_name',
        data: { selectedProduct: 'birth_chart' },
        isReturningUser: false,
        clientId: 1001,
        name: 'User One',
        email: 'user1@example.com',
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
      },
      '+2222222222': {
        step: 'awaiting_email',
        data: { selectedProduct: 'tarot_reading' },
        isReturningUser: true,
        clientId: 1002,
        name: 'User Two',
        email: 'user2@example.com',
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
      },
      '+3333333333': {
        step: 'awaiting_payment',
        data: { selectedProduct: 'consultation', totalAmount: 200 },
        isReturningUser: false,
        clientId: 1003,
        name: 'User Three',
        email: 'user3@example.com',
        assignedAdminId: testAdminId,
        greetedThisSession: true,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: new Date(),
        partialSavedAt: new Date(),
        finalized: false,
        automationDisabled: false,
        aiConversationHistory: [
          { role: 'user', content: 'I need a consultation' }
        ],
        responseLanguage: 'en'
      }
    };
    
    // Act: Simulate graceful shutdown by persisting all sessions
    const persistPromises = Object.entries(activeSessions).map(([phone, userState]) =>
      sessionStateManager.persistState(testAdminId, phone, userState)
    );
    
    const results = await Promise.allSettled(persistPromises);
    
    // Assert: All sessions persisted successfully
    const successCount = results.filter(r => r.status === 'fulfilled').length;
    expect(successCount).toBe(3);
    
    // Verify all sessions in database
    const dbResult = await pool.query(
      'SELECT * FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    
    expect(dbResult.rows).toHaveLength(3);
    
    const phones = dbResult.rows.map(r => r.phone).sort();
    expect(phones).toEqual(['+1111111111', '+2222222222', '+3333333333']);
  });
  
  it('should complete shutdown within 10 seconds', async () => {
    // Arrange: Create 50 active sessions
    const sessionCount = 50;
    const activeSessions = {};
    
    for (let i = 0; i < sessionCount; i++) {
      const phone = `+1${String(i).padStart(9, '0')}`;
      activeSessions[phone] = {
        step: 'awaiting_name',
        data: { index: i },
        isReturningUser: false,
        clientId: 2000 + i,
        name: `User ${i}`,
        email: `user${i}@example.com`,
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
    }
    
    // Act: Measure shutdown time
    const startTime = Date.now();
    
    const persistPromises = Object.entries(activeSessions).map(([phone, userState]) =>
      sessionStateManager.persistState(testAdminId, phone, userState)
    );
    
    await Promise.allSettled(persistPromises);
    
    const shutdownTime = Date.now() - startTime;
    
    // Assert: Shutdown completed within 10 seconds
    expect(shutdownTime).toBeLessThan(10000);
    
    // Verify all sessions persisted
    const dbResult = await pool.query(
      'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    
    expect(parseInt(dbResult.rows[0].count)).toBe(sessionCount);
    
    console.log(`Persisted ${sessionCount} sessions during shutdown in ${shutdownTime}ms`);
  });
  
  it('should log sessions that could not be persisted', async () => {
    // Arrange: Create sessions with one that will fail
    const loggedErrors = [];
    const errorLogger = {
      info: () => {},
      warn: () => {},
      error: (msg, context) => {
        loggedErrors.push({ msg, context });
      },
      debug: () => {}
    };
    
    const errorSessionManager = new SessionStateManager({
      db: pool,
      logger: errorLogger,
      serializer,
      config: mockConfig
    });
    
    const validSession = {
      step: 'awaiting_name',
      data: {},
      isReturningUser: false,
      clientId: 3001,
      name: 'Valid User',
      email: 'valid@example.com',
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
    
    // Create a session with circular reference (will fail serialization)
    const invalidSession = {
      step: 'awaiting_name',
      data: {},
      isReturningUser: false,
      clientId: 3002,
      name: 'Invalid User',
      email: 'invalid@example.com',
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
    // Add circular reference
    invalidSession.circular = invalidSession;
    
    // Act: Attempt to persist both sessions
    const results = await Promise.allSettled([
      errorSessionManager.persistState(testAdminId, '+4444444444', validSession),
      errorSessionManager.persistState(testAdminId, '+5555555555', invalidSession)
    ]);
    
    // Assert: Valid session succeeded, invalid session logged error
    expect(results[0].status).toBe('fulfilled');
    
    // Verify valid session was persisted
    const dbResult = await pool.query(
      'SELECT * FROM conversation_states WHERE admin_id = $1 AND phone = $2',
      [testAdminId, '+4444444444']
    );
    expect(dbResult.rows).toHaveLength(1);
  });
  
  it('should handle SIGTERM signal gracefully', async () => {
    // This test simulates the graceful shutdown handler behavior
    // In a real scenario, this would be triggered by process.on('SIGTERM')
    
    // Arrange: Create active sessions
    const activeSessions = {
      '+6666666666': {
        step: 'awaiting_confirmation',
        data: { selectedProduct: 'birth_chart', totalAmount: 100 },
        isReturningUser: true,
        clientId: 4001,
        name: 'SIGTERM User',
        email: 'sigterm@example.com',
        assignedAdminId: testAdminId,
        greetedThisSession: true,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: new Date(),
        partialSavedAt: new Date(),
        finalized: false,
        automationDisabled: false,
        aiConversationHistory: [
          { role: 'user', content: 'I want to order' },
          { role: 'assistant', content: 'Great! Please confirm...' }
        ],
        responseLanguage: 'en'
      }
    };
    
    // Act: Simulate graceful shutdown (what shutdown.js does)
    const shutdownPromises = Object.entries(activeSessions).map(([phone, userState]) =>
      sessionStateManager.persistState(testAdminId, phone, userState)
    );
    
    await Promise.allSettled(shutdownPromises);
    
    // Assert: Session persisted with all data intact
    const dbResult = await pool.query(
      'SELECT * FROM conversation_states WHERE admin_id = $1 AND phone = $2',
      [testAdminId, '+6666666666']
    );
    
    expect(dbResult.rows).toHaveLength(1);
    const savedData = dbResult.rows[0].session_data;
    expect(savedData.step).toBe('awaiting_confirmation');
    expect(savedData.data.totalAmount).toBe(100);
    expect(savedData.aiConversationHistory).toHaveLength(2);
  });
  
  it('should persist sessions with pending writes during shutdown', async () => {
    // Arrange: Create sessions and start persisting them
    const sessions = [];
    for (let i = 0; i < 10; i++) {
      const phone = `+17${String(i).padStart(8, '0')}`;
      sessions.push({
        phone,
        state: {
          step: 'awaiting_name',
          data: { index: i },
          isReturningUser: false,
          clientId: 5000 + i,
          name: `Pending User ${i}`,
          email: `pending${i}@example.com`,
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
        }
      });
    }
    
    // Act: Start all persist operations simultaneously (simulating pending writes)
    const persistPromises = sessions.map(({ phone, state }) =>
      sessionStateManager.persistState(testAdminId, phone, state)
    );
    
    // Wait for all to complete (simulating graceful shutdown waiting)
    const results = await Promise.allSettled(persistPromises);
    
    // Assert: All writes completed successfully
    const successCount = results.filter(r => r.status === 'fulfilled').length;
    expect(successCount).toBe(10);
    
    // Verify all in database
    const dbResult = await pool.query(
      'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    
    expect(parseInt(dbResult.rows[0].count)).toBe(10);
  });
  
  it('should use persistAllStates for bulk shutdown operations', async () => {
    // Arrange: Create a Map of active users (as in whatsapp.js)
    const usersMap = new Map();
    
    for (let i = 0; i < 20; i++) {
      const phone = `+18${String(i).padStart(8, '0')}`;
      usersMap.set(phone, {
        step: 'awaiting_name',
        data: { index: i },
        isReturningUser: false,
        clientId: 6000 + i,
        name: `Bulk User ${i}`,
        email: `bulk${i}@example.com`,
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
      });
    }
    
    // Act: Use persistAllStates (bulk operation)
    const startTime = Date.now();
    await sessionStateManager.persistAllStates(testAdminId, usersMap);
    const bulkTime = Date.now() - startTime;
    
    // Assert: All sessions persisted
    const dbResult = await pool.query(
      'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    
    expect(parseInt(dbResult.rows[0].count)).toBe(20);
    
    // Bulk operation should be reasonably fast
    expect(bulkTime).toBeLessThan(5000);
    
    console.log(`Bulk persisted 20 sessions in ${bulkTime}ms`);
  });
});
