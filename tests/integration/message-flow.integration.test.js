import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import pg from 'pg';
import { SessionStateManager } from '../../src/persistence/SessionStateManager.js';
import { StateSerializer } from '../../src/persistence/StateSerializer.js';
import { RecoveryManager } from '../../src/persistence/RecoveryManager.js';

const { Pool } = pg;

/**
 * Integration Test: Full Message Flow with Persistence
 * 
 * This test validates the complete end-to-end flow of message processing
 * with state persistence to a real PostgreSQL database.
 * 
 * Requirements tested: 3.1, 4.1, 15.2
 */
describe('Integration: Full Message Flow with Persistence', () => {
  let pool;
  let sessionStateManager;
  let recoveryManager;
  let serializer;
  
  const testAdminId = 999;
  const testPhone = '+1234567890';
  
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
    
    // Create indexes
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_conversation_states_admin_id 
        ON conversation_states(admin_id)
    `);
    
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_conversation_states_last_activity 
        ON conversation_states(last_activity_at)
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
    // Clean up and close connection
    await pool.end();
  });
  
  beforeEach(async () => {
    // Clean up test data before each test
    await pool.query('DELETE FROM conversation_states WHERE admin_id = $1', [testAdminId]);
  });
  
  it('should persist user state after message processing', async () => {
    // Arrange: Create initial user state
    const userState = {
      step: 'awaiting_name',
      data: {
        selectedProduct: 'birth_chart',
        quantity: 1
      },
      isReturningUser: false,
      clientId: 12345,
      name: 'John Doe',
      email: 'john@example.com',
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
    
    // Act: Persist the state (simulating message processing)
    await sessionStateManager.persistState(testAdminId, testPhone, userState);
    
    // Assert: Verify state was persisted to database
    const result = await pool.query(
      'SELECT * FROM conversation_states WHERE admin_id = $1 AND phone = $2',
      [testAdminId, testPhone]
    );
    
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].admin_id).toBe(testAdminId);
    expect(result.rows[0].phone).toBe(testPhone);
    expect(result.rows[0].session_data.step).toBe('awaiting_name');
    expect(result.rows[0].session_data.name).toBe('John Doe');
    expect(result.rows[0].session_data.email).toBe('john@example.com');
  });
  
  it('should update existing state on subsequent messages', async () => {
    // Arrange: Create and persist initial state
    const initialState = {
      step: 'awaiting_name',
      data: {},
      isReturningUser: false,
      clientId: null,
      name: null,
      email: null,
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
    
    await sessionStateManager.persistState(testAdminId, testPhone, initialState);
    
    // Act: Update state (simulating second message)
    const updatedState = {
      ...initialState,
      step: 'awaiting_email',
      name: 'Jane Smith',
      lastUserMessageAt: new Date()
    };
    
    await sessionStateManager.persistState(testAdminId, testPhone, updatedState);
    
    // Assert: Verify state was updated
    const result = await pool.query(
      'SELECT * FROM conversation_states WHERE admin_id = $1 AND phone = $2',
      [testAdminId, testPhone]
    );
    
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].session_data.step).toBe('awaiting_email');
    expect(result.rows[0].session_data.name).toBe('Jane Smith');
  });
  
  it('should recover persisted state on restart', async () => {
    // Arrange: Persist multiple user states
    const user1State = {
      step: 'awaiting_name',
      data: { selectedProduct: 'birth_chart' },
      isReturningUser: false,
      clientId: 12345,
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
    };
    
    const user2State = {
      step: 'awaiting_email',
      data: { selectedProduct: 'tarot_reading' },
      isReturningUser: true,
      clientId: 67890,
      name: 'User Two',
      email: 'user2@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: false,
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: new Date(),
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [],
      responseLanguage: 'es'
    };
    
    await sessionStateManager.persistState(testAdminId, '+1111111111', user1State);
    await sessionStateManager.persistState(testAdminId, '+2222222222', user2State);
    
    // Act: Recover sessions (simulating server restart)
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    
    // Assert: Verify both sessions were recovered
    expect(Object.keys(recovered.users)).toHaveLength(2);
    expect(recovered.users['+1111111111']).toBeDefined();
    expect(recovered.users['+1111111111'].name).toBe('User One');
    expect(recovered.users['+1111111111'].step).toBe('awaiting_name');
    
    expect(recovered.users['+2222222222']).toBeDefined();
    expect(recovered.users['+2222222222'].name).toBe('User Two');
    expect(recovered.users['+2222222222'].step).toBe('awaiting_email');
    expect(recovered.users['+2222222222'].responseLanguage).toBe('es');
  });
  
  it('should maintain conversation continuity across restart', async () => {
    // Arrange: Create a conversation state
    const conversationState = {
      step: 'awaiting_payment',
      data: {
        selectedProduct: 'birth_chart',
        quantity: 2,
        totalAmount: 100,
        appointmentDate: '2024-02-15'
      },
      isReturningUser: true,
      clientId: 54321,
      name: 'Continuous User',
      email: 'continuous@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: 'awaiting_payment',
      awaitingResumeDecision: false,
      lastUserMessageAt: new Date(),
      partialSavedAt: new Date(),
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [
        { role: 'user', content: 'I want a birth chart' },
        { role: 'assistant', content: 'Great! How many would you like?' }
      ],
      responseLanguage: 'en'
    };
    
    // Act: Persist state, then recover it
    await sessionStateManager.persistState(testAdminId, testPhone, conversationState);
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    
    // Assert: Verify all conversation details preserved
    const recoveredUser = recovered.users[testPhone];
    expect(recoveredUser).toBeDefined();
    expect(recoveredUser.step).toBe('awaiting_payment');
    expect(recoveredUser.data.selectedProduct).toBe('birth_chart');
    expect(recoveredUser.data.quantity).toBe(2);
    expect(recoveredUser.data.totalAmount).toBe(100);
    expect(recoveredUser.aiConversationHistory).toHaveLength(2);
    expect(recoveredUser.aiConversationHistory[0].content).toBe('I want a birth chart');
    expect(recoveredUser.resumeStep).toBe('awaiting_payment');
  });
  
  it('should handle Date objects correctly through persistence cycle', async () => {
    // Arrange: Create state with Date objects
    const now = new Date('2024-01-15T10:30:00.000Z');
    const earlier = new Date('2024-01-15T10:00:00.000Z');
    
    const stateWithDates = {
      step: 'awaiting_confirmation',
      data: {},
      isReturningUser: false,
      clientId: 99999,
      name: 'Date Test User',
      email: 'datetest@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: now,
      partialSavedAt: earlier,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [],
      responseLanguage: 'en'
    };
    
    // Act: Persist and recover
    await sessionStateManager.persistState(testAdminId, testPhone, stateWithDates);
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    
    // Assert: Verify Date objects restored correctly
    const recoveredUser = recovered.users[testPhone];
    expect(recoveredUser.lastUserMessageAt).toBeInstanceOf(Date);
    expect(recoveredUser.lastUserMessageAt.toISOString()).toBe(now.toISOString());
    expect(recoveredUser.partialSavedAt).toBeInstanceOf(Date);
    expect(recoveredUser.partialSavedAt.toISOString()).toBe(earlier.toISOString());
  });
  
  it('should update last_activity_at timestamp on each persist', async () => {
    // Arrange: Create initial state
    const userState = {
      step: 'awaiting_name',
      data: {},
      isReturningUser: false,
      clientId: null,
      name: null,
      email: null,
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
    
    // Act: Persist state
    await sessionStateManager.persistState(testAdminId, testPhone, userState);
    
    // Get initial timestamp
    const result1 = await pool.query(
      'SELECT last_activity_at FROM conversation_states WHERE admin_id = $1 AND phone = $2',
      [testAdminId, testPhone]
    );
    const firstTimestamp = result1.rows[0].last_activity_at;
    
    // Wait a bit and persist again
    await new Promise(resolve => setTimeout(resolve, 100));
    await sessionStateManager.persistState(testAdminId, testPhone, userState);
    
    // Get updated timestamp
    const result2 = await pool.query(
      'SELECT last_activity_at FROM conversation_states WHERE admin_id = $1 AND phone = $2',
      [testAdminId, testPhone]
    );
    const secondTimestamp = result2.rows[0].last_activity_at;
    
    // Assert: Verify timestamp was updated
    expect(new Date(secondTimestamp).getTime()).toBeGreaterThan(new Date(firstTimestamp).getTime());
  });
});
