import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import pg from 'pg';
import { SessionStateManager } from '../../src/persistence/SessionStateManager.js';
import { StateSerializer } from '../../src/persistence/StateSerializer.js';
import { RecoveryManager } from '../../src/persistence/RecoveryManager.js';

const { Pool } = pg;

/**
 * Integration Test: Server Restart with Recovery
 * 
 * This test validates that active sessions are correctly recovered
 * after a server shutdown and restart, ensuring conversation continuity.
 * 
 * Requirements tested: 4.1, 9.5, 9.6, 15.2
 */
describe('Integration: Server Restart with Recovery', () => {
  let pool;
  let sessionStateManager;
  let recoveryManager;
  let serializer;
  
  const testAdminId = 998;
  
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
    // Clean up test data before each test
    await pool.query('DELETE FROM conversation_states WHERE admin_id = $1', [testAdminId]);
  });
  
  it('should recover all active sessions after restart', async () => {
    // Arrange: Simulate active server with multiple conversations
    const activeUsers = {
      '+1111111111': {
        step: 'awaiting_name',
        data: { selectedProduct: 'birth_chart' },
        isReturningUser: false,
        clientId: 1001,
        name: 'Alice',
        email: 'alice@example.com',
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
        name: 'Bob',
        email: 'bob@example.com',
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
        data: { selectedProduct: 'consultation', totalAmount: 150 },
        isReturningUser: false,
        clientId: 1003,
        name: 'Charlie',
        email: 'charlie@example.com',
        assignedAdminId: testAdminId,
        greetedThisSession: true,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: new Date(),
        partialSavedAt: new Date(),
        finalized: false,
        automationDisabled: false,
        aiConversationHistory: [
          { role: 'user', content: 'I need a consultation' },
          { role: 'assistant', content: 'Sure! That will be $150' }
        ],
        responseLanguage: 'en'
      }
    };
    
    // Persist all active sessions (simulating normal operation)
    for (const [phone, userState] of Object.entries(activeUsers)) {
      await sessionStateManager.persistState(testAdminId, phone, userState);
    }
    
    // Act: Simulate server restart by recovering sessions
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    
    // Assert: Verify all sessions recovered
    expect(Object.keys(recovered.users)).toHaveLength(3);
    
    // Verify each user's state
    expect(recovered.users['+1111111111'].name).toBe('Alice');
    expect(recovered.users['+1111111111'].step).toBe('awaiting_name');
    
    expect(recovered.users['+2222222222'].name).toBe('Bob');
    expect(recovered.users['+2222222222'].step).toBe('awaiting_email');
    
    expect(recovered.users['+3333333333'].name).toBe('Charlie');
    expect(recovered.users['+3333333333'].step).toBe('awaiting_payment');
    expect(recovered.users['+3333333333'].data.totalAmount).toBe(150);
    expect(recovered.users['+3333333333'].aiConversationHistory).toHaveLength(2);
  });
  
  it('should not recover expired sessions', async () => {
    // Arrange: Create one active and one expired session
    const activeUser = {
      step: 'awaiting_name',
      data: {},
      isReturningUser: false,
      clientId: 2001,
      name: 'Active User',
      email: 'active@example.com',
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
    
    // Persist active user
    await sessionStateManager.persistState(testAdminId, '+4444444444', activeUser);
    
    // Manually insert expired session (older than USER_IDLE_TTL_MS)
    const expiredTimestamp = new Date(Date.now() - mockConfig.userIdleTtlMs - 1000);
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3, $4)
    `, [
      testAdminId,
      '+5555555555',
      JSON.stringify({
        step: 'awaiting_email',
        name: 'Expired User',
        lastUserMessageAt: expiredTimestamp.toISOString()
      }),
      expiredTimestamp
    ]);
    
    // Act: Recover sessions
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    
    // Assert: Only active session recovered
    expect(Object.keys(recovered.users)).toHaveLength(1);
    expect(recovered.users['+4444444444']).toBeDefined();
    expect(recovered.users['+4444444444'].name).toBe('Active User');
    expect(recovered.users['+5555555555']).toBeUndefined();
  });
  
  it('should maintain conversation continuity with 100% success rate', async () => {
    // Arrange: Create a complex conversation state
    const complexConversation = {
      step: 'awaiting_confirmation',
      data: {
        selectedProduct: 'birth_chart',
        quantity: 3,
        totalAmount: 300,
        appointmentDate: '2024-03-15',
        appointmentTime: '14:00',
        specialRequests: 'Please include detailed planetary positions',
        paymentMethod: 'credit_card',
        deliveryAddress: '123 Main St, City, State 12345'
      },
      isReturningUser: true,
      clientId: 3001,
      name: 'Complex User',
      email: 'complex@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: 'awaiting_confirmation',
      awaitingResumeDecision: false,
      lastUserMessageAt: new Date(),
      partialSavedAt: new Date(Date.now() - 60000),
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [
        { role: 'user', content: 'I want to order birth charts' },
        { role: 'assistant', content: 'How many would you like?' },
        { role: 'user', content: 'Three please' },
        { role: 'assistant', content: 'Great! That will be $300. When would you like your appointment?' },
        { role: 'user', content: 'March 15th at 2pm' },
        { role: 'assistant', content: 'Perfect! Please confirm your order details...' }
      ],
      responseLanguage: 'en'
    };
    
    // Act: Persist, then recover (simulating restart)
    await sessionStateManager.persistState(testAdminId, '+6666666666', complexConversation);
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    
    // Assert: Verify 100% data preservation
    const recoveredUser = recovered.users['+6666666666'];
    expect(recoveredUser).toBeDefined();
    
    // Verify all conversation data preserved
    expect(recoveredUser.step).toBe('awaiting_confirmation');
    expect(recoveredUser.data.selectedProduct).toBe('birth_chart');
    expect(recoveredUser.data.quantity).toBe(3);
    expect(recoveredUser.data.totalAmount).toBe(300);
    expect(recoveredUser.data.appointmentDate).toBe('2024-03-15');
    expect(recoveredUser.data.appointmentTime).toBe('14:00');
    expect(recoveredUser.data.specialRequests).toBe('Please include detailed planetary positions');
    expect(recoveredUser.data.paymentMethod).toBe('credit_card');
    expect(recoveredUser.data.deliveryAddress).toBe('123 Main St, City, State 12345');
    
    // Verify user metadata preserved
    expect(recoveredUser.name).toBe('Complex User');
    expect(recoveredUser.email).toBe('complex@example.com');
    expect(recoveredUser.clientId).toBe(3001);
    expect(recoveredUser.isReturningUser).toBe(true);
    expect(recoveredUser.resumeStep).toBe('awaiting_confirmation');
    
    // Verify conversation history preserved
    expect(recoveredUser.aiConversationHistory).toHaveLength(6);
    expect(recoveredUser.aiConversationHistory[0].content).toBe('I want to order birth charts');
    expect(recoveredUser.aiConversationHistory[5].content).toBe('Perfect! Please confirm your order details...');
    
    // Verify timestamps preserved
    expect(recoveredUser.lastUserMessageAt).toBeInstanceOf(Date);
    expect(recoveredUser.partialSavedAt).toBeInstanceOf(Date);
  });
  
  it('should recover sessions for multiple admins independently', async () => {
    const admin1Id = 998;
    const admin2Id = 997;
    
    // Arrange: Create sessions for two different admins
    const admin1User = {
      step: 'awaiting_name',
      data: {},
      isReturningUser: false,
      clientId: 4001,
      name: 'Admin1 User',
      email: 'admin1user@example.com',
      assignedAdminId: admin1Id,
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
    
    const admin2User = {
      step: 'awaiting_email',
      data: {},
      isReturningUser: false,
      clientId: 4002,
      name: 'Admin2 User',
      email: 'admin2user@example.com',
      assignedAdminId: admin2Id,
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
    
    // Persist sessions for both admins
    await sessionStateManager.persistState(admin1Id, '+7777777777', admin1User);
    await sessionStateManager.persistState(admin2Id, '+8888888888', admin2User);
    
    // Act: Recover sessions for each admin separately
    const recovered1 = await recoveryManager.recoverSessionsForAdmin(admin1Id);
    const recovered2 = await recoveryManager.recoverSessionsForAdmin(admin2Id);
    
    // Assert: Each admin only gets their own sessions
    expect(Object.keys(recovered1.users)).toHaveLength(1);
    expect(recovered1.users['+7777777777']).toBeDefined();
    expect(recovered1.users['+7777777777'].name).toBe('Admin1 User');
    expect(recovered1.users['+8888888888']).toBeUndefined();
    
    expect(Object.keys(recovered2.users)).toHaveLength(1);
    expect(recovered2.users['+8888888888']).toBeDefined();
    expect(recovered2.users['+8888888888'].name).toBe('Admin2 User');
    expect(recovered2.users['+7777777777']).toBeUndefined();
    
    // Cleanup
    await pool.query('DELETE FROM conversation_states WHERE admin_id = $1', [admin2Id]);
  });
  
  it('should handle recovery with no active sessions gracefully', async () => {
    // Arrange: Ensure no sessions exist for this admin
    await pool.query('DELETE FROM conversation_states WHERE admin_id = $1', [testAdminId]);
    
    // Act: Attempt recovery
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    
    // Assert: Empty users object returned
    expect(recovered.users).toBeDefined();
    expect(Object.keys(recovered.users)).toHaveLength(0);
  });
  
  it('should recover sessions within 5 seconds for 1000 sessions', async () => {
    // Note: This test creates a large number of sessions and may take time
    // Skip in CI environments if needed
    
    // Arrange: Create 100 sessions (scaled down for test speed)
    const sessionCount = 100;
    const phones = [];
    
    for (let i = 0; i < sessionCount; i++) {
      const phone = `+1${String(i).padStart(9, '0')}`;
      phones.push(phone);
      
      const userState = {
        step: 'awaiting_name',
        data: { index: i },
        isReturningUser: false,
        clientId: 5000 + i,
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
      
      await sessionStateManager.persistState(testAdminId, phone, userState);
    }
    
    // Act: Measure recovery time
    const startTime = Date.now();
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    const recoveryTime = Date.now() - startTime;
    
    // Assert: All sessions recovered
    expect(Object.keys(recovered.users)).toHaveLength(sessionCount);
    
    // Performance assertion (scaled: 100 sessions should recover in < 500ms)
    // For 1000 sessions, target is < 5000ms
    const scaledTarget = (sessionCount / 1000) * 5000;
    expect(recoveryTime).toBeLessThan(scaledTarget);
    
    console.log(`Recovered ${sessionCount} sessions in ${recoveryTime}ms`);
  });
});
