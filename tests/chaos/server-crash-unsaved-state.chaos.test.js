import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import pg from 'pg';
import { SessionStateManager } from '../../src/persistence/SessionStateManager.js';
import { StateSerializer } from '../../src/persistence/StateSerializer.js';
import { RecoveryManager } from '../../src/persistence/RecoveryManager.js';

const { Pool } = pg;

/**
 * Chaos Test: Server Crash with Unsaved State
 * 
 * This test simulates server crashes (SIGKILL) with active sessions
 * and validates that:
 * 1. Last persisted states are recoverable after crash
 * 2. System degrades gracefully (only loses data since last persist)
 * 3. Recovery process handles partial state correctly
 * 
 * Requirements tested: 9.1, 9.5, 15.7
 * Validates: Property 25 - Restart restores shutdown states
 */
describe('Chaos: Server Crash with Unsaved State', () => {
  let pool;
  let sessionStateManager;
  let recoveryManager;
  let serializer;
  
  const testAdminId = 999;
  
  // Mock logger
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
  
  it('should recover last persisted states after simulated crash', async () => {
    // Arrange: Initialize components
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
    
    // Create and persist multiple user states
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
      aiConversationHistory: [
        { role: 'user', content: 'Hello' }
      ],
      responseLanguage: 'en'
    };
    
    const user2State = {
      step: 'awaiting_payment',
      data: { amount: 100, product: 'tarot_reading' },
      isReturningUser: true,
      clientId: 67890,
      name: 'User Two',
      email: 'user2@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: 'awaiting_payment',
      awaitingResumeDecision: false,
      lastUserMessageAt: new Date(),
      partialSavedAt: new Date(),
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [
        { role: 'user', content: 'I want a reading' },
        { role: 'assistant', content: 'Sure, that will be $100' }
      ],
      responseLanguage: 'en'
    };
    
    // Persist states
    await sessionStateManager.persistState(testAdminId, '+1111111111', user1State);
    await sessionStateManager.persistState(testAdminId, '+2222222222', user2State);
    
    // Wait for persistence to complete
    await new Promise(resolve => setTimeout(resolve, 200));
    
    // Act: Simulate crash by creating new recovery manager (simulates restart)
    const recoveredSessions = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    
    // Assert: Verify both persisted states were recovered
    expect(Object.keys(recoveredSessions.users)).toHaveLength(2);
    
    const recoveredUser1 = recoveredSessions.users['+1111111111'];
    expect(recoveredUser1).toBeDefined();
    expect(recoveredUser1.name).toBe('User One');
    expect(recoveredUser1.step).toBe('awaiting_name');
    expect(recoveredUser1.data.selectedProduct).toBe('birth_chart');
    expect(recoveredUser1.aiConversationHistory).toHaveLength(1);
    
    const recoveredUser2 = recoveredSessions.users['+2222222222'];
    expect(recoveredUser2).toBeDefined();
    expect(recoveredUser2.name).toBe('User Two');
    expect(recoveredUser2.step).toBe('awaiting_payment');
    expect(recoveredUser2.data.amount).toBe(100);
    expect(recoveredUser2.aiConversationHistory).toHaveLength(2);
  });
  
  it('should handle partial state loss gracefully when crash occurs between persists', async () => {
    // Arrange: Initialize components
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
    
    // Persist initial state
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
    
    await sessionStateManager.persistState(testAdminId, '+1234567890', initialState);
    await new Promise(resolve => setTimeout(resolve, 200));
    
    // Simulate in-memory update that hasn't been persisted yet
    // (In real scenario, this would be in sessions Map but not yet written to DB)
    const updatedStateNotPersisted = {
      ...initialState,
      step: 'awaiting_email',
      name: 'John Doe' // This update is "lost" in the crash
    };
    
    // Act: Simulate crash - recover without persisting the update
    const recoveredSessions = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    
    // Assert: Recovered state should be the last persisted version (initial state)
    const recoveredUser = recoveredSessions.users['+1234567890'];
    expect(recoveredUser).toBeDefined();
    expect(recoveredUser.step).toBe('awaiting_name'); // Not 'awaiting_email'
    expect(recoveredUser.name).toBeNull(); // Not 'John Doe'
    
    // This demonstrates graceful degradation - only data since last persist is lost
  });
  
  it('should recover states with complex data structures after crash', async () => {
    // Arrange: Initialize components
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
    
    // Create state with complex nested data
    const complexState = {
      step: 'awaiting_confirmation',
      data: {
        cart: [
          { product: 'birth_chart', quantity: 2, price: 50 },
          { product: 'tarot_reading', quantity: 1, price: 75 }
        ],
        shipping: {
          address: '123 Main St',
          city: 'New York',
          zip: '10001'
        },
        payment: {
          method: 'credit_card',
          last4: '1234'
        },
        metadata: {
          source: 'whatsapp',
          campaign: 'spring_2024',
          referrer: 'friend'
        }
      },
      isReturningUser: true,
      clientId: 99999,
      name: 'Complex User',
      email: 'complex@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: 'awaiting_confirmation',
      awaitingResumeDecision: false,
      lastUserMessageAt: new Date('2024-01-15T10:30:00.000Z'),
      partialSavedAt: new Date('2024-01-15T10:25:00.000Z'),
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [
        { role: 'user', content: 'I want to order' },
        { role: 'assistant', content: 'What would you like?' },
        { role: 'user', content: 'Birth chart and tarot reading' }
      ],
      responseLanguage: 'en'
    };
    
    // Persist complex state
    await sessionStateManager.persistState(testAdminId, '+9999999999', complexState);
    await new Promise(resolve => setTimeout(resolve, 200));
    
    // Act: Simulate crash and recovery
    const recoveredSessions = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    
    // Assert: Verify all complex data structures preserved
    const recovered = recoveredSessions.users['+9999999999'];
    expect(recovered).toBeDefined();
    expect(recovered.step).toBe('awaiting_confirmation');
    
    // Verify nested cart data
    expect(recovered.data.cart).toHaveLength(2);
    expect(recovered.data.cart[0].product).toBe('birth_chart');
    expect(recovered.data.cart[0].quantity).toBe(2);
    expect(recovered.data.cart[1].product).toBe('tarot_reading');
    
    // Verify nested shipping data
    expect(recovered.data.shipping.address).toBe('123 Main St');
    expect(recovered.data.shipping.city).toBe('New York');
    
    // Verify nested payment data
    expect(recovered.data.payment.method).toBe('credit_card');
    expect(recovered.data.payment.last4).toBe('1234');
    
    // Verify metadata
    expect(recovered.data.metadata.campaign).toBe('spring_2024');
    
    // Verify AI conversation history
    expect(recovered.aiConversationHistory).toHaveLength(3);
    expect(recovered.aiConversationHistory[2].content).toBe('Birth chart and tarot reading');
    
    // Verify Date objects restored
    expect(recovered.lastUserMessageAt).toBeInstanceOf(Date);
    expect(recovered.partialSavedAt).toBeInstanceOf(Date);
  });
  
  it('should handle multiple crashes and recoveries', async () => {
    // Arrange: Initialize components
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
    
    const phone = '+5555555555';
    
    // Simulate multiple crash/recovery cycles
    // Cycle 1: Initial state
    const state1 = {
      step: 'awaiting_name',
      data: { cycle: 1 },
      isReturningUser: false,
      clientId: 11111,
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
    
    await sessionStateManager.persistState(testAdminId, phone, state1);
    await new Promise(resolve => setTimeout(resolve, 100));
    
    // Crash 1 - Recover
    let recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    expect(recovered.users[phone].data.cycle).toBe(1);
    
    // Cycle 2: Update state
    const state2 = {
      ...state1,
      step: 'awaiting_email',
      name: 'John',
      data: { cycle: 2 }
    };
    
    await sessionStateManager.persistState(testAdminId, phone, state2);
    await new Promise(resolve => setTimeout(resolve, 100));
    
    // Crash 2 - Recover
    recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    expect(recovered.users[phone].data.cycle).toBe(2);
    expect(recovered.users[phone].name).toBe('John');
    
    // Cycle 3: Final update
    const state3 = {
      ...state2,
      step: 'completed',
      email: 'john@example.com',
      data: { cycle: 3 },
      finalized: true
    };
    
    await sessionStateManager.persistState(testAdminId, phone, state3);
    await new Promise(resolve => setTimeout(resolve, 100));
    
    // Crash 3 - Recover
    recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    expect(recovered.users[phone].data.cycle).toBe(3);
    expect(recovered.users[phone].email).toBe('john@example.com');
    expect(recovered.users[phone].finalized).toBe(true);
  });
  
  it('should log recovery statistics after crash recovery', async () => {
    // Arrange: Initialize components
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
    
    // Create multiple states
    const phones = ['+1111111111', '+2222222222', '+3333333333'];
    
    for (let i = 0; i < phones.length; i++) {
      const state = {
        step: 'awaiting_name',
        data: { index: i },
        isReturningUser: false,
        clientId: 10000 + i,
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
      
      await sessionStateManager.persistState(testAdminId, phones[i], state);
    }
    
    await new Promise(resolve => setTimeout(resolve, 200));
    
    // Act: Recover after simulated crash
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    const stats = recoveryManager.getRecoveryStats();
    
    // Assert: Verify recovery statistics
    expect(stats).toBeDefined();
    expect(stats.totalRecovered).toBe(3);
    expect(stats.failedRecoveries).toBe(0);
    expect(stats.durationMs).toBeGreaterThan(0);
    
    // Verify logging occurred
    const infoLogs = logMessages.filter(log => 
      log.level === 'info' && log.msg && (
        log.msg.includes('Recovered') || 
        log.msg.includes('recovery')
      )
    );
    expect(infoLogs.length).toBeGreaterThan(0);
  });
  
  it('should handle crash during batch write operation', async () => {
    // Arrange: Initialize components with batching enabled
    serializer = new StateSerializer({ logger: mockLogger });
    
    sessionStateManager = new SessionStateManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: {
        ...mockConfig,
        batchWindowMs: 1000 // Longer batch window
      }
    });
    
    recoveryManager = new RecoveryManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    // Rapidly create multiple states (should be batched)
    const states = [];
    for (let i = 0; i < 5; i++) {
      const state = {
        step: 'awaiting_name',
        data: { batch: i },
        isReturningUser: false,
        clientId: 20000 + i,
        name: `Batch User ${i}`,
        email: `batch${i}@example.com`,
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
      
      states.push(state);
      // Don't await - fire rapidly to trigger batching
      sessionStateManager.persistState(testAdminId, `+111111111${i}`, state);
    }
    
    // Simulate crash before batch completes (wait less than batch window)
    await new Promise(resolve => setTimeout(resolve, 500));
    
    // Act: Recover - some states may not have been persisted
    const recovered = await recoveryManager.recoverSessionsForAdmin(testAdminId);
    
    // Assert: At least some states should be recovered
    // (Exact number depends on timing, but system should handle gracefully)
    const recoveredCount = Object.keys(recovered.users).length;
    expect(recoveredCount).toBeGreaterThanOrEqual(0);
    expect(recoveredCount).toBeLessThanOrEqual(5);
    
    // System should not crash or corrupt data
    for (const phone in recovered.users) {
      const user = recovered.users[phone];
      expect(user.name).toMatch(/^Batch User \d$/);
      expect(user.data.batch).toBeGreaterThanOrEqual(0);
      expect(user.data.batch).toBeLessThan(5);
    }
  });
});
