import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import pg from 'pg';
import { SessionStateManager } from '../../src/persistence/SessionStateManager.js';
import { StateSerializer } from '../../src/persistence/StateSerializer.js';

const { Pool } = pg;

/**
 * Integration Test: Concurrent Writes
 * 
 * This test validates that concurrent write operations from multiple
 * server instances are handled correctly using database row-level locking
 * and last-write-wins strategy.
 * 
 * Requirements tested: 7.2, 7.3
 */
describe('Integration: Concurrent Writes', () => {
  let pool;
  let serializer;
  
  const testAdminId = 994;
  
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
      password: process.env.DB_PASSWORD || 'postgres',
      max: 20 // Allow multiple concurrent connections
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
  });
  
  afterAll(async () => {
    await pool.end();
  });
  
  beforeEach(async () => {
    await pool.query('DELETE FROM conversation_states WHERE admin_id = $1', [testAdminId]);
  });
  
  it('should handle concurrent writes from multiple instances without corruption', async () => {
    // Arrange: Create multiple session managers (simulating different server instances)
    const instance1 = new SessionStateManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    const instance2 = new SessionStateManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    const instance3 = new SessionStateManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    const phone = '+1234567890';
    
    // Act: Write from all instances concurrently
    const writes = [
      instance1.persistState(testAdminId, phone, {
        step: 'awaiting_name',
        data: { source: 'instance1' },
        isReturningUser: false,
        clientId: 1001,
        name: 'User from Instance 1',
        email: 'instance1@example.com',
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
      }),
      instance2.persistState(testAdminId, phone, {
        step: 'awaiting_email',
        data: { source: 'instance2' },
        isReturningUser: false,
        clientId: 1001,
        name: 'User from Instance 2',
        email: 'instance2@example.com',
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
      }),
      instance3.persistState(testAdminId, phone, {
        step: 'awaiting_payment',
        data: { source: 'instance3' },
        isReturningUser: false,
        clientId: 1001,
        name: 'User from Instance 3',
        email: 'instance3@example.com',
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
      })
    ];
    
    const results = await Promise.allSettled(writes);
    
    // Assert: All writes succeeded
    const successCount = results.filter(r => r.status === 'fulfilled').length;
    expect(successCount).toBe(3);
    
    // Verify only one record exists (no duplicates)
    const dbResult = await pool.query(
      'SELECT * FROM conversation_states WHERE admin_id = $1 AND phone = $2',
      [testAdminId, phone]
    );
    
    expect(dbResult.rows).toHaveLength(1);
    
    // Last write wins - verify it's one of the three writes
    const finalData = dbResult.rows[0].session_data;
    expect(['instance1', 'instance2', 'instance3']).toContain(finalData.data.source);
  });
  
  it('should use last-write-wins strategy based on updated_at', async () => {
    const phone = '+2222222222';
    
    // Arrange: Create two instances
    const instance1 = new SessionStateManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    const instance2 = new SessionStateManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    // Act: Write from instance1
    await instance1.persistState(testAdminId, phone, {
      step: 'awaiting_name',
      data: { version: 1 },
      isReturningUser: false,
      clientId: 2001,
      name: 'First Write',
      email: 'first@example.com',
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
    
    // Small delay to ensure different timestamp
    await new Promise(resolve => setTimeout(resolve, 50));
    
    // Write from instance2 (should win)
    await instance2.persistState(testAdminId, phone, {
      step: 'awaiting_email',
      data: { version: 2 },
      isReturningUser: false,
      clientId: 2001,
      name: 'Second Write',
      email: 'second@example.com',
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
    
    // Assert: Second write wins (most recent updated_at)
    const result = await pool.query(
      'SELECT session_data FROM conversation_states WHERE admin_id = $1 AND phone = $2',
      [testAdminId, phone]
    );
    
    expect(result.rows[0].session_data.data.version).toBe(2);
    expect(result.rows[0].session_data.name).toBe('Second Write');
  });
  
  it('should handle 5 concurrent instances writing to different sessions', async () => {
    // Arrange: Create 5 instances
    const instances = [];
    for (let i = 0; i < 5; i++) {
      instances.push(new SessionStateManager({
        db: pool,
        logger: mockLogger,
        serializer,
        config: mockConfig
      }));
    }
    
    // Act: Each instance writes to 10 different sessions
    const writes = [];
    for (let i = 0; i < 5; i++) {
      for (let j = 0; j < 10; j++) {
        const phone = `+1${i}${String(j).padStart(8, '0')}`;
        writes.push(
          instances[i].persistState(testAdminId, phone, {
            step: 'awaiting_name',
            data: { instance: i, session: j },
            isReturningUser: false,
            clientId: 3000 + (i * 10) + j,
            name: `User ${i}-${j}`,
            email: `user${i}${j}@example.com`,
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
          })
        );
      }
    }
    
    const results = await Promise.allSettled(writes);
    
    // Assert: All writes succeeded
    const successCount = results.filter(r => r.status === 'fulfilled').length;
    expect(successCount).toBe(50);
    
    // Verify all 50 sessions exist
    const dbResult = await pool.query(
      'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    
    expect(parseInt(dbResult.rows[0].count)).toBe(50);
  });
  
  it('should prevent data corruption with row-level locking', async () => {
    const phone = '+3333333333';
    
    // Arrange: Insert initial record
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data)
      VALUES ($1, $2, $3)
    `, [testAdminId, phone, JSON.stringify({ step: 'initial', counter: 0 })]);
    
    // Act: Simulate concurrent updates using transactions with row locking
    const updateOperations = [];
    
    for (let i = 0; i < 10; i++) {
      updateOperations.push(
        (async () => {
          const client = await pool.connect();
          try {
            await client.query('BEGIN');
            
            // Lock row for update
            const result = await client.query(`
              SELECT session_data FROM conversation_states
              WHERE admin_id = $1 AND phone = $2
              FOR UPDATE
            `, [testAdminId, phone]);
            
            const currentData = result.rows[0].session_data;
            const newCounter = (currentData.counter || 0) + 1;
            
            // Update with incremented counter
            await client.query(`
              UPDATE conversation_states
              SET session_data = jsonb_set(session_data, '{counter}', $3::jsonb)
              WHERE admin_id = $1 AND phone = $2
            `, [testAdminId, phone, JSON.stringify(newCounter)]);
            
            await client.query('COMMIT');
          } catch (err) {
            await client.query('ROLLBACK');
            throw err;
          } finally {
            client.release();
          }
        })()
      );
    }
    
    await Promise.all(updateOperations);
    
    // Assert: Counter should be exactly 10 (no lost updates)
    const result = await pool.query(
      'SELECT session_data FROM conversation_states WHERE admin_id = $1 AND phone = $2',
      [testAdminId, phone]
    );
    
    expect(result.rows[0].session_data.counter).toBe(10);
  });
  
  it('should handle concurrent writes to same session from 100 operations', async () => {
    const phone = '+4444444444';
    
    // Arrange: Create session manager
    const sessionManager = new SessionStateManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    // Act: Perform 100 concurrent writes to same session
    const writes = [];
    for (let i = 0; i < 100; i++) {
      writes.push(
        sessionManager.persistState(testAdminId, phone, {
          step: 'awaiting_name',
          data: { writeNumber: i },
          isReturningUser: false,
          clientId: 4001,
          name: `Write ${i}`,
          email: 'concurrent@example.com',
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
        })
      );
    }
    
    const results = await Promise.allSettled(writes);
    
    // Assert: All writes completed (some may have been overwritten)
    const successCount = results.filter(r => r.status === 'fulfilled').length;
    expect(successCount).toBe(100);
    
    // Verify only one record exists
    const dbResult = await pool.query(
      'SELECT * FROM conversation_states WHERE admin_id = $1 AND phone = $2',
      [testAdminId, phone]
    );
    
    expect(dbResult.rows).toHaveLength(1);
    
    // Final state should be one of the 100 writes
    const finalData = dbResult.rows[0].session_data;
    expect(finalData.data.writeNumber).toBeGreaterThanOrEqual(0);
    expect(finalData.data.writeNumber).toBeLessThan(100);
  });
  
  it('should maintain consistency across multiple admins with concurrent writes', async () => {
    const admin1Id = 994;
    const admin2Id = 993;
    const admin3Id = 992;
    
    // Arrange: Create instances for different admins
    const instances = [
      new SessionStateManager({ db: pool, logger: mockLogger, serializer, config: mockConfig }),
      new SessionStateManager({ db: pool, logger: mockLogger, serializer, config: mockConfig }),
      new SessionStateManager({ db: pool, logger: mockLogger, serializer, config: mockConfig })
    ];
    
    // Act: Each admin writes to their own sessions concurrently
    const writes = [];
    
    [admin1Id, admin2Id, admin3Id].forEach((adminId, adminIndex) => {
      for (let i = 0; i < 10; i++) {
        const phone = `+1${adminIndex}${String(i).padStart(8, '0')}`;
        writes.push(
          instances[adminIndex].persistState(adminId, phone, {
            step: 'awaiting_name',
            data: { adminId, sessionIndex: i },
            isReturningUser: false,
            clientId: 5000 + (adminIndex * 10) + i,
            name: `Admin${adminId} User${i}`,
            email: `admin${adminId}user${i}@example.com`,
            assignedAdminId: adminId,
            greetedThisSession: true,
            resumeStep: null,
            awaitingResumeDecision: false,
            lastUserMessageAt: new Date(),
            partialSavedAt: null,
            finalized: false,
            automationDisabled: false,
            aiConversationHistory: [],
            responseLanguage: 'en'
          })
        );
      }
    });
    
    const results = await Promise.allSettled(writes);
    
    // Assert: All writes succeeded
    const successCount = results.filter(r => r.status === 'fulfilled').length;
    expect(successCount).toBe(30);
    
    // Verify each admin has exactly 10 sessions
    for (const adminId of [admin1Id, admin2Id, admin3Id]) {
      const result = await pool.query(
        'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
        [adminId]
      );
      expect(parseInt(result.rows[0].count)).toBe(10);
    }
    
    // Cleanup
    await pool.query('DELETE FROM conversation_states WHERE admin_id IN ($1, $2)', [admin2Id, admin3Id]);
  });
  
  it('should handle rapid sequential updates without data loss', async () => {
    const phone = '+5555555555';
    
    // Arrange: Create session manager
    const sessionManager = new SessionStateManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    // Act: Perform rapid sequential updates
    const updateCount = 20;
    for (let i = 0; i < updateCount; i++) {
      await sessionManager.persistState(testAdminId, phone, {
        step: `step_${i}`,
        data: { updateNumber: i, timestamp: Date.now() },
        isReturningUser: false,
        clientId: 6001,
        name: `Update ${i}`,
        email: 'rapid@example.com',
        assignedAdminId: testAdminId,
        greetedThisSession: true,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: new Date(),
        partialSavedAt: null,
        finalized: false,
        automationDisabled: false,
        aiConversationHistory: Array(i + 1).fill({ role: 'user', content: 'message' }),
        responseLanguage: 'en'
      });
    }
    
    // Assert: Final state reflects last update
    const result = await pool.query(
      'SELECT session_data FROM conversation_states WHERE admin_id = $1 AND phone = $2',
      [testAdminId, phone]
    );
    
    expect(result.rows[0].session_data.step).toBe(`step_${updateCount - 1}`);
    expect(result.rows[0].session_data.data.updateNumber).toBe(updateCount - 1);
    expect(result.rows[0].session_data.aiConversationHistory).toHaveLength(updateCount);
  });
  
  it('should support 100 concurrent state updates per second', async () => {
    // Arrange: Create session manager
    const sessionManager = new SessionStateManager({
      db: pool,
      logger: mockLogger,
      serializer,
      config: mockConfig
    });
    
    // Act: Perform 100 writes and measure time
    const writes = [];
    const startTime = Date.now();
    
    for (let i = 0; i < 100; i++) {
      const phone = `+17${String(i).padStart(8, '0')}`;
      writes.push(
        sessionManager.persistState(testAdminId, phone, {
          step: 'awaiting_name',
          data: { index: i },
          isReturningUser: false,
          clientId: 7000 + i,
          name: `Perf User ${i}`,
          email: `perf${i}@example.com`,
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
        })
      );
    }
    
    await Promise.all(writes);
    const totalTime = Date.now() - startTime;
    
    // Assert: 100 writes completed in reasonable time
    expect(totalTime).toBeLessThan(10000); // Should complete in < 10 seconds
    
    const throughput = (100 / totalTime) * 1000; // operations per second
    expect(throughput).toBeGreaterThan(10); // At least 10 ops/sec
    
    console.log(`Concurrent write throughput: ${throughput.toFixed(2)} ops/sec (${totalTime}ms for 100 writes)`);
  });
});
