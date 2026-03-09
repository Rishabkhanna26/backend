import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import pg from 'pg';
import { SessionCleanupService } from '../../src/persistence/SessionCleanupService.js';
import { StateSerializer } from '../../src/persistence/StateSerializer.js';

const { Pool } = pg;

/**
 * Load Test: Cleanup Performance
 * 
 * This test validates cleanup service performance:
 * - Insert 10,000 expired sessions
 * - Run cleanup service
 * - Verify batch limiting (1000 per run)
 * - Verify cleanup completes efficiently
 * 
 * Requirements tested: 5.5, 5.6
 */
describe('Load Test: Cleanup Performance', () => {
  let pool;
  let cleanupService;
  let serializer;
  
  const testAdminId = 5000;
  
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
    userIdleTtlMs: 21600000, // 6 hours
    cleanupIntervalMs: 900000, // 15 minutes
    cleanupBatchSize: 1000
  };
  
  beforeAll(async () => {
    // Create database connection pool
    pool = new Pool({
      host: process.env.DB_HOST || 'localhost',
      port: process.env.DB_PORT || 5432,
      database: process.env.DB_NAME || 'mex_end_test',
      user: process.env.DB_USER || 'postgres',
      password: process.env.DB_PASSWORD || 'postgres',
      max: 50
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
    
    cleanupService = new SessionCleanupService({
      db: pool,
      logger: mockLogger,
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
   * Helper function to generate a user state
   */
  function generateUserState(sessionId) {
    return {
      step: 'awaiting_confirmation',
      data: {
        sessionId,
        selectedProduct: 'birth_chart',
        quantity: 1
      },
      isReturningUser: true,
      clientId: 60000 + sessionId,
      name: `User ${sessionId}`,
      email: `user${sessionId}@example.com`,
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
  
  /**
   * Helper to insert sessions directly into database with custom timestamps
   */
  async function insertSession(phone, userState, lastActivityAt) {
    const serialized = serializer.serialize(userState);
    
    await pool.query(
      `INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (admin_id, phone) DO UPDATE SET
         session_data = EXCLUDED.session_data,
         last_activity_at = EXCLUDED.last_activity_at`,
      [testAdminId, phone, serialized, lastActivityAt]
    );
  }
  
  it('should clean up 10,000 expired sessions efficiently', async () => {
    const totalSessions = 10000;
    const batchSize = 1000;
    
    console.log('\n=== Cleanup Performance Test ===');
    console.log(`Creating ${totalSessions} expired sessions...`);
    
    // Create expired sessions (7 hours ago, beyond 6 hour TTL)
    const expiredTimestamp = new Date(Date.now() - mockConfig.userIdleTtlMs - 3600000);
    
    const createStart = Date.now();
    
    // Batch insert for faster setup
    for (let batch = 0; batch < totalSessions / batchSize; batch++) {
      const values = [];
      const params = [];
      let paramIndex = 1;
      
      for (let i = 0; i < batchSize; i++) {
        const sessionId = batch * batchSize + i;
        const phone = `+1${String(sessionId).padStart(9, '0')}`;
        const userState = generateUserState(sessionId);
        const serialized = serializer.serialize(userState);
        
        values.push(`($${paramIndex}, $${paramIndex + 1}, $${paramIndex + 2}, $${paramIndex + 3})`);
        params.push(testAdminId, phone, serialized, expiredTimestamp);
        paramIndex += 4;
      }
      
      await pool.query(
        `INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
         VALUES ${values.join(', ')}
         ON CONFLICT (admin_id, phone) DO NOTHING`,
        params
      );
    }
    
    const createEnd = Date.now();
    console.log(`Created ${totalSessions} sessions in ${createEnd - createStart}ms`);
    
    // Verify sessions were created
    const beforeCount = await pool.query(
      'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    expect(parseInt(beforeCount.rows[0].count)).toBe(totalSessions);
    
    // Run cleanup multiple times to delete all expired sessions
    console.log('\nRunning cleanup...');
    const cleanupRuns = [];
    let totalDeleted = 0;
    let runCount = 0;
    
    while (totalDeleted < totalSessions) {
      runCount++;
      const runStart = Date.now();
      await cleanupService.runCleanup();
      const runEnd = Date.now();
      
      const currentCount = await pool.query(
        'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
        [testAdminId]
      );
      const remaining = parseInt(currentCount.rows[0].count);
      const deletedThisRun = (runCount === 1 ? totalSessions : cleanupRuns[runCount - 2].remaining) - remaining;
      
      const runStats = {
        run: runCount,
        duration: runEnd - runStart,
        deleted: deletedThisRun,
        remaining
      };
      
      cleanupRuns.push(runStats);
      totalDeleted += deletedThisRun;
      
      console.log(`Run ${runCount}: Deleted ${deletedThisRun} sessions in ${runStats.duration}ms (${remaining} remaining)`);
      
      if (remaining === 0) break;
    }
    
    // Calculate statistics
    const totalCleanupTime = cleanupRuns.reduce((sum, run) => sum + run.duration, 0);
    const avgRunTime = totalCleanupTime / cleanupRuns.length;
    const maxRunTime = Math.max(...cleanupRuns.map(r => r.duration));
    const minRunTime = Math.min(...cleanupRuns.map(r => r.duration));
    
    console.log('\n=== Results ===');
    console.log(`Total sessions deleted: ${totalDeleted}`);
    console.log(`Cleanup runs: ${runCount}`);
    console.log(`Total cleanup time: ${totalCleanupTime}ms`);
    console.log(`Average run time: ${avgRunTime.toFixed(2)}ms`);
    console.log(`Min run time: ${minRunTime}ms`);
    console.log(`Max run time: ${maxRunTime}ms`);
    console.log(`Throughput: ${(totalDeleted / (totalCleanupTime / 1000)).toFixed(2)} sessions/second`);
    console.log('=================\n');
    
    // Verify all sessions were deleted
    const afterCount = await pool.query(
      'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    expect(parseInt(afterCount.rows[0].count)).toBe(0);
    
    // Verify batch limiting (Requirement 5.6)
    // Each run should delete at most 1000 sessions
    for (const run of cleanupRuns) {
      expect(run.deleted).toBeLessThanOrEqual(mockConfig.cleanupBatchSize);
    }
    
    // Cleanup should be efficient (Requirement 5.5)
    // Average run time should be reasonable (< 5 seconds per batch)
    expect(avgRunTime).toBeLessThan(5000);
  }, 120000); // 2 minute timeout
  
  it('should efficiently clean up finalized sessions', async () => {
    const totalSessions = 5000;
    const finalizedCount = 2500;
    const activeCount = 2500;
    
    console.log('\n=== Finalized Sessions Cleanup Test ===');
    console.log(`Creating ${finalizedCount} finalized and ${activeCount} active sessions...`);
    
    const now = Date.now();
    
    // Create finalized sessions (should be deleted)
    for (let i = 0; i < finalizedCount; i++) {
      const phone = `+2${String(i).padStart(9, '0')}`;
      const userState = generateUserState(i);
      userState.finalized = true;
      const recentTimestamp = new Date(now - 3600000); // 1 hour ago (within TTL)
      
      await insertSession(phone, userState, recentTimestamp);
    }
    
    // Create active sessions (should NOT be deleted)
    for (let i = 0; i < activeCount; i++) {
      const phone = `+3${String(i).padStart(9, '0')}`;
      const userState = generateUserState(i);
      userState.finalized = false;
      const recentTimestamp = new Date(now - 1800000); // 30 minutes ago
      
      await insertSession(phone, userState, recentTimestamp);
    }
    
    // Verify sessions were created
    const beforeCount = await pool.query(
      'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    expect(parseInt(beforeCount.rows[0].count)).toBe(totalSessions);
    
    // Run cleanup
    console.log('\nRunning cleanup...');
    const cleanupStart = Date.now();
    
    let totalDeleted = 0;
    let runCount = 0;
    
    // Run cleanup until no more finalized sessions
    while (true) {
      runCount++;
      await cleanupService.runCleanup();
      
      const currentCount = await pool.query(
        'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
        [testAdminId]
      );
      const remaining = parseInt(currentCount.rows[0].count);
      
      if (remaining === activeCount) {
        // All finalized sessions deleted
        totalDeleted = totalSessions - remaining;
        break;
      }
      
      if (runCount > 10) {
        // Safety limit
        break;
      }
    }
    
    const cleanupEnd = Date.now();
    const cleanupDuration = cleanupEnd - cleanupStart;
    
    console.log(`\nDeleted ${totalDeleted} finalized sessions in ${cleanupDuration}ms (${runCount} runs)`);
    
    // Verify only finalized sessions were deleted
    const afterCount = await pool.query(
      'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    expect(parseInt(afterCount.rows[0].count)).toBe(activeCount);
    
    // Verify no active sessions were deleted
    const activeRemaining = await pool.query(
      `SELECT COUNT(*) as count FROM conversation_states 
       WHERE admin_id = $1 AND (session_data->>'finalized')::boolean = false`,
      [testAdminId]
    );
    expect(parseInt(activeRemaining.rows[0].count)).toBe(activeCount);
    
    console.log('=========================================\n');
  }, 60000);
  
  it('should handle mixed expired and finalized sessions', async () => {
    const expiredCount = 3000;
    const finalizedCount = 2000;
    const activeCount = 1000;
    const totalSessions = expiredCount + finalizedCount + activeCount;
    
    console.log('\n=== Mixed Sessions Cleanup Test ===');
    console.log(`Creating ${expiredCount} expired, ${finalizedCount} finalized, ${activeCount} active sessions...`);
    
    const now = Date.now();
    const expiredTimestamp = new Date(now - mockConfig.userIdleTtlMs - 3600000);
    const recentTimestamp = new Date(now - 1800000);
    
    // Create expired sessions
    for (let i = 0; i < expiredCount; i++) {
      const phone = `+4${String(i).padStart(9, '0')}`;
      const userState = generateUserState(i);
      userState.finalized = false;
      await insertSession(phone, userState, expiredTimestamp);
    }
    
    // Create finalized sessions (recent but finalized)
    for (let i = 0; i < finalizedCount; i++) {
      const phone = `+5${String(i).padStart(9, '0')}`;
      const userState = generateUserState(i);
      userState.finalized = true;
      await insertSession(phone, userState, recentTimestamp);
    }
    
    // Create active sessions
    for (let i = 0; i < activeCount; i++) {
      const phone = `+6${String(i).padStart(9, '0')}`;
      const userState = generateUserState(i);
      userState.finalized = false;
      await insertSession(phone, userState, recentTimestamp);
    }
    
    // Verify sessions were created
    const beforeCount = await pool.query(
      'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    expect(parseInt(beforeCount.rows[0].count)).toBe(totalSessions);
    
    // Run cleanup until all expired/finalized sessions are deleted
    console.log('\nRunning cleanup...');
    const cleanupStart = Date.now();
    
    let runCount = 0;
    const expectedToDelete = expiredCount + finalizedCount;
    
    while (true) {
      runCount++;
      await cleanupService.runCleanup();
      
      const currentCount = await pool.query(
        'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
        [testAdminId]
      );
      const remaining = parseInt(currentCount.rows[0].count);
      
      console.log(`Run ${runCount}: ${remaining} sessions remaining`);
      
      if (remaining === activeCount) {
        break;
      }
      
      if (runCount > 20) {
        // Safety limit
        break;
      }
    }
    
    const cleanupEnd = Date.now();
    const cleanupDuration = cleanupEnd - cleanupStart;
    
    const totalDeleted = totalSessions - activeCount;
    
    console.log(`\n=== Results ===`);
    console.log(`Total deleted: ${totalDeleted} (expected: ${expectedToDelete})`);
    console.log(`Cleanup runs: ${runCount}`);
    console.log(`Total duration: ${cleanupDuration}ms`);
    console.log(`Throughput: ${(totalDeleted / (cleanupDuration / 1000)).toFixed(2)} sessions/second`);
    console.log('=================\n');
    
    // Verify only active sessions remain
    const afterCount = await pool.query(
      'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    expect(parseInt(afterCount.rows[0].count)).toBe(activeCount);
  }, 120000);
  
  it('should maintain cleanup performance with large session data', async () => {
    const numSessions = 2000;
    
    console.log('\n=== Large Session Data Cleanup Test ===');
    console.log(`Creating ${numSessions} expired sessions with large data...`);
    
    const expiredTimestamp = new Date(Date.now() - mockConfig.userIdleTtlMs - 3600000);
    
    // Create sessions with large conversation histories
    for (let i = 0; i < numSessions; i++) {
      const phone = `+7${String(i).padStart(9, '0')}`;
      const userState = generateUserState(i);
      
      // Add large conversation history
      userState.aiConversationHistory = Array(100).fill(null).map((_, idx) => ({
        role: idx % 2 === 0 ? 'user' : 'assistant',
        content: `Message ${idx + 1}: ${Math.random().toString(36).substring(2, 100)}`
      }));
      
      await insertSession(phone, userState, expiredTimestamp);
    }
    
    // Run cleanup
    console.log('\nRunning cleanup...');
    const cleanupStart = Date.now();
    
    let runCount = 0;
    while (true) {
      runCount++;
      await cleanupService.runCleanup();
      
      const currentCount = await pool.query(
        'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
        [testAdminId]
      );
      const remaining = parseInt(currentCount.rows[0].count);
      
      console.log(`Run ${runCount}: ${remaining} sessions remaining`);
      
      if (remaining === 0) break;
      if (runCount > 10) break;
    }
    
    const cleanupEnd = Date.now();
    const cleanupDuration = cleanupEnd - cleanupStart;
    
    console.log(`\n=== Results ===`);
    console.log(`Cleanup runs: ${runCount}`);
    console.log(`Total duration: ${cleanupDuration}ms`);
    console.log(`Average per run: ${(cleanupDuration / runCount).toFixed(2)}ms`);
    console.log('=================\n');
    
    // Verify all sessions deleted
    const afterCount = await pool.query(
      'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    expect(parseInt(afterCount.rows[0].count)).toBe(0);
    
    // Cleanup should remain efficient even with large data
    expect(cleanupDuration / runCount).toBeLessThan(5000);
  }, 120000);
});
