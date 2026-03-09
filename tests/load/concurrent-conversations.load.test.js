import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import pg from 'pg';
import { SessionStateManager } from '../../src/persistence/SessionStateManager.js';
import { StateSerializer } from '../../src/persistence/StateSerializer.js';

const { Pool } = pg;

/**
 * Load Test: Concurrent Conversations
 * 
 * This test validates system performance under realistic concurrent load:
 * - 100 concurrent conversations
 * - 10 messages per conversation
 * - p95 persistence latency < 100ms
 * - p99 persistence latency < 500ms
 * - Message processing overhead < 50ms
 * 
 * Requirements tested: 11.1, 11.2, 11.4, 11.6, 15.3
 */
describe('Load Test: Concurrent Conversations', () => {
  let pool;
  let sessionStateManager;
  let serializer;
  
  const testAdminId = 1000;
  const numConversations = 100;
  const messagesPerConversation = 10;
  
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
    // Create database connection pool with higher limits for load testing
    pool = new Pool({
      host: process.env.DB_HOST || 'localhost',
      port: process.env.DB_PORT || 5432,
      database: process.env.DB_NAME || 'mex_end_test',
      user: process.env.DB_USER || 'postgres',
      password: process.env.DB_PASSWORD || 'postgres',
      max: 50, // Increased pool size for load testing
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
    
    // Initialize components
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
    // Clean up test data
    await pool.query('DELETE FROM conversation_states WHERE admin_id = $1', [testAdminId]);
  });
  
  /**
   * Helper function to calculate percentiles
   */
  function calculatePercentile(sortedArray, percentile) {
    const index = Math.ceil((percentile / 100) * sortedArray.length) - 1;
    return sortedArray[index];
  }
  
  /**
   * Helper function to generate a user state
   */
  function generateUserState(conversationId, messageNum) {
    return {
      step: `step_${messageNum}`,
      data: {
        conversationId,
        messageNum,
        selectedProduct: 'birth_chart',
        quantity: 1
      },
      isReturningUser: messageNum > 0,
      clientId: 10000 + conversationId,
      name: `User ${conversationId}`,
      email: `user${conversationId}@example.com`,
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: new Date(),
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: Array(messageNum).fill(null).map((_, i) => ({
        role: i % 2 === 0 ? 'user' : 'assistant',
        content: `Message ${i + 1}`
      })),
      responseLanguage: 'en'
    };
  }
  
  it('should handle 100 concurrent conversations with 10 messages each', async () => {
    const latencies = [];
    const overheads = [];
    
    // Simulate 100 concurrent conversations
    const conversationPromises = [];
    
    for (let convId = 0; convId < numConversations; convId++) {
      const phone = `+1${String(convId).padStart(9, '0')}`;
      
      // Each conversation sends 10 messages
      const conversationPromise = (async () => {
        for (let msgNum = 0; msgNum < messagesPerConversation; msgNum++) {
          const userState = generateUserState(convId, msgNum);
          
          // Measure total time (including message processing simulation)
          const totalStart = Date.now();
          
          // Simulate message processing overhead (5-10ms)
          await new Promise(resolve => setTimeout(resolve, Math.random() * 5 + 5));
          
          // Measure persistence time
          const persistStart = Date.now();
          await sessionStateManager.persistState(testAdminId, phone, userState);
          const persistEnd = Date.now();
          
          const totalEnd = Date.now();
          
          const persistLatency = persistEnd - persistStart;
          const totalOverhead = totalEnd - totalStart;
          
          latencies.push(persistLatency);
          overheads.push(totalOverhead);
        }
      })();
      
      conversationPromises.push(conversationPromise);
    }
    
    // Wait for all conversations to complete
    const startTime = Date.now();
    await Promise.all(conversationPromises);
    const endTime = Date.now();
    
    const totalDuration = endTime - startTime;
    
    // Sort latencies for percentile calculation
    const sortedLatencies = latencies.sort((a, b) => a - b);
    const sortedOverheads = overheads.sort((a, b) => a - b);
    
    // Calculate statistics
    const p50Latency = calculatePercentile(sortedLatencies, 50);
    const p95Latency = calculatePercentile(sortedLatencies, 95);
    const p99Latency = calculatePercentile(sortedLatencies, 99);
    const maxLatency = sortedLatencies[sortedLatencies.length - 1];
    const avgLatency = latencies.reduce((a, b) => a + b, 0) / latencies.length;
    
    const p50Overhead = calculatePercentile(sortedOverheads, 50);
    const p95Overhead = calculatePercentile(sortedOverheads, 95);
    const p99Overhead = calculatePercentile(sortedOverheads, 99);
    const maxOverhead = sortedOverheads[sortedOverheads.length - 1];
    const avgOverhead = overheads.reduce((a, b) => a + b, 0) / overheads.length;
    
    const totalMessages = numConversations * messagesPerConversation;
    const throughput = (totalMessages / totalDuration) * 1000; // messages per second
    
    // Log performance metrics
    console.log('\n=== Load Test Results: Concurrent Conversations ===');
    console.log(`Total conversations: ${numConversations}`);
    console.log(`Messages per conversation: ${messagesPerConversation}`);
    console.log(`Total messages: ${totalMessages}`);
    console.log(`Total duration: ${totalDuration}ms`);
    console.log(`Throughput: ${throughput.toFixed(2)} messages/second`);
    console.log('\nPersistence Latency:');
    console.log(`  Average: ${avgLatency.toFixed(2)}ms`);
    console.log(`  p50: ${p50Latency}ms`);
    console.log(`  p95: ${p95Latency}ms`);
    console.log(`  p99: ${p99Latency}ms`);
    console.log(`  Max: ${maxLatency}ms`);
    console.log('\nMessage Processing Overhead:');
    console.log(`  Average: ${avgOverhead.toFixed(2)}ms`);
    console.log(`  p50: ${p50Overhead}ms`);
    console.log(`  p95: ${p95Overhead}ms`);
    console.log(`  p99: ${p99Overhead}ms`);
    console.log(`  Max: ${maxOverhead}ms`);
    console.log('===================================================\n');
    
    // Verify all messages were persisted
    const result = await pool.query(
      'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    expect(parseInt(result.rows[0].count)).toBe(numConversations);
    
    // Assert performance requirements
    // Requirement 11.1: p95 persistence latency < 100ms
    expect(p95Latency).toBeLessThan(100);
    
    // Requirement 11.2: p99 persistence latency < 500ms
    expect(p99Latency).toBeLessThan(500);
    
    // Requirement 11.6: Message processing overhead < 50ms
    expect(p95Overhead).toBeLessThan(50);
    
    // Requirement 11.4: Support at least 100 concurrent state updates per second
    expect(throughput).toBeGreaterThan(100);
  }, 60000); // 60 second timeout for load test
  
  it('should maintain performance with varying message sizes', async () => {
    const latencies = [];
    const numTests = 50;
    
    // Test with varying AI conversation history sizes
    for (let i = 0; i < numTests; i++) {
      const phone = `+2${String(i).padStart(9, '0')}`;
      
      // Create state with varying history sizes (0 to 100 messages)
      const historySize = Math.floor(Math.random() * 100);
      const userState = {
        step: 'awaiting_response',
        data: { test: true },
        isReturningUser: true,
        clientId: 20000 + i,
        name: `Test User ${i}`,
        email: `test${i}@example.com`,
        assignedAdminId: testAdminId,
        greetedThisSession: true,
        resumeStep: null,
        awaitingResumeDecision: false,
        lastUserMessageAt: new Date(),
        partialSavedAt: null,
        finalized: false,
        automationDisabled: false,
        aiConversationHistory: Array(historySize).fill(null).map((_, idx) => ({
          role: idx % 2 === 0 ? 'user' : 'assistant',
          content: `Message ${idx + 1}: ${Math.random().toString(36).substring(2, 50)}`
        })),
        responseLanguage: 'en'
      };
      
      const start = Date.now();
      await sessionStateManager.persistState(testAdminId, phone, userState);
      const end = Date.now();
      
      latencies.push(end - start);
    }
    
    const sortedLatencies = latencies.sort((a, b) => a - b);
    const p95 = calculatePercentile(sortedLatencies, 95);
    const p99 = calculatePercentile(sortedLatencies, 99);
    
    console.log('\n=== Varying Message Sizes Test ===');
    console.log(`p95 latency: ${p95}ms`);
    console.log(`p99 latency: ${p99}ms`);
    console.log('==================================\n');
    
    // Performance should remain consistent regardless of message size
    expect(p95).toBeLessThan(100);
    expect(p99).toBeLessThan(500);
  }, 30000);
  
  it('should handle burst traffic patterns', async () => {
    const latencies = [];
    const burstSize = 50;
    const numBursts = 5;
    
    // Simulate burst traffic: 5 bursts of 50 concurrent messages
    for (let burst = 0; burst < numBursts; burst++) {
      const burstPromises = [];
      
      for (let i = 0; i < burstSize; i++) {
        const phone = `+3${String(burst * burstSize + i).padStart(9, '0')}`;
        const userState = generateUserState(burst * burstSize + i, burst);
        
        const promise = (async () => {
          const start = Date.now();
          await sessionStateManager.persistState(testAdminId, phone, userState);
          const end = Date.now();
          latencies.push(end - start);
        })();
        
        burstPromises.push(promise);
      }
      
      await Promise.all(burstPromises);
      
      // Small delay between bursts
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    const sortedLatencies = latencies.sort((a, b) => a - b);
    const p95 = calculatePercentile(sortedLatencies, 95);
    const p99 = calculatePercentile(sortedLatencies, 99);
    
    console.log('\n=== Burst Traffic Test ===');
    console.log(`Total bursts: ${numBursts}`);
    console.log(`Messages per burst: ${burstSize}`);
    console.log(`p95 latency: ${p95}ms`);
    console.log(`p99 latency: ${p99}ms`);
    console.log('==========================\n');
    
    // Performance should remain within targets during bursts
    expect(p95).toBeLessThan(100);
    expect(p99).toBeLessThan(500);
  }, 30000);
});
