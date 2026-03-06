import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import pg from 'pg';
import { SessionStateManager } from '../../src/persistence/SessionStateManager.js';
import { StateSerializer } from '../../src/persistence/StateSerializer.js';

const { Pool } = pg;

/**
 * Load Test: Sustained Message Throughput
 * 
 * This test validates system performance under sustained load:
 * - Simulate sustained load: 50 messages/second for 5 minutes
 * - Verify system maintains performance targets
 * - Verify no memory leaks or degradation
 * 
 * Requirements tested: 11.4, 11.7
 */
describe('Load Test: Sustained Message Throughput', () => {
  let pool;
  let sessionStateManager;
  let serializer;
  
  const testAdminId = 3000;
  
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
   * Helper function to generate a user state
   */
  function generateUserState(conversationId, messageNum) {
    return {
      step: `step_${messageNum % 10}`,
      data: {
        conversationId,
        messageNum,
        selectedProduct: 'birth_chart',
        quantity: 1
      },
      isReturningUser: messageNum > 0,
      clientId: 40000 + conversationId,
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
      aiConversationHistory: Array(Math.min(messageNum, 20)).fill(null).map((_, i) => ({
        role: i % 2 === 0 ? 'user' : 'assistant',
        content: `Message ${i + 1}`
      })),
      responseLanguage: 'en'
    };
  }
  
  /**
   * Helper to calculate percentiles
   */
  function calculatePercentile(sortedArray, percentile) {
    const index = Math.ceil((percentile / 100) * sortedArray.length) - 1;
    return sortedArray[index];
  }
  
  /**
   * Helper to get memory usage
   */
  function getMemoryUsage() {
    const usage = process.memoryUsage();
    return {
      heapUsed: Math.round(usage.heapUsed / 1024 / 1024), // MB
      heapTotal: Math.round(usage.heapTotal / 1024 / 1024), // MB
      external: Math.round(usage.external / 1024 / 1024), // MB
      rss: Math.round(usage.rss / 1024 / 1024) // MB
    };
  }
  
  it('should maintain 50 messages/second for 5 minutes', async () => {
    const targetRate = 50; // messages per second
    const durationSeconds = 300; // 5 minutes
    const totalMessages = targetRate * durationSeconds;
    const intervalMs = 1000 / targetRate; // 20ms between messages
    
    console.log('\n=== Sustained Throughput Test ===');
    console.log(`Target rate: ${targetRate} messages/second`);
    console.log(`Duration: ${durationSeconds} seconds (5 minutes)`);
    console.log(`Total messages: ${totalMessages}`);
    console.log(`Interval: ${intervalMs}ms between messages`);
    console.log('Starting test...\n');
    
    const latencies = [];
    const memorySnapshots = [];
    let messagesSent = 0;
    let messagesCompleted = 0;
    let errors = 0;
    
    // Track initial memory
    const initialMemory = getMemoryUsage();
    memorySnapshots.push({ time: 0, ...initialMemory });
    
    // Use a pool of conversations to simulate realistic load
    const numConversations = 100;
    let currentConversation = 0;
    
    const startTime = Date.now();
    
    // Send messages at target rate
    const sendMessage = async () => {
      if (messagesSent >= totalMessages) {
        return;
      }
      
      messagesSent++;
      const messageId = messagesSent;
      const conversationId = currentConversation % numConversations;
      currentConversation++;
      
      const phone = `+1${String(conversationId).padStart(9, '0')}`;
      const userState = generateUserState(conversationId, Math.floor(messageId / numConversations));
      
      const persistStart = Date.now();
      
      try {
        await sessionStateManager.persistState(testAdminId, phone, userState);
        const persistEnd = Date.now();
        latencies.push(persistEnd - persistStart);
        messagesCompleted++;
      } catch (error) {
        errors++;
      }
      
      // Log progress every 30 seconds
      const elapsed = Date.now() - startTime;
      if (messageId % (targetRate * 30) === 0) {
        const currentRate = (messagesCompleted / (elapsed / 1000)).toFixed(2);
        const memory = getMemoryUsage();
        memorySnapshots.push({ time: elapsed / 1000, ...memory });
        
        console.log(`Progress: ${messageId}/${totalMessages} messages`);
        console.log(`  Elapsed: ${Math.floor(elapsed / 1000)}s`);
        console.log(`  Current rate: ${currentRate} msg/s`);
        console.log(`  Completed: ${messagesCompleted}, Errors: ${errors}`);
        console.log(`  Memory: ${memory.heapUsed}MB heap, ${memory.rss}MB RSS`);
      }
    };
    
    // Schedule messages at target rate
    const promises = [];
    for (let i = 0; i < totalMessages; i++) {
      // Wait for the appropriate interval
      await new Promise(resolve => setTimeout(resolve, intervalMs));
      promises.push(sendMessage());
    }
    
    // Wait for all messages to complete
    await Promise.all(promises);
    
    const endTime = Date.now();
    const totalDuration = endTime - startTime;
    const actualRate = (messagesCompleted / (totalDuration / 1000)).toFixed(2);
    
    // Capture final memory
    const finalMemory = getMemoryUsage();
    memorySnapshots.push({ time: totalDuration / 1000, ...finalMemory });
    
    // Calculate latency statistics
    const sortedLatencies = latencies.sort((a, b) => a - b);
    const p50 = calculatePercentile(sortedLatencies, 50);
    const p95 = calculatePercentile(sortedLatencies, 95);
    const p99 = calculatePercentile(sortedLatencies, 99);
    const max = sortedLatencies[sortedLatencies.length - 1];
    const avg = latencies.reduce((a, b) => a + b, 0) / latencies.length;
    
    // Calculate memory growth
    const memoryGrowth = finalMemory.heapUsed - initialMemory.heapUsed;
    const memoryGrowthPercent = ((memoryGrowth / initialMemory.heapUsed) * 100).toFixed(2);
    
    // Log final results
    console.log('\n=== Test Results ===');
    console.log(`Total duration: ${(totalDuration / 1000).toFixed(2)}s`);
    console.log(`Messages sent: ${messagesSent}`);
    console.log(`Messages completed: ${messagesCompleted}`);
    console.log(`Errors: ${errors}`);
    console.log(`Actual rate: ${actualRate} messages/second`);
    console.log('\nLatency Statistics:');
    console.log(`  Average: ${avg.toFixed(2)}ms`);
    console.log(`  p50: ${p50}ms`);
    console.log(`  p95: ${p95}ms`);
    console.log(`  p99: ${p99}ms`);
    console.log(`  Max: ${max}ms`);
    console.log('\nMemory Usage:');
    console.log(`  Initial: ${initialMemory.heapUsed}MB heap, ${initialMemory.rss}MB RSS`);
    console.log(`  Final: ${finalMemory.heapUsed}MB heap, ${finalMemory.rss}MB RSS`);
    console.log(`  Growth: ${memoryGrowth}MB (${memoryGrowthPercent}%)`);
    console.log('====================\n');
    
    // Assertions
    // Should complete most messages successfully
    expect(messagesCompleted).toBeGreaterThan(totalMessages * 0.95);
    
    // Should maintain target rate (within 10% tolerance)
    expect(parseFloat(actualRate)).toBeGreaterThan(targetRate * 0.9);
    
    // Performance should remain within targets
    expect(p95).toBeLessThan(100);
    expect(p99).toBeLessThan(500);
    
    // Memory growth should be reasonable (less than 100% growth)
    expect(Math.abs(parseFloat(memoryGrowthPercent))).toBeLessThan(100);
    
    // Should have minimal errors
    expect(errors).toBeLessThan(totalMessages * 0.01); // Less than 1% error rate
  }, 360000); // 6 minute timeout (5 min test + 1 min buffer)
  
  it('should handle sustained load with varying conversation patterns', async () => {
    const targetRate = 30; // messages per second (lower for faster test)
    const durationSeconds = 60; // 1 minute
    const totalMessages = targetRate * durationSeconds;
    const intervalMs = 1000 / targetRate;
    
    console.log('\n=== Varying Patterns Test ===');
    console.log(`Target rate: ${targetRate} messages/second`);
    console.log(`Duration: ${durationSeconds} seconds`);
    console.log('Starting test...\n');
    
    const latencies = [];
    let messagesCompleted = 0;
    
    // Mix of conversation patterns:
    // - 50% new conversations (short history)
    // - 30% ongoing conversations (medium history)
    // - 20% long conversations (large history)
    const numConversations = 50;
    const conversationStates = new Map();
    
    const startTime = Date.now();
    
    for (let i = 0; i < totalMessages; i++) {
      await new Promise(resolve => setTimeout(resolve, intervalMs));
      
      const conversationId = i % numConversations;
      const phone = `+2${String(conversationId).padStart(9, '0')}`;
      
      // Determine conversation type
      const rand = Math.random();
      let messageNum;
      
      if (rand < 0.5) {
        // New conversation (0-5 messages)
        messageNum = Math.floor(Math.random() * 5);
      } else if (rand < 0.8) {
        // Ongoing conversation (5-20 messages)
        messageNum = 5 + Math.floor(Math.random() * 15);
      } else {
        // Long conversation (20-50 messages)
        messageNum = 20 + Math.floor(Math.random() * 30);
      }
      
      const userState = generateUserState(conversationId, messageNum);
      
      const persistStart = Date.now();
      await sessionStateManager.persistState(testAdminId, phone, userState);
      const persistEnd = Date.now();
      
      latencies.push(persistEnd - persistStart);
      messagesCompleted++;
      
      if (i % (targetRate * 15) === 0 && i > 0) {
        const elapsed = Date.now() - startTime;
        const currentRate = (messagesCompleted / (elapsed / 1000)).toFixed(2);
        console.log(`Progress: ${i}/${totalMessages} - Rate: ${currentRate} msg/s`);
      }
    }
    
    const endTime = Date.now();
    const totalDuration = endTime - startTime;
    const actualRate = (messagesCompleted / (totalDuration / 1000)).toFixed(2);
    
    const sortedLatencies = latencies.sort((a, b) => a - b);
    const p95 = calculatePercentile(sortedLatencies, 95);
    const p99 = calculatePercentile(sortedLatencies, 99);
    
    console.log('\n=== Results ===');
    console.log(`Actual rate: ${actualRate} messages/second`);
    console.log(`p95 latency: ${p95}ms`);
    console.log(`p99 latency: ${p99}ms`);
    console.log('===============\n');
    
    expect(messagesCompleted).toBe(totalMessages);
    expect(parseFloat(actualRate)).toBeGreaterThan(targetRate * 0.9);
    expect(p95).toBeLessThan(100);
    expect(p99).toBeLessThan(500);
  }, 120000); // 2 minute timeout
  
  it('should maintain performance under continuous load without degradation', async () => {
    // Test for performance degradation over time
    const targetRate = 40;
    const testIntervals = 5; // 5 intervals of 30 seconds each
    const intervalDuration = 30; // seconds
    const messagesPerInterval = targetRate * intervalDuration;
    
    console.log('\n=== Performance Degradation Test ===');
    console.log(`Testing ${testIntervals} intervals of ${intervalDuration}s each`);
    console.log(`Target rate: ${targetRate} messages/second\n`);
    
    const intervalStats = [];
    
    for (let interval = 0; interval < testIntervals; interval++) {
      const latencies = [];
      const numConversations = 20;
      
      console.log(`Interval ${interval + 1}/${testIntervals}...`);
      
      const intervalStart = Date.now();
      
      for (let i = 0; i < messagesPerInterval; i++) {
        await new Promise(resolve => setTimeout(resolve, 1000 / targetRate));
        
        const conversationId = i % numConversations;
        const phone = `+3${String(conversationId).padStart(9, '0')}`;
        const userState = generateUserState(conversationId, interval * messagesPerInterval + i);
        
        const persistStart = Date.now();
        await sessionStateManager.persistState(testAdminId, phone, userState);
        const persistEnd = Date.now();
        
        latencies.push(persistEnd - persistStart);
      }
      
      const intervalEnd = Date.now();
      const intervalDurationMs = intervalEnd - intervalStart;
      
      const sortedLatencies = latencies.sort((a, b) => a - b);
      const p95 = calculatePercentile(sortedLatencies, 95);
      const p99 = calculatePercentile(sortedLatencies, 99);
      const avg = latencies.reduce((a, b) => a + b, 0) / latencies.length;
      
      const stats = {
        interval: interval + 1,
        duration: intervalDurationMs,
        avgLatency: avg,
        p95Latency: p95,
        p99Latency: p99
      };
      
      intervalStats.push(stats);
      
      console.log(`  Duration: ${intervalDurationMs}ms`);
      console.log(`  Avg latency: ${avg.toFixed(2)}ms`);
      console.log(`  p95: ${p95}ms, p99: ${p99}ms`);
    }
    
    console.log('\n=== Degradation Analysis ===');
    
    // Compare first and last intervals
    const firstInterval = intervalStats[0];
    const lastInterval = intervalStats[testIntervals - 1];
    
    const avgLatencyChange = ((lastInterval.avgLatency - firstInterval.avgLatency) / firstInterval.avgLatency * 100).toFixed(2);
    const p95LatencyChange = ((lastInterval.p95Latency - firstInterval.p95Latency) / firstInterval.p95Latency * 100).toFixed(2);
    
    console.log(`First interval avg latency: ${firstInterval.avgLatency.toFixed(2)}ms`);
    console.log(`Last interval avg latency: ${lastInterval.avgLatency.toFixed(2)}ms`);
    console.log(`Change: ${avgLatencyChange}%`);
    console.log(`\nFirst interval p95: ${firstInterval.p95Latency}ms`);
    console.log(`Last interval p95: ${lastInterval.p95Latency}ms`);
    console.log(`Change: ${p95LatencyChange}%`);
    console.log('============================\n');
    
    // All intervals should meet performance targets
    for (const stats of intervalStats) {
      expect(stats.p95Latency).toBeLessThan(100);
      expect(stats.p99Latency).toBeLessThan(500);
    }
    
    // Performance should not degrade significantly (less than 50% increase)
    expect(Math.abs(parseFloat(avgLatencyChange))).toBeLessThan(50);
    expect(Math.abs(parseFloat(p95LatencyChange))).toBeLessThan(50);
  }, 180000); // 3 minute timeout
});
