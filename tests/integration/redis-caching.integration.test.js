import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import pg from 'pg';
import { createClient } from 'redis';
import { SessionStateManager } from '../../src/persistence/SessionStateManager.js';
import { StateSerializer } from '../../src/persistence/StateSerializer.js';
import { CacheLayer } from '../../src/persistence/CacheLayer.js';

const { Pool } = pg;

/**
 * Integration Test: Redis Caching
 * 
 * This test validates Redis caching functionality with real Redis and PostgreSQL,
 * including cache hit/miss scenarios, fallback behavior, and pub/sub invalidation.
 * 
 * Requirements tested: 6.1, 6.2, 6.3, 6.4, 6.6, 7.6, 15.6
 */
describe('Integration: Redis Caching', () => {
  let pool;
  let redisClient;
  let cacheLayer;
  let sessionStateManager;
  let serializer;
  
  const testAdminId = 996;
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
    
    // Create Redis client
    const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
    redisClient = createClient({ url: redisUrl });
    
    redisClient.on('error', (err) => {
      console.error('Redis Client Error:', err);
    });
    
    await redisClient.connect();
    
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
    
    cacheLayer = new CacheLayer({
      redisClient,
      logger: mockLogger,
      config: mockConfig
    });
    
    sessionStateManager = new SessionStateManager({
      db: pool,
      logger: mockLogger,
      serializer,
      cacheLayer,
      config: mockConfig
    });
  });
  
  afterAll(async () => {
    await redisClient.quit();
    await pool.end();
  });
  
  beforeEach(async () => {
    // Clean up test data
    await pool.query('DELETE FROM conversation_states WHERE admin_id = $1', [testAdminId]);
    
    // Clear Redis cache
    const keys = await redisClient.keys(`conversation:${testAdminId}:*`);
    if (keys.length > 0) {
      await redisClient.del(keys);
    }
  });
  
  it('should cache state in Redis after persistence', async () => {
    // Arrange: Create user state
    const userState = {
      step: 'awaiting_name',
      data: { selectedProduct: 'birth_chart' },
      isReturningUser: false,
      clientId: 1001,
      name: 'Cache Test User',
      email: 'cache@example.com',
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
    
    // Act: Persist state (should write to both DB and cache)
    await sessionStateManager.persistState(testAdminId, testPhone, userState);
    
    // Assert: Verify state in Redis
    const cacheKey = `conversation:${testAdminId}:${testPhone}`;
    const cachedData = await redisClient.get(cacheKey);
    
    expect(cachedData).toBeTruthy();
    const parsedCache = JSON.parse(cachedData);
    expect(parsedCache.name).toBe('Cache Test User');
    expect(parsedCache.step).toBe('awaiting_name');
  });
  
  it('should return cached data on cache hit', async () => {
    // Arrange: Persist state to DB and cache
    const userState = {
      step: 'awaiting_email',
      data: {},
      isReturningUser: false,
      clientId: 1002,
      name: 'Hit Test User',
      email: 'hit@example.com',
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
    
    await sessionStateManager.persistState(testAdminId, testPhone, userState);
    
    // Act: Load state (should hit cache)
    const loadedState = await sessionStateManager.loadState(testAdminId, testPhone);
    
    // Assert: State loaded from cache
    expect(loadedState).toBeTruthy();
    expect(loadedState.name).toBe('Hit Test User');
    expect(loadedState.step).toBe('awaiting_email');
  });
  
  it('should load from database on cache miss and populate cache', async () => {
    // Arrange: Insert directly into database (bypassing cache)
    const userState = {
      step: 'awaiting_payment',
      data: { totalAmount: 150 },
      isReturningUser: true,
      clientId: 1003,
      name: 'Miss Test User',
      email: 'miss@example.com',
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
    
    const serialized = serializer.serialize(userState);
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data)
      VALUES ($1, $2, $3)
    `, [testAdminId, testPhone, serialized]);
    
    // Verify cache is empty
    const cacheKey = `conversation:${testAdminId}:${testPhone}`;
    const cachedBefore = await redisClient.get(cacheKey);
    expect(cachedBefore).toBeNull();
    
    // Act: Load state (should miss cache, load from DB, populate cache)
    const loadedState = await sessionStateManager.loadState(testAdminId, testPhone);
    
    // Assert: State loaded correctly
    expect(loadedState).toBeTruthy();
    expect(loadedState.name).toBe('Miss Test User');
    expect(loadedState.data.totalAmount).toBe(150);
    
    // Verify cache was populated
    const cachedAfter = await redisClient.get(cacheKey);
    expect(cachedAfter).toBeTruthy();
    const parsedCache = JSON.parse(cachedAfter);
    expect(parsedCache.name).toBe('Miss Test User');
  });
  
  it('should fall back to database when Redis is unavailable', async () => {
    // Arrange: Create a session manager without cache
    const noCacheManager = new SessionStateManager({
      db: pool,
      logger: mockLogger,
      serializer,
      cacheLayer: null, // No cache layer
      config: mockConfig
    });
    
    const userState = {
      step: 'awaiting_name',
      data: {},
      isReturningUser: false,
      clientId: 1004,
      name: 'Fallback User',
      email: 'fallback@example.com',
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
    
    // Act: Persist and load without cache
    await noCacheManager.persistState(testAdminId, '+9999999999', userState);
    const loadedState = await noCacheManager.loadState(testAdminId, '+9999999999');
    
    // Assert: Operations succeed without cache
    expect(loadedState).toBeTruthy();
    expect(loadedState.name).toBe('Fallback User');
  });
  
  it('should respect cache TTL matching USER_IDLE_TTL_MS', async () => {
    // Arrange: Create state with short TTL for testing
    const shortTtlConfig = { ...mockConfig, userIdleTtlMs: 2000 }; // 2 seconds
    
    const shortTtlCache = new CacheLayer({
      redisClient,
      logger: mockLogger,
      config: shortTtlConfig
    });
    
    const userState = {
      step: 'awaiting_name',
      data: {},
      isReturningUser: false,
      clientId: 1005,
      name: 'TTL Test User',
      email: 'ttl@example.com',
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
    
    // Act: Set cache with TTL
    await shortTtlCache.set(testAdminId, '+8888888888', userState, shortTtlConfig.userIdleTtlMs);
    
    // Verify cache exists
    const cacheKey = `conversation:${testAdminId}:+8888888888`;
    const cachedData = await redisClient.get(cacheKey);
    expect(cachedData).toBeTruthy();
    
    // Check TTL
    const ttl = await redisClient.ttl(cacheKey);
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(2); // Should be ~2 seconds
  });
  
  it('should invalidate cache on delete', async () => {
    // Arrange: Persist state (creates cache entry)
    const userState = {
      step: 'awaiting_name',
      data: {},
      isReturningUser: false,
      clientId: 1006,
      name: 'Delete Test User',
      email: 'delete@example.com',
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
    
    await sessionStateManager.persistState(testAdminId, '+7777777777', userState);
    
    // Verify cache exists
    const cacheKey = `conversation:${testAdminId}:+7777777777`;
    const cachedBefore = await redisClient.get(cacheKey);
    expect(cachedBefore).toBeTruthy();
    
    // Act: Delete state
    await sessionStateManager.deleteState(testAdminId, '+7777777777');
    
    // Assert: Cache invalidated
    const cachedAfter = await redisClient.get(cacheKey);
    expect(cachedAfter).toBeNull();
  });
  
  it('should handle pub/sub cache invalidation across instances', async () => {
    // Arrange: Create second Redis client (simulating another instance)
    const redisClient2 = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
    await redisClient2.connect();
    
    const cacheLayer2 = new CacheLayer({
      redisClient: redisClient2,
      logger: mockLogger,
      config: mockConfig
    });
    
    // Set up invalidation listener on second instance
    const invalidations = [];
    cacheLayer2.subscribeToInvalidations((adminId, phone) => {
      invalidations.push({ adminId, phone });
    });
    
    // Wait for subscription to be ready
    await new Promise(resolve => setTimeout(resolve, 100));
    
    // Act: Publish invalidation from first instance
    await cacheLayer.publishInvalidation(testAdminId, testPhone);
    
    // Wait for message to propagate
    await new Promise(resolve => setTimeout(resolve, 200));
    
    // Assert: Second instance received invalidation
    expect(invalidations.length).toBeGreaterThan(0);
    expect(invalidations[0].adminId).toBe(testAdminId);
    expect(invalidations[0].phone).toBe(testPhone);
    
    // Cleanup
    await redisClient2.quit();
  });
  
  it('should achieve 80% cache hit rate for active conversations', async () => {
    // Arrange: Create multiple conversations
    const conversationCount = 20;
    const phones = [];
    
    for (let i = 0; i < conversationCount; i++) {
      const phone = `+1${String(i).padStart(9, '0')}`;
      phones.push(phone);
      
      const userState = {
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
      
      await sessionStateManager.persistState(testAdminId, phone, userState);
    }
    
    // Act: Simulate active conversation pattern (repeated reads)
    let cacheHits = 0;
    let totalReads = 0;
    
    // Read each conversation 5 times (simulating active messaging)
    for (let round = 0; round < 5; round++) {
      for (const phone of phones) {
        // Check if in cache before loading
        const cacheKey = `conversation:${testAdminId}:${phone}`;
        const inCache = await redisClient.exists(cacheKey);
        
        if (inCache) {
          cacheHits++;
        }
        
        await sessionStateManager.loadState(testAdminId, phone);
        totalReads++;
      }
    }
    
    // Calculate hit rate
    const hitRate = (cacheHits / totalReads) * 100;
    
    // Assert: Cache hit rate >= 80%
    expect(hitRate).toBeGreaterThanOrEqual(80);
    
    console.log(`Cache hit rate: ${hitRate.toFixed(2)}% (${cacheHits}/${totalReads})`);
  });
  
  it('should handle concurrent cache operations without corruption', async () => {
    // Arrange: Create multiple concurrent operations
    const operations = [];
    
    for (let i = 0; i < 10; i++) {
      const phone = `+19${String(i).padStart(8, '0')}`;
      const userState = {
        step: 'awaiting_name',
        data: { index: i },
        isReturningUser: false,
        clientId: 3000 + i,
        name: `Concurrent User ${i}`,
        email: `concurrent${i}@example.com`,
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
      
      // Start persist and load operations concurrently
      operations.push(sessionStateManager.persistState(testAdminId, phone, userState));
      operations.push(sessionStateManager.loadState(testAdminId, phone));
    }
    
    // Act: Execute all operations concurrently
    const results = await Promise.allSettled(operations);
    
    // Assert: No operations failed
    const failures = results.filter(r => r.status === 'rejected');
    expect(failures).toHaveLength(0);
    
    // Verify all states persisted correctly
    const dbResult = await pool.query(
      'SELECT COUNT(*) as count FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    
    expect(parseInt(dbResult.rows[0].count)).toBe(10);
  });
});
