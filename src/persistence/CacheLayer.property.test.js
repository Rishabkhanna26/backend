import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import fc from 'fast-check';
import CacheLayer from './CacheLayer.js';
import { createClient } from 'redis';

/**
 * Property-Based Tests for CacheLayer
 * 
 * These tests validate universal properties that should hold true
 * for all valid inputs and scenarios.
 * 
 * Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 7.6, 14.3
 */

// Mock Redis client
vi.mock('redis', () => ({
  createClient: vi.fn()
}));

// Mock logger
const mockLogger = {
  info: vi.fn(),
  warn: vi.fn(),
  debug: vi.fn(),
  error: vi.fn()
};

describe('CacheLayer Property Tests', () => {
  let cacheLayer;
  let mockRedisClient;
  let mockSubscriber;
  
  beforeEach(() => {
    // Reset mocks
    vi.clearAllMocks();
    
    // Create mock Redis clients
    mockRedisClient = {
      connect: vi.fn().mockResolvedValue(undefined),
      quit: vi.fn().mockResolvedValue(undefined),
      get: vi.fn(),
      setEx: vi.fn(),
      del: vi.fn(),
      keys: vi.fn(),
      publish: vi.fn(),
      subscribe: vi.fn(),
      on: vi.fn(),
      isOpen: true
    };
    
    mockSubscriber = {
      connect: vi.fn().mockResolvedValue(undefined),
      quit: vi.fn().mockResolvedValue(undefined),
      subscribe: vi.fn(),
      on: vi.fn()
    };
    
    mockRedisClient.duplicate = vi.fn().mockReturnValue(mockSubscriber);
    
    createClient.mockReturnValue(mockRedisClient);
  });
  
  afterEach(async () => {
    if (cacheLayer) {
      await cacheLayer.close();
    }
  });
  
  /**
   * Property 11: Cache TTL matches configuration
   * 
   * **Validates: Requirements 6.1**
   * 
   * For any conversation state cached in Redis, the cache entry should have
   * a TTL equal to USER_IDLE_TTL_MS, ensuring cache expiration aligns with
   * session expiration policy.
   */
  it('property: cache TTL matches configuration', async () => {
    const testConfig = {
      userIdleTtlMs: 21600000 // 6 hours
    };
    
    cacheLayer = new CacheLayer({
      redisUrl: 'redis://localhost:6379',
      logger: mockLogger,
      config: testConfig
    });
    
    // Wait for initialization
    await new Promise(resolve => setTimeout(resolve, 100));
    cacheLayer.available = true;
    
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 1000 }), // adminId
        fc.string({ minLength: 10, maxLength: 15 }), // phone
        fc.record({
          step: fc.string(),
          data: fc.object(),
          lastUserMessageAt: fc.date()
        }), // userState
        async (adminId, phone, userState) => {
          // Reset mock
          mockRedisClient.setEx.mockClear();
          
          // Set cache with configured TTL
          await cacheLayer.set(adminId, phone, userState, testConfig.userIdleTtlMs);
          
          // Verify setEx was called with correct TTL in seconds
          expect(mockRedisClient.setEx).toHaveBeenCalledWith(
            `conversation:${adminId}:${phone}`,
            21600, // 6 hours in seconds
            expect.any(String)
          );
        }
      ),
      { numRuns: 100 }
    );
  });
  
  /**
   * Property 12: Cache read-through pattern
   * 
   * **Validates: Requirements 6.2, 6.4**
   * 
   * For any state read operation when Redis is configured, the CacheLayer
   * should first check Redis, and on cache miss, return null (allowing the
   * caller to load from database and populate the cache).
   */
  it('property: cache read-through pattern', async () => {
    cacheLayer = new CacheLayer({
      redisUrl: 'redis://localhost:6379',
      logger: mockLogger,
      config: { userIdleTtlMs: 21600000 }
    });
    
    await new Promise(resolve => setTimeout(resolve, 100));
    cacheLayer.available = true;
    
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 1000 }),
        fc.string({ minLength: 10, maxLength: 15 }),
        fc.record({
          step: fc.string(),
          data: fc.object(),
          lastUserMessageAt: fc.date()
        }),
        fc.boolean(), // cache hit or miss
        async (adminId, phone, userState, cacheHit) => {
          mockRedisClient.get.mockClear();
          
          if (cacheHit) {
            // Simulate cache hit
            mockRedisClient.get.mockResolvedValue(JSON.stringify(userState));
            
            const result = await cacheLayer.get(adminId, phone);
            
            // Should return cached value
            expect(result).toBeDefined();
            expect(result.step).toBe(userState.step);
          } else {
            // Simulate cache miss
            mockRedisClient.get.mockResolvedValue(null);
            
            const result = await cacheLayer.get(adminId, phone);
            
            // Should return null on cache miss
            expect(result).toBeNull();
          }
          
          // Verify Redis was checked
          expect(mockRedisClient.get).toHaveBeenCalledWith(
            `conversation:${adminId}:${phone}`
          );
        }
      ),
      { numRuns: 100 }
    );
  });
  
  /**
   * Property 13: Cache write-through pattern
   * 
   * **Validates: Requirements 6.3**
   * 
   * For any state write operation when Redis is configured, the CacheLayer
   * should update Redis with the appropriate TTL. The caller is responsible
   * for also updating the database to ensure consistency.
   */
  it('property: cache write-through pattern', async () => {
    cacheLayer = new CacheLayer({
      redisUrl: 'redis://localhost:6379',
      logger: mockLogger,
      config: { userIdleTtlMs: 21600000 }
    });
    
    await new Promise(resolve => setTimeout(resolve, 100));
    cacheLayer.available = true;
    
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 1000 }),
        fc.string({ minLength: 10, maxLength: 15 }),
        fc.record({
          step: fc.string(),
          data: fc.object(),
          lastUserMessageAt: fc.date()
        }),
        fc.integer({ min: 1000, max: 86400000 }), // TTL between 1s and 24h
        async (adminId, phone, userState, ttlMs) => {
          mockRedisClient.setEx.mockClear();
          mockRedisClient.setEx.mockResolvedValue('OK');
          
          // Write to cache
          const result = await cacheLayer.set(adminId, phone, userState, ttlMs);
          
          // Should succeed
          expect(result).toBe(true);
          
          // Verify Redis was updated with correct TTL
          expect(mockRedisClient.setEx).toHaveBeenCalledWith(
            `conversation:${adminId}:${phone}`,
            Math.floor(ttlMs / 1000),
            expect.any(String)
          );
          
          // Verify serialized data is valid JSON
          const serializedData = mockRedisClient.setEx.mock.calls[0][2];
          const parsed = JSON.parse(serializedData);
          expect(parsed.step).toBe(userState.step);
        }
      ),
      { numRuns: 100 }
    );
  });
  
  /**
   * Property 14: Cache key format consistency
   * 
   * **Validates: Requirements 6.5**
   * 
   * For any cache operation, the CacheLayer should use keys in the format
   * "conversation:{admin_id}:{phone}", ensuring consistent key naming across
   * all cache operations.
   */
  it('property: cache key format consistency', async () => {
    cacheLayer = new CacheLayer({
      redisUrl: 'redis://localhost:6379',
      logger: mockLogger,
      config: { userIdleTtlMs: 21600000 }
    });
    
    await new Promise(resolve => setTimeout(resolve, 100));
    cacheLayer.available = true;
    
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 1000 }),
        fc.string({ minLength: 10, maxLength: 15 }),
        fc.record({
          step: fc.string(),
          data: fc.object()
        }),
        async (adminId, phone, userState) => {
          const expectedKey = `conversation:${adminId}:${phone}`;
          
          // Test get operation
          mockRedisClient.get.mockClear();
          mockRedisClient.get.mockResolvedValue(null);
          await cacheLayer.get(adminId, phone);
          expect(mockRedisClient.get).toHaveBeenCalledWith(expectedKey);
          
          // Test set operation
          mockRedisClient.setEx.mockClear();
          mockRedisClient.setEx.mockResolvedValue('OK');
          await cacheLayer.set(adminId, phone, userState, 1000);
          expect(mockRedisClient.setEx).toHaveBeenCalledWith(
            expectedKey,
            expect.any(Number),
            expect.any(String)
          );
          
          // Test delete operation
          mockRedisClient.del.mockClear();
          mockRedisClient.del.mockResolvedValue(1);
          await cacheLayer.delete(adminId, phone);
          expect(mockRedisClient.del).toHaveBeenCalledWith(expectedKey);
        }
      ),
      { numRuns: 100 }
    );
  });
  
  /**
   * Property 15: Redis failure fallback
   * 
   * **Validates: Requirements 6.6, 14.3**
   * 
   * For any Redis connection failure or timeout, the CacheLayer should fall
   * back gracefully without throwing errors, allowing the system to continue
   * operating with direct database access.
   */
  it('property: Redis failure fallback', async () => {
    cacheLayer = new CacheLayer({
      redisUrl: 'redis://localhost:6379',
      logger: mockLogger,
      config: { userIdleTtlMs: 21600000 }
    });
    
    await new Promise(resolve => setTimeout(resolve, 100));
    
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 1000 }),
        fc.string({ minLength: 10, maxLength: 15 }),
        fc.record({
          step: fc.string(),
          data: fc.object()
        }),
        async (adminId, phone, userState) => {
          // Simulate Redis unavailable
          cacheLayer.available = false;
          mockRedisClient.isOpen = false;
          
          // All operations should return gracefully without throwing
          const getResult = await cacheLayer.get(adminId, phone);
          expect(getResult).toBeNull();
          
          const setResult = await cacheLayer.set(adminId, phone, userState, 1000);
          expect(setResult).toBe(false);
          
          const deleteResult = await cacheLayer.delete(adminId, phone);
          expect(deleteResult).toBe(false);
          
          const invalidateResult = await cacheLayer.invalidate(adminId);
          expect(invalidateResult).toBe(0);
          
          const publishResult = await cacheLayer.publishInvalidation(adminId, phone);
          expect(publishResult).toBe(false);
          
          // Verify isAvailable returns false
          expect(cacheLayer.isAvailable()).toBe(false);
          
          // Verify warnings were logged (not errors)
          // The system should continue operating
        }
      ),
      { numRuns: 50 }
    );
  });
  
  /**
   * Property: Redis errors are handled gracefully
   * 
   * For any Redis operation that throws an error, the CacheLayer should
   * catch the error, log a warning, and return a safe default value.
   */
  it('property: Redis errors are handled gracefully', async () => {
    cacheLayer = new CacheLayer({
      redisUrl: 'redis://localhost:6379',
      logger: mockLogger,
      config: { userIdleTtlMs: 21600000 }
    });
    
    await new Promise(resolve => setTimeout(resolve, 100));
    cacheLayer.available = true;
    
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 1000 }),
        fc.string({ minLength: 10, maxLength: 15 }),
        fc.record({
          step: fc.string(),
          data: fc.object()
        }),
        async (adminId, phone, userState) => {
          // Simulate Redis errors
          mockRedisClient.get.mockRejectedValue(new Error('Redis connection timeout'));
          mockRedisClient.setEx.mockRejectedValue(new Error('Redis connection timeout'));
          mockRedisClient.del.mockRejectedValue(new Error('Redis connection timeout'));
          
          // All operations should handle errors gracefully
          const getResult = await cacheLayer.get(adminId, phone);
          expect(getResult).toBeNull();
          
          const setResult = await cacheLayer.set(adminId, phone, userState, 1000);
          expect(setResult).toBe(false);
          
          const deleteResult = await cacheLayer.delete(adminId, phone);
          expect(deleteResult).toBe(false);
          
          // Verify warnings were logged
          expect(mockLogger.warn).toHaveBeenCalled();
        }
      ),
      { numRuns: 50 }
    );
  });
});
