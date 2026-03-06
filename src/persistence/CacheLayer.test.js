import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import CacheLayer from './CacheLayer.js';
import { createClient } from 'redis';

/**
 * Unit Tests for CacheLayer
 * 
 * Tests specific examples and edge cases for the CacheLayer component.
 * 
 * Requirements: 6.1, 6.2, 6.3, 6.4, 6.6
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

describe('CacheLayer Unit Tests', () => {
  let cacheLayer;
  let mockRedisClient;
  let mockSubscriber;
  
  beforeEach(() => {
    vi.clearAllMocks();
    
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
  
  describe('Initialization', () => {
    it('should initialize with Redis URL', async () => {
      cacheLayer = new CacheLayer({
        redisUrl: 'redis://localhost:6379',
        logger: mockLogger,
        config: { userIdleTtlMs: 21600000 }
      });
      
      await new Promise(resolve => setTimeout(resolve, 100));
      
      expect(createClient).toHaveBeenCalled();
      expect(mockRedisClient.connect).toHaveBeenCalled();
      expect(mockSubscriber.connect).toHaveBeenCalled();
    });
    
    it('should not initialize Redis when URL not provided', () => {
      cacheLayer = new CacheLayer({
        redisUrl: null,
        logger: mockLogger,
        config: { userIdleTtlMs: 21600000 }
      });
      
      expect(createClient).not.toHaveBeenCalled();
      expect(cacheLayer.isAvailable()).toBe(false);
    });
    
    it('should handle Redis connection errors gracefully', async () => {
      mockRedisClient.connect.mockRejectedValue(new Error('Connection failed'));
      
      cacheLayer = new CacheLayer({
        redisUrl: 'redis://localhost:6379',
        logger: mockLogger,
        config: { userIdleTtlMs: 21600000 }
      });
      
      await new Promise(resolve => setTimeout(resolve, 100));
      
      expect(mockLogger.warn).toHaveBeenCalledWith(
        expect.stringContaining('Failed to initialize Redis'),
        expect.any(Object)
      );
      expect(cacheLayer.isAvailable()).toBe(false);
    });
  });
  
  describe('Cache Operations with Redis Available', () => {
    beforeEach(async () => {
      cacheLayer = new CacheLayer({
        redisUrl: 'redis://localhost:6379',
        logger: mockLogger,
        config: { userIdleTtlMs: 21600000 }
      });
      
      await new Promise(resolve => setTimeout(resolve, 100));
      cacheLayer.available = true;
    });
    
    it('should get cached value on cache hit', async () => {
      const userState = {
        step: 'awaiting_name',
        data: { test: 'value' },
        lastUserMessageAt: new Date()
      };
      
      mockRedisClient.get.mockResolvedValue(JSON.stringify(userState));
      
      const result = await cacheLayer.get(1, '+1234567890');
      
      expect(result).toBeDefined();
      expect(result.step).toBe('awaiting_name');
      expect(mockRedisClient.get).toHaveBeenCalledWith('conversation:1:+1234567890');
    });
    
    it('should return null on cache miss', async () => {
      mockRedisClient.get.mockResolvedValue(null);
      
      const result = await cacheLayer.get(1, '+1234567890');
      
      expect(result).toBeNull();
      expect(mockRedisClient.get).toHaveBeenCalledWith('conversation:1:+1234567890');
    });
    
    it('should set cache with correct TTL', async () => {
      const userState = {
        step: 'awaiting_name',
        data: { test: 'value' }
      };
      
      mockRedisClient.setEx.mockResolvedValue('OK');
      
      const result = await cacheLayer.set(1, '+1234567890', userState, 21600000);
      
      expect(result).toBe(true);
      expect(mockRedisClient.setEx).toHaveBeenCalledWith(
        'conversation:1:+1234567890',
        21600, // 6 hours in seconds
        expect.any(String)
      );
      
      // Verify serialized data
      const serializedData = mockRedisClient.setEx.mock.calls[0][2];
      const parsed = JSON.parse(serializedData);
      expect(parsed.step).toBe('awaiting_name');
    });
    
    it('should delete cache entry', async () => {
      mockRedisClient.del.mockResolvedValue(1);
      
      const result = await cacheLayer.delete(1, '+1234567890');
      
      expect(result).toBe(true);
      expect(mockRedisClient.del).toHaveBeenCalledWith('conversation:1:+1234567890');
    });
    
    it('should invalidate all admin cache entries', async () => {
      mockRedisClient.keys.mockResolvedValue([
        'conversation:1:+1111111111',
        'conversation:1:+2222222222',
        'conversation:1:+3333333333'
      ]);
      mockRedisClient.del.mockResolvedValue(3);
      
      const result = await cacheLayer.invalidate(1);
      
      expect(result).toBe(3);
      expect(mockRedisClient.keys).toHaveBeenCalledWith('conversation:1:*');
      expect(mockRedisClient.del).toHaveBeenCalledWith([
        'conversation:1:+1111111111',
        'conversation:1:+2222222222',
        'conversation:1:+3333333333'
      ]);
    });
    
    it('should return 0 when invalidating admin with no cache entries', async () => {
      mockRedisClient.keys.mockResolvedValue([]);
      
      const result = await cacheLayer.invalidate(1);
      
      expect(result).toBe(0);
      expect(mockRedisClient.del).not.toHaveBeenCalled();
    });
  });
  
  describe('Cache Operations with Redis Unavailable', () => {
    beforeEach(() => {
      cacheLayer = new CacheLayer({
        redisUrl: 'redis://localhost:6379',
        logger: mockLogger,
        config: { userIdleTtlMs: 21600000 }
      });
      
      cacheLayer.available = false;
      mockRedisClient.isOpen = false;
    });
    
    it('should return null on get when Redis unavailable', async () => {
      const result = await cacheLayer.get(1, '+1234567890');
      
      expect(result).toBeNull();
      expect(mockRedisClient.get).not.toHaveBeenCalled();
    });
    
    it('should return false on set when Redis unavailable', async () => {
      const userState = { step: 'awaiting_name' };
      const result = await cacheLayer.set(1, '+1234567890', userState, 1000);
      
      expect(result).toBe(false);
      expect(mockRedisClient.setEx).not.toHaveBeenCalled();
    });
    
    it('should return false on delete when Redis unavailable', async () => {
      const result = await cacheLayer.delete(1, '+1234567890');
      
      expect(result).toBe(false);
      expect(mockRedisClient.del).not.toHaveBeenCalled();
    });
    
    it('should return 0 on invalidate when Redis unavailable', async () => {
      const result = await cacheLayer.invalidate(1);
      
      expect(result).toBe(0);
      expect(mockRedisClient.keys).not.toHaveBeenCalled();
    });
  });
  
  describe('Pub/Sub for Multi-Instance Cache Invalidation', () => {
    beforeEach(async () => {
      cacheLayer = new CacheLayer({
        redisUrl: 'redis://localhost:6379',
        logger: mockLogger,
        config: { userIdleTtlMs: 21600000 }
      });
      
      await new Promise(resolve => setTimeout(resolve, 100));
      cacheLayer.available = true;
    });
    
    it('should publish invalidation message', async () => {
      mockRedisClient.publish.mockResolvedValue(1);
      
      const result = await cacheLayer.publishInvalidation(1, '+1234567890');
      
      expect(result).toBe(true);
      expect(mockRedisClient.publish).toHaveBeenCalledWith(
        'conversation:invalidate',
        JSON.stringify({ adminId: 1, phone: '+1234567890' })
      );
    });
    
    it('should subscribe to invalidation messages', async () => {
      const callback = vi.fn();
      mockSubscriber.subscribe.mockResolvedValue(undefined);
      
      const result = await cacheLayer.subscribeToInvalidations(callback);
      
      expect(result).toBe(true);
      expect(mockSubscriber.subscribe).toHaveBeenCalledWith(
        'conversation:invalidate',
        expect.any(Function)
      );
    });
    
    it('should handle invalidation messages', async () => {
      const callback = vi.fn();
      let messageHandler;
      
      mockSubscriber.subscribe.mockImplementation((channel, handler) => {
        messageHandler = handler;
        return Promise.resolve();
      });
      
      await cacheLayer.subscribeToInvalidations(callback);
      
      // Simulate receiving invalidation message
      const message = JSON.stringify({ adminId: 1, phone: '+1234567890' });
      messageHandler(message);
      
      expect(callback).toHaveBeenCalledWith(1, '+1234567890');
    });
    
    it('should handle multiple callbacks for invalidation', async () => {
      const callback1 = vi.fn();
      const callback2 = vi.fn();
      let messageHandler;
      
      mockSubscriber.subscribe.mockImplementation((channel, handler) => {
        messageHandler = handler;
        return Promise.resolve();
      });
      
      await cacheLayer.subscribeToInvalidations(callback1);
      await cacheLayer.subscribeToInvalidations(callback2);
      
      // Simulate receiving invalidation message
      const message = JSON.stringify({ adminId: 1, phone: '+1234567890' });
      messageHandler(message);
      
      expect(callback1).toHaveBeenCalledWith(1, '+1234567890');
      expect(callback2).toHaveBeenCalledWith(1, '+1234567890');
    });
    
    it('should handle malformed invalidation messages', async () => {
      const callback = vi.fn();
      let messageHandler;
      
      mockSubscriber.subscribe.mockImplementation((channel, handler) => {
        messageHandler = handler;
        return Promise.resolve();
      });
      
      await cacheLayer.subscribeToInvalidations(callback);
      
      // Simulate receiving malformed message
      messageHandler('invalid json');
      
      expect(callback).not.toHaveBeenCalled();
      expect(mockLogger.error).toHaveBeenCalledWith(
        expect.stringContaining('Failed to parse invalidation message'),
        expect.any(Object)
      );
    });
    
    it('should return false when publishing with Redis unavailable', async () => {
      cacheLayer.available = false;
      mockRedisClient.isOpen = false;
      
      const result = await cacheLayer.publishInvalidation(1, '+1234567890');
      
      expect(result).toBe(false);
      expect(mockRedisClient.publish).not.toHaveBeenCalled();
    });
    
    it('should return false when subscribing with Redis unavailable', async () => {
      cacheLayer.available = false;
      mockRedisClient.isOpen = false;
      
      const result = await cacheLayer.subscribeToInvalidations(vi.fn());
      
      expect(result).toBe(false);
    });
  });
  
  describe('Error Handling', () => {
    beforeEach(async () => {
      cacheLayer = new CacheLayer({
        redisUrl: 'redis://localhost:6379',
        logger: mockLogger,
        config: { userIdleTtlMs: 21600000 }
      });
      
      await new Promise(resolve => setTimeout(resolve, 100));
      cacheLayer.available = true;
    });
    
    it('should handle get errors gracefully', async () => {
      mockRedisClient.get.mockRejectedValue(new Error('Connection timeout'));
      
      const result = await cacheLayer.get(1, '+1234567890');
      
      expect(result).toBeNull();
      expect(mockLogger.warn).toHaveBeenCalledWith(
        expect.stringContaining('Failed to get from cache'),
        expect.any(Object)
      );
    });
    
    it('should handle set errors gracefully', async () => {
      mockRedisClient.setEx.mockRejectedValue(new Error('Connection timeout'));
      
      const result = await cacheLayer.set(1, '+1234567890', { step: 'test' }, 1000);
      
      expect(result).toBe(false);
      expect(mockLogger.warn).toHaveBeenCalledWith(
        expect.stringContaining('Failed to set cache'),
        expect.any(Object)
      );
    });
    
    it('should handle delete errors gracefully', async () => {
      mockRedisClient.del.mockRejectedValue(new Error('Connection timeout'));
      
      const result = await cacheLayer.delete(1, '+1234567890');
      
      expect(result).toBe(false);
      expect(mockLogger.warn).toHaveBeenCalledWith(
        expect.stringContaining('Failed to delete from cache'),
        expect.any(Object)
      );
    });
    
    it('should handle invalidate errors gracefully', async () => {
      mockRedisClient.keys.mockRejectedValue(new Error('Connection timeout'));
      
      const result = await cacheLayer.invalidate(1);
      
      expect(result).toBe(0);
      expect(mockLogger.warn).toHaveBeenCalledWith(
        expect.stringContaining('Failed to invalidate admin cache'),
        expect.any(Object)
      );
    });
    
    it('should handle publish errors gracefully', async () => {
      mockRedisClient.publish.mockRejectedValue(new Error('Connection timeout'));
      
      const result = await cacheLayer.publishInvalidation(1, '+1234567890');
      
      expect(result).toBe(false);
      expect(mockLogger.warn).toHaveBeenCalledWith(
        expect.stringContaining('Failed to publish invalidation'),
        expect.any(Object)
      );
    });
  });
  
  describe('Phone Number Masking', () => {
    beforeEach(async () => {
      cacheLayer = new CacheLayer({
        redisUrl: 'redis://localhost:6379',
        logger: mockLogger,
        config: { userIdleTtlMs: 21600000 }
      });
      
      await new Promise(resolve => setTimeout(resolve, 100));
      cacheLayer.available = true;
    });
    
    it('should mask phone numbers in logs', async () => {
      mockRedisClient.get.mockResolvedValue(null);
      
      await cacheLayer.get(1, '+1234567890');
      
      expect(mockLogger.debug).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          phone: '****7890'
        })
      );
    });
    
    it('should mask short phone numbers', async () => {
      mockRedisClient.get.mockResolvedValue(null);
      
      await cacheLayer.get(1, '123');
      
      expect(mockLogger.debug).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          phone: '****'
        })
      );
    });
  });
  
  describe('Cache Hit Rate Tracking', () => {
    beforeEach(async () => {
      cacheLayer = new CacheLayer({
        redisUrl: 'redis://localhost:6379',
        logger: mockLogger,
        config: { userIdleTtlMs: 21600000 }
      });
      
      await new Promise(resolve => setTimeout(resolve, 100));
      cacheLayer.available = true;
    });
    
    it('should log cache hits', async () => {
      mockRedisClient.get.mockResolvedValue(JSON.stringify({ step: 'test' }));
      
      await cacheLayer.get(1, '+1234567890');
      
      expect(mockLogger.debug).toHaveBeenCalledWith(
        expect.stringContaining('Cache hit'),
        expect.any(Object)
      );
    });
    
    it('should log cache misses', async () => {
      mockRedisClient.get.mockResolvedValue(null);
      
      await cacheLayer.get(1, '+1234567890');
      
      expect(mockLogger.debug).toHaveBeenCalledWith(
        expect.stringContaining('Cache miss'),
        expect.any(Object)
      );
    });
  });
  
  describe('Connection Lifecycle', () => {
    it('should close Redis connections', async () => {
      cacheLayer = new CacheLayer({
        redisUrl: 'redis://localhost:6379',
        logger: mockLogger,
        config: { userIdleTtlMs: 21600000 }
      });
      
      await new Promise(resolve => setTimeout(resolve, 100));
      
      await cacheLayer.close();
      
      expect(mockRedisClient.quit).toHaveBeenCalled();
      expect(mockSubscriber.quit).toHaveBeenCalled();
      expect(cacheLayer.available).toBe(false);
    });
    
    it('should handle close errors gracefully', async () => {
      mockRedisClient.quit.mockRejectedValue(new Error('Already closed'));
      
      cacheLayer = new CacheLayer({
        redisUrl: 'redis://localhost:6379',
        logger: mockLogger,
        config: { userIdleTtlMs: 21600000 }
      });
      
      await new Promise(resolve => setTimeout(resolve, 100));
      await cacheLayer.close();
      
      expect(mockLogger.warn).toHaveBeenCalledWith(
        expect.stringContaining('Error closing Redis connections'),
        expect.any(Object)
      );
    });
  });
});
