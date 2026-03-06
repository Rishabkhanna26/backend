import { describe, it, expect } from 'vitest';
import { persistenceConfig } from './config.js';

/**
 * Configuration Module Tests
 * 
 * These tests verify that the configuration module correctly loads
 * and validates environment variables. Since the config is loaded
 * once on module import, these tests verify the current configuration
 * based on the .env file.
 */

describe('Persistence Configuration', () => {
  describe('Configuration structure', () => {
    it('should have all required configuration properties', () => {
      expect(persistenceConfig).toBeDefined();
      expect(persistenceConfig).toHaveProperty('enabled');
      expect(persistenceConfig).toHaveProperty('redisEnabled');
      expect(persistenceConfig).toHaveProperty('redisUrl');
      expect(persistenceConfig).toHaveProperty('userIdleTtlMs');
      expect(persistenceConfig).toHaveProperty('cleanupIntervalMs');
      expect(persistenceConfig).toHaveProperty('retentionDays');
      expect(persistenceConfig).toHaveProperty('retryAttempts');
      expect(persistenceConfig).toHaveProperty('retryDelayMs');
      expect(persistenceConfig).toHaveProperty('batchWindowMs');
      expect(persistenceConfig).toHaveProperty('circuitBreakerThreshold');
      expect(persistenceConfig).toHaveProperty('circuitBreakerResetMs');
    });
    
    it('should have correct types for all properties', () => {
      expect(typeof persistenceConfig.enabled).toBe('boolean');
      expect(typeof persistenceConfig.redisEnabled).toBe('boolean');
      expect(typeof persistenceConfig.userIdleTtlMs).toBe('number');
      expect(typeof persistenceConfig.cleanupIntervalMs).toBe('number');
      expect(typeof persistenceConfig.retentionDays).toBe('number');
      expect(typeof persistenceConfig.retryAttempts).toBe('number');
      expect(typeof persistenceConfig.retryDelayMs).toBe('number');
      expect(typeof persistenceConfig.batchWindowMs).toBe('number');
      expect(typeof persistenceConfig.circuitBreakerThreshold).toBe('number');
      expect(typeof persistenceConfig.circuitBreakerResetMs).toBe('number');
    });
  });
  
  describe('Configuration values', () => {
    it('should have positive TTL values', () => {
      expect(persistenceConfig.userIdleTtlMs).toBeGreaterThan(0);
      expect(persistenceConfig.cleanupIntervalMs).toBeGreaterThan(0);
    });
    
    it('should have positive retention days', () => {
      expect(persistenceConfig.retentionDays).toBeGreaterThan(0);
    });
    
    it('should have at least 1 retry attempt', () => {
      expect(persistenceConfig.retryAttempts).toBeGreaterThanOrEqual(1);
    });
    
    it('should have positive batch window', () => {
      expect(persistenceConfig.batchWindowMs).toBeGreaterThanOrEqual(0);
    });
    
    it('should have at least 1 circuit breaker threshold', () => {
      expect(persistenceConfig.circuitBreakerThreshold).toBeGreaterThanOrEqual(1);
    });
    
    it('should have positive circuit breaker reset time', () => {
      expect(persistenceConfig.circuitBreakerResetMs).toBeGreaterThan(0);
    });
  });
  
  describe('Redis configuration', () => {
    it('should have consistent Redis configuration', () => {
      if (persistenceConfig.redisEnabled) {
        expect(persistenceConfig.redisUrl).toBeTruthy();
        expect(typeof persistenceConfig.redisUrl).toBe('string');
      } else {
        expect(persistenceConfig.redisUrl).toBeNull();
      }
    });
  });
  
  describe('Default values', () => {
    it('should use sensible defaults when not configured', () => {
      // These are the expected defaults from the design document
      // The actual values may differ if environment variables are set
      
      // Default user idle TTL: 6 hours
      if (process.env.USER_IDLE_TTL_MS === undefined) {
        expect(persistenceConfig.userIdleTtlMs).toBe(21600000);
      }
      
      // Default cleanup interval: 15 minutes
      if (process.env.CLEANUP_INTERVAL_MS === undefined) {
        expect(persistenceConfig.cleanupIntervalMs).toBe(900000);
      }
      
      // Default retention: 90 days
      if (process.env.CONVERSATION_STATE_RETENTION_DAYS === undefined) {
        expect(persistenceConfig.retentionDays).toBe(90);
      }
      
      // Default retry attempts: 3
      if (process.env.PERSISTENCE_RETRY_ATTEMPTS === undefined) {
        expect(persistenceConfig.retryAttempts).toBe(3);
      }
      
      // Default batch window: 500ms
      if (process.env.PERSISTENCE_BATCH_WINDOW_MS === undefined) {
        expect(persistenceConfig.batchWindowMs).toBe(500);
      }
      
      // Default circuit breaker threshold: 10
      if (process.env.CIRCUIT_BREAKER_THRESHOLD === undefined) {
        expect(persistenceConfig.circuitBreakerThreshold).toBe(10);
      }
      
      // Default circuit breaker reset: 60 seconds
      if (process.env.CIRCUIT_BREAKER_RESET_MS === undefined) {
        expect(persistenceConfig.circuitBreakerResetMs).toBe(60000);
      }
    });
  });
});
