import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import fc from 'fast-check';

/**
 * Property-Based Tests for Feature Flag Behavior
 * 
 * Feature: session-state-persistence
 * Property 23: Gradual migration support
 * **Validates: Requirements 8.7, 12.1, 12.2, 12.3, 12.4**
 * 
 * Tests that the system functions correctly with ENABLE_SESSION_PERSISTENCE
 * in both enabled and disabled states, supporting gradual migration.
 */

describe('Feature Flag Behavior - Property Tests', () => {
  let originalEnv;

  beforeEach(() => {
    // Save original environment
    originalEnv = { ...process.env };
    
    // Clear module cache to allow re-importing with different env vars
    vi.resetModules();
  });

  afterEach(() => {
    // Restore original environment
    process.env = originalEnv;
    vi.resetModules();
  });

  /**
   * Property 23: Gradual migration support
   * **Validates: Requirements 8.7, 12.1, 12.2, 12.3, 12.4**
   * 
   * For any configuration state (enabled/disabled), the persistence layer
   * should initialize correctly and report its mode accurately.
   */
  it('property: persistence layer initializes correctly in both enabled and disabled modes', async () => {
    // Test with ENABLE_SESSION_PERSISTENCE=false
    process.env.ENABLE_SESSION_PERSISTENCE = 'false';
    
    // Re-import config with new environment
    const { persistenceConfig: disabledConfig } = await import('./config.js');
    
    // Verify persistence is disabled
    expect(disabledConfig.enabled).toBe(false);
    expect(disabledConfig).toHaveProperty('enabled');
    expect(disabledConfig).toHaveProperty('userIdleTtlMs');
    expect(disabledConfig).toHaveProperty('cleanupIntervalMs');
    
    // Clear modules for next test
    vi.resetModules();
    
    // Test with ENABLE_SESSION_PERSISTENCE=true
    process.env.ENABLE_SESSION_PERSISTENCE = 'true';
    
    // Re-import config with new environment
    const { persistenceConfig: enabledConfig } = await import('./config.js');
    
    // Verify persistence is enabled
    expect(enabledConfig.enabled).toBe(true);
    expect(enabledConfig).toHaveProperty('enabled');
    expect(enabledConfig).toHaveProperty('userIdleTtlMs');
    expect(enabledConfig).toHaveProperty('cleanupIntervalMs');
  });

  /**
   * Property: Configuration values remain consistent regardless of feature flag
   * 
   * The TTL, cleanup, and other configuration values should be loaded correctly
   * regardless of whether persistence is enabled or disabled.
   */
  it('property: configuration values are consistent regardless of feature flag state', async () => {
    const configArbitrary = fc.record({
      enabled: fc.boolean(),
      userIdleTtl: fc.integer({ min: 1000, max: 86400000 }), // 1 second to 24 hours
      cleanupInterval: fc.integer({ min: 1000, max: 3600000 }), // 1 second to 1 hour
      retentionDays: fc.integer({ min: 1, max: 365 }),
      retryAttempts: fc.integer({ min: 1, max: 10 }),
      batchWindow: fc.integer({ min: 0, max: 5000 }),
      circuitBreakerThreshold: fc.integer({ min: 1, max: 100 }),
      circuitBreakerReset: fc.integer({ min: 1000, max: 300000 }),
    });

    await fc.assert(
      fc.asyncProperty(configArbitrary, async (config) => {
        // Set environment variables
        process.env.ENABLE_SESSION_PERSISTENCE = config.enabled.toString();
        process.env.USER_IDLE_TTL_MS = config.userIdleTtl.toString();
        process.env.CLEANUP_INTERVAL_MS = config.cleanupInterval.toString();
        process.env.CONVERSATION_STATE_RETENTION_DAYS = config.retentionDays.toString();
        process.env.PERSISTENCE_RETRY_ATTEMPTS = config.retryAttempts.toString();
        process.env.PERSISTENCE_BATCH_WINDOW_MS = config.batchWindow.toString();
        process.env.CIRCUIT_BREAKER_THRESHOLD = config.circuitBreakerThreshold.toString();
        process.env.CIRCUIT_BREAKER_RESET_MS = config.circuitBreakerReset.toString();
        
        // Clear module cache
        vi.resetModules();
        
        // Import config
        const { persistenceConfig } = await import('./config.js');
        
        // Verify feature flag state
        expect(persistenceConfig.enabled).toBe(config.enabled);
        
        // Verify all configuration values are loaded correctly
        expect(persistenceConfig.userIdleTtlMs).toBe(config.userIdleTtl);
        expect(persistenceConfig.cleanupIntervalMs).toBe(config.cleanupInterval);
        expect(persistenceConfig.retentionDays).toBe(config.retentionDays);
        expect(persistenceConfig.retryAttempts).toBe(config.retryAttempts);
        expect(persistenceConfig.batchWindowMs).toBe(config.batchWindow);
        expect(persistenceConfig.circuitBreakerThreshold).toBe(config.circuitBreakerThreshold);
        expect(persistenceConfig.circuitBreakerResetMs).toBe(config.circuitBreakerReset);
        
        // Verify configuration is valid (positive values)
        expect(persistenceConfig.userIdleTtlMs).toBeGreaterThan(0);
        expect(persistenceConfig.cleanupIntervalMs).toBeGreaterThan(0);
        expect(persistenceConfig.retentionDays).toBeGreaterThan(0);
        expect(persistenceConfig.retryAttempts).toBeGreaterThanOrEqual(1);
        expect(persistenceConfig.batchWindowMs).toBeGreaterThanOrEqual(0);
        expect(persistenceConfig.circuitBreakerThreshold).toBeGreaterThanOrEqual(1);
        expect(persistenceConfig.circuitBreakerResetMs).toBeGreaterThan(0);
      }),
      { numRuns: 50 } // Run 50 iterations with different configurations
    );
  });

  /**
   * Property: Default values are applied when environment variables are missing
   * 
   * When environment variables are not set, the system should use sensible defaults
   * from the design document.
   */
  it('property: default values are applied when environment variables are missing', async () => {
    // Clear all persistence-related environment variables
    delete process.env.ENABLE_SESSION_PERSISTENCE;
    delete process.env.USER_IDLE_TTL_MS;
    delete process.env.CLEANUP_INTERVAL_MS;
    delete process.env.CONVERSATION_STATE_RETENTION_DAYS;
    delete process.env.PERSISTENCE_RETRY_ATTEMPTS;
    delete process.env.PERSISTENCE_BATCH_WINDOW_MS;
    delete process.env.CIRCUIT_BREAKER_THRESHOLD;
    delete process.env.CIRCUIT_BREAKER_RESET_MS;
    delete process.env.REDIS_URL;
    
    // Clear module cache
    vi.resetModules();
    
    // Import config
    const { persistenceConfig } = await import('./config.js');
    
    // Verify defaults from design document
    expect(persistenceConfig.enabled).toBe(false); // Default: disabled
    expect(persistenceConfig.userIdleTtlMs).toBe(21600000); // 6 hours
    expect(persistenceConfig.cleanupIntervalMs).toBe(900000); // 15 minutes
    expect(persistenceConfig.retentionDays).toBe(90);
    expect(persistenceConfig.retryAttempts).toBe(3);
    expect(persistenceConfig.batchWindowMs).toBe(500);
    expect(persistenceConfig.circuitBreakerThreshold).toBe(10);
    expect(persistenceConfig.circuitBreakerResetMs).toBe(60000); // 60 seconds
    expect(persistenceConfig.redisEnabled).toBe(false);
    expect(persistenceConfig.redisUrl).toBeNull();
  });

  /**
   * Property: Redis configuration is optional and independent of persistence flag
   * 
   * Redis can be enabled or disabled independently of the persistence feature flag.
   */
  it('property: Redis configuration is independent of persistence feature flag', async () => {
    const testCases = [
      { persistence: true, redis: 'redis://localhost:6379' },
      { persistence: true, redis: null },
      { persistence: false, redis: 'redis://localhost:6379' },
      { persistence: false, redis: null },
    ];

    for (const testCase of testCases) {
      // Set environment
      process.env.ENABLE_SESSION_PERSISTENCE = testCase.persistence.toString();
      if (testCase.redis) {
        process.env.REDIS_URL = testCase.redis;
      } else {
        delete process.env.REDIS_URL;
      }
      
      // Clear module cache
      vi.resetModules();
      
      // Import config
      const { persistenceConfig } = await import('./config.js');
      
      // Verify persistence flag
      expect(persistenceConfig.enabled).toBe(testCase.persistence);
      
      // Verify Redis configuration
      if (testCase.redis) {
        expect(persistenceConfig.redisEnabled).toBe(true);
        expect(persistenceConfig.redisUrl).toBe(testCase.redis);
      } else {
        expect(persistenceConfig.redisEnabled).toBe(false);
        expect(persistenceConfig.redisUrl).toBeNull();
      }
    }
  });

  /**
   * Property: Boolean parsing handles various string formats
   * 
   * The feature flag should correctly parse various boolean string representations.
   */
  it('property: boolean feature flag parsing handles various formats', async () => {
    const booleanArbitrary = fc.constantFrom(
      'true', 'TRUE', 'True',
      'false', 'FALSE', 'False',
      '1', '0', '', 'yes', 'no'
    );

    await fc.assert(
      fc.asyncProperty(booleanArbitrary, async (value) => {
        process.env.ENABLE_SESSION_PERSISTENCE = value;
        
        // Clear module cache
        vi.resetModules();
        
        // Import config
        const { persistenceConfig } = await import('./config.js');
        
        // Verify parsing
        const expectedEnabled = value.toLowerCase() === 'true';
        expect(persistenceConfig.enabled).toBe(expectedEnabled);
        
        // Config should always be valid
        expect(typeof persistenceConfig.enabled).toBe('boolean');
      }),
      { numRuns: 20 }
    );
  });

  /**
   * Property: Invalid configuration values are rejected with clear errors
   * 
   * When invalid configuration values are provided, the system should fail
   * with clear validation errors rather than silently using invalid values.
   */
  it('property: invalid configuration values are rejected', async () => {
    const invalidConfigs = [
      { key: 'USER_IDLE_TTL_MS', value: '-1000' },
      { key: 'CLEANUP_INTERVAL_MS', value: '-500' },
      { key: 'CONVERSATION_STATE_RETENTION_DAYS', value: '-10' },
      { key: 'PERSISTENCE_RETRY_ATTEMPTS', value: '0' },
      { key: 'PERSISTENCE_BATCH_WINDOW_MS', value: '-100' },
      { key: 'CIRCUIT_BREAKER_THRESHOLD', value: '0' },
      { key: 'CIRCUIT_BREAKER_RESET_MS', value: '-1000' },
    ];

    for (const invalidConfig of invalidConfigs) {
      // Set invalid value
      process.env[invalidConfig.key] = invalidConfig.value;
      
      // Clear module cache
      vi.resetModules();
      
      // Importing config should throw validation error
      await expect(async () => {
        await import('./config.js');
      }).rejects.toThrow(/configuration validation failed/i);
      
      // Clean up
      delete process.env[invalidConfig.key];
    }
  });
});
