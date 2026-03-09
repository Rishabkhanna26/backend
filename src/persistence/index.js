import { createClient } from 'redis';
import db from '../../config/database.js';
import logger from '../../config/logger.js';
import { Sentry } from '../../config/sentry.js';
import { persistenceConfig } from './config.js';
import { SessionStateManager } from './SessionStateManager.js';
import { RecoveryManager } from './RecoveryManager.js';
import { SessionCleanupService } from './SessionCleanupService.js';
import { StateSerializer } from './StateSerializer.js';

/**
 * Persistence Layer Initialization
 * 
 * Main entry point for the session state persistence layer.
 * Conditionally initializes components based on configuration.
 * 
 * Requirements: 12.1, 12.2, 12.3
 */

let sessionStateManager = null;
let recoveryManager = null;
let sessionCleanupService = null;
let cacheLayer = null;
let redisClient = null;

/**
 * Initialize Redis client if REDIS_URL is provided
 */
async function initializeRedis() {
  if (!persistenceConfig.redisEnabled) {
    logger.info('Redis caching: DISABLED (REDIS_URL not provided)');
    return null;
  }
  
  try {
    const client = createClient({
      url: persistenceConfig.redisUrl,
      socket: {
        reconnectStrategy: (retries) => {
          // Exponential backoff with max 3 seconds
          const delay = Math.min(retries * 100, 3000);
          logger.warn(`Redis reconnecting in ${delay}ms (attempt ${retries})`);
          return delay;
        },
      },
    });
    
    // Handle Redis errors
    client.on('error', (err) => {
      logger.error('Redis client error', { error: err.message });
    });
    
    client.on('connect', () => {
      logger.info('✅ Redis client connected');
    });
    
    client.on('reconnecting', () => {
      logger.warn('Redis client reconnecting...');
    });
    
    client.on('ready', () => {
      logger.info('✅ Redis client ready');
    });
    
    // Connect to Redis
    await client.connect();
    
    logger.info('✅ Redis caching: ENABLED', {
      url: persistenceConfig.redisUrl.replace(/:[^:@]+@/, ':***@'), // Mask password
    });
    
    return client;
  } catch (error) {
    logger.error('Failed to initialize Redis client', {
      error: error.message,
      stack: error.stack,
    });
    logger.warn('Continuing without Redis caching (will use database only)');
    return null;
  }
}

/**
 * Initialize CacheLayer wrapper around Redis client
 */
async function initializeCacheLayer(redisClient) {
  if (!redisClient) {
    return null;
  }
  
  try {
    // Dynamically import CacheLayer only if Redis is available
    const { CacheLayer } = await import('./CacheLayer.js');
    
    const cache = new CacheLayer({
      redisClient,
      logger,
      config: {
        ttlMs: persistenceConfig.userIdleTtlMs,
      },
    });
    
    logger.info('✅ Cache layer initialized');
    return cache;
  } catch (error) {
    // CacheLayer may not be implemented yet (task 8)
    if (error.code === 'ERR_MODULE_NOT_FOUND') {
      logger.warn('CacheLayer not implemented yet, continuing without cache');
    } else {
      logger.error('Failed to initialize cache layer', {
        error: error.message,
        stack: error.stack,
      });
    }
    return null;
  }
}

/**
 * Initialize all persistence components
 */
async function initializePersistence() {
  if (!persistenceConfig.enabled) {
    logger.info('Persistence layer: NOT INITIALIZED (ENABLE_SESSION_PERSISTENCE=false)');
    return;
  }
  
  try {
    // Initialize Redis client (optional)
    redisClient = await initializeRedis();
    
    // Initialize cache layer (optional, requires Redis)
    cacheLayer = await initializeCacheLayer(redisClient);
    
    // Initialize SessionStateManager
    sessionStateManager = new SessionStateManager({
      db,
      logger,
      sentry: Sentry,
      cacheLayer,
      config: {
        enabled: persistenceConfig.enabled,
        retryAttempts: persistenceConfig.retryAttempts,
        retryDelayMs: persistenceConfig.retryDelayMs,
        batchWindowMs: persistenceConfig.batchWindowMs,
        circuitBreakerThreshold: persistenceConfig.circuitBreakerThreshold,
        circuitBreakerResetMs: persistenceConfig.circuitBreakerResetMs,
      },
    });
    
    logger.info('✅ SessionStateManager initialized');
    
    // Initialize RecoveryManager
    const serializer = new StateSerializer();
    recoveryManager = new RecoveryManager({
      db,
      logger,
      serializer,
      config: {
        userIdleTtlMs: persistenceConfig.userIdleTtlMs,
      },
    });
    
    logger.info('✅ RecoveryManager initialized');
    
    // Initialize SessionCleanupService
    sessionCleanupService = new SessionCleanupService({
      db,
      logger,
      config: {
        cleanupIntervalMs: persistenceConfig.cleanupIntervalMs,
        userIdleTtlMs: persistenceConfig.userIdleTtlMs,
        retentionDays: persistenceConfig.retentionDays,
      },
    });
    
    // Start cleanup service
    sessionCleanupService.start();
    
    logger.info('✅ SessionCleanupService initialized and started');
    
    logger.info('🎉 Persistence layer fully initialized');
  } catch (error) {
    logger.error('Failed to initialize persistence layer', {
      error: error.message,
      stack: error.stack,
    });
    throw error;
  }
}

/**
 * Graceful shutdown of persistence layer
 */
async function shutdownPersistence() {
  logger.info('Shutting down persistence layer...');
  
  try {
    // Stop cleanup service
    if (sessionCleanupService) {
      sessionCleanupService.stop();
      logger.info('SessionCleanupService stopped');
    }
    
    // Close Redis connection
    if (redisClient) {
      await redisClient.quit();
      logger.info('Redis client disconnected');
    }
    
    logger.info('✅ Persistence layer shutdown complete');
  } catch (error) {
    logger.error('Error during persistence layer shutdown', {
      error: error.message,
      stack: error.stack,
    });
  }
}

// Initialize persistence layer on module load
await initializePersistence();

// Export initialized components
export {
  sessionStateManager,
  recoveryManager,
  sessionCleanupService,
  cacheLayer,
  redisClient,
  persistenceConfig,
  shutdownPersistence,
};

// Export default object with all components
export default {
  sessionStateManager,
  recoveryManager,
  sessionCleanupService,
  cacheLayer,
  redisClient,
  config: persistenceConfig,
  shutdown: shutdownPersistence,
};
