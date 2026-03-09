import dotenv from 'dotenv';
import logger from '../../config/logger.js';

dotenv.config();

/**
 * Persistence Configuration Module
 * 
 * Centralizes all configuration for the session state persistence layer.
 * Validates required environment variables and provides sensible defaults.
 * 
 * Requirements: 12.1, 12.2, 12.3, 12.7
 */

// Parse boolean environment variable
const parseBoolean = (value, defaultValue = false) => {
  if (value === undefined || value === null) return defaultValue;
  return value.toLowerCase() === 'true';
};

// Parse integer environment variable
const parseInt = (value, defaultValue) => {
  const parsed = Number.parseInt(value, 10);
  return isNaN(parsed) ? defaultValue : parsed;
};

// Persistence feature flag
const ENABLE_SESSION_PERSISTENCE = parseBoolean(
  process.env.ENABLE_SESSION_PERSISTENCE,
  false
);

// Redis configuration (optional)
const REDIS_URL = process.env.REDIS_URL || null;

// Session TTL configuration
const USER_IDLE_TTL_MS = parseInt(
  process.env.USER_IDLE_TTL_MS,
  21600000 // 6 hours default
);

// Cleanup configuration
const CLEANUP_INTERVAL_MS = parseInt(
  process.env.CLEANUP_INTERVAL_MS,
  900000 // 15 minutes default
);

// Data retention configuration
const CONVERSATION_STATE_RETENTION_DAYS = parseInt(
  process.env.CONVERSATION_STATE_RETENTION_DAYS,
  90
);

// Retry configuration
const PERSISTENCE_RETRY_ATTEMPTS = parseInt(
  process.env.PERSISTENCE_RETRY_ATTEMPTS,
  3
);

// Batching configuration
const PERSISTENCE_BATCH_WINDOW_MS = parseInt(
  process.env.PERSISTENCE_BATCH_WINDOW_MS,
  500
);

// Circuit breaker configuration
const CIRCUIT_BREAKER_THRESHOLD = parseInt(
  process.env.CIRCUIT_BREAKER_THRESHOLD,
  10
);

const CIRCUIT_BREAKER_RESET_MS = parseInt(
  process.env.CIRCUIT_BREAKER_RESET_MS,
  60000 // 60 seconds default
);

// Export configuration object
export const persistenceConfig = {
  // Feature flags
  enabled: ENABLE_SESSION_PERSISTENCE,
  redisEnabled: REDIS_URL !== null,
  
  // Redis configuration
  redisUrl: REDIS_URL,
  
  // TTL configuration
  userIdleTtlMs: USER_IDLE_TTL_MS,
  
  // Cleanup configuration
  cleanupIntervalMs: CLEANUP_INTERVAL_MS,
  retentionDays: CONVERSATION_STATE_RETENTION_DAYS,
  
  // Retry configuration
  retryAttempts: PERSISTENCE_RETRY_ATTEMPTS,
  retryDelayMs: 100, // Initial delay, will use exponential backoff
  
  // Batching configuration
  batchWindowMs: PERSISTENCE_BATCH_WINDOW_MS,
  
  // Circuit breaker configuration
  circuitBreakerThreshold: CIRCUIT_BREAKER_THRESHOLD,
  circuitBreakerResetMs: CIRCUIT_BREAKER_RESET_MS,
};

// Validate configuration
function validateConfig() {
  const errors = [];
  
  // Validate TTL values
  if (persistenceConfig.userIdleTtlMs < 0) {
    errors.push('USER_IDLE_TTL_MS must be a positive number');
  }
  
  if (persistenceConfig.cleanupIntervalMs < 0) {
    errors.push('CLEANUP_INTERVAL_MS must be a positive number');
  }
  
  if (persistenceConfig.retentionDays < 0) {
    errors.push('CONVERSATION_STATE_RETENTION_DAYS must be a positive number');
  }
  
  // Validate retry configuration
  if (persistenceConfig.retryAttempts < 1) {
    errors.push('PERSISTENCE_RETRY_ATTEMPTS must be at least 1');
  }
  
  // Validate batching configuration
  if (persistenceConfig.batchWindowMs < 0) {
    errors.push('PERSISTENCE_BATCH_WINDOW_MS must be a positive number');
  }
  
  // Validate circuit breaker configuration
  if (persistenceConfig.circuitBreakerThreshold < 1) {
    errors.push('CIRCUIT_BREAKER_THRESHOLD must be at least 1');
  }
  
  if (persistenceConfig.circuitBreakerResetMs < 0) {
    errors.push('CIRCUIT_BREAKER_RESET_MS must be a positive number');
  }
  
  if (errors.length > 0) {
    throw new Error(`Persistence configuration validation failed:\n${errors.join('\n')}`);
  }
}

// Log configuration on startup
function logConfiguration() {
  if (!persistenceConfig.enabled) {
    logger.info('📦 Session persistence: DISABLED (using in-memory storage only)');
    return;
  }
  
  logger.info('📦 Session persistence: ENABLED', {
    redis: persistenceConfig.redisEnabled ? 'enabled' : 'disabled',
    userIdleTtl: `${persistenceConfig.userIdleTtlMs}ms (${persistenceConfig.userIdleTtlMs / 3600000}h)`,
    cleanupInterval: `${persistenceConfig.cleanupIntervalMs}ms (${persistenceConfig.cleanupIntervalMs / 60000}min)`,
    retentionDays: persistenceConfig.retentionDays,
    retryAttempts: persistenceConfig.retryAttempts,
    batchWindow: `${persistenceConfig.batchWindowMs}ms`,
    circuitBreaker: {
      threshold: persistenceConfig.circuitBreakerThreshold,
      resetMs: `${persistenceConfig.circuitBreakerResetMs}ms`,
    },
  });
}

// Validate and log configuration on module load
try {
  validateConfig();
  logConfiguration();
} catch (error) {
  logger.error('Failed to initialize persistence configuration', {
    error: error.message,
  });
  throw error;
}

export default persistenceConfig;
