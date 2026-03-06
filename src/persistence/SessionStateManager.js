import StateSerializer from './StateSerializer.js';
import CircuitBreaker from './CircuitBreaker.js';
import BatchWriter from './BatchWriter.js';

/**
 * SessionStateManager orchestrates state persistence operations with retry logic,
 * circuit breaker pattern, optional Redis caching, and batching for rapid updates.
 * 
 * Core component for managing conversation state persistence to PostgreSQL with
 * cache-through pattern for optimal performance.
 * 
 * Requirements: 3.1, 3.2, 3.3, 3.7, 13.3, 13.4, 14.4, 14.5, 14.6
 */
class SessionStateManager {
  /**
   * Create a SessionStateManager instance
   * 
   * @param {Object} options - Configuration options
   * @param {Object} options.db - Database connection pool
   * @param {Object} options.logger - Winston logger instance
   * @param {Object} options.sentry - Sentry error tracking instance (optional)
   * @param {Object} options.cacheLayer - Redis cache layer instance (optional)
   * @param {Object} options.config - Configuration object
   * @param {boolean} options.config.enabled - Enable/disable persistence
   * @param {number} options.config.retryAttempts - Number of retry attempts (default: 3)
   * @param {number} options.config.retryDelayMs - Initial retry delay in ms (default: 100)
   * @param {number} options.config.batchWindowMs - Batching window in ms (default: 500)
   * @param {number} options.config.circuitBreakerThreshold - Failures before opening circuit (default: 10)
   * @param {number} options.config.circuitBreakerResetMs - Time before auto-reset in ms (default: 60000)
   */
  constructor({ db, logger, sentry = null, cacheLayer = null, config = {} }) {
    this.db = db;
    this.logger = logger;
    this.sentry = sentry;
    this.cacheLayer = cacheLayer;
    this.serializer = new StateSerializer();
    
    // Configuration
    this.config = {
      enabled: config.enabled !== undefined ? config.enabled : true,
      retryAttempts: config.retryAttempts || 3,
      retryDelayMs: config.retryDelayMs || 100,
      batchWindowMs: config.batchWindowMs || 500,
      circuitBreakerThreshold: config.circuitBreakerThreshold || 10,
      circuitBreakerResetMs: config.circuitBreakerResetMs || 60000,
      ...config
    };

    // Initialize circuit breaker
    this.circuitBreaker = new CircuitBreaker({
      threshold: this.config.circuitBreakerThreshold,
      resetMs: this.config.circuitBreakerResetMs,
      logger: this.logger
    });

    // Initialize batch writer
    this.batchWriter = new BatchWriter({
      flushIntervalMs: this.config.batchWindowMs,
      writeFn: async (adminId, phone, serializedState) => {
        return await this._retryWithBackoff(async () => {
          return await this._upsertState(adminId, phone, serializedState);
        });
      },
      logger: this.logger
    });

    // Initialize metrics tracking
    this.metrics = {
      state_save_duration: [],
      state_load_duration: [],
      persistence_errors: 0,
      cache_hits: 0,
      cache_misses: 0,
      total_saves: 0,
      total_loads: 0,
      slow_operations: 0
    };

    this.logger.info('SessionStateManager initialized', {
      enabled: this.config.enabled,
      retryAttempts: this.config.retryAttempts,
      batchWindowMs: this.config.batchWindowMs,
      cacheEnabled: !!this.cacheLayer,
      circuitBreakerThreshold: this.config.circuitBreakerThreshold,
      circuitBreakerResetMs: this.config.circuitBreakerResetMs
    });
  }

  /**
   * Check if persistence is enabled
   * 
   * @returns {boolean} True if persistence is enabled
   */
  isEnabled() {
    return this.config.enabled;
  }

  /**
   * Check if circuit breaker is open
   * 
   * @returns {boolean} True if circuit is open (falling back to in-memory)
   */
  isCircuitOpen() {
    return this.circuitBreaker.isOpen();
  }

  /**
   * Reset the circuit breaker manually
   * Used for testing or manual intervention
   */
  resetCircuit() {
    this.circuitBreaker.reset();
  }

  /**
   * Persist a user's conversation state to the database
   * 
   * Implements async upsert with batching, retry logic, and optional cache update.
   * Uses parameterized queries to prevent SQL injection.
   * Falls back to in-memory when circuit breaker is open.
   * 
   * Batching: Multiple updates to the same (adminId, phone) within the batch window
   * (default 500ms) are coalesced into a single database write.
   * 
   * @param {number} adminId - The admin ID
   * @param {string} phone - The user's phone number
   * @param {Object} userState - The user's conversation state object
   * @returns {Promise<boolean>} True if persistence succeeded (or scheduled for batching)
   */
  async persistState(adminId, phone, userState) {
    if (!this.isEnabled()) {
      return false;
    }

    // Check circuit breaker - fail fast if open
    if (this.circuitBreaker.isOpen()) {
      this.logger.debug('SessionStateManager: Circuit breaker open, skipping persistence', {
        adminId,
        phone: this._maskPhone(phone),
        circuitState: this.circuitBreaker.getState()
      });
      return false;
    }

    const startTime = Date.now();

    try {
      // Validate inputs
      if (!adminId || !phone || !userState) {
        this.logger.warn('SessionStateManager: Invalid parameters for persistState', {
          hasAdminId: !!adminId,
          hasPhone: !!phone,
          hasUserState: !!userState
        });
        return false;
      }

      // Serialize the user state
      const serialized = this.serializer.serialize(userState);
      if (!serialized) {
        this.logger.error('SessionStateManager: Failed to serialize user state', {
          adminId,
          phone: this._maskPhone(phone)
        });
        return false;
      }

      // Update cache if available (fire-and-forget)
      if (this.cacheLayer && this.cacheLayer.isAvailable()) {
        this.cacheLayer.set(adminId, phone, userState).catch(err => {
          this.logger.warn('SessionStateManager: Cache update failed', {
            adminId,
            phone: this._maskPhone(phone),
            error: err.message
          });
        });
      }

      // Schedule for batching - this coalesces rapid updates
      this.batchWriter.schedule(adminId, phone, userState, serialized);

      const duration = Date.now() - startTime;

      // Track metrics
      this.metrics.state_save_duration.push(duration);
      this.metrics.total_saves++;

      // Keep only last 1000 measurements to prevent memory growth
      if (this.metrics.state_save_duration.length > 1000) {
        this.metrics.state_save_duration.shift();
      }

      // Log warning for slow operations (>100ms)
      if (duration > 100) {
        this.metrics.slow_operations++;
        this.logger.warn('SessionStateManager: Slow persistence operation detected', {
          adminId,
          phone: this._maskPhone(phone),
          duration,
          threshold: 100,
          operation: 'persistState'
        });
      }

      this.logger.debug('SessionStateManager: State scheduled for batching', {
        adminId,
        phone: this._maskPhone(phone),
        duration,
        pendingBatchSize: this.batchWriter.getPendingCount()
      });

      // Record success with circuit breaker (scheduling is considered success)
      this.circuitBreaker.recordSuccess();

      return true;
    } catch (error) {
      const duration = Date.now() - startTime;
      
      // Track error metrics
      this.metrics.persistence_errors++;
      
      // Record failure with circuit breaker
      this.circuitBreaker.recordFailure();
      
      // Log error with full context
      this.logger.error('SessionStateManager: Failed to schedule state persistence', {
        adminId,
        phone: this._maskPhone(phone),
        duration,
        error: error.message,
        stack: error.stack,
        circuitState: this.circuitBreaker.getState(),
        failures: this.circuitBreaker.getFailures(),
        totalErrors: this.metrics.persistence_errors
      });

      // Report to Sentry if available (emit Sentry alert for persistence failures)
      if (this.sentry) {
        this.sentry.captureException(error, {
          tags: {
            component: 'SessionStateManager',
            operation: 'persistState',
            circuitState: this.circuitBreaker.getState()
          },
          extra: {
            adminId,
            phone: this._maskPhone(phone),
            duration,
            failures: this.circuitBreaker.getFailures(),
            totalErrors: this.metrics.persistence_errors
          }
        });
      }

      return false;
    }
  }

  /**
   * Load a user's conversation state from the database
   * 
   * Implements cache-through pattern: check cache first, then database.
   * Populates cache on cache miss.
   * 
   * @param {number} adminId - The admin ID
   * @param {string} phone - The user's phone number
   * @returns {Promise<Object|null>} The user state object or null if not found
   */
  async loadState(adminId, phone) {
    if (!this.isEnabled()) {
      return null;
    }

    const startTime = Date.now();

    try {
      // Validate inputs
      if (!adminId || !phone) {
        this.logger.warn('SessionStateManager: Invalid parameters for loadState', {
          hasAdminId: !!adminId,
          hasPhone: !!phone
        });
        return null;
      }

      // Check cache first if available
      if (this.cacheLayer && this.cacheLayer.isAvailable()) {
        try {
          const cached = await this.cacheLayer.get(adminId, phone);
          if (cached) {
            const duration = Date.now() - startTime;
            
            // Track cache hit
            this.metrics.cache_hits++;
            this.metrics.total_loads++;
            
            this.logger.debug('SessionStateManager: State loaded from cache', {
              adminId,
              phone: this._maskPhone(phone),
              duration,
              cacheHit: true
            });
            return cached;
          } else {
            // Track cache miss
            this.metrics.cache_misses++;
          }
        } catch (err) {
          // Track cache miss on error
          this.metrics.cache_misses++;
          
          this.logger.warn('SessionStateManager: Cache read failed, falling back to database', {
            adminId,
            phone: this._maskPhone(phone),
            error: err.message
          });
        }
      }

      // Load from database
      const query = `
        SELECT session_data
        FROM conversation_states
        WHERE admin_id = $1 AND phone = $2
      `;

      const [rows] = await this.db.query(query, [adminId, phone]);

      if (!rows || rows.length === 0) {
        const duration = Date.now() - startTime;
        this.logger.debug('SessionStateManager: No state found in database', {
          adminId,
          phone: this._maskPhone(phone),
          duration
        });
        return null;
      }

      // Deserialize the state
      const userState = this.serializer.deserialize(JSON.stringify(rows[0].session_data));
      
      if (!userState) {
        this.logger.error('SessionStateManager: Failed to deserialize state', {
          adminId,
          phone: this._maskPhone(phone)
        });
        return null;
      }

      // Populate cache on cache miss (fire-and-forget)
      if (this.cacheLayer && this.cacheLayer.isAvailable()) {
        this.cacheLayer.set(adminId, phone, userState).catch(err => {
          this.logger.warn('SessionStateManager: Failed to populate cache', {
            adminId,
            phone: this._maskPhone(phone),
            error: err.message
          });
        });
      }

      const duration = Date.now() - startTime;
      
      // Track metrics
      this.metrics.state_load_duration.push(duration);
      this.metrics.total_loads++;

      // Keep only last 1000 measurements to prevent memory growth
      if (this.metrics.state_load_duration.length > 1000) {
        this.metrics.state_load_duration.shift();
      }

      // Log warning for slow operations (>100ms)
      if (duration > 100) {
        this.metrics.slow_operations++;
        this.logger.warn('SessionStateManager: Slow load operation detected', {
          adminId,
          phone: this._maskPhone(phone),
          duration,
          threshold: 100,
          operation: 'loadState'
        });
      }
      
      this.logger.debug('SessionStateManager: State loaded from database', {
        adminId,
        phone: this._maskPhone(phone),
        duration,
        cacheHit: false
      });

      return userState;
    } catch (error) {
      const duration = Date.now() - startTime;
      
      // Track error metrics
      this.metrics.persistence_errors++;
      
      // Log error with full context
      this.logger.error('SessionStateManager: Failed to load state', {
        adminId,
        phone: this._maskPhone(phone),
        duration,
        error: error.message,
        stack: error.stack,
        totalErrors: this.metrics.persistence_errors
      });

      // Report to Sentry if available (emit Sentry alert for persistence failures)
      if (this.sentry) {
        this.sentry.captureException(error, {
          tags: {
            component: 'SessionStateManager',
            operation: 'loadState'
          },
          extra: {
            adminId,
            phone: this._maskPhone(phone),
            duration,
            totalErrors: this.metrics.persistence_errors
          }
        });
      }

      return null;
    }
  }

  /**
   * Delete a user's conversation state from the database
   * 
   * Implements cache invalidation to ensure consistency.
   * Uses parameterized queries to prevent SQL injection.
   * 
   * @param {number} adminId - The admin ID
   * @param {string} phone - The user's phone number
   * @returns {Promise<boolean>} True if deletion succeeded
   */
  async deleteState(adminId, phone) {
    if (!this.isEnabled()) {
      return false;
    }

    const startTime = Date.now();

    try {
      // Validate inputs
      if (!adminId || !phone) {
        this.logger.warn('SessionStateManager: Invalid parameters for deleteState', {
          hasAdminId: !!adminId,
          hasPhone: !!phone
        });
        return false;
      }

      // Invalidate cache first (fire-and-forget)
      if (this.cacheLayer && this.cacheLayer.isAvailable()) {
        this.cacheLayer.delete(adminId, phone).catch(err => {
          this.logger.warn('SessionStateManager: Cache invalidation failed', {
            adminId,
            phone: this._maskPhone(phone),
            error: err.message
          });
        });
      }

      // Delete from database
      const query = `
        DELETE FROM conversation_states
        WHERE admin_id = $1 AND phone = $2
      `;

      await this.db.query(query, [adminId, phone]);

      const duration = Date.now() - startTime;
      this.logger.debug('SessionStateManager: State deleted successfully', {
        adminId,
        phone: this._maskPhone(phone),
        duration
      });

      return true;
    } catch (error) {
      const duration = Date.now() - startTime;
      
      // Track error metrics
      this.metrics.persistence_errors++;
      
      // Log error with full context
      this.logger.error('SessionStateManager: Failed to delete state', {
        adminId,
        phone: this._maskPhone(phone),
        duration,
        error: error.message,
        stack: error.stack,
        totalErrors: this.metrics.persistence_errors
      });

      // Report to Sentry if available (emit Sentry alert for persistence failures)
      if (this.sentry) {
        this.sentry.captureException(error, {
          tags: {
            component: 'SessionStateManager',
            operation: 'deleteState'
          },
          extra: {
            adminId,
            phone: this._maskPhone(phone),
            duration,
            totalErrors: this.metrics.persistence_errors
          }
        });
      }

      return false;
    }
  }
  /**
   * Persist all conversation states for an admin (bulk operation)
   *
   * Used during graceful shutdown to persist all active sessions at once.
   * Uses Promise.allSettled to handle partial failures gracefully.
   * Logs statistics about successful and failed operations.
   *
   * @param {number} adminId - The admin ID
   * @param {Object} usersMap - Map of phone -> userState objects
   * @returns {Promise<Object>} Statistics: { total, succeeded, failed, duration }
   */
  /**
     * Persist all conversation states for an admin (bulk operation)
     * 
     * Used during graceful shutdown to persist all active sessions at once.
     * Uses Promise.allSettled to handle partial failures gracefully.
     * Bypasses batching to ensure immediate writes.
     * Logs statistics about successful and failed operations.
     * 
     * @param {number} adminId - The admin ID
     * @param {Object} usersMap - Map of phone -> userState objects
     * @returns {Promise<Object>} Statistics: { total, succeeded, failed, duration }
     */
    async persistAllStates(adminId, usersMap) {
      if (!this.isEnabled()) {
        return { total: 0, succeeded: 0, failed: 0, duration: 0 };
      }

      const startTime = Date.now();

      try {
        // Validate inputs
        if (!adminId || !usersMap || typeof usersMap !== 'object') {
          this.logger.warn('SessionStateManager: Invalid parameters for persistAllStates', {
            hasAdminId: !!adminId,
            hasUsersMap: !!usersMap,
            usersMapType: typeof usersMap
          });
          return { total: 0, succeeded: 0, failed: 0, duration: 0 };
        }

        const phones = Object.keys(usersMap);
        const total = phones.length;

        if (total === 0) {
          this.logger.debug('SessionStateManager: No states to persist for bulk operation', {
            adminId
          });
          return { total: 0, succeeded: 0, failed: 0, duration: 0 };
        }

        this.logger.info('SessionStateManager: Starting bulk persist operation', {
          adminId,
          total
        });

        // Create promises for all persist operations
        // Bypass batching for immediate writes during shutdown
        const promises = phones.map(async (phone) => {
          const userState = usersMap[phone];

          try {
            // Serialize the user state
            const serialized = this.serializer.serialize(userState);
            if (!serialized) {
              throw new Error('Serialization failed');
            }

            // Write directly to database with retry logic
            await this._retryWithBackoff(async () => {
              return await this._upsertState(adminId, phone, serialized);
            });

            return { phone, success: true, error: null };
          } catch (error) {
            return { phone, success: false, error };
          }
        });

        // Wait for all operations to complete (settled)
        const results = await Promise.allSettled(promises);

        // Count successes and failures
        let succeeded = 0;
        let failed = 0;
        const failedPhones = [];

        results.forEach(result => {
          if (result.status === 'fulfilled') {
            if (result.value.success) {
              succeeded++;
            } else {
              failed++;
              failedPhones.push({
                phone: this._maskPhone(result.value.phone),
                error: result.value.error?.message || 'Unknown error'
              });
            }
          } else {
            failed++;
            failedPhones.push({
              phone: 'unknown',
              error: result.reason?.message || 'Promise rejected'
            });
          }
        });

        const duration = Date.now() - startTime;

        this.logger.info('SessionStateManager: Bulk persist operation completed', {
          adminId,
          total,
          succeeded,
          failed,
          duration,
          successRate: total > 0 ? ((succeeded / total) * 100).toFixed(2) + '%' : '0%'
        });

        // Log failed operations if any
        if (failed > 0) {
          this.logger.warn('SessionStateManager: Some states failed to persist in bulk operation', {
            adminId,
            failed,
            failedPhones: failedPhones.slice(0, 10) // Log first 10 failures
          });
        }

        return { total, succeeded, failed, duration };
      } catch (error) {
        const duration = Date.now() - startTime;

        this.logger.error('SessionStateManager: Bulk persist operation failed', {
          adminId,
          duration,
          error: error.message,
          stack: error.stack
        });

        // Report to Sentry if available
        if (this.sentry) {
          this.sentry.captureException(error, {
            tags: {
              component: 'SessionStateManager',
              operation: 'persistAllStates'
            },
            extra: {
              adminId,
              duration
            }
          });
        }

        return { total: 0, succeeded: 0, failed: 0, duration };
      }
    }

  /**
   * Load all conversation states for an admin (bulk operation)
   *
   * Used during server startup to restore all active sessions at once.
   * Uses Promise.allSettled to handle partial failures gracefully.
   * Logs statistics about successful and failed operations.
   *
   * @param {number} adminId - The admin ID
   * @returns {Promise<Object>} Object with { users: {phone: userState}, stats: {total, succeeded, failed, duration} }
   */
  async loadAllStates(adminId) {
    if (!this.isEnabled()) {
      return { users: {}, stats: { total: 0, succeeded: 0, failed: 0, duration: 0 } };
    }

    const startTime = Date.now();

    try {
      // Validate inputs
      if (!adminId) {
        this.logger.warn('SessionStateManager: Invalid parameters for loadAllStates', {
          hasAdminId: !!adminId
        });
        return { users: {}, stats: { total: 0, succeeded: 0, failed: 0, duration: 0 } };
      }

      this.logger.info('SessionStateManager: Starting bulk load operation', {
        adminId
      });

      // Query all active sessions for this admin
      const query = `
        SELECT phone, session_data
        FROM conversation_states
        WHERE admin_id = $1
        ORDER BY last_activity_at DESC
      `;

      const [rows] = await this.db.query(query, [adminId]);

      if (!rows || rows.length === 0) {
        const duration = Date.now() - startTime;
        this.logger.info('SessionStateManager: No states found for bulk load', {
          adminId,
          duration
        });
        return { users: {}, stats: { total: 0, succeeded: 0, failed: 0, duration } };
      }

      const total = rows.length;

      this.logger.info('SessionStateManager: Found states to load', {
        adminId,
        total
      });

      // Deserialize all states in parallel
      const promises = rows.map(row => {
        return Promise.resolve()
          .then(() => {
            const userState = this.serializer.deserialize(JSON.stringify(row.session_data));
            if (!userState) {
              throw new Error('Deserialization failed');
            }
            return { phone: row.phone, userState, error: null };
          })
          .catch(error => {
            return { phone: row.phone, userState: null, error };
          });
      });

      // Wait for all deserialization operations to complete
      const results = await Promise.allSettled(promises);

      // Build users map and count successes/failures
      const users = {};
      let succeeded = 0;
      let failed = 0;
      const failedPhones = [];

      results.forEach(result => {
        if (result.status === 'fulfilled') {
          const { phone, userState, error } = result.value;
          if (userState && !error) {
            users[phone] = userState;
            succeeded++;
          } else {
            failed++;
            failedPhones.push({
              phone: this._maskPhone(phone),
              error: error?.message || 'Unknown error'
            });
          }
        } else {
          failed++;
          failedPhones.push({
            phone: 'unknown',
            error: result.reason?.message || 'Promise rejected'
          });
        }
      });

      const duration = Date.now() - startTime;

      this.logger.info('SessionStateManager: Bulk load operation completed', {
        adminId,
        total,
        succeeded,
        failed,
        duration,
        successRate: total > 0 ? ((succeeded / total) * 100).toFixed(2) + '%' : '0%'
      });

      // Log failed operations if any
      if (failed > 0) {
        this.logger.warn('SessionStateManager: Some states failed to load in bulk operation', {
          adminId,
          failed,
          failedPhones: failedPhones.slice(0, 10) // Log first 10 failures
        });
      }

      return {
        users,
        stats: { total, succeeded, failed, duration }
      };
    } catch (error) {
      const duration = Date.now() - startTime;

      this.logger.error('SessionStateManager: Bulk load operation failed', {
        adminId,
        duration,
        error: error.message,
        stack: error.stack
      });

      // Report to Sentry if available
      if (this.sentry) {
        this.sentry.captureException(error, {
          tags: {
            component: 'SessionStateManager',
            operation: 'loadAllStates'
          },
          extra: {
            adminId,
            duration
          }
        });
      }

      return { users: {}, stats: { total: 0, succeeded: 0, failed: 0, duration } };
    }
  }

  /**
   * Internal method to perform database upsert
   * 
   * Uses INSERT ... ON CONFLICT for atomic upsert operation.
   * All queries use parameterized placeholders to prevent SQL injection.
   * 
   * @param {number} adminId - The admin ID
   * @param {string} phone - The user's phone number
   * @param {string} serializedState - JSON string of the state
   * @returns {Promise<boolean>} True if upsert succeeded
   * @private
   */
  async _upsertState(adminId, phone, serializedState) {
    const query = `
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3, NOW())
      ON CONFLICT (admin_id, phone)
      DO UPDATE SET
        session_data = EXCLUDED.session_data,
        last_activity_at = EXCLUDED.last_activity_at
    `;

    await this.db.query(query, [adminId, phone, serializedState]);
    return true;
  }

  /**
   * Retry a function with exponential backoff
   * 
   * @param {Function} fn - Async function to retry
   * @returns {Promise<*>} Result of the function
   * @private
   */
  async _retryWithBackoff(fn) {
    let lastError;

    for (let attempt = 0; attempt < this.config.retryAttempts; attempt++) {
      try {
        return await fn();
      } catch (error) {
        lastError = error;

        if (attempt < this.config.retryAttempts - 1) {
          const backoffDelay = this.config.retryDelayMs * Math.pow(2, attempt);
          
          this.logger.warn('SessionStateManager: Retry attempt', {
            attempt: attempt + 1,
            maxAttempts: this.config.retryAttempts,
            backoffDelay,
            error: error.message
          });

          await new Promise(resolve => setTimeout(resolve, backoffDelay));
        }
      }
    }

    // All retries exhausted
    throw lastError;
  }

  /**
   * Mask phone number for logging (show only last 4 digits)
   * 
   * @param {string} phone - The phone number to mask
   * @returns {string} Masked phone number
   * @private
   */
  _maskPhone(phone) {
    if (!phone || phone.length <= 4) {
      return '****';
    }
    return '****' + phone.slice(-4);
  }

  /**
   * Mask email address for logging (show only first char and domain)
   * 
   * @param {string} email - The email address to mask
   * @returns {string} Masked email address
   * @private
   */
  _maskEmail(email) {
    if (!email || !email.includes('@')) {
      return '****@****.***';
    }
    const [local, domain] = email.split('@');
    return local.charAt(0) + '***@' + domain;
  }

  /**
   * Mask sensitive data in objects for logging
   * Masks phone numbers and email addresses
   * 
   * @param {Object} data - Data object that may contain sensitive fields
   * @returns {Object} Object with sensitive fields masked
   * @private
   */
  _maskSensitiveData(data) {
    if (!data || typeof data !== 'object') {
      return data;
    }

    const masked = { ...data };

    // Mask phone fields
    if (masked.phone) {
      masked.phone = this._maskPhone(masked.phone);
    }

    // Mask email fields
    if (masked.email) {
      masked.email = this._maskEmail(masked.email);
    }

    return masked;
  }

  /**
   * Flush any pending batched writes immediately
   * 
   * Useful for graceful shutdown or testing.
   * 
   * @returns {Promise<Object>} Statistics about the flush operation
   */
  async flushBatch() {
    return await this.batchWriter.flush();
  }

  /**
   * Get the number of pending batched writes
   * 
   * @returns {number} Number of pending writes
   */
  getPendingBatchCount() {
    return this.batchWriter.getPendingCount();
  }

  /**
   * Get metrics and observability data
   * 
   * Returns comprehensive metrics for monitoring and debugging:
   * - state_save_duration: Average, p50, p95, p99 latencies for save operations
   * - state_load_duration: Average, p50, p95, p99 latencies for load operations
   * - persistence_errors: Total count of persistence errors
   * - cache_hit_rate: Percentage of cache hits vs total loads
   * - total_operations: Total saves and loads
   * - slow_operations: Count of operations exceeding 100ms
   * - circuit_breaker_state: Current circuit breaker status
   * 
   * @returns {Object} Metrics object with all tracked statistics
   */
  getMetrics() {
    // Calculate percentiles for save duration
    const saveDurations = [...this.metrics.state_save_duration].sort((a, b) => a - b);
    const saveP50 = this._percentile(saveDurations, 50);
    const saveP95 = this._percentile(saveDurations, 95);
    const saveP99 = this._percentile(saveDurations, 99);
    const saveAvg = saveDurations.length > 0
      ? saveDurations.reduce((sum, val) => sum + val, 0) / saveDurations.length
      : 0;

    // Calculate percentiles for load duration
    const loadDurations = [...this.metrics.state_load_duration].sort((a, b) => a - b);
    const loadP50 = this._percentile(loadDurations, 50);
    const loadP95 = this._percentile(loadDurations, 95);
    const loadP99 = this._percentile(loadDurations, 99);
    const loadAvg = loadDurations.length > 0
      ? loadDurations.reduce((sum, val) => sum + val, 0) / loadDurations.length
      : 0;

    // Calculate cache hit rate
    const totalCacheRequests = this.metrics.cache_hits + this.metrics.cache_misses;
    const cacheHitRate = totalCacheRequests > 0
      ? (this.metrics.cache_hits / totalCacheRequests) * 100
      : 0;

    return {
      state_save_duration: {
        avg: Math.round(saveAvg * 100) / 100,
        p50: saveP50,
        p95: saveP95,
        p99: saveP99,
        samples: saveDurations.length
      },
      state_load_duration: {
        avg: Math.round(loadAvg * 100) / 100,
        p50: loadP50,
        p95: loadP95,
        p99: loadP99,
        samples: loadDurations.length
      },
      persistence_errors: this.metrics.persistence_errors,
      cache_hit_rate: Math.round(cacheHitRate * 100) / 100,
      cache_hits: this.metrics.cache_hits,
      cache_misses: this.metrics.cache_misses,
      total_saves: this.metrics.total_saves,
      total_loads: this.metrics.total_loads,
      slow_operations: this.metrics.slow_operations,
      circuit_breaker_state: this.circuitBreaker.getState(),
      circuit_breaker_failures: this.circuitBreaker.getFailures(),
      pending_batch_count: this.batchWriter.getPendingCount()
    };
  }

  /**
   * Calculate percentile from sorted array
   * 
   * @param {Array<number>} sortedArray - Sorted array of numbers
   * @param {number} percentile - Percentile to calculate (0-100)
   * @returns {number} Percentile value
   * @private
   */
  _percentile(sortedArray, percentile) {
    if (sortedArray.length === 0) return 0;
    
    const index = Math.ceil((percentile / 100) * sortedArray.length) - 1;
    return sortedArray[Math.max(0, index)];
  }

  /**
   * Shutdown the session state manager and flush any pending writes
   * 
   * @returns {Promise<Object>} Statistics about the final flush
   */
  async shutdown() {
    this.logger.info('SessionStateManager: Shutting down');
    return await this.batchWriter.shutdown();
  }
}

export { SessionStateManager };
