/**
 * SessionCleanupService handles periodic cleanup of expired and finalized conversation states.
 * 
 * Runs a cleanup job every 15 minutes (configurable) to delete:
 * 1. Sessions where last_activity_at is older than USER_IDLE_TTL_MS
 * 2. Sessions where the user has finalized their lead or order (finalized=true)
 * 
 * Implements batch limiting (1000 records per run) to avoid long-running transactions.
 * Uses database indexes for efficient queries. Logs statistics after each cleanup run.
 * Handles errors gracefully and retries on the next scheduled run.
 * 
 * Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 10.6
 */
class SessionCleanupService {
  /**
   * Create a SessionCleanupService instance
   * 
   * @param {Object} options - Configuration options
   * @param {Object} options.db - Database connection pool
   * @param {Object} options.logger - Winston logger instance
   * @param {Object} options.config - Configuration object
   * @param {number} options.config.cleanupIntervalMs - Cleanup interval in milliseconds (default: 900000 = 15 minutes)
   * @param {number} options.config.userIdleTtlMs - User idle TTL in milliseconds (default: 21600000 = 6 hours)
   * @param {number} options.config.batchLimit - Maximum records to delete per batch (default: 1000)
   */
  constructor({ db, logger, config = {} }) {
    this.db = db;
    this.logger = logger;
    
    // Configuration
    this.config = {
      cleanupIntervalMs: config.cleanupIntervalMs || 900000, // 15 minutes default
      userIdleTtlMs: config.userIdleTtlMs || 21600000, // 6 hours default
      batchLimit: config.batchLimit || 1000,
      ...config
    };

    // Cleanup timer
    this.cleanupTimer = null;
    this.isRunning = false;

    // Cleanup statistics
    this.stats = {
      totalRuns: 0,
      totalSessionsDeleted: 0,
      totalErrors: 0,
      lastRunTime: null,
      lastRunDuration: 0,
      lastRunDeleted: 0,
      lastRunError: null
    };

    this.logger.info('SessionCleanupService initialized', {
      cleanupIntervalMs: this.config.cleanupIntervalMs,
      cleanupIntervalMinutes: (this.config.cleanupIntervalMs / 60000).toFixed(2),
      userIdleTtlMs: this.config.userIdleTtlMs,
      userIdleTtlHours: (this.config.userIdleTtlMs / 3600000).toFixed(2),
      batchLimit: this.config.batchLimit
    });
  }

  /**
   * Start the periodic cleanup service
   * 
   * Begins running cleanup operations at the configured interval.
   * Safe to call multiple times - will not create duplicate timers.
   */
  start() {
    if (this.cleanupTimer) {
      this.logger.warn('SessionCleanupService: Already running, ignoring start request');
      return;
    }

    this.logger.info('SessionCleanupService: Starting periodic cleanup', {
      intervalMs: this.config.cleanupIntervalMs,
      intervalMinutes: (this.config.cleanupIntervalMs / 60000).toFixed(2)
    });

    // Run cleanup immediately on start
    this.runCleanup().catch(err => {
      this.logger.error('SessionCleanupService: Initial cleanup failed', {
        error: err.message,
        stack: err.stack
      });
    });

    // Schedule periodic cleanup
    this.cleanupTimer = setInterval(() => {
      this.runCleanup().catch(err => {
        this.logger.error('SessionCleanupService: Scheduled cleanup failed', {
          error: err.message,
          stack: err.stack
        });
      });
    }, this.config.cleanupIntervalMs);

    this.isRunning = true;
  }

  /**
   * Stop the periodic cleanup service
   * 
   * Halts the cleanup timer. Safe to call multiple times.
   */
  stop() {
    if (!this.cleanupTimer) {
      this.logger.warn('SessionCleanupService: Not running, ignoring stop request');
      return;
    }

    this.logger.info('SessionCleanupService: Stopping periodic cleanup');

    clearInterval(this.cleanupTimer);
    this.cleanupTimer = null;
    this.isRunning = false;
  }

  /**
   * Execute a cleanup operation
   * 
   * Deletes conversation states that meet either of these criteria:
   * 1. last_activity_at is older than USER_IDLE_TTL_MS (expired sessions)
   * 2. session_data->>'finalized' = 'true' (finalized sessions)
   * 
   * Limits deletions to batchLimit (default 1000) records per run to avoid
   * long-running transactions. Uses indexes for efficient queries.
   * 
   * Logs statistics after each run. On error, logs the error and will retry
   * on the next scheduled run.
   * 
   * @returns {Promise<Object>} Cleanup statistics: { deleted, duration, error }
   */
  async runCleanup() {
    const startTime = Date.now();

    try {
      this.logger.info('SessionCleanupService: Starting cleanup run');

      // Calculate the cutoff time for expired sessions
      const cutoffTime = new Date(Date.now() - this.config.userIdleTtlMs);

      // Delete expired and finalized sessions in a single query
      // Uses indexes: idx_conversation_states_last_activity and idx_conversation_states_session_data (GIN)
      const query = `
        DELETE FROM conversation_states
        WHERE id IN (
          SELECT id FROM conversation_states
          WHERE last_activity_at < $1
             OR (session_data->>'finalized')::boolean = true
          LIMIT $2
        )
      `;

      const result = await this.db.query(query, [cutoffTime, this.config.batchLimit]);

      // Extract the number of deleted rows
      // For PostgreSQL: result is [{ rowCount }] or result[0] is { rowCount }
      // For MySQL: result is [rows, fields] where rows[0] has affectedRows
      let deleted = 0;
      
      if (Array.isArray(result)) {
        if (result[0]?.rowCount !== undefined) {
          // PostgreSQL format: [{ rowCount: X }]
          deleted = result[0].rowCount;
        } else if (Array.isArray(result[0]) && result[0][0]?.affectedRows !== undefined) {
          // MySQL format: [[{ affectedRows: X }], fields]
          deleted = result[0][0].affectedRows;
        } else if (result[0]?.affectedRows !== undefined) {
          // Alternative MySQL format: [{ affectedRows: X }]
          deleted = result[0].affectedRows;
        }
      } else if (result?.rowCount !== undefined) {
        // Direct PostgreSQL result: { rowCount: X }
        deleted = result.rowCount;
      }

      const duration = Date.now() - startTime;

      // Update statistics
      this.stats.totalRuns++;
      this.stats.totalSessionsDeleted += deleted;
      this.stats.lastRunTime = new Date();
      this.stats.lastRunDuration = duration;
      this.stats.lastRunDeleted = deleted;
      this.stats.lastRunError = null;

      // Log cleanup statistics
      this.logger.info('SessionCleanupService: Cleanup run completed', {
        deleted,
        duration,
        cutoffTime: cutoffTime.toISOString(),
        batchLimit: this.config.batchLimit,
        batchLimitReached: deleted >= this.config.batchLimit,
        totalRuns: this.stats.totalRuns,
        totalSessionsDeleted: this.stats.totalSessionsDeleted
      });

      // Warn if batch limit was reached (more sessions may need cleanup)
      if (deleted >= this.config.batchLimit) {
        this.logger.warn('SessionCleanupService: Batch limit reached, more sessions may need cleanup', {
          deleted,
          batchLimit: this.config.batchLimit,
          suggestion: 'Consider running cleanup more frequently or increasing batch limit'
        });
      }

      return { deleted, duration, error: null };
    } catch (error) {
      const duration = Date.now() - startTime;

      // Update error statistics
      this.stats.totalErrors++;
      this.stats.lastRunTime = new Date();
      this.stats.lastRunDuration = duration;
      this.stats.lastRunDeleted = 0;
      this.stats.lastRunError = error.message;

      // Log error with full context
      this.logger.error('SessionCleanupService: Cleanup run failed', {
        duration,
        error: error.message,
        stack: error.stack,
        totalErrors: this.stats.totalErrors,
        retrySchedule: `Next retry in ${(this.config.cleanupIntervalMs / 60000).toFixed(2)} minutes`
      });

      return { deleted: 0, duration, error: error.message };
    }
  }

  /**
   * Get cleanup statistics
   * 
   * Returns comprehensive statistics about cleanup operations:
   * - totalRuns: Total number of cleanup runs executed
   * - totalSessionsDeleted: Total sessions deleted across all runs
   * - totalErrors: Total number of cleanup errors
   * - lastRunTime: Timestamp of last cleanup run
   * - lastRunDuration: Duration of last cleanup run in milliseconds
   * - lastRunDeleted: Number of sessions deleted in last run
   * - lastRunError: Error message from last run (null if successful)
   * - isRunning: Whether the cleanup service is currently active
   * 
   * @returns {Object} Cleanup statistics
   */
  getCleanupStats() {
    return {
      ...this.stats,
      isRunning: this.isRunning,
      config: {
        cleanupIntervalMs: this.config.cleanupIntervalMs,
        userIdleTtlMs: this.config.userIdleTtlMs,
        batchLimit: this.config.batchLimit
      }
    };
  }

  /**
   * Check if the cleanup service is running
   * 
   * @returns {boolean} True if the service is running
   */
  isActive() {
    return this.isRunning;
  }
}

export { SessionCleanupService };
