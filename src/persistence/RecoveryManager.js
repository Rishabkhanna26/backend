import { StateSerializer } from './StateSerializer.js';

/**
 * RecoveryManager handles restoration of active conversation sessions on server startup.
 * 
 * Loads conversation states from the database where last_activity_at is within the
 * configured USER_IDLE_TTL_MS window, deserializes them, and restores them to the
 * in-memory sessions Map grouped by admin_id.
 * 
 * Implements error handling for corrupted sessions - skips invalid sessions without
 * failing the entire recovery process. Tracks and logs recovery statistics.
 * 
 * Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 14.2
 */
class RecoveryManager {
  /**
   * Create a RecoveryManager instance
   * 
   * @param {Object} options - Configuration options
   * @param {Object} options.db - Database connection pool
   * @param {Object} options.logger - Winston logger instance
   * @param {StateSerializer} options.serializer - StateSerializer instance (optional, creates new if not provided)
   * @param {Object} options.config - Configuration object
   * @param {number} options.config.userIdleTtlMs - User idle TTL in milliseconds (default: 21600000 = 6 hours)
   */
  constructor({ db, logger, serializer = null, config = {} }) {
    this.db = db;
    this.logger = logger;
    this.serializer = serializer || new StateSerializer();
    
    // Configuration
    this.config = {
      userIdleTtlMs: config.userIdleTtlMs || 21600000, // 6 hours default
      ...config
    };

    // Recovery statistics
    this.stats = {
      totalRecovered: 0,
      failedRecoveries: 0,
      recoveryDurationMs: 0,
      lastRecoveryTime: null
    };

    this.logger.info('RecoveryManager initialized', {
      userIdleTtlMs: this.config.userIdleTtlMs,
      userIdleTtlHours: (this.config.userIdleTtlMs / 3600000).toFixed(2)
    });
  }

  /**
   * Recover all active conversation sessions from the database
   * 
   * Queries the database for all sessions where last_activity_at is within
   * the USER_IDLE_TTL_MS window. Deserializes each session and groups them
   * by admin_id for restoration to the in-memory sessions Map.
   * 
   * Skips sessions with deserialization errors without failing the entire
   * recovery process. Logs detailed statistics about the recovery operation.
   * 
   * Performance target: 1000 sessions in < 5 seconds
   * 
   * @returns {Promise<Object>} Recovery result with structure:
   *   {
   *     sessionsByAdmin: { [adminId]: { [phone]: userState } },
   *     stats: { totalRecovered, failedRecoveries, recoveryDurationMs }
   *   }
   */
  async recoverSessions() {
    const startTime = Date.now();

    try {
      this.logger.info('RecoveryManager: Starting session recovery');

      // Calculate the cutoff time for active sessions
      const cutoffTime = new Date(Date.now() - this.config.userIdleTtlMs);

      // Query all active sessions across all admins
      const query = `
        SELECT admin_id, phone, session_data, last_activity_at
        FROM conversation_states
        WHERE last_activity_at > $1
        ORDER BY admin_id, last_activity_at DESC
      `;

      const [rows] = await this.db.query(query, [cutoffTime]);

      if (!rows || rows.length === 0) {
        const duration = Date.now() - startTime;
        this.logger.info('RecoveryManager: No active sessions found to recover', {
          cutoffTime: cutoffTime.toISOString(),
          duration
        });

        this.stats = {
          totalRecovered: 0,
          failedRecoveries: 0,
          recoveryDurationMs: duration,
          lastRecoveryTime: new Date()
        };

        return {
          sessionsByAdmin: {},
          stats: this.stats
        };
      }

      this.logger.info('RecoveryManager: Found sessions to recover', {
        totalSessions: rows.length,
        cutoffTime: cutoffTime.toISOString()
      });

      // Group sessions by admin_id and deserialize
      const sessionsByAdmin = {};
      let totalRecovered = 0;
      let failedRecoveries = 0;
      const failedSessions = [];

      for (const row of rows) {
        const { admin_id, phone, session_data, last_activity_at } = row;

        try {
          // Deserialize the session data
          const userState = this.serializer.deserialize(JSON.stringify(session_data));

          if (!userState) {
            throw new Error('Deserialization returned null');
          }

          // Initialize admin group if not exists
          if (!sessionsByAdmin[admin_id]) {
            sessionsByAdmin[admin_id] = {};
          }

          // Add to admin's sessions
          sessionsByAdmin[admin_id][phone] = userState;
          totalRecovered++;

          this.logger.debug('RecoveryManager: Session recovered successfully', {
            adminId: admin_id,
            phone: this._maskPhone(phone),
            lastActivity: last_activity_at
          });
        } catch (error) {
          failedRecoveries++;
          
          failedSessions.push({
            adminId: admin_id,
            phone: this._maskPhone(phone),
            error: error.message,
            lastActivity: last_activity_at
          });

          this.logger.error('RecoveryManager: Failed to recover session', {
            adminId: admin_id,
            phone: this._maskPhone(phone),
            lastActivity: last_activity_at,
            error: error.message,
            stack: error.stack
          });
        }
      }

      const duration = Date.now() - startTime;

      // Update statistics
      this.stats = {
        totalRecovered,
        failedRecoveries,
        recoveryDurationMs: duration,
        lastRecoveryTime: new Date()
      };

      // Log recovery summary
      const adminCount = Object.keys(sessionsByAdmin).length;
      this.logger.info('RecoveryManager: Session recovery completed', {
        totalSessions: rows.length,
        totalRecovered,
        failedRecoveries,
        adminCount,
        duration,
        successRate: rows.length > 0 ? ((totalRecovered / rows.length) * 100).toFixed(2) + '%' : '0%',
        performanceTarget: '< 5000ms for 1000 sessions',
        performanceMet: rows.length <= 1000 ? (duration < 5000 ? 'YES' : 'NO') : 'N/A'
      });

      // Log failed recoveries if any
      if (failedRecoveries > 0) {
        this.logger.warn('RecoveryManager: Some sessions failed to recover', {
          failedCount: failedRecoveries,
          failedSessions: failedSessions.slice(0, 10) // Log first 10 failures
        });
      }

      // Log per-admin statistics
      for (const [adminId, sessions] of Object.entries(sessionsByAdmin)) {
        const sessionCount = Object.keys(sessions).length;
        this.logger.info('RecoveryManager: Sessions recovered for admin', {
          adminId,
          sessionCount
        });
      }

      return {
        sessionsByAdmin,
        stats: this.stats
      };
    } catch (error) {
      const duration = Date.now() - startTime;

      this.logger.error('RecoveryManager: Session recovery failed', {
        duration,
        error: error.message,
        stack: error.stack
      });

      // Update statistics with failure
      this.stats = {
        totalRecovered: 0,
        failedRecoveries: 0,
        recoveryDurationMs: duration,
        lastRecoveryTime: new Date()
      };

      // Don't throw - return empty result to allow server to start
      return {
        sessionsByAdmin: {},
        stats: this.stats
      };
    }
  }

  /**
   * Recover conversation sessions for a specific admin
   * 
   * Queries the database for sessions belonging to a specific admin where
   * last_activity_at is within the USER_IDLE_TTL_MS window. Deserializes
   * each session and returns them as a map of phone -> userState.
   * 
   * Useful for recovering sessions when a specific admin's WhatsApp client
   * reconnects or restarts.
   * 
   * @param {number} adminId - The admin ID to recover sessions for
   * @returns {Promise<Object>} Recovery result with structure:
   *   {
   *     users: { [phone]: userState },
   *     stats: { totalRecovered, failedRecoveries, recoveryDurationMs }
   *   }
   */
  async recoverSessionsForAdmin(adminId) {
    const startTime = Date.now();

    try {
      // Validate input
      if (!adminId) {
        this.logger.warn('RecoveryManager: Invalid adminId provided for recovery', {
          hasAdminId: !!adminId
        });
        return {
          users: {},
          stats: { totalRecovered: 0, failedRecoveries: 0, recoveryDurationMs: 0 }
        };
      }

      this.logger.info('RecoveryManager: Starting session recovery for admin', {
        adminId
      });

      // Calculate the cutoff time for active sessions
      const cutoffTime = new Date(Date.now() - this.config.userIdleTtlMs);

      // Query active sessions for this specific admin
      const query = `
        SELECT phone, session_data, last_activity_at
        FROM conversation_states
        WHERE admin_id = $1 AND last_activity_at > $2
        ORDER BY last_activity_at DESC
      `;

      const [rows] = await this.db.query(query, [adminId, cutoffTime]);

      if (!rows || rows.length === 0) {
        const duration = Date.now() - startTime;
        this.logger.info('RecoveryManager: No active sessions found for admin', {
          adminId,
          cutoffTime: cutoffTime.toISOString(),
          duration
        });

        return {
          users: {},
          stats: { totalRecovered: 0, failedRecoveries: 0, recoveryDurationMs: duration }
        };
      }

      this.logger.info('RecoveryManager: Found sessions to recover for admin', {
        adminId,
        totalSessions: rows.length,
        cutoffTime: cutoffTime.toISOString()
      });

      // Deserialize sessions
      const users = {};
      let totalRecovered = 0;
      let failedRecoveries = 0;
      const failedSessions = [];

      for (const row of rows) {
        const { phone, session_data, last_activity_at } = row;

        try {
          // Deserialize the session data
          const userState = this.serializer.deserialize(JSON.stringify(session_data));

          if (!userState) {
            throw new Error('Deserialization returned null');
          }

          // Add to users map
          users[phone] = userState;
          totalRecovered++;

          this.logger.debug('RecoveryManager: Session recovered for admin', {
            adminId,
            phone: this._maskPhone(phone),
            lastActivity: last_activity_at
          });
        } catch (error) {
          failedRecoveries++;
          
          failedSessions.push({
            phone: this._maskPhone(phone),
            error: error.message,
            lastActivity: last_activity_at
          });

          this.logger.error('RecoveryManager: Failed to recover session for admin', {
            adminId,
            phone: this._maskPhone(phone),
            lastActivity: last_activity_at,
            error: error.message,
            stack: error.stack
          });
        }
      }

      const duration = Date.now() - startTime;

      // Log recovery summary
      this.logger.info('RecoveryManager: Session recovery completed for admin', {
        adminId,
        totalSessions: rows.length,
        totalRecovered,
        failedRecoveries,
        duration,
        successRate: rows.length > 0 ? ((totalRecovered / rows.length) * 100).toFixed(2) + '%' : '0%'
      });

      // Log failed recoveries if any
      if (failedRecoveries > 0) {
        this.logger.warn('RecoveryManager: Some sessions failed to recover for admin', {
          adminId,
          failedCount: failedRecoveries,
          failedSessions: failedSessions.slice(0, 10) // Log first 10 failures
        });
      }

      return {
        users,
        stats: { totalRecovered, failedRecoveries, recoveryDurationMs: duration }
      };
    } catch (error) {
      const duration = Date.now() - startTime;

      this.logger.error('RecoveryManager: Session recovery failed for admin', {
        adminId,
        duration,
        error: error.message,
        stack: error.stack
      });

      // Don't throw - return empty result to allow admin to start
      return {
        users: {},
        stats: { totalRecovered: 0, failedRecoveries: 0, recoveryDurationMs: duration }
      };
    }
  }

  /**
   * Get recovery statistics
   * 
   * Returns statistics from the last recovery operation including:
   * - totalRecovered: Number of sessions successfully recovered
   * - failedRecoveries: Number of sessions that failed to recover
   * - recoveryDurationMs: Time taken for recovery in milliseconds
   * - lastRecoveryTime: Timestamp of last recovery operation
   * 
   * @returns {Object} Recovery statistics
   */
  getRecoveryStats() {
    return { ...this.stats };
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
}

export { RecoveryManager };
