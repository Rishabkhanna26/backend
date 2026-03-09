/**
 * BatchWriter coalesces rapid state updates within a time window
 * to reduce database write load.
 * 
 * Implements batching strategy where multiple updates to the same
 * (adminId, phone) within the batch window are coalesced into a
 * single database write containing the most recent state.
 * 
 * Requirements: 3.7
 */
class BatchWriter {
  /**
   * Create a BatchWriter instance
   * 
   * @param {Object} options - Configuration options
   * @param {number} options.flushIntervalMs - Time window for batching (default: 500ms)
   * @param {Function} options.writeFn - Async function to write a single state (adminId, phone, serializedState)
   * @param {Object} options.logger - Winston logger instance
   */
  constructor({ flushIntervalMs = 500, writeFn, logger }) {
    this.flushIntervalMs = flushIntervalMs;
    this.writeFn = writeFn;
    this.logger = logger;
    
    // Map of pending writes: key -> {adminId, phone, userState, serializedState, timestamp}
    this.pending = new Map();
    
    // Timer for scheduled flush
    this.timer = null;
    
    // Track if we're currently flushing
    this.isFlushing = false;
    
    this.logger.debug('BatchWriter initialized', {
      flushIntervalMs: this.flushIntervalMs
    });
  }

  /**
   * Schedule a state update for batching
   * 
   * If multiple updates for the same (adminId, phone) occur within the
   * batch window, only the most recent state will be persisted.
   * 
   * @param {number} adminId - The admin ID
   * @param {string} phone - The user's phone number
   * @param {Object} userState - The user's conversation state object
   * @param {string} serializedState - JSON string of the state
   */
  schedule(adminId, phone, userState, serializedState) {
    const key = `${adminId}:${phone}`;
    
    // Store or update the pending write with the latest state
    this.pending.set(key, {
      adminId,
      phone,
      userState,
      serializedState,
      timestamp: Date.now()
    });

    // Start flush timer if not already running
    if (!this.timer && !this.isFlushing) {
      this.timer = setTimeout(() => this.flush(), this.flushIntervalMs);
    }
  }

  /**
   * Flush all pending writes to the database
   * 
   * Writes are executed in parallel using Promise.allSettled to ensure
   * one failure doesn't block others.
   * 
   * @returns {Promise<Object>} Statistics about the flush operation
   */
  async flush() {
    // Clear the timer
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    // If no pending writes, return early
    if (this.pending.size === 0) {
      return { total: 0, succeeded: 0, failed: 0 };
    }

    // Mark as flushing
    this.isFlushing = true;

    // Get all pending writes and clear the map
    const batch = Array.from(this.pending.values());
    this.pending.clear();

    const startTime = Date.now();
    
    this.logger.debug('BatchWriter: Flushing batch', {
      batchSize: batch.length
    });

    // Write all in parallel
    const results = await Promise.allSettled(
      batch.map(item => this.writeFn(item.adminId, item.phone, item.serializedState))
    );

    // Count successes and failures
    const succeeded = results.filter(r => r.status === 'fulfilled' && r.value === true).length;
    const failed = results.filter(r => r.status === 'rejected' || r.value === false).length;
    const duration = Date.now() - startTime;

    this.logger.debug('BatchWriter: Batch flushed', {
      total: batch.length,
      succeeded,
      failed,
      duration
    });

    // Mark flushing complete
    this.isFlushing = false;

    // If new writes came in during flush, schedule another flush
    if (this.pending.size > 0 && !this.timer) {
      this.timer = setTimeout(() => this.flush(), this.flushIntervalMs);
    }

    return { total: batch.length, succeeded, failed, duration };
  }

  /**
   * Get the number of pending writes
   * 
   * @returns {number} Number of pending writes
   */
  getPendingCount() {
    return this.pending.size;
  }

  /**
   * Check if a flush is currently in progress
   * 
   * @returns {boolean} True if flushing
   */
  isFlushInProgress() {
    return this.isFlushing;
  }

  /**
   * Shutdown the batch writer and flush any pending writes
   * 
   * @returns {Promise<Object>} Statistics about the final flush
   */
  async shutdown() {
    this.logger.info('BatchWriter: Shutting down');
    
    // Clear any pending timer
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    // Flush any remaining writes
    return await this.flush();
  }
}

export default BatchWriter;
