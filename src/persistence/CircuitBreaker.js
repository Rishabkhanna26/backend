/**
 * CircuitBreaker implements the circuit breaker pattern for resilience.
 * 
 * Tracks consecutive failures and opens the circuit after a threshold is exceeded.
 * Auto-resets the circuit after a timeout period.
 * 
 * States:
 * - CLOSED: Normal operation, requests pass through
 * - OPEN: Circuit is open, requests fail fast
 * - HALF_OPEN: Testing if service recovered, single request allowed
 * 
 * Requirements: 14.4, 14.5, 14.6
 */
class CircuitBreaker {
  /**
   * Create a CircuitBreaker instance
   * 
   * @param {Object} options - Configuration options
   * @param {number} options.threshold - Number of consecutive failures before opening (default: 10)
   * @param {number} options.resetMs - Time in ms before auto-reset attempt (default: 60000)
   * @param {Object} options.logger - Winston logger instance
   */
  constructor({ threshold = 10, resetMs = 60000, logger }) {
    this.threshold = threshold;
    this.resetMs = resetMs;
    this.logger = logger;
    
    this.failures = 0;
    this.state = 'CLOSED'; // CLOSED, OPEN, HALF_OPEN
    this.lastFailureTime = null;
    this.resetTimer = null;

    this.logger.info('CircuitBreaker initialized', {
      threshold: this.threshold,
      resetMs: this.resetMs
    });
  }

  /**
   * Record a successful operation
   * Resets failure count and closes the circuit
   */
  recordSuccess() {
    if (this.state === 'HALF_OPEN') {
      this.logger.info('CircuitBreaker: Recovery successful, closing circuit', {
        previousFailures: this.failures
      });
    }

    this.failures = 0;
    this.state = 'CLOSED';
    
    // Clear any pending reset timer
    if (this.resetTimer) {
      clearTimeout(this.resetTimer);
      this.resetTimer = null;
    }
  }

  /**
   * Record a failed operation
   * Increments failure count and opens circuit if threshold exceeded
   */
  recordFailure() {
    this.failures++;
    this.lastFailureTime = Date.now();

    this.logger.warn('CircuitBreaker: Failure recorded', {
      failures: this.failures,
      threshold: this.threshold,
      state: this.state
    });

    if (this.failures >= this.threshold && this.state === 'CLOSED') {
      this._openCircuit();
    } else if (this.state === 'HALF_OPEN') {
      // Failed during recovery attempt, reopen circuit
      this.logger.error('CircuitBreaker: Recovery attempt failed, reopening circuit');
      this._openCircuit();
    }
  }

  /**
   * Check if the circuit is open
   * 
   * @returns {boolean} True if circuit is open (requests should fail fast)
   */
  isOpen() {
    return this.state === 'OPEN';
  }

  /**
   * Get current circuit state
   * 
   * @returns {string} Current state: CLOSED, OPEN, or HALF_OPEN
   */
  getState() {
    return this.state;
  }

  /**
   * Get current failure count
   * 
   * @returns {number} Number of consecutive failures
   */
  getFailures() {
    return this.failures;
  }

  /**
   * Open the circuit breaker
   * Schedules auto-reset after timeout period
   * @private
   */
  _openCircuit() {
    this.state = 'OPEN';
    
    this.logger.error('CircuitBreaker: Circuit opened - falling back to in-memory storage', {
      failures: this.failures,
      threshold: this.threshold,
      resetMs: this.resetMs
    });

    // Schedule auto-reset
    if (this.resetTimer) {
      clearTimeout(this.resetTimer);
    }

    this.resetTimer = setTimeout(() => {
      this._attemptReset();
    }, this.resetMs);
  }

  /**
   * Attempt to reset the circuit breaker
   * Transitions to HALF_OPEN state for testing
   * @private
   */
  _attemptReset() {
    this.state = 'HALF_OPEN';
    this.failures = 0;
    this.resetTimer = null;

    this.logger.info('CircuitBreaker: Attempting recovery (HALF_OPEN)', {
      resetMs: this.resetMs
    });
  }

  /**
   * Manually reset the circuit breaker
   * Used for testing or manual intervention
   */
  reset() {
    this.logger.info('CircuitBreaker: Manual reset', {
      previousState: this.state,
      previousFailures: this.failures
    });

    this.failures = 0;
    this.state = 'CLOSED';
    this.lastFailureTime = null;

    if (this.resetTimer) {
      clearTimeout(this.resetTimer);
      this.resetTimer = null;
    }
  }
}

export default CircuitBreaker;
