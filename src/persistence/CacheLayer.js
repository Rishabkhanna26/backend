import { createClient } from 'redis';
import logger from '../../config/logger.js';

/**
 * CacheLayer - Optional Redis-based caching for conversation states
 * 
 * Provides high-performance caching with read-through and write-through patterns.
 * Falls back to direct database access when Redis is unavailable.
 * Supports multi-instance cache invalidation via Redis pub/sub.
 * 
 * Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 7.6, 14.3
 */
class CacheLayer {
  /**
   * Create a CacheLayer instance
   * 
   * @param {Object} options - Configuration options
   * @param {Object} options.redisClient - Existing Redis client instance (optional)
   * @param {string} options.redisUrl - Redis connection URL (optional, used if redisClient not provided)
   * @param {Object} options.logger - Winston logger instance
   * @param {Object} options.config - Persistence configuration
   */
  constructor({ redisClient, redisUrl, logger: loggerInstance, config }) {
    this.logger = loggerInstance || logger;
    this.config = config;
    this.client = null;
    this.subscriber = null;
    this.available = false;
    this.invalidationCallbacks = [];
    this.ownsClient = false; // Track if we created the client
    
    // Use provided Redis client or create new one
    if (redisClient) {
      this._useExistingClient(redisClient);
    } else if (redisUrl) {
      this.redisUrl = redisUrl;
      this._initializeRedis();
    } else {
      this.logger.info('CacheLayer: No Redis client or URL provided, caching disabled');
    }
  }
  
  /**
   * Use an existing Redis client
   * @private
   */
  async _useExistingClient(redisClient) {
    try {
      this.client = redisClient;
      this.ownsClient = false;
      
      // Create subscriber client for pub/sub
      this.subscriber = this.client.duplicate();
      
      // Set up event handlers
      this.client.on('error', (err) => {
        this.available = false;
        this.logger.warn('CacheLayer: Redis client error', { error: err.message });
      });
      
      this.client.on('ready', () => {
        this.available = true;
        this.logger.debug('CacheLayer: Redis client ready');
      });
      
      this.subscriber.on('error', (err) => {
        this.logger.warn('CacheLayer: Redis subscriber error', { error: err.message });
      });
      
      // Connect subscriber if not already connected
      if (!this.subscriber.isOpen) {
        await this.subscriber.connect();
      }
      
      // Check if main client is ready
      this.available = this.client.isOpen;
      
      this.logger.info('CacheLayer: Using existing Redis client');
    } catch (error) {
      this.available = false;
      this.logger.warn('CacheLayer: Failed to use existing Redis client, falling back to database-only', {
        error: error.message
      });
    }
  }
  
  /**
   * Initialize Redis client and subscriber
   * @private
   */
  async _initializeRedis() {
    try {
      // Create main Redis client
      this.client = createClient({
        url: this.redisUrl,
        socket: {
          reconnectStrategy: (retries) => {
            if (retries > 10) {
              this.logger.error('CacheLayer: Max Redis reconnection attempts reached');
              return new Error('Max reconnection attempts reached');
            }
            // Exponential backoff: 100ms, 200ms, 400ms, 800ms, etc.
            const delay = Math.min(100 * Math.pow(2, retries), 5000);
            this.logger.warn(`CacheLayer: Reconnecting to Redis in ${delay}ms (attempt ${retries + 1})`);
            return delay;
          }
        }
      });
      
      this.ownsClient = true;
      
      // Create subscriber client for pub/sub
      this.subscriber = this.client.duplicate();
      
      // Set up event handlers
      this.client.on('error', (err) => {
        this.available = false;
        this.logger.warn('CacheLayer: Redis client error', { error: err.message });
      });
      
      this.client.on('connect', () => {
        this.logger.info('CacheLayer: Redis client connecting...');
      });
      
      this.client.on('ready', () => {
        this.available = true;
        this.logger.info('CacheLayer: Redis client ready');
      });
      
      this.subscriber.on('error', (err) => {
        this.logger.warn('CacheLayer: Redis subscriber error', { error: err.message });
      });
      
      // Connect both clients
      await Promise.all([
        this.client.connect(),
        this.subscriber.connect()
      ]);
      
      this.logger.info('CacheLayer: Redis clients connected successfully');
    } catch (error) {
      this.available = false;
      this.logger.warn('CacheLayer: Failed to initialize Redis, falling back to database-only', {
        error: error.message
      });
    }
  }
  
  /**
   * Generate cache key for a conversation state
   * Format: "conversation:{adminId}:{phone}"
   * 
   * @param {number} adminId - Admin ID
   * @param {string} phone - Phone number
   * @returns {string} Cache key
   * 
   * Requirements: 6.5
   */
  _getCacheKey(adminId, phone) {
    return `conversation:${adminId}:${phone}`;
  }
  
  /**
   * Get conversation state from cache
   * 
   * @param {number} adminId - Admin ID
   * @param {string} phone - Phone number
   * @returns {Promise<Object|null>} Cached state or null if not found
   * 
   * Requirements: 6.2
   */
  async get(adminId, phone) {
    if (!this.isAvailable()) {
      return null;
    }
    
    try {
      const key = this._getCacheKey(adminId, phone);
      const cached = await this.client.get(key);
      
      if (cached) {
        this.logger.debug('CacheLayer: Cache hit', { adminId, phone: this._maskPhone(phone) });
        return JSON.parse(cached);
      }
      
      this.logger.debug('CacheLayer: Cache miss', { adminId, phone: this._maskPhone(phone) });
      return null;
    } catch (error) {
      this.logger.warn('CacheLayer: Failed to get from cache, falling back to database', {
        adminId,
        phone: this._maskPhone(phone),
        error: error.message
      });
      return null;
    }
  }
  
  /**
   * Set conversation state in cache
   * 
   * @param {number} adminId - Admin ID
   * @param {string} phone - Phone number
   * @param {Object} userState - User state object
   * @param {number} ttlMs - Time to live in milliseconds
   * @returns {Promise<boolean>} True if successful, false otherwise
   * 
   * Requirements: 6.1, 6.3
   */
  async set(adminId, phone, userState, ttlMs) {
    if (!this.isAvailable()) {
      return false;
    }
    
    try {
      const key = this._getCacheKey(adminId, phone);
      const value = JSON.stringify(userState);
      const ttlSeconds = Math.floor(ttlMs / 1000);
      
      await this.client.setEx(key, ttlSeconds, value);
      
      this.logger.debug('CacheLayer: Cache set', {
        adminId,
        phone: this._maskPhone(phone),
        ttl: `${ttlSeconds}s`
      });
      
      return true;
    } catch (error) {
      this.logger.warn('CacheLayer: Failed to set cache', {
        adminId,
        phone: this._maskPhone(phone),
        error: error.message
      });
      return false;
    }
  }
  
  /**
   * Delete conversation state from cache
   * 
   * @param {number} adminId - Admin ID
   * @param {string} phone - Phone number
   * @returns {Promise<boolean>} True if successful, false otherwise
   */
  async delete(adminId, phone) {
    if (!this.isAvailable()) {
      return false;
    }
    
    try {
      const key = this._getCacheKey(adminId, phone);
      await this.client.del(key);
      
      this.logger.debug('CacheLayer: Cache deleted', {
        adminId,
        phone: this._maskPhone(phone)
      });
      
      return true;
    } catch (error) {
      this.logger.warn('CacheLayer: Failed to delete from cache', {
        adminId,
        phone: this._maskPhone(phone),
        error: error.message
      });
      return false;
    }
  }
  
  /**
   * Invalidate all conversation states for an admin
   * 
   * @param {number} adminId - Admin ID
   * @returns {Promise<number>} Number of keys deleted
   */
  async invalidate(adminId) {
    if (!this.isAvailable()) {
      return 0;
    }
    
    try {
      const pattern = `conversation:${adminId}:*`;
      const keys = await this.client.keys(pattern);
      
      if (keys.length === 0) {
        return 0;
      }
      
      await this.client.del(keys);
      
      this.logger.info('CacheLayer: Invalidated admin cache', {
        adminId,
        keysDeleted: keys.length
      });
      
      return keys.length;
    } catch (error) {
      this.logger.warn('CacheLayer: Failed to invalidate admin cache', {
        adminId,
        error: error.message
      });
      return 0;
    }
  }
  
  /**
   * Publish cache invalidation message to other instances
   * 
   * @param {number} adminId - Admin ID
   * @param {string} phone - Phone number
   * @returns {Promise<boolean>} True if successful, false otherwise
   * 
   * Requirements: 7.6
   */
  async publishInvalidation(adminId, phone) {
    if (!this.isAvailable()) {
      return false;
    }
    
    try {
      const message = JSON.stringify({ adminId, phone });
      await this.client.publish('conversation:invalidate', message);
      
      this.logger.debug('CacheLayer: Published invalidation', {
        adminId,
        phone: this._maskPhone(phone)
      });
      
      return true;
    } catch (error) {
      this.logger.warn('CacheLayer: Failed to publish invalidation', {
        adminId,
        phone: this._maskPhone(phone),
        error: error.message
      });
      return false;
    }
  }
  
  /**
   * Subscribe to cache invalidation messages from other instances
   * 
   * @param {Function} callback - Callback function to handle invalidation messages
   * @returns {Promise<boolean>} True if successful, false otherwise
   * 
   * Requirements: 7.6
   */
  async subscribeToInvalidations(callback) {
    if (!this.isAvailable() || !this.subscriber) {
      return false;
    }
    
    try {
      // Store callback for later use
      this.invalidationCallbacks.push(callback);
      
      // Subscribe to invalidation channel
      await this.subscriber.subscribe('conversation:invalidate', (message) => {
        try {
          const { adminId, phone } = JSON.parse(message);
          
          this.logger.debug('CacheLayer: Received invalidation', {
            adminId,
            phone: this._maskPhone(phone)
          });
          
          // Call all registered callbacks
          this.invalidationCallbacks.forEach(cb => {
            try {
              cb(adminId, phone);
            } catch (error) {
              this.logger.error('CacheLayer: Error in invalidation callback', {
                error: error.message
              });
            }
          });
        } catch (error) {
          this.logger.error('CacheLayer: Failed to parse invalidation message', {
            error: error.message,
            message
          });
        }
      });
      
      this.logger.info('CacheLayer: Subscribed to invalidation channel');
      return true;
    } catch (error) {
      this.logger.warn('CacheLayer: Failed to subscribe to invalidations', {
        error: error.message
      });
      return false;
    }
  }
  
  /**
   * Check if Redis is available
   * 
   * @returns {boolean} True if Redis is available, false otherwise
   * 
   * Requirements: 6.6, 14.3
   */
  isAvailable() {
    return this.available && this.client && this.client.isOpen;
  }
  
  /**
   * Mask phone number for logging (show only last 4 digits)
   * 
   * @param {string} phone - Phone number
   * @returns {string} Masked phone number
   * @private
   * 
   * Requirements: 13.2
   */
  _maskPhone(phone) {
    if (!phone || phone.length <= 4) {
      return '****';
    }
    return '****' + phone.slice(-4);
  }
  
  /**
   * Close Redis connections
   * Only closes connections if we created them (ownsClient = true)
   * 
   * @returns {Promise<void>}
   */
  async close() {
    try {
      // Only close subscriber (we always create this)
      if (this.subscriber) {
        await this.subscriber.quit();
      }
      
      // Only close main client if we created it
      if (this.client && this.ownsClient) {
        await this.client.quit();
      }
      
      this.available = false;
      this.logger.info('CacheLayer: Redis connections closed');
    } catch (error) {
      this.logger.warn('CacheLayer: Error closing Redis connections', {
        error: error.message
      });
    }
  }
}

export { CacheLayer };
