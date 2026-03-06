import pg from "pg";
import dotenv from "dotenv";
import logger from "./logger.js";

dotenv.config();

const { Pool } = pg;

// Database connection configuration with proper pooling
const poolConfig = {
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  
  // Connection pool settings
  max: 20, // Maximum number of clients in the pool
  min: 2, // Minimum number of clients in the pool
  idleTimeoutMillis: 30000, // Close idle clients after 30 seconds
  connectionTimeoutMillis: 5000, // Return error after 5 seconds if connection cannot be established
  
  // Statement timeout
  statement_timeout: 30000, // 30 seconds
  query_timeout: 30000, // 30 seconds
};

const pool = new Pool(poolConfig);

// Handle pool errors
pool.on('error', (err, client) => {
  logger.error('Unexpected error on idle database client', { error: err.message, stack: err.stack });
});

// Handle pool connection
pool.on('connect', (client) => {
  logger.debug('New database client connected');
});

// Handle pool removal
pool.on('remove', (client) => {
  logger.debug('Database client removed from pool');
});

// Format query to use PostgreSQL positional parameters
const formatQuery = (text, params = []) => {
  if (!params.length) return text;
  let index = 0;
  return text.replace(/\?/g, () => `$${++index}`);
};

// Database query wrapper with logging and error handling
export const db = {
  query: async (text, params = []) => {
    const start = Date.now();
    const sql = formatQuery(text, params);
    
    try {
      const result = await pool.query(sql, params);
      const duration = Date.now() - start;
      
      // Log slow queries (>1 second)
      if (duration > 1000) {
        logger.warn('Slow query detected', {
          duration,
          query: sql.substring(0, 100),
          params: params.length,
        });
      }
      
      return [result.rows, result];
    } catch (error) {
      logger.error('Database query error', {
        error: error.message,
        query: sql.substring(0, 100),
        params: params.length,
        stack: error.stack,
      });
      throw error;
    }
  },
  
  // Get pool statistics
  getPoolStats: () => {
    return {
      total: pool.totalCount,
      idle: pool.idleCount,
      waiting: pool.waitingCount,
    };
  },
  
  // Graceful shutdown
  end: async () => {
    logger.info('Closing database pool...');
    await pool.end();
    logger.info('Database pool closed');
  },
};

// Log initial connection
logger.info('✅ PostgreSQL pool initialized', {
  max: poolConfig.max,
  min: poolConfig.min,
  idleTimeout: poolConfig.idleTimeoutMillis,
});

// Graceful shutdown handler
process.on('SIGTERM', async () => {
  await db.end();
});

process.on('SIGINT', async () => {
  await db.end();
});

export default db;
