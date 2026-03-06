import logger from '../config/logger.js';
import { sessionStateManager, shutdownPersistence } from './persistence/index.js';
import { sessions } from './whatsapp.js';
import { db } from './db.js';

/**
 * Graceful Shutdown Handler
 * 
 * Handles SIGTERM and SIGINT signals to ensure all active session states
 * are persisted before the server shuts down.
 * 
 * Requirements: 9.1, 9.2, 9.3, 9.4, 12.1, 12.2, 12.3
 * 
 * Task 10.4: Create graceful shutdown handler
 * - Register SIGTERM and SIGINT handlers
 * - Call sessionStateManager.persistAllStates() for all active sessions
 * - Use Promise.allSettled for parallel persistence
 * - Close database connections
 * - Complete within 10 seconds
 * - Log sessions that couldn't be persisted
 */

let isShuttingDown = false;
const SHUTDOWN_TIMEOUT_MS = 10000; // 10 seconds

/**
 * Graceful shutdown function
 * 
 * @param {string} signal - The signal that triggered shutdown (SIGTERM or SIGINT)
 */
export async function gracefulShutdown(signal) {
  if (isShuttingDown) {
    logger.warn(`Shutdown already in progress, ignoring ${signal}`);
    return;
  }
  
  isShuttingDown = true;
  logger.info(`🛑 Received ${signal}, starting graceful shutdown...`);
  
  // Set a timeout to force exit if shutdown takes too long
  const forceExitTimer = setTimeout(() => {
    logger.error('⚠️ Graceful shutdown timeout exceeded (10s), forcing exit');
    process.exit(1);
  }, SHUTDOWN_TIMEOUT_MS);
  
  try {
    // Persist all active sessions
    if (sessionStateManager && sessionStateManager.isEnabled()) {
      logger.info('Persisting all active session states...');
      
      const persistencePromises = [];
      let totalSessions = 0;
      
      for (const [adminId, session] of sessions.entries()) {
        if (session && session.users) {
          const userCount = Object.keys(session.users).length;
          if (userCount > 0) {
            totalSessions += userCount;
            persistencePromises.push(
              sessionStateManager.persistAllStates(adminId, session.users)
                .then(() => ({ adminId, success: true, userCount }))
                .catch(err => ({ adminId, success: false, userCount, error: err.message }))
            );
          }
        }
      }
      
      if (persistencePromises.length > 0) {
        logger.info(`Persisting ${totalSessions} sessions across ${persistencePromises.length} admins...`);
        
        const results = await Promise.allSettled(persistencePromises);
        
        let successCount = 0;
        let failureCount = 0;
        const failedAdmins = [];
        
        results.forEach((result, index) => {
          if (result.status === 'fulfilled') {
            const { adminId, success, userCount, error } = result.value;
            if (success) {
              successCount += userCount;
              logger.info(`✅ Persisted ${userCount} sessions for admin ${adminId}`);
            } else {
              failureCount += userCount;
              failedAdmins.push({ adminId, userCount, error });
              logger.error(`❌ Failed to persist ${userCount} sessions for admin ${adminId}`, { error });
            }
          } else {
            failureCount++;
            logger.error(`❌ Persistence promise rejected`, { error: result.reason });
          }
        });
        
        logger.info(`Session persistence complete: ${successCount} succeeded, ${failureCount} failed`);
        
        if (failedAdmins.length > 0) {
          logger.error('⚠️ Some sessions could not be persisted:', {
            failedAdmins: failedAdmins.map(f => ({ adminId: f.adminId, count: f.userCount }))
          });
        }
      } else {
        logger.info('No active sessions to persist');
      }
    } else {
      logger.info('Session persistence disabled, skipping state persistence');
    }
    
    // Shutdown persistence layer (cleanup service, Redis, etc.)
    if (shutdownPersistence) {
      logger.info('Shutting down persistence layer...');
      await shutdownPersistence();
    }
    
    // Close database connections
    logger.info('Closing database connections...');
    await db.end();
    logger.info('✅ Database connections closed');
    
    clearTimeout(forceExitTimer);
    logger.info('✅ Graceful shutdown complete');
    process.exit(0);
  } catch (err) {
    clearTimeout(forceExitTimer);
    logger.error('❌ Error during graceful shutdown', {
      error: err.message,
      stack: err.stack
    });
    process.exit(1);
  }
}

// Register signal handlers
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

logger.info('✅ Graceful shutdown handlers registered (SIGTERM, SIGINT)');
