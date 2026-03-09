#!/usr/bin/env node

/**
 * Migration Execution Script
 * 
 * Executes SQL migrations for the session state persistence feature.
 * Supports both up (apply) and down (rollback) migrations.
 * 
 * Usage:
 *   node scripts/run-migration.js up 001_create_conversation_states
 *   node scripts/run-migration.js down 001_create_conversation_states
 * 
 * Requirements: 12.5
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import pg from 'pg';
import logger from '../config/logger.js';

// Load environment variables
dotenv.config();

const { Pool } = pg;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

/**
 * Execute a migration file
 * @param {string} direction - 'up' or 'down'
 * @param {string} migrationName - Name of the migration (without extension)
 */
async function runMigration(direction, migrationName) {
  const validDirections = ['up', 'down'];
  
  if (!validDirections.includes(direction)) {
    logger.error('Invalid migration direction', { direction, validDirections });
    throw new Error(`Direction must be one of: ${validDirections.join(', ')}`);
  }

  // Determine the migration file path
  let migrationFile;
  if (direction === 'up') {
    migrationFile = path.join(__dirname, '..', 'migrations', `${migrationName}.sql`);
  } else {
    // For rollback, look for the rollback-specific file
    // Replace the first part after the number with 'rollback_'
    const rollbackFile = path.join(__dirname, '..', 'migrations', `${migrationName.replace(/^(\d+)_create_/, '$1_rollback_')}.sql`);
    if (fs.existsSync(rollbackFile)) {
      migrationFile = rollbackFile;
    } else {
      migrationFile = path.join(__dirname, '..', 'migrations', `${migrationName}.sql`);
    }
  }

  // Check if migration file exists
  if (!fs.existsSync(migrationFile)) {
    logger.error('Migration file not found', { migrationFile });
    throw new Error(`Migration file not found: ${migrationFile}`);
  }

  logger.info(`Running ${direction} migration`, { 
    migration: migrationName,
    file: path.basename(migrationFile)
  });

  // Read the migration SQL
  const sql = fs.readFileSync(migrationFile, 'utf8');

  // Execute the migration
  const client = await pool.connect();
  const startTime = Date.now();

  try {
    await client.query(sql);
    const duration = Date.now() - startTime;
    
    logger.info(`Migration ${direction} completed successfully`, {
      migration: migrationName,
      duration: `${duration}ms`
    });

    return { success: true, duration };
  } catch (error) {
    const duration = Date.now() - startTime;
    
    logger.error(`Migration ${direction} failed`, {
      migration: migrationName,
      duration: `${duration}ms`,
      error: error.message,
      stack: error.stack
    });

    throw error;
  } finally {
    client.release();
  }
}

/**
 * Main execution function
 */
async function main() {
  const args = process.argv.slice(2);
  
  if (args.length < 2) {
    console.error('Usage: node scripts/run-migration.js <up|down> <migration-name>');
    console.error('Example: node scripts/run-migration.js up 001_create_conversation_states');
    process.exit(1);
  }

  const [direction, migrationName] = args;

  try {
    logger.info('Starting migration execution', { direction, migrationName });
    
    const result = await runMigration(direction, migrationName);
    
    logger.info('Migration execution completed', { 
      success: result.success,
      duration: result.duration
    });

    await pool.end();
    process.exit(0);
  } catch (error) {
    logger.error('Migration execution failed', { 
      error: error.message,
      stack: error.stack 
    });
    
    await pool.end();
    process.exit(1);
  }
}

// Run the script
main();
