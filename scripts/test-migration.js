#!/usr/bin/env node

/**
 * Migration Test Script
 * 
 * Tests the conversation_states migration against local PostgreSQL:
 * 1. Run migration up - verify table and indexes created
 * 2. Run migration down - verify clean rollback
 * 3. Test foreign key CASCADE behavior
 * 
 * Task: 1.4 Test migration scripts against local PostgreSQL
 * Requirements: 1.7, 12.6
 */

import pg from 'pg';
import dotenv from 'dotenv';
import logger from '../config/logger.js';

dotenv.config();

const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

/**
 * Check if table exists
 */
async function tableExists(tableName) {
  const query = `
    SELECT EXISTS (
      SELECT FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_name = $1
    );
  `;
  const result = await pool.query(query, [tableName]);
  return result.rows[0].exists;
}

/**
 * Check if index exists
 */
async function indexExists(indexName) {
  const query = `
    SELECT EXISTS (
      SELECT FROM pg_indexes 
      WHERE schemaname = 'public' 
      AND indexname = $1
    );
  `;
  const result = await pool.query(query, [indexName]);
  return result.rows[0].exists;
}

/**
 * Check if trigger exists
 */
async function triggerExists(triggerName, tableName) {
  const query = `
    SELECT EXISTS (
      SELECT FROM information_schema.triggers 
      WHERE trigger_schema = 'public' 
      AND trigger_name = $1
      AND event_object_table = $2
    );
  `;
  const result = await pool.query(query, [triggerName, tableName]);
  return result.rows[0].exists;
}

/**
 * Check if function exists
 */
async function functionExists(functionName) {
  const query = `
    SELECT EXISTS (
      SELECT FROM pg_proc 
      WHERE proname = $1
    );
  `;
  const result = await pool.query(query, [functionName]);
  return result.rows[0].exists;
}

/**
 * Get table constraints
 */
async function getTableConstraints(tableName) {
  const query = `
    SELECT 
      conname as constraint_name,
      contype as constraint_type
    FROM pg_constraint
    WHERE conrelid = $1::regclass;
  `;
  const result = await pool.query(query, [tableName]);
  return result.rows;
}

/**
 * Get foreign key details
 */
async function getForeignKeyDetails(tableName) {
  const query = `
    SELECT
      tc.constraint_name,
      tc.table_name,
      kcu.column_name,
      ccu.table_name AS foreign_table_name,
      ccu.column_name AS foreign_column_name,
      rc.delete_rule
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
    JOIN information_schema.referential_constraints AS rc
      ON tc.constraint_name = rc.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_name = $1;
  `;
  const result = await pool.query(query, [tableName]);
  return result.rows;
}

/**
 * Test 1: Verify table and indexes created after migration up
 */
async function testMigrationUp() {
  console.log('\n=== Test 1: Verify Migration Up ===\n');
  
  const tests = [];
  
  // Check table exists
  const tableExistsResult = await tableExists('conversation_states');
  tests.push({
    name: 'Table conversation_states exists',
    passed: tableExistsResult,
    expected: true,
    actual: tableExistsResult
  });
  
  if (!tableExistsResult) {
    console.error('❌ Table does not exist. Cannot continue tests.');
    return tests;
  }
  
  // Check indexes
  const indexes = [
    'idx_conversation_states_admin_id',
    'idx_conversation_states_last_activity',
    'idx_conversation_states_admin_activity',
    'idx_conversation_states_session_data'
  ];
  
  for (const indexName of indexes) {
    const exists = await indexExists(indexName);
    tests.push({
      name: `Index ${indexName} exists`,
      passed: exists,
      expected: true,
      actual: exists
    });
  }
  
  // Check trigger
  const triggerExistsResult = await triggerExists('trigger_conversation_states_updated_at', 'conversation_states');
  tests.push({
    name: 'Trigger trigger_conversation_states_updated_at exists',
    passed: triggerExistsResult,
    expected: true,
    actual: triggerExistsResult
  });
  
  // Check function
  const functionExistsResult = await functionExists('update_conversation_states_updated_at');
  tests.push({
    name: 'Function update_conversation_states_updated_at exists',
    passed: functionExistsResult,
    expected: true,
    actual: functionExistsResult
  });
  
  // Check constraints
  const constraints = await getTableConstraints('conversation_states');
  const hasUniqueConstraint = constraints.some(c => 
    c.constraint_name === 'unique_admin_phone' && c.constraint_type === 'u'
  );
  tests.push({
    name: 'Unique constraint unique_admin_phone exists',
    passed: hasUniqueConstraint,
    expected: true,
    actual: hasUniqueConstraint
  });
  
  const hasForeignKey = constraints.some(c => c.constraint_type === 'f');
  tests.push({
    name: 'Foreign key constraint exists',
    passed: hasForeignKey,
    expected: true,
    actual: hasForeignKey
  });
  
  // Check foreign key CASCADE behavior
  const fkDetails = await getForeignKeyDetails('conversation_states');
  const hasCascadeDelete = fkDetails.some(fk => 
    fk.foreign_table_name === 'admins' && 
    fk.delete_rule === 'CASCADE'
  );
  tests.push({
    name: 'Foreign key has CASCADE delete rule',
    passed: hasCascadeDelete,
    expected: true,
    actual: hasCascadeDelete
  });
  
  return tests;
}

/**
 * Test 2: Test foreign key CASCADE behavior
 */
async function testCascadeBehavior() {
  console.log('\n=== Test 2: Test Foreign Key CASCADE Behavior ===\n');
  
  const tests = [];
  const client = await pool.connect();
  
  try {
    await client.query('BEGIN');
    
    // Create a test admin
    const adminResult = await client.query(`
      INSERT INTO admins (email, password_hash, name, phone, admin_tier, status, automation_enabled, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
      RETURNING id
    `, ['test_cascade@example.com', 'test_password_hash', 'Test Cascade Admin', '+1234567890', 'client_admin', 'active', true]);
    
    const testAdminId = adminResult.rows[0].id;
    
    // Insert a conversation state for this admin
    await client.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3, NOW())
    `, [testAdminId, '+9876543210', JSON.stringify({ step: 'test', data: {} })]);
    
    // Verify conversation state exists
    const beforeDelete = await client.query(
      'SELECT COUNT(*) FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    const countBefore = parseInt(beforeDelete.rows[0].count);
    
    tests.push({
      name: 'Conversation state inserted successfully',
      passed: countBefore === 1,
      expected: 1,
      actual: countBefore
    });
    
    // Delete the admin
    await client.query('DELETE FROM admins WHERE id = $1', [testAdminId]);
    
    // Verify conversation state was CASCADE deleted
    const afterDelete = await client.query(
      'SELECT COUNT(*) FROM conversation_states WHERE admin_id = $1',
      [testAdminId]
    );
    const countAfter = parseInt(afterDelete.rows[0].count);
    
    tests.push({
      name: 'Conversation state CASCADE deleted with admin',
      passed: countAfter === 0,
      expected: 0,
      actual: countAfter
    });
    
    await client.query('ROLLBACK');
  } catch (error) {
    await client.query('ROLLBACK');
    tests.push({
      name: 'CASCADE behavior test',
      passed: false,
      expected: 'No error',
      actual: error.message
    });
  } finally {
    client.release();
  }
  
  return tests;
}

/**
 * Test 3: Verify clean rollback after migration down
 */
async function testMigrationDown() {
  console.log('\n=== Test 3: Verify Migration Down (Clean Rollback) ===\n');
  
  const tests = [];
  
  // Check table does not exist
  const tableExistsResult = await tableExists('conversation_states');
  tests.push({
    name: 'Table conversation_states removed',
    passed: !tableExistsResult,
    expected: false,
    actual: tableExistsResult
  });
  
  // Check trigger removed
  const triggerExistsResult = await triggerExists('trigger_conversation_states_updated_at', 'conversation_states');
  tests.push({
    name: 'Trigger removed',
    passed: !triggerExistsResult,
    expected: false,
    actual: triggerExistsResult
  });
  
  // Check function removed
  const functionExistsResult = await functionExists('update_conversation_states_updated_at');
  tests.push({
    name: 'Function removed',
    passed: !functionExistsResult,
    expected: false,
    actual: functionExistsResult
  });
  
  return tests;
}

/**
 * Print test results
 */
function printResults(tests) {
  let passed = 0;
  let failed = 0;
  
  for (const test of tests) {
    if (test.passed) {
      console.log(`✅ ${test.name}`);
      passed++;
    } else {
      console.log(`❌ ${test.name}`);
      console.log(`   Expected: ${test.expected}`);
      console.log(`   Actual: ${test.actual}`);
      failed++;
    }
  }
  
  console.log(`\nResults: ${passed} passed, ${failed} failed\n`);
  return failed === 0;
}

/**
 * Main test execution
 */
async function main() {
  console.log('=================================================');
  console.log('Migration Test Suite');
  console.log('Testing: 001_create_conversation_states migration');
  console.log('=================================================');
  
  let allTestsPassed = true;
  
  try {
    // Check if table exists to determine which tests to run
    const tableExistsNow = await tableExists('conversation_states');
    
    if (tableExistsNow) {
      console.log('\n📊 Table exists - Testing migration UP state\n');
      
      // Test 1: Verify migration up
      const upTests = await testMigrationUp();
      const upPassed = printResults(upTests);
      allTestsPassed = allTestsPassed && upPassed;
      
      // Test 2: Test CASCADE behavior
      const cascadeTests = await testCascadeBehavior();
      const cascadePassed = printResults(cascadeTests);
      allTestsPassed = allTestsPassed && cascadePassed;
    } else {
      console.log('\n📊 Table does not exist - Testing migration DOWN state\n');
      
      // Test 3: Verify migration down
      const downTests = await testMigrationDown();
      const downPassed = printResults(downTests);
      allTestsPassed = allTestsPassed && downPassed;
    }
    
    // Instructions for manual testing
    console.log('=================================================');
    console.log('Manual Testing Instructions:');
    console.log('=================================================');
    console.log('\n1. To test migration UP:');
    console.log('   node scripts/run-migration.js up 001_create_conversation_states');
    console.log('   node scripts/test-migration.js');
    console.log('\n2. To test migration DOWN:');
    console.log('   node scripts/run-migration.js down 001_create_conversation_states');
    console.log('   node scripts/test-migration.js');
    console.log('\n3. To test full cycle:');
    console.log('   node scripts/run-migration.js up 001_create_conversation_states');
    console.log('   node scripts/test-migration.js');
    console.log('   node scripts/run-migration.js down 001_create_conversation_states');
    console.log('   node scripts/test-migration.js');
    console.log('=================================================\n');
    
    await pool.end();
    
    if (allTestsPassed) {
      console.log('✅ All tests passed!\n');
      process.exit(0);
    } else {
      console.log('❌ Some tests failed.\n');
      process.exit(1);
    }
  } catch (error) {
    logger.error('Test execution failed', { 
      error: error.message,
      stack: error.stack 
    });
    await pool.end();
    process.exit(1);
  }
}

main();
