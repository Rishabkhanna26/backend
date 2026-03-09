import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import pg from 'pg';
import { readFile } from 'fs/promises';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const { Pool } = pg;
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

/**
 * Integration Test: Database Schema and Constraints
 * 
 * This test validates the database schema, indexes, constraints, and triggers
 * for the conversation_states table.
 * 
 * Requirements tested: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7
 */
describe('Integration: Database Schema and Constraints', () => {
  let pool;
  
  const testAdminId = 995;
  
  beforeAll(async () => {
    pool = new Pool({
      host: process.env.DB_HOST || 'localhost',
      port: process.env.DB_PORT || 5432,
      database: process.env.DB_NAME || 'mex_end_test',
      user: process.env.DB_USER || 'postgres',
      password: process.env.DB_PASSWORD || 'postgres'
    });
  });
  
  afterAll(async () => {
    await pool.end();
  });
  
  beforeEach(async () => {
    // Clean up test data
    await pool.query('DELETE FROM conversation_states WHERE admin_id = $1', [testAdminId]);
  });
  
  it('should create conversation_states table with correct schema', async () => {
    // Act: Query table schema
    const result = await pool.query(`
      SELECT column_name, data_type, is_nullable, column_default
      FROM information_schema.columns
      WHERE table_name = 'conversation_states'
      ORDER BY ordinal_position
    `);
    
    // Assert: Verify all required columns exist
    const columns = result.rows.reduce((acc, row) => {
      acc[row.column_name] = {
        type: row.data_type,
        nullable: row.is_nullable === 'YES',
        default: row.column_default
      };
      return acc;
    }, {});
    
    expect(columns.id).toBeDefined();
    expect(columns.id.type).toBe('integer');
    
    expect(columns.admin_id).toBeDefined();
    expect(columns.admin_id.type).toBe('integer');
    expect(columns.admin_id.nullable).toBe(false);
    
    expect(columns.phone).toBeDefined();
    expect(columns.phone.type).toBe('character varying');
    expect(columns.phone.nullable).toBe(false);
    
    expect(columns.session_data).toBeDefined();
    expect(columns.session_data.type).toBe('jsonb');
    expect(columns.session_data.nullable).toBe(false);
    
    expect(columns.last_activity_at).toBeDefined();
    expect(columns.last_activity_at.type).toBe('timestamp with time zone');
    expect(columns.last_activity_at.nullable).toBe(false);
    
    expect(columns.created_at).toBeDefined();
    expect(columns.created_at.type).toBe('timestamp with time zone');
    expect(columns.created_at.nullable).toBe(false);
    
    expect(columns.updated_at).toBeDefined();
    expect(columns.updated_at.type).toBe('timestamp with time zone');
    expect(columns.updated_at.nullable).toBe(false);
  });
  
  it('should have composite unique constraint on (admin_id, phone)', async () => {
    // Arrange: Insert a conversation state
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data)
      VALUES ($1, $2, $3)
    `, [testAdminId, '+1234567890', JSON.stringify({ step: 'awaiting_name' })]);
    
    // Act & Assert: Attempt to insert duplicate should fail
    await expect(
      pool.query(`
        INSERT INTO conversation_states (admin_id, phone, session_data)
        VALUES ($1, $2, $3)
      `, [testAdminId, '+1234567890', JSON.stringify({ step: 'awaiting_email' })])
    ).rejects.toThrow(/unique_admin_phone|duplicate key/i);
  });
  
  it('should allow same phone for different admins', async () => {
    const admin1Id = 995;
    const admin2Id = 994;
    const phone = '+1111111111';
    
    // Act: Insert same phone for two different admins
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data)
      VALUES ($1, $2, $3)
    `, [admin1Id, phone, JSON.stringify({ step: 'awaiting_name' })]);
    
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data)
      VALUES ($1, $2, $3)
    `, [admin2Id, phone, JSON.stringify({ step: 'awaiting_email' })]);
    
    // Assert: Both records exist
    const result = await pool.query(`
      SELECT admin_id, phone FROM conversation_states
      WHERE phone = $1
      ORDER BY admin_id
    `, [phone]);
    
    expect(result.rows).toHaveLength(2);
    expect(result.rows[0].admin_id).toBe(admin2Id);
    expect(result.rows[1].admin_id).toBe(admin1Id);
    
    // Cleanup
    await pool.query('DELETE FROM conversation_states WHERE admin_id = $1', [admin2Id]);
  });
  
  it('should have required indexes for performance', async () => {
    // Act: Query indexes
    const result = await pool.query(`
      SELECT indexname, indexdef
      FROM pg_indexes
      WHERE tablename = 'conversation_states'
    `);
    
    const indexes = result.rows.reduce((acc, row) => {
      acc[row.indexname] = row.indexdef;
      return acc;
    }, {});
    
    // Assert: Verify required indexes exist
    expect(indexes.idx_conversation_states_admin_id).toBeDefined();
    expect(indexes.idx_conversation_states_admin_id).toContain('admin_id');
    
    expect(indexes.idx_conversation_states_last_activity).toBeDefined();
    expect(indexes.idx_conversation_states_last_activity).toContain('last_activity_at');
    
    expect(indexes.idx_conversation_states_admin_activity).toBeDefined();
    expect(indexes.idx_conversation_states_admin_activity).toContain('admin_id');
    expect(indexes.idx_conversation_states_admin_activity).toContain('last_activity_at');
    
    // GIN index for JSONB
    expect(indexes.idx_conversation_states_session_data).toBeDefined();
    expect(indexes.idx_conversation_states_session_data).toContain('session_data');
    expect(indexes.idx_conversation_states_session_data).toContain('gin');
  });
  
  it('should have updated_at trigger that updates timestamp', async () => {
    // Arrange: Insert a record
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data)
      VALUES ($1, $2, $3)
    `, [testAdminId, '+2222222222', JSON.stringify({ step: 'awaiting_name' })]);
    
    // Get initial timestamps
    const result1 = await pool.query(`
      SELECT created_at, updated_at
      FROM conversation_states
      WHERE admin_id = $1 AND phone = $2
    `, [testAdminId, '+2222222222']);
    
    const initialCreatedAt = result1.rows[0].created_at;
    const initialUpdatedAt = result1.rows[0].updated_at;
    
    // Wait a bit
    await new Promise(resolve => setTimeout(resolve, 100));
    
    // Act: Update the record
    await pool.query(`
      UPDATE conversation_states
      SET session_data = $3
      WHERE admin_id = $1 AND phone = $2
    `, [testAdminId, '+2222222222', JSON.stringify({ step: 'awaiting_email' })]);
    
    // Get updated timestamps
    const result2 = await pool.query(`
      SELECT created_at, updated_at
      FROM conversation_states
      WHERE admin_id = $1 AND phone = $2
    `, [testAdminId, '+2222222222']);
    
    const finalCreatedAt = result2.rows[0].created_at;
    const finalUpdatedAt = result2.rows[0].updated_at;
    
    // Assert: created_at unchanged, updated_at changed
    expect(new Date(finalCreatedAt).getTime()).toBe(new Date(initialCreatedAt).getTime());
    expect(new Date(finalUpdatedAt).getTime()).toBeGreaterThan(new Date(initialUpdatedAt).getTime());
  });
  
  it('should store JSONB data efficiently', async () => {
    // Arrange: Create complex session data
    const complexData = {
      step: 'awaiting_confirmation',
      data: {
        selectedProduct: 'birth_chart',
        quantity: 5,
        totalAmount: 500,
        appointmentDate: '2024-03-15',
        specialRequests: 'Detailed analysis with planetary positions',
        nested: {
          level1: {
            level2: {
              level3: 'deep value'
            }
          }
        }
      },
      isReturningUser: true,
      clientId: 12345,
      name: 'JSONB Test User',
      email: 'jsonb@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: 'awaiting_confirmation',
      awaitingResumeDecision: false,
      lastUserMessageAt: { __type: 'Date', value: '2024-01-15T10:30:00.000Z' },
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: [
        { role: 'user', content: 'I want to order' },
        { role: 'assistant', content: 'Great! How many?' },
        { role: 'user', content: 'Five please' }
      ],
      responseLanguage: 'en'
    };
    
    // Act: Insert complex data
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data)
      VALUES ($1, $2, $3)
    `, [testAdminId, '+3333333333', JSON.stringify(complexData)]);
    
    // Query using JSONB operators
    const result = await pool.query(`
      SELECT session_data
      FROM conversation_states
      WHERE admin_id = $1
        AND phone = $2
        AND session_data->>'step' = 'awaiting_confirmation'
        AND (session_data->'data'->>'totalAmount')::int > 100
    `, [testAdminId, '+3333333333']);
    
    // Assert: JSONB query works correctly
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].session_data.name).toBe('JSONB Test User');
    expect(result.rows[0].session_data.data.nested.level1.level2.level3).toBe('deep value');
    expect(result.rows[0].session_data.aiConversationHistory).toHaveLength(3);
  });
  
  it('should enforce NOT NULL constraints', async () => {
    // Act & Assert: Attempt to insert without required fields
    
    // Missing admin_id
    await expect(
      pool.query(`
        INSERT INTO conversation_states (phone, session_data)
        VALUES ($1, $2)
      `, ['+4444444444', JSON.stringify({ step: 'awaiting_name' })])
    ).rejects.toThrow(/null value|violates not-null/i);
    
    // Missing phone
    await expect(
      pool.query(`
        INSERT INTO conversation_states (admin_id, session_data)
        VALUES ($1, $2)
      `, [testAdminId, JSON.stringify({ step: 'awaiting_name' })])
    ).rejects.toThrow(/null value|violates not-null/i);
    
    // Missing session_data
    await expect(
      pool.query(`
        INSERT INTO conversation_states (admin_id, phone)
        VALUES ($1, $2)
      `, [testAdminId, '+5555555555'])
    ).rejects.toThrow(/null value|violates not-null/i);
  });
  
  it('should set default timestamps on insert', async () => {
    // Act: Insert without specifying timestamps
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data)
      VALUES ($1, $2, $3)
    `, [testAdminId, '+6666666666', JSON.stringify({ step: 'awaiting_name' })]);
    
    // Query timestamps
    const result = await pool.query(`
      SELECT created_at, updated_at, last_activity_at
      FROM conversation_states
      WHERE admin_id = $1 AND phone = $2
    `, [testAdminId, '+6666666666']);
    
    // Assert: All timestamps set automatically
    expect(result.rows[0].created_at).toBeTruthy();
    expect(result.rows[0].updated_at).toBeTruthy();
    expect(result.rows[0].last_activity_at).toBeTruthy();
    
    // Timestamps should be recent (within last minute)
    const now = Date.now();
    const createdAt = new Date(result.rows[0].created_at).getTime();
    expect(now - createdAt).toBeLessThan(60000);
  });
  
  it('should support upsert operations (INSERT ... ON CONFLICT)', async () => {
    // Arrange: Insert initial record
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data)
      VALUES ($1, $2, $3)
    `, [testAdminId, '+7777777777', JSON.stringify({ step: 'awaiting_name', name: 'Initial' })]);
    
    // Act: Upsert (update existing record)
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
      VALUES ($1, $2, $3, NOW())
      ON CONFLICT (admin_id, phone)
      DO UPDATE SET
        session_data = EXCLUDED.session_data,
        last_activity_at = EXCLUDED.last_activity_at
    `, [testAdminId, '+7777777777', JSON.stringify({ step: 'awaiting_email', name: 'Updated' })]);
    
    // Assert: Record updated, not duplicated
    const result = await pool.query(`
      SELECT session_data FROM conversation_states
      WHERE admin_id = $1 AND phone = $2
    `, [testAdminId, '+7777777777']);
    
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].session_data.step).toBe('awaiting_email');
    expect(result.rows[0].session_data.name).toBe('Updated');
  });
  
  it('should handle large JSONB documents efficiently', async () => {
    // Arrange: Create large conversation history
    const largeHistory = [];
    for (let i = 0; i < 100; i++) {
      largeHistory.push({
        role: i % 2 === 0 ? 'user' : 'assistant',
        content: `Message ${i}: ${'Lorem ipsum dolor sit amet '.repeat(10)}`,
        timestamp: new Date().toISOString()
      });
    }
    
    const largeData = {
      step: 'awaiting_confirmation',
      data: { selectedProduct: 'birth_chart' },
      isReturningUser: true,
      clientId: 99999,
      name: 'Large Data User',
      email: 'large@example.com',
      assignedAdminId: testAdminId,
      greetedThisSession: true,
      resumeStep: null,
      awaitingResumeDecision: false,
      lastUserMessageAt: { __type: 'Date', value: new Date().toISOString() },
      partialSavedAt: null,
      finalized: false,
      automationDisabled: false,
      aiConversationHistory: largeHistory,
      responseLanguage: 'en'
    };
    
    // Act: Insert large document
    const startTime = Date.now();
    await pool.query(`
      INSERT INTO conversation_states (admin_id, phone, session_data)
      VALUES ($1, $2, $3)
    `, [testAdminId, '+8888888888', JSON.stringify(largeData)]);
    const insertTime = Date.now() - startTime;
    
    // Query large document
    const queryStart = Date.now();
    const result = await pool.query(`
      SELECT session_data FROM conversation_states
      WHERE admin_id = $1 AND phone = $2
    `, [testAdminId, '+8888888888']);
    const queryTime = Date.now() - queryStart;
    
    // Assert: Operations complete efficiently
    expect(insertTime).toBeLessThan(1000); // < 1 second
    expect(queryTime).toBeLessThan(500);   // < 500ms
    expect(result.rows[0].session_data.aiConversationHistory).toHaveLength(100);
    
    console.log(`Large JSONB insert: ${insertTime}ms, query: ${queryTime}ms`);
  });
  
  it('should support efficient cleanup queries using indexes', async () => {
    // Arrange: Insert multiple records with different timestamps
    const now = Date.now();
    const sixHoursAgo = new Date(now - 6 * 60 * 60 * 1000);
    const twelveHoursAgo = new Date(now - 12 * 60 * 60 * 1000);
    
    // Recent records
    for (let i = 0; i < 5; i++) {
      await pool.query(`
        INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
        VALUES ($1, $2, $3, NOW())
      `, [testAdminId, `+100000000${i}`, JSON.stringify({ step: 'awaiting_name' })]);
    }
    
    // Old records
    for (let i = 0; i < 5; i++) {
      await pool.query(`
        INSERT INTO conversation_states (admin_id, phone, session_data, last_activity_at)
        VALUES ($1, $2, $3, $4)
      `, [testAdminId, `+200000000${i}`, JSON.stringify({ step: 'awaiting_name' }), twelveHoursAgo]);
    }
    
    // Act: Run cleanup query (using index)
    const startTime = Date.now();
    const result = await pool.query(`
      DELETE FROM conversation_states
      WHERE admin_id = $1
        AND last_activity_at < $2
    `, [testAdminId, sixHoursAgo]);
    const queryTime = Date.now() - startTime;
    
    // Assert: Old records deleted efficiently
    expect(result.rowCount).toBe(5);
    expect(queryTime).toBeLessThan(100); // Should be fast with index
    
    // Verify recent records remain
    const remaining = await pool.query(`
      SELECT COUNT(*) as count FROM conversation_states
      WHERE admin_id = $1
    `, [testAdminId]);
    
    expect(parseInt(remaining.rows[0].count)).toBe(5);
  });
});
