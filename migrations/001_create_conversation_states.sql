-- Migration: Create conversation_states table
-- Description: Creates the conversation_states table for persistent session storage
-- Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7

BEGIN;

-- Create conversation_states table
CREATE TABLE IF NOT EXISTS conversation_states (
  id SERIAL PRIMARY KEY,
  admin_id INTEGER NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
  phone VARCHAR(20) NOT NULL,
  session_data JSONB NOT NULL,
  last_activity_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  
  -- Composite unique constraint on (admin_id, phone)
  CONSTRAINT unique_admin_phone UNIQUE (admin_id, phone)
);

-- Create index on admin_id for admin-specific queries
CREATE INDEX IF NOT EXISTS idx_conversation_states_admin_id 
  ON conversation_states(admin_id);

-- Create index on last_activity_at for cleanup queries
CREATE INDEX IF NOT EXISTS idx_conversation_states_last_activity 
  ON conversation_states(last_activity_at);

-- Create composite index on (admin_id, last_activity_at) for efficient admin-specific cleanup
CREATE INDEX IF NOT EXISTS idx_conversation_states_admin_activity 
  ON conversation_states(admin_id, last_activity_at);

-- Create GIN index on session_data for JSONB queries
CREATE INDEX IF NOT EXISTS idx_conversation_states_session_data 
  ON conversation_states USING GIN(session_data);

-- Create trigger function to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_conversation_states_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to call the update function before each UPDATE
CREATE TRIGGER trigger_conversation_states_updated_at
  BEFORE UPDATE ON conversation_states
  FOR EACH ROW
  EXECUTE FUNCTION update_conversation_states_updated_at();

COMMIT;
