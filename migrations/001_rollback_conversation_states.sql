-- Down migration
BEGIN;

DROP TRIGGER IF EXISTS trigger_conversation_states_updated_at ON conversation_states;
DROP FUNCTION IF EXISTS update_conversation_states_updated_at();
DROP TABLE IF EXISTS conversation_states CASCADE;

COMMIT;
