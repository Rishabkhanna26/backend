# Migration Scripts

This directory contains scripts for managing database migrations.

## run-migration.js

Executes SQL migrations for the session state persistence feature.

### Usage

**Run an up migration (apply changes):**
```bash
node backend/scripts/run-migration.js up 001_create_conversation_states
```

**Run a down migration (rollback changes):**
```bash
node backend/scripts/run-migration.js down 001_create_conversation_states
```

### Features

- ✅ Supports both up and down migrations
- ✅ Logs migration results with timestamps and duration
- ✅ Validates migration files exist before execution
- ✅ Provides detailed error messages on failure
- ✅ Uses existing database connection configuration
- ✅ Gracefully handles connection cleanup

### Migration File Naming Convention

- **Up migrations**: `{number}_{name}.sql` (e.g., `001_create_conversation_states.sql`)
- **Down migrations**: `{number}_rollback_{name}.sql` (e.g., `001_rollback_conversation_states.sql`)

### Environment Variables

The script uses the following environment variables from `.env`:

- `DATABASE_URL` - PostgreSQL connection string (required)
- `LOG_LEVEL` - Logging level (default: 'info')
- `NODE_ENV` - Environment (development/production)

### Exit Codes

- `0` - Migration completed successfully
- `1` - Migration failed or invalid arguments

### Examples

```bash
# Apply the conversation_states table migration
node backend/scripts/run-migration.js up 001_create_conversation_states

# Rollback the conversation_states table migration
node backend/scripts/run-migration.js down 001_create_conversation_states
```

### Logging

All migration operations are logged using Winston logger with the following information:

- Migration name and direction (up/down)
- Execution duration
- Success/failure status
- Error details (if failed)

Logs are written to:
- Console (always)
- `logs/combined.log` (production)
- `logs/error.log` (errors only, production)
