# Integration Tests for Session State Persistence

This directory contains integration tests that validate the session state persistence feature with real PostgreSQL and Redis instances.

## Overview

The integration tests cover:

1. **Message Flow** - Full end-to-end message processing with persistence
2. **Server Restart** - Session recovery after server shutdown and restart
3. **Graceful Shutdown** - Zero data loss during deployments
4. **Redis Caching** - Cache hit/miss scenarios and fallback behavior
5. **Database Schema** - Schema, indexes, constraints, and triggers
6. **Concurrent Writes** - Multi-instance concurrent write handling

## Prerequisites

### PostgreSQL

You need a PostgreSQL database for testing. You can use:

1. **Local PostgreSQL**:
   ```bash
   # Install PostgreSQL (if not already installed)
   # macOS
   brew install postgresql
   brew services start postgresql
   
   # Ubuntu/Debian
   sudo apt-get install postgresql
   sudo service postgresql start
   
   # Create test database
   createdb mex_end_test
   ```

2. **Docker PostgreSQL**:
   ```bash
   docker run -d \
     --name postgres-test \
     -e POSTGRES_PASSWORD=postgres \
     -e POSTGRES_DB=mex_end_test \
     -p 5432:5432 \
     postgres:15
   ```

### Redis (Optional)

Redis is required for caching tests. You can use:

1. **Local Redis**:
   ```bash
   # macOS
   brew install redis
   brew services start redis
   
   # Ubuntu/Debian
   sudo apt-get install redis-server
   sudo service redis-server start
   ```

2. **Docker Redis**:
   ```bash
   docker run -d \
     --name redis-test \
     -p 6379:6379 \
     redis:7
   ```

## Configuration

Set environment variables for test database connection:

```bash
# PostgreSQL connection
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=mex_end_test
export DB_USER=postgres
export DB_PASSWORD=postgres

# Redis connection (optional)
export REDIS_URL=redis://localhost:6379
```

Or create a `.env.test` file:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mex_end_test
DB_USER=postgres
DB_PASSWORD=postgres
REDIS_URL=redis://localhost:6379
```

## Running Tests

### Run All Integration Tests

```bash
npm test tests/integration
```

### Run Specific Test Suite

```bash
# Message flow tests
npm test tests/integration/message-flow.integration.test.js

# Server restart tests
npm test tests/integration/server-restart.integration.test.js

# Graceful shutdown tests
npm test tests/integration/graceful-shutdown.integration.test.js

# Redis caching tests
npm test tests/integration/redis-caching.integration.test.js

# Database schema tests
npm test tests/integration/database-schema.integration.test.js

# Concurrent writes tests
npm test tests/integration/concurrent-writes.integration.test.js
```

### Run with Watch Mode

```bash
npm run test:watch tests/integration
```

### Run with UI

```bash
npm run test:ui
```

## Test Requirements

### Database Setup

The tests automatically create the `conversation_states` table if it doesn't exist. However, for a clean test environment, you may want to run the migration script first:

```bash
node backend/scripts/run-migration.js up
```

### Cleanup

Tests clean up their own data using `beforeEach` hooks. However, if you want to manually clean the test database:

```bash
# Drop and recreate test database
dropdb mex_end_test
createdb mex_end_test

# Or run rollback migration
node backend/scripts/run-migration.js down
```

## Test Coverage

### Requirements Validated

The integration tests validate the following requirements from the spec:

- **3.1, 4.1, 15.2**: Full message flow with persistence
- **4.1, 9.5, 9.6, 15.2**: Server restart with recovery
- **9.1, 9.2, 9.3, 9.4**: Graceful shutdown
- **6.1, 6.2, 6.3, 6.4, 6.6, 7.6, 15.6**: Redis caching
- **1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7**: Database schema and constraints
- **7.2, 7.3**: Concurrent writes

### Performance Targets

The tests validate these performance targets:

- Session recovery: 1000 sessions in < 5 seconds
- Graceful shutdown: Complete within 10 seconds
- Cache hit rate: >= 80% for active conversations
- Concurrent writes: >= 100 operations per second

## Troubleshooting

### Connection Errors

If you see connection errors:

1. Verify PostgreSQL is running:
   ```bash
   psql -h localhost -U postgres -d mex_end_test -c "SELECT 1"
   ```

2. Check Redis is running (if using cache tests):
   ```bash
   redis-cli ping
   ```

3. Verify environment variables are set correctly

### Test Failures

If tests fail:

1. Check database logs for errors
2. Ensure test database is clean (no conflicting data)
3. Verify you have sufficient database connections (max: 20)
4. Check for port conflicts (5432 for PostgreSQL, 6379 for Redis)

### Slow Tests

If tests are slow:

1. Ensure database has proper indexes (run migration script)
2. Check database connection pool settings
3. Consider using local database instead of remote
4. Reduce test data size for faster execution

## CI/CD Integration

For CI/CD pipelines, use Docker Compose to set up test dependencies:

```yaml
# docker-compose.test.yml
version: '3.8'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: mex_end_test
    ports:
      - "5432:5432"
    
  redis:
    image: redis:7
    ports:
      - "6379:6379"
```

Run tests in CI:

```bash
# Start dependencies
docker-compose -f docker-compose.test.yml up -d

# Wait for services to be ready
sleep 5

# Run tests
npm test tests/integration

# Cleanup
docker-compose -f docker-compose.test.yml down
```

## Notes

- Integration tests use real database connections and may be slower than unit tests
- Tests are isolated and clean up their own data
- Each test uses a unique admin_id to avoid conflicts
- Tests can run in parallel if using different admin_ids
- Redis tests gracefully skip if Redis is unavailable (except redis-caching.integration.test.js)
