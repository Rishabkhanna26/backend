# Load and Performance Tests

This directory contains load and performance tests for the session state persistence feature. These tests validate system performance under realistic load conditions and ensure the system meets performance targets.

## Test Files

### 1. concurrent-conversations.load.test.js
Tests system performance with concurrent conversations:
- 100 concurrent conversations with 10 messages each
- Validates p95 persistence latency < 100ms
- Validates p99 persistence latency < 500ms
- Validates message processing overhead < 50ms
- Tests burst traffic patterns and varying message sizes

**Requirements tested:** 11.1, 11.2, 11.4, 11.6, 15.3

### 2. session-recovery.load.test.js
Tests session recovery performance on server startup:
- Creates 1000 active sessions in database
- Measures recovery time on startup
- Validates recovery completes in < 5 seconds
- Tests recovery with mixed session ages and large conversation histories

**Requirements tested:** 4.7, 11.3, 15.4

### 3. sustained-throughput.load.test.js
Tests system performance under sustained load:
- Simulates sustained load: 50 messages/second for 5 minutes
- Validates system maintains performance targets
- Monitors memory usage to detect leaks
- Tests performance degradation over time

**Requirements tested:** 11.4, 11.7

### 4. cache-performance.load.test.js
Tests Redis cache performance under load:
- Simulates 500 active conversations with Redis enabled
- Measures cache hit rate (target: 80%)
- Validates cache reads < 10ms for p99
- Tests cache eviction and concurrent access

**Requirements tested:** 6.7, 11.5

### 5. cleanup-performance.load.test.js
Tests cleanup service performance:
- Inserts 10,000 expired sessions
- Runs cleanup service
- Validates batch limiting (1000 per run)
- Tests cleanup of finalized and mixed sessions

**Requirements tested:** 5.5, 5.6

## Prerequisites

### Database Setup
Load tests require a PostgreSQL test database:

```bash
# Create test database
createdb mex_end_test

# Or use Docker
docker run -d \
  --name postgres-test \
  -e POSTGRES_DB=mex_end_test \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:15
```

### Redis Setup (for cache tests)
Cache performance tests require Redis:

```bash
# Install Redis locally
# macOS
brew install redis
brew services start redis

# Or use Docker
docker run -d \
  --name redis-test \
  -p 6379:6379 \
  redis:7
```

### Environment Variables
Set the following environment variables for test configuration:

```bash
# Database configuration
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=mex_end_test
export DB_USER=postgres
export DB_PASSWORD=postgres

# Redis configuration (for cache tests)
export REDIS_URL=redis://localhost:6379
```

## Running the Tests

### Run All Load Tests
```bash
npm test tests/load/
```

### Run Individual Test Files
```bash
# Concurrent conversations test
npm test tests/load/concurrent-conversations.load.test.js

# Session recovery test
npm test tests/load/session-recovery.load.test.js

# Sustained throughput test
npm test tests/load/sustained-throughput.load.test.js

# Cache performance test (requires Redis)
npm test tests/load/cache-performance.load.test.js

# Cleanup performance test
npm test tests/load/cleanup-performance.load.test.js
```

### Run with Vitest Watch Mode
```bash
npm run test:watch tests/load/
```

### Run with UI
```bash
npm run test:ui
```

## Test Timeouts

Load tests have extended timeouts due to their nature:
- Concurrent conversations: 60 seconds
- Session recovery: 30 seconds
- Sustained throughput: 360 seconds (6 minutes)
- Cache performance: 120 seconds
- Cleanup performance: 120 seconds

## Performance Targets

The tests validate the following performance targets:

### Latency Targets
- **p95 persistence latency:** < 100ms
- **p99 persistence latency:** < 500ms
- **Message processing overhead:** < 50ms
- **Cache read latency (p99):** < 10ms

### Throughput Targets
- **Concurrent state updates:** ≥ 100 updates/second
- **Sustained message rate:** ≥ 50 messages/second
- **Session recovery:** 1000 sessions in < 5 seconds

### Cache Targets
- **Cache hit rate:** ≥ 80%
- **Database load reduction:** ≥ 80%

### Cleanup Targets
- **Batch size limit:** ≤ 1000 sessions per run
- **Cleanup efficiency:** < 5 seconds per batch

## Interpreting Results

Each test outputs detailed performance metrics:

### Latency Metrics
- **Average:** Mean latency across all operations
- **p50 (median):** 50th percentile latency
- **p95:** 95th percentile latency (95% of operations complete within this time)
- **p99:** 99th percentile latency (99% of operations complete within this time)
- **Max:** Maximum latency observed

### Throughput Metrics
- **Messages/second:** Number of messages processed per second
- **Sessions/second:** Number of sessions recovered or cleaned per second

### Memory Metrics
- **Heap Used:** JavaScript heap memory usage
- **RSS:** Resident Set Size (total memory allocated)
- **Memory Growth:** Change in memory usage over test duration

## Troubleshooting

### Tests Failing Due to Timeouts
- Increase test timeouts in the test files
- Check database and Redis performance
- Ensure sufficient system resources

### Database Connection Errors
- Verify PostgreSQL is running
- Check database credentials in environment variables
- Ensure test database exists

### Redis Connection Errors
- Verify Redis is running
- Check REDIS_URL environment variable
- Cache tests will skip if Redis is unavailable

### Performance Targets Not Met
- Check system resources (CPU, memory, disk I/O)
- Verify database indexes are created
- Check for other processes consuming resources
- Consider hardware limitations

## CI/CD Integration

Load tests can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run Load Tests
  run: |
    docker-compose up -d postgres redis
    npm test tests/load/
  env:
    DB_HOST: localhost
    DB_PORT: 5432
    DB_NAME: mex_end_test
    DB_USER: postgres
    DB_PASSWORD: postgres
    REDIS_URL: redis://localhost:6379
```

## Notes

- Load tests create and delete test data automatically
- Tests use `admin_id` values 1000-5000 to avoid conflicts
- Database is cleaned before each test
- Tests are designed to be idempotent and can be run multiple times
- Some tests (especially sustained throughput) may take several minutes to complete

## Performance Monitoring

For production monitoring, consider:
- Setting up metrics collection (Prometheus, Grafana)
- Configuring alerts for performance degradation
- Regular load testing to establish baselines
- Monitoring database query performance
- Tracking cache hit rates in production

## Related Documentation

- [Integration Tests](../integration/README.md)
- [Design Document](../../.kiro/specs/session-state-persistence/design.md)
- [Requirements Document](../../.kiro/specs/session-state-persistence/requirements.md)
- [Tasks Document](../../.kiro/specs/session-state-persistence/tasks.md)
