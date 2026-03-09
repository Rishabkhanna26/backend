# Chaos Engineering Tests

This directory contains chaos engineering tests for the session state persistence system. These tests simulate real-world failure scenarios to validate system resilience, graceful degradation, and recovery capabilities.

## Overview

Chaos engineering tests intentionally inject failures into the system to verify that:
- The system continues operating under adverse conditions
- Failures are handled gracefully without data corruption
- Recovery mechanisms work correctly when conditions improve
- Error handling and logging provide adequate observability

## Test Files

### 1. database-connection-loss.chaos.test.js

**Purpose**: Simulates database connection failures during message processing.

**Scenarios Tested**:
- Complete database unavailability (connection refused)
- Transient connection failures with retry logic
- Automatic reconnection when database becomes available
- In-memory fallback when persistence fails

**Requirements Validated**: 14.1, 14.6, 15.7  
**Property Validated**: Property 38 - Database failure fallback and retry

**Key Assertions**:
- System continues operating with in-memory fallback
- Errors are logged but don't crash the application
- Retry logic attempts reconnection with exponential backoff
- Persistence resumes automatically when database recovers

### 2. redis-connection-loss.chaos.test.js

**Purpose**: Simulates Redis cache failures with active caching enabled.

**Scenarios Tested**:
- Complete Redis unavailability
- Intermittent Redis failures
- Redis timeout scenarios
- Mixed Redis and database operations

**Requirements Validated**: 6.6, 14.3, 15.7  
**Property Validated**: Property 15 - Redis failure fallback

**Key Assertions**:
- System falls back to direct database access when Redis fails
- Cache failures are logged as warnings (not errors)
- Operations complete quickly without waiting for Redis timeout
- System maintains functionality without cache layer

### 3. server-crash-unsaved-state.chaos.test.js

**Purpose**: Simulates server crashes (SIGKILL) with active sessions.

**Scenarios Tested**:
- Recovery of last persisted states after crash
- Partial state loss between persist operations
- Complex nested data structure recovery
- Multiple crash/recovery cycles
- Crash during batch write operations

**Requirements Validated**: 9.1, 9.5, 15.7  
**Property Validated**: Property 25 - Restart restores shutdown states

**Key Assertions**:
- Last persisted states are fully recoverable
- System gracefully handles loss of data since last persist
- Complex data structures (nested objects, arrays, Dates) are preserved
- Recovery statistics are logged correctly
- No data corruption occurs across multiple crashes

### 4. network-partition.chaos.test.js

**Purpose**: Simulates network partitions between application and database.

**Scenarios Tested**:
- Circuit breaker activation after consecutive failures
- Fallback to in-memory storage when circuit is open
- Automatic recovery when partition heals
- Intermittent network issues (non-consecutive failures)
- Circuit breaker state transitions
- Data consistency during partition

**Requirements Validated**: 14.4, 14.5, 14.6, 15.7  
**Property Validated**: Property 39 - Circuit breaker activation

**Key Assertions**:
- Circuit breaker opens after threshold failures (configurable)
- System switches to in-memory mode when circuit is open
- Critical alerts are logged for circuit breaker events
- Circuit automatically attempts to close after timeout
- No database queries attempted while circuit is open
- Data consistency maintained for successfully persisted states

### 5. corrupted-session-data.chaos.test.js

**Purpose**: Simulates corrupted session data in the database.

**Scenarios Tested**:
- Invalid JSON structures
- Malformed JSONB data
- Invalid Date objects
- Missing required fields
- Type mismatches
- Empty session data
- Multiple corrupted sessions mixed with valid ones

**Requirements Validated**: 4.6, 14.2, 15.7  
**Property Validated**: Property 8 - Recovery skips corrupted sessions

**Key Assertions**:
- Recovery skips corrupted sessions without crashing
- Valid sessions are still recovered successfully
- Errors are logged with appropriate context (admin_id, phone)
- Recovery statistics track failed recoveries
- System continues operating despite data corruption
- No cascading failures from corrupted data

## Running the Tests

### Run all chaos tests:
```bash
npm test tests/chaos/
```

### Run individual test files:
```bash
npm test tests/chaos/database-connection-loss.chaos.test.js
npm test tests/chaos/redis-connection-loss.chaos.test.js
npm test tests/chaos/server-crash-unsaved-state.chaos.test.js
npm test tests/chaos/network-partition.chaos.test.js
npm test tests/chaos/corrupted-session-data.chaos.test.js
```

### Run with coverage:
```bash
npm test -- --coverage tests/chaos/
```

## Prerequisites

- PostgreSQL database running (for integration with real database)
- Test database configured via environment variables:
  - `DB_HOST` (default: localhost)
  - `DB_PORT` (default: 5432)
  - `DB_NAME` (default: mex_end_test)
  - `DB_USER` (default: postgres)
  - `DB_PASSWORD` (default: postgres)

## Test Configuration

Chaos tests use modified configuration values for faster execution:
- `retryDelayMs`: 50ms (vs 100ms in production)
- `circuitBreakerThreshold`: 5 (vs 10 in production)
- `circuitBreakerResetMs`: 2000ms (vs 60000ms in production)

These shorter timeouts allow tests to complete quickly while still validating the behavior.

## What These Tests Validate

### Resilience Patterns

1. **Retry with Exponential Backoff**: Transient failures are retried with increasing delays
2. **Circuit Breaker**: Persistent failures trigger circuit breaker to prevent cascading failures
3. **Graceful Degradation**: System continues operating with reduced functionality
4. **Fallback Mechanisms**: In-memory storage used when persistence unavailable
5. **Automatic Recovery**: System resumes normal operation when conditions improve

### Error Handling

1. **Non-Crashing Failures**: Errors are logged but don't crash the application
2. **Contextual Logging**: Errors include relevant context (admin_id, phone, operation)
3. **Appropriate Log Levels**: Critical failures are errors, optional features (cache) are warnings
4. **Recovery Statistics**: Metrics track success/failure rates for observability

### Data Integrity

1. **No Data Corruption**: Failures don't corrupt successfully persisted data
2. **Atomic Operations**: Partial writes don't leave database in inconsistent state
3. **Type Preservation**: Complex data types (Date, nested objects) survive serialization
4. **Validation**: Corrupted data is detected and skipped during recovery

## Interpreting Test Results

### Success Criteria

All chaos tests should pass, demonstrating that:
- System remains available during failures
- Data integrity is maintained
- Recovery mechanisms work correctly
- Observability (logging, metrics) is adequate

### Common Failure Modes

If tests fail, check:
1. **Database connectivity**: Ensure test database is running and accessible
2. **Timing issues**: Chaos tests use timeouts; slow systems may need longer waits
3. **Configuration**: Verify test config values match expectations
4. **Component initialization**: Ensure all persistence components are properly initialized

## Best Practices

1. **Isolation**: Each test cleans up its data before/after execution
2. **Realistic Scenarios**: Tests simulate real-world failure conditions
3. **Comprehensive Coverage**: Tests cover multiple failure modes per component
4. **Observable Behavior**: Tests verify both functionality and logging
5. **Fast Execution**: Tests use shorter timeouts for quick feedback

## Related Documentation

- [Design Document](../../.kiro/specs/session-state-persistence/design.md) - Architecture and resilience patterns
- [Requirements](../../.kiro/specs/session-state-persistence/requirements.md) - Requirement 14 (Error Recovery and Resilience)
- [Integration Tests](../integration/README.md) - End-to-end integration tests
- [Load Tests](../load/README.md) - Performance and scalability tests

## Maintenance

When modifying persistence components:
1. Run chaos tests to ensure resilience is maintained
2. Add new chaos tests for new failure modes
3. Update test scenarios if error handling changes
4. Keep test timeouts synchronized with component behavior
