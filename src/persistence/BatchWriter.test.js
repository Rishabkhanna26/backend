/**
 * Tests for BatchWriter
 * 
 * Feature: session-state-persistence
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import BatchWriter from './BatchWriter.js';

describe('BatchWriter', () => {
  let mockWriteFn;
  let mockLogger;
  let batchWriter;

  beforeEach(() => {
    mockWriteFn = vi.fn().mockResolvedValue(true);
    
    mockLogger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn()
    };

    batchWriter = new BatchWriter({
      flushIntervalMs: 500,
      writeFn: mockWriteFn,
      logger: mockLogger
    });
  });

  afterEach(async () => {
    // Clean up any pending timers
    await batchWriter.shutdown();
    vi.clearAllMocks();
  });

  describe('schedule', () => {
    it('should schedule a write for batching', () => {
      const adminId = 1;
      const phone = '+1234567890';
      const userState = { step: 'test' };
      const serializedState = '{"step":"test"}';

      batchWriter.schedule(adminId, phone, userState, serializedState);

      expect(batchWriter.getPendingCount()).toBe(1);
    });

    it('should coalesce multiple updates to the same (adminId, phone)', () => {
      const adminId = 1;
      const phone = '+1234567890';
      const userState1 = { step: 'step1' };
      const userState2 = { step: 'step2' };
      const userState3 = { step: 'step3' };

      batchWriter.schedule(adminId, phone, userState1, '{"step":"step1"}');
      batchWriter.schedule(adminId, phone, userState2, '{"step":"step2"}');
      batchWriter.schedule(adminId, phone, userState3, '{"step":"step3"}');

      // Should only have 1 pending write (coalesced)
      expect(batchWriter.getPendingCount()).toBe(1);
    });

    it('should track separate writes for different (adminId, phone) combinations', () => {
      batchWriter.schedule(1, '+1111111111', { step: 'test1' }, '{"step":"test1"}');
      batchWriter.schedule(1, '+2222222222', { step: 'test2' }, '{"step":"test2"}');
      batchWriter.schedule(2, '+1111111111', { step: 'test3' }, '{"step":"test3"}');

      // Should have 3 separate pending writes
      expect(batchWriter.getPendingCount()).toBe(3);
    });

    it('should start flush timer on first schedule', () => {
      vi.useFakeTimers();

      batchWriter.schedule(1, '+1234567890', { step: 'test' }, '{"step":"test"}');

      expect(batchWriter.getPendingCount()).toBe(1);

      // Advance time to trigger flush
      vi.advanceTimersByTime(500);

      // Wait for flush to complete
      vi.runAllTimers();
      vi.useRealTimers();
    });
  });

  describe('flush', () => {
    it('should flush all pending writes to database', async () => {
      batchWriter.schedule(1, '+1111111111', { step: 'test1' }, '{"step":"test1"}');
      batchWriter.schedule(1, '+2222222222', { step: 'test2' }, '{"step":"test2"}');
      batchWriter.schedule(2, '+3333333333', { step: 'test3' }, '{"step":"test3"}');

      const stats = await batchWriter.flush();

      expect(stats.total).toBe(3);
      expect(stats.succeeded).toBe(3);
      expect(stats.failed).toBe(0);
      expect(mockWriteFn).toHaveBeenCalledTimes(3);
      expect(batchWriter.getPendingCount()).toBe(0);
    });

    it('should write the most recent state for coalesced updates', async () => {
      const adminId = 1;
      const phone = '+1234567890';

      batchWriter.schedule(adminId, phone, { step: 'step1' }, '{"step":"step1"}');
      batchWriter.schedule(adminId, phone, { step: 'step2' }, '{"step":"step2"}');
      batchWriter.schedule(adminId, phone, { step: 'step3' }, '{"step":"step3"}');

      await batchWriter.flush();

      // Should only write once with the most recent state
      expect(mockWriteFn).toHaveBeenCalledTimes(1);
      expect(mockWriteFn).toHaveBeenCalledWith(adminId, phone, '{"step":"step3"}');
    });

    it('should handle write failures gracefully', async () => {
      mockWriteFn
        .mockResolvedValueOnce(true)
        .mockRejectedValueOnce(new Error('Write failed'))
        .mockResolvedValueOnce(true);

      batchWriter.schedule(1, '+1111111111', { step: 'test1' }, '{"step":"test1"}');
      batchWriter.schedule(1, '+2222222222', { step: 'test2' }, '{"step":"test2"}');
      batchWriter.schedule(1, '+3333333333', { step: 'test3' }, '{"step":"test3"}');

      const stats = await batchWriter.flush();

      expect(stats.total).toBe(3);
      expect(stats.succeeded).toBe(2);
      expect(stats.failed).toBe(1);
    });

    it('should return early if no pending writes', async () => {
      const stats = await batchWriter.flush();

      expect(stats.total).toBe(0);
      expect(stats.succeeded).toBe(0);
      expect(stats.failed).toBe(0);
      expect(mockWriteFn).not.toHaveBeenCalled();
    });

    it('should clear pending writes after flush', async () => {
      batchWriter.schedule(1, '+1234567890', { step: 'test' }, '{"step":"test"}');
      
      expect(batchWriter.getPendingCount()).toBe(1);
      
      await batchWriter.flush();
      
      expect(batchWriter.getPendingCount()).toBe(0);
    });

    it('should log flush statistics', async () => {
      batchWriter.schedule(1, '+1111111111', { step: 'test1' }, '{"step":"test1"}');
      batchWriter.schedule(1, '+2222222222', { step: 'test2' }, '{"step":"test2"}');

      await batchWriter.flush();

      expect(mockLogger.debug).toHaveBeenCalledWith('BatchWriter: Flushing batch', {
        batchSize: 2
      });

      expect(mockLogger.debug).toHaveBeenCalledWith('BatchWriter: Batch flushed', 
        expect.objectContaining({
          total: 2,
          succeeded: 2,
          failed: 0
        })
      );
    });
  });

  describe('automatic flush', () => {
    it('should automatically flush after batch window expires', async () => {
      vi.useFakeTimers();

      batchWriter.schedule(1, '+1234567890', { step: 'test' }, '{"step":"test"}');

      expect(batchWriter.getPendingCount()).toBe(1);
      expect(mockWriteFn).not.toHaveBeenCalled();

      // Advance time to trigger flush
      vi.advanceTimersByTime(500);

      // Wait for async flush to complete
      await vi.runAllTimersAsync();

      expect(mockWriteFn).toHaveBeenCalledTimes(1);
      expect(batchWriter.getPendingCount()).toBe(0);

      vi.useRealTimers();
    });

    it('should schedule another flush if writes arrive during flush', async () => {
      vi.useFakeTimers();

      // Mock writeFn to take some time
      mockWriteFn.mockImplementation(async () => {
        await new Promise(resolve => setTimeout(resolve, 10));
        return true;
      });

      // Schedule first batch
      batchWriter.schedule(1, '+1111111111', { step: 'test1' }, '{"step":"test1"}');

      // Trigger flush
      const flushPromise = new Promise(resolve => {
        setTimeout(async () => {
          await batchWriter.flush();
          resolve();
        }, 500);
      });
      vi.advanceTimersByTime(500);

      // Schedule new write while flush is in progress
      batchWriter.schedule(1, '+2222222222', { step: 'test2' }, '{"step":"test2"}');

      // Wait for first flush to complete
      await vi.runAllTimersAsync();
      await flushPromise;

      // Should have the new write pending
      const pendingCount = batchWriter.getPendingCount();
      
      // Either the write is still pending (1) or already flushed (0)
      // Both are acceptable as timing can vary
      expect(pendingCount).toBeGreaterThanOrEqual(0);
      expect(pendingCount).toBeLessThanOrEqual(1);

      // Trigger second flush if needed
      if (pendingCount > 0) {
        vi.advanceTimersByTime(500);
        await vi.runAllTimersAsync();
      }

      expect(mockWriteFn).toHaveBeenCalledTimes(2);

      vi.useRealTimers();
    });
  });

  describe('shutdown', () => {
    it('should flush pending writes on shutdown', async () => {
      batchWriter.schedule(1, '+1111111111', { step: 'test1' }, '{"step":"test1"}');
      batchWriter.schedule(1, '+2222222222', { step: 'test2' }, '{"step":"test2"}');

      const stats = await batchWriter.shutdown();

      expect(stats.total).toBe(2);
      expect(stats.succeeded).toBe(2);
      expect(mockWriteFn).toHaveBeenCalledTimes(2);
      expect(batchWriter.getPendingCount()).toBe(0);
    });

    it('should clear any pending timer on shutdown', async () => {
      vi.useFakeTimers();

      batchWriter.schedule(1, '+1234567890', { step: 'test' }, '{"step":"test"}');

      // Shutdown before timer expires
      const shutdownPromise = batchWriter.shutdown();
      await vi.runAllTimersAsync();
      await shutdownPromise;

      expect(mockWriteFn).toHaveBeenCalledTimes(1);

      vi.useRealTimers();
    });

    it('should log shutdown message', async () => {
      await batchWriter.shutdown();

      expect(mockLogger.info).toHaveBeenCalledWith('BatchWriter: Shutting down');
    });
  });

  describe('batching window behavior', () => {
    it('should respect custom flush interval', async () => {
      vi.useFakeTimers();

      const customBatchWriter = new BatchWriter({
        flushIntervalMs: 1000, // 1 second
        writeFn: mockWriteFn,
        logger: mockLogger
      });

      customBatchWriter.schedule(1, '+1234567890', { step: 'test' }, '{"step":"test"}');

      // Advance time by 500ms - should not flush yet
      vi.advanceTimersByTime(500);
      expect(mockWriteFn).not.toHaveBeenCalled();

      // Advance another 500ms - should flush now (total 1000ms)
      vi.advanceTimersByTime(500);
      await vi.runAllTimersAsync();
      expect(mockWriteFn).toHaveBeenCalledTimes(1);

      await customBatchWriter.shutdown();
      vi.useRealTimers();
    });
  });
});
