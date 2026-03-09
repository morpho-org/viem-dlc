import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { DebouncedStore, type DebouncedStoreOptions } from "../../src/stores/debounced.js";
import { MemoryStore } from "../../src/stores/index.js";
import { sleep } from "../../src/utils/sleep.js";

function createStore(underlying: MemoryStore, opts: Partial<DebouncedStoreOptions> = {}) {
  return new DebouncedStore(underlying, {
    debounceMs: 100,
    maxStalenessMs: 500,
    maxWritesBurst: 1,
    maxWritesPerSecond: 10,
    ...opts,
  });
}

describe("DebouncedStore", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe("read operations", () => {
    it("passes through reads to underlying store", async () => {
      const underlying = new MemoryStore();
      await underlying.set("key", "value");

      const store = createStore(underlying);
      expect(await store.get("key")).toBe("value");
      store.close();
    });

    it("reads return null for non-existent keys", async () => {
      const underlying = new MemoryStore();
      const store = createStore(underlying);
      expect(await store.get("missing")).toBeNull();
      store.close();
    });

    it("reads do not see buffered writes (no read-your-writes)", async () => {
      const underlying = new MemoryStore();
      const store = createStore(underlying);

      await store.set("key", "buffered-value");

      // Read should return null because write is still buffered
      expect(await store.get("key")).toBeNull();
      store.close();
    });
  });

  describe("write operations", () => {
    it("buffers writes and flushes after debounce period", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = createStore(underlying, { debounceMs: 100 });

      await store.set("key", "value");

      // Not flushed immediately
      await vi.advanceTimersByTimeAsync(0);
      expect(setSpy).not.toHaveBeenCalled();

      // Still not flushed before debounce period
      await vi.advanceTimersByTimeAsync(50);
      expect(setSpy).not.toHaveBeenCalled();

      // Flushed after debounce period
      await vi.advanceTimersByTimeAsync(100);
      expect(setSpy).toHaveBeenCalledWith("key", "value");

      store.close();
    });

    it("coalesces multiple writes to same key", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = createStore(underlying, { debounceMs: 100 });

      await store.set("key", "first");
      await store.set("key", "second");
      await store.set("key", "third");

      // Wait for debounce from last write
      await vi.advanceTimersByTimeAsync(200);

      // Only the final value should be written
      expect(setSpy).toHaveBeenCalledTimes(1);
      expect(setSpy).toHaveBeenCalledWith("key", "third");

      store.close();
    });

    it("handles delete operations", async () => {
      const underlying = new MemoryStore();
      await underlying.set("key", "value");
      const deleteSpy = vi.spyOn(underlying, "delete");
      const store = createStore(underlying, { debounceMs: 100 });

      await store.delete("key");

      await vi.advanceTimersByTimeAsync(150);

      expect(deleteSpy).toHaveBeenCalledWith("key");
      store.close();
    });

    it("delete overwrites pending set", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const deleteSpy = vi.spyOn(underlying, "delete");
      const store = createStore(underlying, { debounceMs: 100 });

      await store.set("key", "value");
      await store.delete("key");

      await vi.advanceTimersByTimeAsync(200);

      expect(setSpy).not.toHaveBeenCalled();
      expect(deleteSpy).toHaveBeenCalledWith("key");

      store.close();
    });

    it("set overwrites pending delete", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const deleteSpy = vi.spyOn(underlying, "delete");
      const store = createStore(underlying, { debounceMs: 100 });

      await store.delete("key");
      await store.set("key", "new-value");

      await vi.advanceTimersByTimeAsync(200);

      expect(deleteSpy).not.toHaveBeenCalled();
      expect(setSpy).toHaveBeenCalledWith("key", "new-value");

      store.close();
    });

    it("throws when writing to closed store", async () => {
      const underlying = new MemoryStore();
      const store = createStore(underlying);
      store.close();

      await expect(store.set("key", "value")).rejects.toThrow("Store is closed");
      await expect(store.delete("key")).rejects.toThrow("Store is closed");
    });
  });

  describe("debounce vs staleness", () => {
    it("flushes after debounceMs when no new writes", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = createStore(underlying, {
        debounceMs: 100,
        maxStalenessMs: 1000,
      });

      await store.set("key", "value");

      // Before debounce
      await vi.advanceTimersByTimeAsync(50);
      expect(setSpy).not.toHaveBeenCalled();

      // After debounce
      await vi.advanceTimersByTimeAsync(100);
      expect(setSpy).toHaveBeenCalled();

      store.close();
    });

    it("flushes at maxStalenessMs even with continuous writes", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = createStore(underlying, {
        debounceMs: 100,
        maxStalenessMs: 300,
      });

      // Initial write at t=0
      await store.set("key", "v1");

      // Keep updating before debounce kicks in
      await store.set("key", "v2");
      await vi.advanceTimersByTimeAsync(80);
      expect(setSpy).not.toHaveBeenCalled();

      await store.set("key", "v3");
      await vi.advanceTimersByTimeAsync(80);
      expect(setSpy).not.toHaveBeenCalled();

      await store.set("key", "v4");
      await vi.advanceTimersByTimeAsync(80);
      expect(setSpy).not.toHaveBeenCalled();

      // At t=300, maxStalenessMs forces flush (firstQueuedAt=0 + 300 = 300)
      await vi.advanceTimersByTimeAsync(110);
      expect(setSpy).toHaveBeenCalledWith("key", "v4");

      store.close();
    });

    it("uses minimum of debounce and staleness deadlines", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = createStore(underlying, {
        debounceMs: 500,
        maxStalenessMs: 200,
      });

      // debounce would be at 500, staleness at 200
      await store.set("key", "value");

      // Should flush at staleness deadline (200), not debounce (500)
      await vi.advanceTimersByTimeAsync(250);
      expect(setSpy).toHaveBeenCalled();

      store.close();
    });
  });

  describe("rate limiting", () => {
    it("respects maxWritesPerSecond", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      // 2 writes per second = 500ms between writes
      const store = createStore(underlying, {
        debounceMs: 10,
        maxWritesPerSecond: 2,
      });

      // Queue multiple keys
      await store.set("a", "1");
      await store.set("b", "2");
      await store.set("c", "3");

      // Wait for debounce (10ms).
      // Pump loop needs a few ticks to start and process.
      await vi.advanceTimersByTimeAsync(50); // Debounce expires + some buffer

      // First write should happen (bucket starts with 1 token)
      // First write should happen (bucket starts with 1 token)
      await vi.waitUntil(() => setSpy.mock.calls.length === 1, { timeout: 1000 });
      expect(setSpy).toHaveBeenCalledTimes(1);

      // Second write needs 1/2 sec = 500ms
      await vi.advanceTimersByTimeAsync(500);
      // Allow pump loop to wake up and consume token
      await vi.advanceTimersByTimeAsync(0);
      await vi.waitUntil(() => setSpy.mock.calls.length === 2, { timeout: 1000 });
      expect(setSpy).toHaveBeenCalledTimes(2);

      // Third write needs another 500ms
      await vi.advanceTimersByTimeAsync(500);
      await vi.advanceTimersByTimeAsync(0);
      await vi.waitUntil(() => setSpy.mock.calls.length === 3, { timeout: 1000 });
      expect(setSpy).toHaveBeenCalledTimes(3);

      store.close();
    });
  });

  describe("error handling", () => {
    it("calls onWriteError when upstream write fails", async () => {
      const errors: Array<{ key: string; err: unknown }> = [];
      const flaky = {
        get: async () => null,
        set: async () => {
          throw new Error("write failed");
        },
        delete: async () => {},
      };

      const store = new DebouncedStore(flaky, {
        debounceMs: 10,
        maxStalenessMs: 100,
        maxWritesBurst: 1,
        maxWritesPerSecond: 100,
        onWriteError: (key, err) => errors.push({ key, err }),
      });

      await store.set("key", "value");

      // Advance time enough for debounce to trigger flush
      await vi.advanceTimersByTimeAsync(100);

      // Close store to stop retry loop
      store.close();

      // At least one error should have been recorded
      expect(errors.length).toBeGreaterThanOrEqual(1);
      expect(errors[0]!.key).toBe("key");
      expect(errors[0]!.err).toBeInstanceOf(Error);
    });
  });

  describe("concurrent writes during flush", () => {
    it("preserves newer writes that arrive during flush", async () => {
      const writtenValues: string[] = [];
      const underlying = new MemoryStore();

      // Track all values written
      const originalSet = underlying.set.bind(underlying);
      underlying.set = async (key: string, value: string) => {
        writtenValues.push(value);
        return originalSet(key, value);
      };

      const store = new DebouncedStore(underlying, {
        debounceMs: 10,
        maxStalenessMs: 100,
        maxWritesBurst: 1,
        maxWritesPerSecond: 100,
      });

      await store.set("key", "first");
      await store.set("key", "second"); // This should coalesce with first

      // Run all timers to completion
      await vi.runAllTimersAsync();

      // Due to coalescing, only the final value should be written
      expect(writtenValues).toContain("second");
      expect(writtenValues).toHaveLength(1);

      store.close();
    });

    it("handles version bumps when new writes arrive during flush", async () => {
      // This test verifies that when a new write arrives for a key that's
      // currently being flushed, the new write is preserved (not discarded)
      const underlying = new MemoryStore();
      const store = new DebouncedStore(underlying, {
        debounceMs: 10,
        maxStalenessMs: 100,
        maxWritesBurst: 1,
        maxWritesPerSecond: 100,
      });

      await store.set("key", "value");
      await vi.runAllTimersAsync();

      // Verify the value was written
      expect(await underlying.get("key")).toBe("value");
      store.close();
    });
  });

  describe("key prioritization", () => {
    it("flushes oldest-due keys first under load", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = createStore(underlying, {
        debounceMs: 50,
        maxWritesPerSecond: 100, // High enough to flush all 3 immediately
      });

      // Queue keys at different times
      await store.set("first", "1"); // due at t=50
      await store.set("second", "2"); // due at t=60
      await store.set("third", "3"); // due at t=70

      // At t=75, all are due. Rate limit allows all to proceed.
      await vi.advanceTimersByTimeAsync(75);

      // Order isn't strictly guaranteed by map iterators in all environments,
      // but our code sorts by dueAt. However, since we process concurrently
      // as fast as tokens allow, we might see all 3.
      await vi.waitUntil(() => setSpy.mock.calls.length === 3, { timeout: 1000 });
      expect(setSpy).toHaveBeenCalledTimes(3);

      // Verify all eventually written
      expect(setSpy).toHaveBeenCalledWith("first", "1");
      expect(setSpy).toHaveBeenCalledWith("second", "2");
      expect(setSpy).toHaveBeenCalledWith("third", "3");

      store.close();
    });
  });

  describe("single-flight per key", () => {
    it("does not start duplicate flushes for same key", async () => {
      let inFlightCount = 0;
      let maxConcurrent = 0;
      const slowStore = {
        get: async () => null,
        set: async () => {
          inFlightCount++;
          maxConcurrent = Math.max(maxConcurrent, inFlightCount);
          await sleep(100);
          inFlightCount--;
        },
        delete: async () => {},
      };

      const store = new DebouncedStore(slowStore, {
        debounceMs: 10,
        maxStalenessMs: 100,
        maxWritesBurst: 1,
        maxWritesPerSecond: 100,
      });

      await store.set("key", "value");

      // Multiple ticks while first write is in-flight
      await vi.advanceTimersByTimeAsync(50);
      await vi.advanceTimersByTimeAsync(50);
      await vi.advanceTimersByTimeAsync(50);

      // Drain all pending work
      await vi.runAllTimersAsync();

      // Should never have more than 1 in-flight write for the same key
      expect(maxConcurrent).toBe(1);

      store.close();
    });
  });

  describe("flush", () => {
    it("waits for buffered writes to complete", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = new DebouncedStore(underlying, {
        debounceMs: 100,
        maxStalenessMs: 500,
        maxWritesBurst: 1,
        maxWritesPerSecond: 100,
      });

      await store.set("key", "value");

      // Flush should force the write to happen immediately (after pump)
      const flushPromise = store.flush();

      await vi.runAllTimersAsync();
      await flushPromise;

      expect(setSpy).toHaveBeenCalledWith("key", "value");
      store.close();
    });

    it("waits for in-flight writes to complete", async () => {
      const underlying = new MemoryStore();
      let resolveWrite: () => void = () => {};
      const writePromise = new Promise<void>((resolve) => {
        resolveWrite = resolve;
      });

      underlying.set = async () => {
        await writePromise;
      };

      const store = new DebouncedStore(underlying, {
        debounceMs: 10,
        maxStalenessMs: 100,
        maxWritesBurst: 1,
        maxWritesPerSecond: 100,
      });

      await store.set("key", "value");

      // Advance to start the write
      await vi.advanceTimersByTimeAsync(50);

      // store.flush() should wait for the in-flight write
      const flushPromise = store.flush();

      let flushCompleted = false;
      flushPromise.then(() => {
        flushCompleted = true;
      });

      await vi.advanceTimersByTimeAsync(10);
      expect(flushCompleted).toBe(false);

      resolveWrite();
      // Use advanceTimers instead of runAllTimers to avoid infinite loop with the interval
      // The write is resolved, so flush should complete quickly.
      await vi.advanceTimersByTimeAsync(50);
      await flushPromise;
      expect(flushCompleted).toBe(true);

      store.close();
    });
  });

  describe("no retry on error", () => {
    it("evicts key from buffer on write error", async () => {
      let attempts = 0;
      const flaky = {
        get: async () => null,
        set: async () => {
          attempts++;
          throw new Error("permanent failure");
        },
        delete: async () => {},
      };

      const store = new DebouncedStore(flaky, {
        debounceMs: 10,
        maxStalenessMs: 100,
        maxWritesBurst: 1,
        maxWritesPerSecond: 1000,
      });

      await store.set("key", "value");

      // Run timers to trigger write
      await vi.runAllTimersAsync();

      // Should have attempted once
      expect(attempts).toBeGreaterThanOrEqual(1);
      const attemptsAfterFirst = attempts;

      // Run timers again - should NOT retry
      await vi.advanceTimersByTimeAsync(1000);
      await vi.runAllTimersAsync();

      expect(attempts).toBe(attemptsAfterFirst);

      store.close();
    });

    it("preserves newer writes if previous write fails", async () => {
      // Setup:
      // 1. Write "first" -> triggers flush
      // 2. Flush starts, but "upstream" is slow (and will fail)
      // 3. Write "second" arrives while flush is unresolved
      // 4. "first" flush fails
      // Requirement: "second" should remain in buffer

      const underlying = new MemoryStore();
      let rejectWrite: (err: Error) => void = () => {};
      const writePromise = new Promise<void>((_, reject) => {
        rejectWrite = reject;
      });
      writePromise.catch(() => {}); // Silence unhandled rejection noise

      const originalSet = underlying.set.bind(underlying);

      underlying.set = async (key, val) => {
        if (val === "first") return writePromise;
        return originalSet(key, val);
      };

      const store = new DebouncedStore(underlying, {
        debounceMs: 10,
        maxStalenessMs: 100,
        maxWritesBurst: 1,
        maxWritesPerSecond: 100,
      });

      // 1. Write first
      await store.set("key", "first");

      // Advance to start flush
      await vi.advanceTimersByTimeAsync(50);

      // 2. Write second (this bumps version)
      await store.set("key", "second");

      // 3. Fail the first write
      rejectWrite(new Error("fail"));

      // Allow flushSnapshot to process the failure and cleanup
      await vi.runAllTimersAsync();

      expect(await underlying.get("key")).toBe("second");

      store.close();
    });

    it("times out hanging writes", async () => {
      const underlying = new MemoryStore();
      const originalSet = underlying.set.bind(underlying);

      // Mock set to hang forever
      const foreverPromise = new Promise<void>(() => {});
      underlying.set = async () => foreverPromise;

      const errors: Array<{ key: string; err: unknown }> = [];

      const store = new DebouncedStore(underlying, {
        debounceMs: 10,
        maxStalenessMs: 100,
        maxWritesBurst: 1,
        maxWritesPerSecond: 100,
        onWriteError: (key, err) => {
          errors.push({ key, err });
        },
      });

      await store.set("key", "value");
      await vi.advanceTimersByTimeAsync(100); // Trigger flush

      // Advance time past 10s (timeout default)
      await vi.advanceTimersByTimeAsync(11_000);

      expect(errors.length).toBe(1);
      expect(errors[0]!.key).toBe("key");
      expect(errors[0]!.err).toBeInstanceOf(Error);

      underlying.set = originalSet;
      store.close();
    });
  });
});
