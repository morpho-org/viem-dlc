import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { DebouncedStore, type DebouncedStoreOptions } from "../../src/stores/debounced.js";
import { MemoryStore } from "../../src/stores/index.js";
import { sleep } from "../../src/utils/sleep.js";

function createStore(underlying: MemoryStore, opts: Partial<DebouncedStoreOptions> = {}) {
  return new DebouncedStore(underlying, {
    debounceMs: 100,
    maxDelayMs: 300,
    maxStalenessMs: 500,
    maxWritesBurst: 1,
    maxWritesPerSecond: 10,
    ...opts,
  });
}

async function advanceUntil(check: () => boolean, { stepMs = 10, timeoutMs = 2_000 } = {}) {
  const iterations = Math.ceil(timeoutMs / stepMs);

  for (let i = 0; i < iterations; i++) {
    if (check()) return;
    await vi.advanceTimersByTimeAsync(stepMs);
  }
}

describe("DebouncedStore", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  describe("reads", () => {
    it("passes through to the underlying store", async () => {
      const underlying = new MemoryStore();
      await underlying.set("key", "value");

      const store = createStore(underlying);

      expect(await store.get("key")).toBe("value");
    });

    it("does not see buffered writes", async () => {
      const underlying = new MemoryStore();
      const store = createStore(underlying);

      await store.set("key", "buffered");

      expect(await store.get("key")).toBeNull();
    });
  });

  describe("buffering", () => {
    it("flushes after the debounce period", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = createStore(underlying, { debounceMs: 100, maxDelayMs: 1_000 });

      await store.set("key", "value");

      await vi.advanceTimersByTimeAsync(99);
      expect(setSpy).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(1);
      expect(setSpy).toHaveBeenCalledWith("key", "value");
    });

    it("coalesces multiple writes to the same key", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = createStore(underlying, { debounceMs: 100, maxDelayMs: 1_000 });

      await store.set("key", "first");
      await store.set("key", "second");
      await store.set("key", "third");

      await vi.advanceTimersByTimeAsync(100);

      expect(setSpy).toHaveBeenCalledTimes(1);
      expect(setSpy).toHaveBeenCalledWith("key", "third");
    });

    it("lets later deletes overwrite pending sets", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const deleteSpy = vi.spyOn(underlying, "delete");
      const store = createStore(underlying, { debounceMs: 100, maxDelayMs: 1_000 });

      await store.set("key", "value");
      await store.delete("key");

      await vi.advanceTimersByTimeAsync(100);

      expect(setSpy).not.toHaveBeenCalled();
      expect(deleteSpy).toHaveBeenCalledWith("key");
    });

    it("lets later sets overwrite pending deletes", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const deleteSpy = vi.spyOn(underlying, "delete");
      const store = createStore(underlying, { debounceMs: 100, maxDelayMs: 1_000 });

      await store.delete("key");
      await store.set("key", "value");

      await vi.advanceTimersByTimeAsync(100);

      expect(deleteSpy).not.toHaveBeenCalled();
      expect(setSpy).toHaveBeenCalledWith("key", "value");
    });
  });

  describe("timing", () => {
    it("flushes at maxDelayMs under continuous writes", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = createStore(underlying, {
        debounceMs: 100,
        maxDelayMs: 250,
        maxStalenessMs: 1_000,
      });

      await store.set("key", "v1");

      await vi.advanceTimersByTimeAsync(80);
      await store.set("key", "v2");

      await vi.advanceTimersByTimeAsync(80);
      await store.set("key", "v3");

      await vi.advanceTimersByTimeAsync(80);
      expect(setSpy).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(10);
      expect(setSpy).toHaveBeenCalledTimes(1);
      expect(setSpy).toHaveBeenCalledWith("key", "v3");
    });

    it("uses the earlier of debounceMs and maxDelayMs", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = createStore(underlying, {
        debounceMs: 500,
        maxDelayMs: 200,
        maxStalenessMs: 1_000,
      });

      await store.set("key", "value");

      await vi.advanceTimersByTimeAsync(199);
      expect(setSpy).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(1);
      expect(setSpy).toHaveBeenCalledWith("key", "value");
    });
  });

  describe("rate limiting", () => {
    it("respects maxWritesPerSecond during normal pumping", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = createStore(underlying, {
        debounceMs: 10,
        maxDelayMs: 1_000,
        maxStalenessMs: 2_000,
        maxWritesPerSecond: 2,
      });

      await store.set("a", "1");
      await store.set("b", "2");
      await store.set("c", "3");

      await vi.advanceTimersByTimeAsync(10);
      expect(setSpy).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(499);
      expect(setSpy).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(1);
      await vi.advanceTimersByTimeAsync(0);
      expect(setSpy).toHaveBeenCalledTimes(2);

      await vi.advanceTimersByTimeAsync(500);
      await vi.advanceTimersByTimeAsync(0);
      expect(setSpy).toHaveBeenCalledTimes(3);
    });
  });

  describe("error handling", () => {
    it("calls onWriteError when an upstream write fails", async () => {
      const errors: Array<{ key: string; err: unknown }> = [];
      const store = new DebouncedStore(
        {
          get: async () => null,
          set: async () => {
            throw new Error("write failed");
          },
          delete: async () => {},
          flush: async () => {},
        },
        {
          debounceMs: 10,
          maxDelayMs: 100,
          maxStalenessMs: 100,
          maxWritesBurst: 1,
          maxWritesPerSecond: 100,
          onWriteError: (key, err) => errors.push({ key, err }),
        },
      );

      await store.set("key", "value");
      await vi.advanceTimersByTimeAsync(10);

      expect(errors).toHaveLength(1);
      expect(errors[0]!.key).toBe("key");
      expect(errors[0]!.err).toBeInstanceOf(Error);
    });

    it("preserves newer writes when an older in-flight write fails", async () => {
      const underlying = new MemoryStore();
      let rejectWrite: (err: Error) => void = () => {};
      const firstWrite = new Promise<void>((_, reject) => {
        rejectWrite = reject;
      });
      firstWrite.catch(() => {});

      const originalSet = underlying.set.bind(underlying);
      underlying.set = async (key, value) => {
        if (value === "first") return firstWrite;
        return originalSet(key, value);
      };

      const store = createStore(underlying, {
        debounceMs: 10,
        maxDelayMs: 100,
        maxStalenessMs: 20_000,
      });

      await store.set("key", "first");
      await vi.advanceTimersByTimeAsync(10);

      await store.set("key", "second");
      rejectWrite(new Error("fail"));

      await vi.runAllTimersAsync();

      expect(await underlying.get("key")).toBe("second");
    });

    it("times out hanging writes", async () => {
      const errors: Array<{ key: string; err: unknown }> = [];
      const underlying = new MemoryStore();
      underlying.set = async () => new Promise<void>(() => {});

      const store = createStore(underlying, {
        debounceMs: 10,
        maxDelayMs: 100,
        maxStalenessMs: 100,
        onWriteError: (key, err) => errors.push({ key, err }),
      });

      await store.set("key", "value");
      await vi.advanceTimersByTimeAsync(10);
      await vi.advanceTimersByTimeAsync(10_000);

      expect(errors).toHaveLength(1);
      expect(errors[0]!.key).toBe("key");
      expect(errors[0]!.err).toBeInstanceOf(Error);
    });
  });

  describe("single-flight per key", () => {
    it("does not start duplicate writes for the same key", async () => {
      let inFlightCount = 0;
      let maxConcurrent = 0;
      const store = new DebouncedStore(
        {
          get: async () => null,
          set: async () => {
            inFlightCount++;
            maxConcurrent = Math.max(maxConcurrent, inFlightCount);
            await sleep(100);
            inFlightCount--;
          },
          delete: async () => {},
          flush: async () => {},
        },
        {
          debounceMs: 10,
          maxDelayMs: 100,
          maxStalenessMs: 100,
          maxWritesBurst: 1,
          maxWritesPerSecond: 100,
        },
      );

      await store.set("key", "first");
      await vi.advanceTimersByTimeAsync(10);
      await store.set("key", "second");

      await vi.runAllTimersAsync();

      expect(maxConcurrent).toBe(1);
    });
  });

  describe("flush", () => {
    it("falls through to the underlying store when nothing is buffered", async () => {
      const underlying = new MemoryStore();
      const flushSpy = vi.spyOn(underlying, "flush");
      const store = createStore(underlying);

      await store.flush();

      expect(flushSpy).toHaveBeenCalledTimes(1);
    });

    it("bypasses debounce and drains buffered writes immediately", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = createStore(underlying, {
        debounceMs: 1_000,
        maxDelayMs: 10_000,
      });

      await store.set("key", "value");

      let resolved = false;
      const flushPromise = store.flush().then(() => {
        resolved = true;
      });
      await advanceUntil(() => resolved);
      await flushPromise;

      expect(setSpy).toHaveBeenCalledTimes(1);
      expect(setSpy).toHaveBeenCalledWith("key", "value");
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

      const store = createStore(underlying, {
        debounceMs: 10,
        maxDelayMs: 100,
      });

      await store.set("key", "value");
      await vi.advanceTimersByTimeAsync(10);

      const flushPromise = store.flush();
      let resolved = false;
      void flushPromise.then(() => {
        resolved = true;
      });

      await vi.advanceTimersByTimeAsync(100);
      expect(resolved).toBe(false);

      resolveWrite();
      await advanceUntil(() => resolved);
      await flushPromise;

      expect(resolved).toBe(true);
    });

    it("does not let an older in-flight write satisfy a boundary that saw a newer version", async () => {
      const underlying = new MemoryStore();
      let resolveFirstWrite: () => void = () => {};
      let resolveSecondWrite: () => void = () => {};
      const firstWrite = new Promise<void>((resolve) => {
        resolveFirstWrite = resolve;
      });
      const secondWrite = new Promise<void>((resolve) => {
        resolveSecondWrite = resolve;
      });

      const originalSet = underlying.set.bind(underlying);
      underlying.set = async (key, value) => {
        if (value === "first") {
          await firstWrite;
          return originalSet(key, value);
        }
        if (value === "second") {
          await secondWrite;
          return originalSet(key, value);
        }

        return originalSet(key, value);
      };

      const store = createStore(underlying, {
        debounceMs: 10,
        maxDelayMs: 100,
      });

      await store.set("key", "first");
      await vi.advanceTimersByTimeAsync(10);
      await store.set("key", "second");

      let resolved = false;
      const flushPromise = store.flush().then(() => {
        resolved = true;
      });

      resolveFirstWrite();
      await advanceUntil(() => (underlying.get("key") as string | null) === "first");
      expect(resolved).toBe(false);

      resolveSecondWrite();
      await advanceUntil(() => resolved);
      await flushPromise;

      expect(await underlying.get("key")).toBe("second");
    });

    it("allows writes during an active flush without extending an earlier boundary", async () => {
      const underlying = new MemoryStore();
      let resolveInitial: () => void = () => {};
      let resolveLater: () => void = () => {};
      const initialWrite = new Promise<void>((resolve) => {
        resolveInitial = resolve;
      });
      const laterWrite = new Promise<void>((resolve) => {
        resolveLater = resolve;
      });

      const originalSet = underlying.set.bind(underlying);
      underlying.set = async (key, value) => {
        if (key === "initial") {
          await initialWrite;
          return originalSet(key, value);
        }
        if (key === "later") {
          await laterWrite;
          return originalSet(key, value);
        }

        return originalSet(key, value);
      };

      const store = createStore(underlying, {
        debounceMs: 10,
        maxDelayMs: 100,
      });

      await store.set("initial", "value");
      await vi.advanceTimersByTimeAsync(10);

      let firstResolved = false;
      const firstFlush = store.flush().then(() => {
        firstResolved = true;
      });

      await store.set("later", "value");

      let secondResolved = false;
      const secondFlush = store.flush().then(() => {
        secondResolved = true;
      });

      await vi.advanceTimersByTimeAsync(100);
      expect(firstResolved).toBe(false);
      expect(secondResolved).toBe(false);

      resolveInitial();
      await advanceUntil(() => firstResolved);
      expect(firstResolved).toBe(true);
      expect(secondResolved).toBe(false);

      resolveLater();
      await advanceUntil(() => secondResolved);
      await firstFlush;
      await secondFlush;

      expect(await underlying.get("initial")).toBe("value");
      expect(await underlying.get("later")).toBe("value");
    });

    it("still drops stale buffered entries during flush", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = createStore(underlying, {
        debounceMs: 1_000,
        maxDelayMs: 10_000,
        maxStalenessMs: 100,
      });

      await store.set("key", "value");
      await vi.advanceTimersByTimeAsync(101);

      let resolved = false;
      const flushPromise = store.flush().then(() => {
        resolved = true;
      });
      await advanceUntil(() => resolved);
      await flushPromise;

      expect(setSpy).not.toHaveBeenCalled();
      expect(await underlying.get("key")).toBeNull();
    });

    it("respects rate limiting while bypassing debounce", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = createStore(underlying, {
        debounceMs: 1_000,
        maxDelayMs: 10_000,
        maxStalenessMs: 2_000,
        maxWritesPerSecond: 1,
      });

      await store.set("a", "1");
      await store.set("b", "2");

      let resolved = false;
      const flushPromise = store.flush().then(() => {
        resolved = true;
      });

      await vi.advanceTimersByTimeAsync(100);
      expect(setSpy).toHaveBeenCalledTimes(1);
      expect(resolved).toBe(false);

      await advanceUntil(() => resolved, { timeoutMs: 2_000 });
      await flushPromise;
      expect(setSpy).toHaveBeenCalledTimes(2);
      expect(resolved).toBe(true);
    });

    it("runs the underlying store flush once per flush call", async () => {
      let resolveFlush: () => void = () => {};
      const flushBarrier = new Promise<void>((resolve) => {
        resolveFlush = resolve;
      });
      const underlying = {
        get: async () => null,
        set: async () => {},
        delete: async () => {},
        flush: vi.fn(async () => {
          await flushBarrier;
        }),
      };

      const store = new DebouncedStore(underlying, {
        debounceMs: 10,
        maxDelayMs: 100,
        maxStalenessMs: 100,
        maxWritesBurst: 1,
        maxWritesPerSecond: 100,
      });

      await store.set("key", "value");

      const firstFlush = store.flush();
      const secondFlush = store.flush();

      await advanceUntil(() => underlying.flush.mock.calls.length === 2);
      expect(underlying.flush).toHaveBeenCalledTimes(2);

      resolveFlush();
      await firstFlush;
      await secondFlush;
    });

    it("completes a flush that starts behind an already in-flight write", async () => {
      const underlying = new MemoryStore();
      let resolveWrite: () => void = () => {};
      const writePromise = new Promise<void>((resolve) => {
        resolveWrite = resolve;
      });

      underlying.set = async () => {
        await writePromise;
      };

      const store = createStore(underlying, {
        debounceMs: 10,
        maxDelayMs: 100,
      });

      await store.set("key", "value");
      await vi.advanceTimersByTimeAsync(10);

      let resolved = false;
      const flushPromise = store.flush().then(() => {
        resolved = true;
      });

      await vi.advanceTimersByTimeAsync(100);
      expect(resolved).toBe(false);

      resolveWrite();
      await advanceUntil(() => resolved);
      await flushPromise;

      expect(resolved).toBe(true);
    });
  });
});
