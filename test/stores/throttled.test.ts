import { describe, expect, it, vi } from "vitest";

import { MemoryStore } from "../../src/stores/index.js";
import { ThrottledStore, type ThrottledStoreOptions } from "../../src/stores/throttled.js";
import { sleep } from "../../src/utils/sleep.js";

function createStore(underlying: MemoryStore, opts: Partial<ThrottledStoreOptions> = {}) {
  return new ThrottledStore(underlying, {
    maxWritesBurst: 10,
    maxWritesPerSecond: 1000,
    maxConcurrent: Infinity,
    maxStalenessMs: Infinity,
    ...opts,
  });
}

describe("ThrottledStore", () => {
  describe("reads", () => {
    it("passes through to the underlying store", async () => {
      const underlying = new MemoryStore();
      underlying.set("key", [Buffer.from("value")]);

      const store = createStore(underlying);
      expect(await store.get("key")).toEqual([Buffer.from("value")]);
    });

    it("returns null for missing keys", async () => {
      const underlying = new MemoryStore();
      const store = createStore(underlying);
      expect(await store.get("missing")).toBeNull();
    });
  });

  describe("basic writes", () => {
    it("propagates set to the underlying store", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = createStore(underlying);

      store.set("key", [Buffer.from("value")]);
      await store.flush();

      expect(setSpy).toHaveBeenCalledWith("key", [Buffer.from("value")]);
      expect(underlying.get("key")).toEqual([Buffer.from("value")]);
    });

    it("propagates delete to the underlying store", async () => {
      const underlying = new MemoryStore();
      underlying.set("key", [Buffer.from("value")]);
      const deleteSpy = vi.spyOn(underlying, "delete");
      const store = createStore(underlying);

      store.delete("key");
      await store.flush();

      expect(deleteSpy).toHaveBeenCalledWith("key");
      expect(underlying.get("key")).toBeNull();
    });
  });

  describe("per-key coalescing", () => {
    it("coalesces multiple sets to the same key", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = createStore(underlying);

      store.set("key", [Buffer.from("first")]);
      store.set("key", [Buffer.from("second")]);
      store.set("key", [Buffer.from("third")]);

      await store.flush();

      expect(setSpy).toHaveBeenCalledTimes(1);
      expect(setSpy).toHaveBeenCalledWith("key", [Buffer.from("third")]);
    });

    it("coalesces set then delete to a single delete", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const deleteSpy = vi.spyOn(underlying, "delete");
      const store = createStore(underlying);

      store.set("key", [Buffer.from("value")]);
      store.delete("key");

      await store.flush();

      expect(setSpy).not.toHaveBeenCalled();
      expect(deleteSpy).toHaveBeenCalledWith("key");
    });

    it("coalesces delete then set to a single set", async () => {
      const underlying = new MemoryStore();
      underlying.set("key", [Buffer.from("old")]);
      const setSpy = vi.spyOn(underlying, "set");
      const deleteSpy = vi.spyOn(underlying, "delete");
      const store = createStore(underlying);

      store.delete("key");
      store.set("key", [Buffer.from("new")]);

      await store.flush();

      expect(deleteSpy).not.toHaveBeenCalled();
      expect(setSpy).toHaveBeenCalledTimes(1);
      expect(setSpy).toHaveBeenCalledWith("key", [Buffer.from("new")]);
    });

    it("keeps independent keys independent", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const store = createStore(underlying);

      store.set("a", [Buffer.from("1")]);
      store.set("b", [Buffer.from("2")]);

      await store.flush();

      expect(setSpy).toHaveBeenCalledTimes(2);
      expect(underlying.get("a")).toEqual([Buffer.from("1")]);
      expect(underlying.get("b")).toEqual([Buffer.from("2")]);
    });
  });

  describe("re-queue after coalesced write", () => {
    it("writes the latest value when new ops arrive during in-flight write", async () => {
      const underlying = new MemoryStore();
      const originalSet = underlying.set.bind(underlying);
      const setSpy = vi.spyOn(underlying, "set");

      // Make the underlying store async so we can interleave ops
      let resolveWrite!: () => void;
      const writeGate = new Promise<void>((r) => {
        resolveWrite = r;
      });
      setSpy.mockImplementationOnce(async (key, value) => {
        await writeGate;
        originalSet(key, value);
      });

      const store = createStore(underlying);

      // First set — will be admitted and block on writeGate
      store.set("key", [Buffer.from("v1")]);

      // Give microtasks time to run so the job is admitted
      await sleep(1);

      // Second set — arrives while v1 is in-flight
      store.set("key", [Buffer.from("v2")]);

      // Release the first write
      resolveWrite();
      await store.flush();

      // Should have written v1, then re-queued and written v2
      expect(setSpy).toHaveBeenCalledTimes(2);
      expect(setSpy).toHaveBeenNthCalledWith(1, "key", [Buffer.from("v1")]);
      expect(setSpy).toHaveBeenNthCalledWith(2, "key", [Buffer.from("v2")]);
      expect(underlying.get("key")).toEqual([Buffer.from("v2")]);
    });
  });

  describe("flush", () => {
    it("waits for all pending writes", async () => {
      const underlying = new MemoryStore();
      const store = createStore(underlying);

      store.set("a", [Buffer.from("1")]);
      store.set("b", [Buffer.from("2")]);
      store.set("c", [Buffer.from("3")]);

      await store.flush();

      expect(underlying.get("a")).toEqual([Buffer.from("1")]);
      expect(underlying.get("b")).toEqual([Buffer.from("2")]);
      expect(underlying.get("c")).toEqual([Buffer.from("3")]);
    });

    it("waits for re-queued writes", async () => {
      const underlying = new MemoryStore();
      const originalSet = underlying.set.bind(underlying);
      const setSpy = vi.spyOn(underlying, "set");

      let resolveWrite!: () => void;
      const writeGate = new Promise<void>((r) => {
        resolveWrite = r;
      });
      setSpy.mockImplementationOnce(async (key, value) => {
        await writeGate;
        originalSet(key, value);
      });

      const store = createStore(underlying);

      store.set("key", [Buffer.from("v1")]);
      await sleep(1);
      store.set("key", [Buffer.from("v2")]);

      // Start flush — should not resolve until both v1 and v2 are written
      let flushed = false;
      const flushPromise = store.flush().then(() => {
        flushed = true;
      });

      await sleep(1);
      expect(flushed).toBe(false); // blocked on v1

      resolveWrite();
      await flushPromise;

      expect(flushed).toBe(true);
      expect(underlying.get("key")).toEqual([Buffer.from("v2")]);
    });

    it("completes immediately on empty store", async () => {
      const underlying = new MemoryStore();
      const flushSpy = vi.spyOn(underlying, "flush");
      const store = createStore(underlying);

      await store.flush();

      expect(flushSpy).toHaveBeenCalled();
    });

    it("calls underlying flush after draining", async () => {
      const underlying = new MemoryStore();
      const flushSpy = vi.spyOn(underlying, "flush");
      const store = createStore(underlying);

      store.set("key", [Buffer.from("value")]);
      await store.flush();

      expect(flushSpy).toHaveBeenCalledTimes(1);
    });

    it("does not let writes added after flush start extend that flush", async () => {
      const underlying = new MemoryStore();
      const originalSet = underlying.set.bind(underlying);
      const setSpy = vi.spyOn(underlying, "set");

      let resolveInitial!: () => void;
      const initialWrite = new Promise<void>((r) => {
        resolveInitial = r;
      });
      let resolveLater!: () => void;
      const laterWrite = new Promise<void>((r) => {
        resolveLater = r;
      });
      setSpy.mockImplementation(async (key, value) => {
        if (key === "initial") {
          await initialWrite;
        }
        if (key === "later") {
          await laterWrite;
        }
        originalSet(key, value);
      });

      const store = createStore(underlying);

      store.set("initial", [Buffer.from("v1")]);
      await sleep(1);

      let firstResolved = false;
      const firstFlush = store.flush().then(() => {
        firstResolved = true;
      });

      store.set("later", [Buffer.from("v2")]);

      await sleep(20);
      expect(firstResolved).toBe(false);
      expect(underlying.get("later")).toBeNull();

      resolveInitial();
      await firstFlush;

      expect(firstResolved).toBe(true);
      expect(underlying.get("initial")).toEqual([Buffer.from("v1")]);
      expect(underlying.get("later")).toBeNull();

      resolveLater();
      await store.flush();
      expect(underlying.get("later")).toEqual([Buffer.from("v2")]);
    });
  });

  describe("error handling", () => {
    it("upstream write errors do not hang flush", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      setSpy.mockRejectedValueOnce(new Error("upstream failure"));

      const store = createStore(underlying);

      store.set("key", [Buffer.from("value")]);
      await store.flush(); // should resolve, not hang

      // Key was cleaned up despite the error
      store.set("key", [Buffer.from("retry")]);
      await store.flush();

      expect(setSpy).toHaveBeenCalledTimes(2);
      expect(underlying.get("key")).toEqual([Buffer.from("retry")]);
    });

    it("calls onWriteError with key, error, and duration", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");
      const error = new Error("boom");
      setSpy.mockRejectedValueOnce(error);

      const onWriteError = vi.fn();
      const store = createStore(underlying, { onWriteError });

      store.set("key", [Buffer.from("value")]);
      await store.flush();

      expect(onWriteError).toHaveBeenCalledTimes(1);
      expect(onWriteError).toHaveBeenCalledWith("key", error, expect.any(Number));
    });
  });

  describe("maxStalenessMs", () => {
    it("drops stale writes and clears pending", async () => {
      const underlying = new MemoryStore();
      const originalSet = underlying.set.bind(underlying);
      const setSpy = vi.spyOn(underlying, "set");

      // 1 concurrent slot so we can block the queue
      const store = createStore(underlying, {
        maxConcurrent: 1,
        maxStalenessMs: 20,
      });

      let resolveGate!: () => void;
      const gate = new Promise<void>((r) => {
        resolveGate = r;
      });
      setSpy.mockImplementationOnce(async (key, value) => {
        await gate;
        originalSet(key, value);
      });

      // First write takes the concurrency slot
      store.set("blocker", [Buffer.from("blocks")]);
      await sleep(1);

      // Second write queues behind it
      store.set("stale", [Buffer.from("should-not-persist")]);

      // Wait for the second job to become stale
      await sleep(50);

      // Release the blocker
      resolveGate();
      await store.flush();

      expect(underlying.get("blocker")).toEqual([Buffer.from("blocks")]);
      expect(underlying.get("stale")).toBeNull(); // discarded
      // setSpy: once for "blocker", zero for "stale"
      expect(setSpy).toHaveBeenCalledTimes(1);
    });

    it("does not re-queue discarded ops", async () => {
      const underlying = new MemoryStore();
      const originalSet = underlying.set.bind(underlying);
      const setSpy = vi.spyOn(underlying, "set");

      const store = createStore(underlying, {
        maxConcurrent: 1,
        maxStalenessMs: 20,
      });

      let resolveGate!: () => void;
      const gate = new Promise<void>((r) => {
        resolveGate = r;
      });
      setSpy.mockImplementationOnce(async (key, value) => {
        await gate;
        originalSet(key, value);
      });

      store.set("blocker", [Buffer.from("blocks")]);
      await sleep(1);
      store.set("stale", [Buffer.from("v1")]);
      store.set("stale", [Buffer.from("v2")]); // coalesced with v1

      await sleep(50);
      resolveGate();
      await store.flush();

      // The "stale" key should have been discarded entirely, not re-queued
      expect(underlying.get("stale")).toBeNull();
    });

    it("flush resolves when stale entries are discarded", async () => {
      const underlying = new MemoryStore();
      const originalSet = underlying.set.bind(underlying);
      const setSpy = vi.spyOn(underlying, "set");

      const store = createStore(underlying, {
        maxConcurrent: 1,
        maxStalenessMs: 20,
      });

      let resolveGate!: () => void;
      const gate = new Promise<void>((r) => {
        resolveGate = r;
      });
      setSpy.mockImplementationOnce(async (key, value) => {
        await gate;
        originalSet(key, value);
      });

      store.set("blocker", [Buffer.from("blocks")]);
      await sleep(1);
      store.set("stale", [Buffer.from("value")]);

      // Flush snapshots both keys
      const flushPromise = store.flush();

      // Wait for stale to expire, then release blocker
      await sleep(50);
      resolveGate();

      // Flush should resolve — stale key was discarded, blocker was written
      await flushPromise;

      expect(underlying.get("blocker")).toEqual([Buffer.from("blocks")]);
      expect(underlying.get("stale")).toBeNull();
    });

    it("measures staleness from the most recent same-key update", async () => {
      const underlying = new MemoryStore();
      const originalSet = underlying.set.bind(underlying);
      const setSpy = vi.spyOn(underlying, "set");

      const store = createStore(underlying, {
        maxConcurrent: 1,
        maxStalenessMs: 40,
      });

      let resolveGate!: () => void;
      const gate = new Promise<void>((r) => {
        resolveGate = r;
      });
      setSpy.mockImplementationOnce(async (key, value) => {
        await gate;
        originalSet(key, value);
      });

      store.set("blocker", [Buffer.from("blocks")]);
      await sleep(1);

      store.set("stale", [Buffer.from("v1")]);
      await sleep(30);
      store.set("stale", [Buffer.from("v2")]);

      // Old queue age is now > 40ms, but the latest update is still fresh.
      await sleep(25);
      resolveGate();
      await store.flush();

      expect(setSpy).toHaveBeenCalledTimes(2);
      expect(underlying.get("stale")).toEqual([Buffer.from("v2")]);
    });
  });

  describe("concurrency", () => {
    it("enforces maxConcurrent", async () => {
      const underlying = new MemoryStore();
      const originalSet = underlying.set.bind(underlying);
      const setSpy = vi.spyOn(underlying, "set");

      let concurrentNow = 0;
      let concurrentMax = 0;
      setSpy.mockImplementation(async (key, value) => {
        concurrentNow++;
        concurrentMax = Math.max(concurrentMax, concurrentNow);
        await sleep(20);
        concurrentNow--;
        originalSet(key, value);
      });

      const store = createStore(underlying, { maxConcurrent: 2 });

      store.set("a", [Buffer.from("1")]);
      store.set("b", [Buffer.from("2")]);
      store.set("c", [Buffer.from("3")]);
      store.set("d", [Buffer.from("4")]);

      await store.flush();

      expect(setSpy).toHaveBeenCalledTimes(4);
      expect(concurrentMax).toBe(2);
    });
  });

  describe("flush under continuous writes", () => {
    it("flush resolves even when same-key writes keep arriving during in-flight write", async () => {
      const underlying = new MemoryStore();
      const originalSet = underlying.set.bind(underlying);
      const setSpy = vi.spyOn(underlying, "set");

      let resolveFirst!: () => void;
      const firstGate = new Promise<void>((r) => {
        resolveFirst = r;
      });
      setSpy.mockImplementationOnce(async (key, value) => {
        await firstGate;
        originalSet(key, value);
      });

      const store = createStore(underlying);

      // v1 write starts and blocks
      store.set("key", [Buffer.from("v1")]);
      await sleep(1);

      // flush snapshots {key: 1}
      let flushed = false;
      const flushPromise = store.flush().then(() => {
        flushed = true;
      });

      // v2 arrives while v1 is in-flight — bumps version, coalesced
      store.set("key", [Buffer.from("v2")]);

      // Release v1 — flush should resolve because v1 (version >= snapshot) was written
      resolveFirst();
      await flushPromise;

      expect(flushed).toBe(true);
      // v2 is still pending and will be written by re-queue
      await store.flush();
      expect(underlying.get("key")).toEqual([Buffer.from("v2")]);
    });
  });

  describe("multiple concurrent flushes", () => {
    it("each flush calls underlying flush independently", async () => {
      const underlying = new MemoryStore();
      const flushSpy = vi.spyOn(underlying, "flush");
      const store = createStore(underlying);

      store.set("a", [Buffer.from("1")]);
      store.set("b", [Buffer.from("2")]);

      await Promise.all([store.flush(), store.flush()]);

      expect(flushSpy).toHaveBeenCalledTimes(2);
    });
  });

  describe("flush mid-flight", () => {
    it("flush started after a write is already in-flight still resolves", async () => {
      const underlying = new MemoryStore();
      const originalSet = underlying.set.bind(underlying);
      const setSpy = vi.spyOn(underlying, "set");

      let resolveWrite!: () => void;
      const writeGate = new Promise<void>((r) => {
        resolveWrite = r;
      });
      setSpy.mockImplementationOnce(async (key, value) => {
        await writeGate;
        originalSet(key, value);
      });

      const store = createStore(underlying);

      store.set("key", [Buffer.from("value")]);
      await sleep(1); // fn starts, blocks on writeGate

      // Flush starts while the write is in-flight
      const flushPromise = store.flush();

      resolveWrite();
      await flushPromise;

      expect(underlying.get("key")).toEqual([Buffer.from("value")]);
    });
  });

  describe("rate limiting", () => {
    it("respects token bucket", async () => {
      const underlying = new MemoryStore();
      const setSpy = vi.spyOn(underlying, "set");

      // 1 burst, 10/sec refill — second write must wait ~100ms
      const store = createStore(underlying, {
        maxWritesBurst: 1,
        maxWritesPerSecond: 10,
        maxConcurrent: Infinity,
      });

      const start = Date.now();
      store.set("a", [Buffer.from("1")]);
      store.set("b", [Buffer.from("2")]);
      await store.flush();

      const elapsed = Date.now() - start;
      expect(setSpy).toHaveBeenCalledTimes(2);
      expect(elapsed).toBeGreaterThanOrEqual(80); // ~100ms for second token
    });
  });
});
