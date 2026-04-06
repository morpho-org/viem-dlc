import { Buffer } from "buffer";
import { zstdCompressSync } from "zlib";

import { afterEach, describe, expect, it, vi } from "vitest";

import { CompressedLinesBlob, type Codec, createSlot, type Entry, LazyNdjsonMap } from "../../src/internal/index.js";
import { parse, stringify } from "../../src/utils/json.js";

const codec: Codec<string> = {
  fromJson: (value) => parse<string>(value, "throw"),
  toJson: stringify,
};

/** Options that effectively disable auto-flush so tests control timing. 24h is safe for 32-bit setTimeout. */
const noAutoFlush = { debounceMs: 86_400_000, maxDelayMs: 86_400_000, maxStalenessMs: Infinity };
/** Options that trigger auto-flush immediately (0ms debounce and maxDelay). */
const immediateAutoFlush = { debounceMs: 0, maxDelayMs: 0, maxStalenessMs: Infinity };

function serializeLine(key: string, value: string) {
  return `{"key":${JSON.stringify(key)},"value":${stringify(value)}}`;
}

function deferred<T = void>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function abortError() {
  const error = new Error("The operation was aborted.");
  error.name = "AbortError";
  return error;
}

async function collectRecords<T, K extends string>(map: LazyNdjsonMap<T, K>) {
  const records: Entry<T, K>[] = [];
  for await (const record of map.records()) {
    records.push({ key: record.key, value: record.value });
  }
  return records;
}

type TimerEntry = {
  callback: () => void;
  delay: number;
};

const installManualTimers = () => {
  let now = 0;
  let nextTimerId = 1;
  const timers = new Map<number, TimerEntry>();

  vi.spyOn(Date, "now").mockImplementation(() => now);
  vi.spyOn(globalThis, "setTimeout").mockImplementation(((callback: () => void, delay = 0) => {
    const id = nextTimerId++;
    timers.set(id, { callback, delay });
    return id as unknown as ReturnType<typeof setTimeout>;
  }) as typeof setTimeout);
  vi.spyOn(globalThis, "clearTimeout").mockImplementation(((timer?: ReturnType<typeof setTimeout>) => {
    if (typeof timer === "number") timers.delete(timer);
  }) as typeof clearTimeout);

  return {
    getNow: () => now,
    setNow: (value: number) => {
      now = value;
    },
    timers,
  };
};

afterEach(() => {
  vi.restoreAllMocks();
});

describe("LazyNdjsonMap", () => {
  it("exposes pending rawValue without parsing and caches parsed value on demand", async () => {
    const toJson = vi.fn(stringify);
    const fromJson = vi.fn((value: string) => parse<string>(value, "throw"));
    const ndjson = new LazyNdjsonMap<string, string>(
      { toJson, fromJson },
      noAutoFlush,
      createSlot(),
    );

    ndjson.upsert([{ key: "a", value: "alpha" }]);

    const iterator = ndjson.records();
    const first = await iterator.next();
    const record = first.value;

    expect(first.done).toBe(false);
    expect(record?.rawValue).toBe(stringify("alpha"));
    expect(fromJson).not.toHaveBeenCalled();
    expect(toJson).toHaveBeenCalledTimes(1);

    expect(record?.value).toBe("alpha");
    expect(record?.value).toBe("alpha");
    expect(fromJson).toHaveBeenCalledTimes(1);
  });

  it("merge-sorts pending writes with flushed data in sorted key order", async () => {
    const source = [serializeLine("x", "old-x"), serializeLine("y", "keep-y"), ""].join("\n");
    const ndjson = new LazyNdjsonMap<string, string>(
      codec,
      noAutoFlush,
      createSlot(zstdCompressSync(Buffer.from(source))),
    );

    ndjson.upsert([{ key: "x", value: "new-x" }]);
    ndjson.upsert([{ key: "z", value: "tail-z" }]);

    expect(await collectRecords(ndjson)).toEqual([
      { key: "x", value: "new-x" },
      { key: "y", value: "keep-y" },
      { key: "z", value: "tail-z" },
    ]);

    const reduced = await ndjson.reduce<string[]>((acc, record) => {
      acc.push(`${record.key}:${record.value}`);
      return acc;
    }, []);
    expect(reduced).toEqual(["x:new-x", "y:keep-y", "z:tail-z"]);
  });

  it("interleaves pending keys that sort before all flushed keys", async () => {
    const source = [serializeLine("m", "old-m"), serializeLine("z", "keep-z"), ""].join("\n");
    const ndjson = new LazyNdjsonMap<string, string>(
      codec,
      noAutoFlush,
      createSlot(zstdCompressSync(Buffer.from(source))),
    );

    ndjson.upsert([{ key: "a", value: "new-a" }]);
    ndjson.upsert([{ key: "m", value: "new-m" }]);

    expect(await collectRecords(ndjson)).toEqual([
      { key: "a", value: "new-a" },
      { key: "m", value: "new-m" },
      { key: "z", value: "keep-z" },
    ]);

    const reduced = await ndjson.reduce<string[]>((acc, record) => {
      acc.push(`${record.key}:${record.value}`);
      return acc;
    }, []);
    expect(reduced).toEqual(["a:new-a", "m:new-m", "z:keep-z"]);
  });

  it("auto-flush snapshots the current pending set and leaves later writes for a subsequent auto-flush", async () => {
    const originalRewrite = CompressedLinesBlob.prototype.rewrite;
    const entered = deferred();
    const release = deferred();
    let callCount = 0;

    const spy = vi.spyOn(CompressedLinesBlob.prototype, "rewrite").mockImplementation(async function (
      this: CompressedLinesBlob,
      run,
      signal,
    ) {
      callCount += 1;
      if (callCount === 1) {
        entered.resolve();
        await release.promise;
      }
      return originalRewrite.call(this, run, signal);
    });

    const ndjson = new LazyNdjsonMap<string, string>(codec, immediateAutoFlush, createSlot());

    ndjson.upsert([{ key: "a", value: "alpha" }]);
    await entered.promise;

    // Write arrives while the first auto-flush is in progress — not included in its snapshot
    ndjson.upsert([{ key: "b", value: "beta" }]);

    release.resolve();

    // Debounce re-triggers after the first auto-flush settles; wait for both to complete
    await vi.waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

    // Snapshot isolation: two separate writes happened
    expect(await collectRecords(ndjson)).toEqual([
      { key: "a", value: "alpha" },
      { key: "b", value: "beta" },
    ]);
  });

  it("queues concurrent flushes and drains writes that arrive during a flush", async () => {
    const originalRewrite = CompressedLinesBlob.prototype.rewrite;
    const entered = deferred();
    const release = deferred();
    let callCount = 0;

    const spy = vi.spyOn(CompressedLinesBlob.prototype, "rewrite").mockImplementation(async function (
      this: CompressedLinesBlob,
      run,
      signal,
    ) {
      callCount += 1;
      if (callCount === 1) {
        entered.resolve();
        await release.promise;
      }
      return originalRewrite.call(this, run, signal);
    });

    const ndjson = new LazyNdjsonMap<string, string>(codec, noAutoFlush, createSlot());
    ndjson.upsert([{ key: "a", value: "alpha" }]);

    const firstFlush = ndjson.flush();
    const secondFlush = ndjson.flush();

    // Concurrent flushes queue independently (no coalescing)
    expect(secondFlush).not.toBe(firstFlush);

    await entered.promise;
    ndjson.upsert([{ key: "b", value: "beta" }]);
    release.resolve();

    await Promise.all([firstFlush, secondFlush]);

    // First flush drained "a", second flush picked up "b" that arrived during first
    expect(spy).toHaveBeenCalledTimes(2);
    expect(await collectRecords(ndjson)).toEqual([
      { key: "a", value: "alpha" },
      { key: "b", value: "beta" },
    ]);
  });

  it("flushAndFold snapshots pending entries and leaves later writes pending", async () => {
    const originalRewrite = CompressedLinesBlob.prototype.rewrite;
    const entered = deferred();
    const release = deferred();
    let callCount = 0;

    vi.spyOn(CompressedLinesBlob.prototype, "rewrite").mockImplementation(async function (
      this: CompressedLinesBlob,
      run,
      signal,
    ) {
      callCount += 1;
      if (callCount === 1) {
        entered.resolve();
        await release.promise;
      }
      return originalRewrite.call(this, run, signal);
    });

    const ndjson = new LazyNdjsonMap<string, string>(codec, noAutoFlush, createSlot());
    ndjson.upsert([{ key: "a", value: "alpha" }]);

    const fold = ndjson.flushAndFold<string[]>((acc, record) => {
      acc.push(`${record.key}:${record.value}`);
      return acc;
    }, []);

    await entered.promise;
    ndjson.upsert([{ key: "b", value: "beta" }]);
    release.resolve();

    expect(await fold).toEqual(["a:alpha"]);
    expect(callCount).toBe(1);
    expect(await collectRecords(ndjson)).toEqual([
      { key: "a", value: "alpha" },
      { key: "b", value: "beta" },
    ]);
  });

  it("caps the debounce delay at the remaining maxDelay as the window fills", () => {
    const clock = installManualTimers();
    const ndjson = new LazyNdjsonMap<string, string>(
      codec,
      { debounceMs: 100, maxDelayMs: 150, maxStalenessMs: Infinity },
      createSlot(),
    );

    ndjson.upsert([{ key: "a", value: "alpha" }]);
    // debounce=100, maxDelayRemaining=150 → min(100,150)=100
    expect([...clock.timers.values()].map((t) => t.delay)).toEqual([100]);

    clock.setNow(80);
    ndjson.upsert([{ key: "b", value: "beta" }]);
    // debounce=100, maxDelayRemaining=150-80=70 → min(100,70)=70
    expect([...clock.timers.values()].map((t) => t.delay)).toEqual([70]);
  });

  it("drops stale pending work when the timer fires late (simulating freeze/thaw)", () => {
    const clock = installManualTimers();
    const spy = vi.spyOn(CompressedLinesBlob.prototype, "rewrite");

    const ndjson = new LazyNdjsonMap<string, string>(
      codec,
      { debounceMs: 100, maxDelayMs: 1_000, maxStalenessMs: 50 },
      createSlot(),
    );

    ndjson.upsert([{ key: "a", value: "stale" }]);
    clock.setNow(1_000); // simulate long freeze
    clock.timers.values().next().value?.callback(); // fire the timer late

    expect(spy).not.toHaveBeenCalled();
  });

  it("drops stale queued work after an in-flight auto-flush settles", async () => {
    const clock = installManualTimers();
    const originalRewrite = CompressedLinesBlob.prototype.rewrite;
    const entered = deferred();
    const release = deferred();
    let callCount = 0;

    vi.spyOn(CompressedLinesBlob.prototype, "rewrite").mockImplementation(async function (
      this: CompressedLinesBlob,
      run,
      signal,
    ) {
      callCount += 1;
      if (callCount === 1) {
        entered.resolve();
        await release.promise;
      }
      return originalRewrite.call(this, run, signal);
    });

    const ndjson = new LazyNdjsonMap<string, string>(
      codec,
      { debounceMs: 100, maxDelayMs: 500, maxStalenessMs: 50 },
      createSlot(),
    );

    // upsert "a" at t=0, then fire the timer to start the flush
    ndjson.upsert([{ key: "a", value: "first" }]);
    clock.timers.values().next().value?.callback();
    await entered.promise;

    // Write arrives at t=10 while flush is running
    clock.setNow(10);
    ndjson.upsert([{ key: "b", value: "stale" }]);

    // Simulate freeze: advance past staleness threshold (100 - 10 = 90 > 50)
    clock.setNow(100);
    release.resolve();

    // Let the finally callback and any subsequent microtasks settle
    await vi.waitFor(() => expect(callCount).toBe(1));

    // "b" was stale — no second auto-flush should have fired
    expect(callCount).toBe(1);
  });

  it("re-arms the timer after an in-flight auto-flush settles with fresh pending work", async () => {
    const originalRewrite = CompressedLinesBlob.prototype.rewrite;
    const entered = deferred();
    const release = deferred();
    let callCount = 0;

    const spy = vi.spyOn(CompressedLinesBlob.prototype, "rewrite").mockImplementation(async function (
      this: CompressedLinesBlob,
      run,
      signal,
    ) {
      callCount += 1;
      if (callCount === 1) {
        entered.resolve();
        await release.promise;
      }
      return originalRewrite.call(this, run, signal);
    });

    const ndjson = new LazyNdjsonMap<string, string>(
      codec,
      { ...immediateAutoFlush, maxStalenessMs: Infinity },
      createSlot(),
    );

    ndjson.upsert([{ key: "a", value: "alpha" }]);
    await entered.promise;

    // Write arrives while first auto-flush is running
    ndjson.upsert([{ key: "b", value: "beta" }]);
    release.resolve();

    // Should re-arm and eventually flush "b"
    await vi.waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

    expect(await collectRecords(ndjson)).toEqual([
      { key: "a", value: "alpha" },
      { key: "b", value: "beta" },
    ]);
  });

  describe("flushAndFold", () => {
    it("folds through all entries and persists them", async () => {
      const source = [serializeLine("b", "stored-b"), ""].join("\n");
      const ndjson = new LazyNdjsonMap<string, string>(
        codec,
        noAutoFlush,
        createSlot(zstdCompressSync(Buffer.from(source))),
      );

      ndjson.upsert([
        { key: "a", value: "new-a" },
        { key: "b", value: "new-b" },
      ]);

      const result = await ndjson.flushAndFold<string[]>(
        (acc, record) => {
          acc.push(`${record.key}:${record.value}`);
          return acc;
        },
        [],
      );

      expect(result).toEqual(["a:new-a", "b:new-b"]);
      // Entries are persisted — a subsequent read with no pending should match
      expect(await collectRecords(ndjson)).toEqual([
        { key: "a", value: "new-a" },
        { key: "b", value: "new-b" },
      ]);
    });

    it("delegates to reduce (no rewrite) when pending is empty", async () => {
      const spy = vi.spyOn(CompressedLinesBlob.prototype, "rewrite");
      const source = [serializeLine("x", "val"), ""].join("\n");
      const ndjson = new LazyNdjsonMap<string, string>(
        codec,
        noAutoFlush,
        createSlot(zstdCompressSync(Buffer.from(source))),
      );

      const result = await ndjson.flushAndFold<string[]>(
        (acc, record) => {
          acc.push(record.key);
          return acc;
        },
        [],
      );

      expect(result).toEqual(["x"]);
      expect(spy).not.toHaveBeenCalled();
    });
  });

  it("preserves a key in pending when it is overwritten during flush", async () => {
    const originalRewrite = CompressedLinesBlob.prototype.rewrite;
    const entered = deferred();
    const release = deferred();
    let callCount = 0;

    vi.spyOn(CompressedLinesBlob.prototype, "rewrite").mockImplementation(async function (
      this: CompressedLinesBlob,
      run,
      signal,
    ) {
      callCount += 1;
      if (callCount === 1) {
        entered.resolve();
        await release.promise;
      }
      return originalRewrite.call(this, run, signal);
    });

    const ndjson = new LazyNdjsonMap<string, string>(codec, noAutoFlush, createSlot());
    ndjson.upsert([{ key: "a", value: "v1" }]);

    const flushP = ndjson.flush();
    await entered.promise;

    // Overwrite same key with a new value while flush is in progress
    ndjson.upsert([{ key: "a", value: "v2" }]);
    release.resolve();
    await flushP;

    // "a" should still be pending with the new value — next flush writes it
    await ndjson.flush();

    expect(await collectRecords(ndjson)).toEqual([{ key: "a", value: "v2" }]);
  });

  it("aborts an in-flight auto-flush before an explicit flush retries the same pending entries", async () => {
    const originalRewrite = CompressedLinesBlob.prototype.rewrite;
    const entered = deferred();
    let callCount = 0;

    const spy = vi.spyOn(CompressedLinesBlob.prototype, "rewrite").mockImplementation(async function (
      this: CompressedLinesBlob,
      run,
      signal,
    ) {
      callCount += 1;

      if (callCount === 1) {
        entered.resolve();
        await new Promise<never>((_, reject) => {
          if (signal?.aborted) {
            reject(abortError());
            return;
          }

          signal?.addEventListener(
            "abort",
            () => {
              reject(abortError());
            },
            { once: true },
          );
        });
      }

      return originalRewrite.call(this, run, signal);
    });

    const ndjson = new LazyNdjsonMap<string, string>(codec, immediateAutoFlush, createSlot());
    ndjson.upsert([{ key: "a", value: "alpha" }]);

    await entered.promise;
    await ndjson.flush();

    expect(spy).toHaveBeenCalledTimes(2);
    expect((spy.mock.calls[0]?.[1] as AbortSignal | undefined)?.aborted).toBe(true);
    expect(await collectRecords(ndjson)).toEqual([{ key: "a", value: "alpha" }]);
  });
});
