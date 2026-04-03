import { Buffer } from "buffer";
import { zstdCompressSync } from "zlib";

import { afterEach, describe, expect, it, vi } from "vitest";

import { type Codec, createSlot, type Entry, LazyNdjsonMap, NdjsonMap } from "../../src/internal/index.js";
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
  it("merge-sorts pending writes with flushed data in sorted key order", async () => {
    const source = [serializeLine("x", "old-x"), serializeLine("y", "keep-y"), ""].join("\n");
    const map = new LazyNdjsonMap<string, string>(
      codec,
      noAutoFlush,
      createSlot(zstdCompressSync(Buffer.from(source))),
    );

    map.upsert([{ key: "x", value: "new-x" }]);
    map.upsert([{ key: "z", value: "tail-z" }]);

    expect(await collectRecords(map)).toEqual([
      { key: "x", value: "new-x" },
      { key: "y", value: "keep-y" },
      { key: "z", value: "tail-z" },
    ]);

    const reduced = await map.reduce<string[]>((acc, record) => {
      acc.push(`${record.key}:${record.value}`);
      return acc;
    }, []);
    expect(reduced).toEqual(["x:new-x", "y:keep-y", "z:tail-z"]);
  });

  it("interleaves pending keys that sort before all flushed keys", async () => {
    const source = [serializeLine("m", "old-m"), serializeLine("z", "keep-z"), ""].join("\n");
    const map = new LazyNdjsonMap<string, string>(
      codec,
      noAutoFlush,
      createSlot(zstdCompressSync(Buffer.from(source))),
    );

    map.upsert([{ key: "a", value: "new-a" }]);
    map.upsert([{ key: "m", value: "new-m" }]);

    expect(await collectRecords(map)).toEqual([
      { key: "a", value: "new-a" },
      { key: "m", value: "new-m" },
      { key: "z", value: "keep-z" },
    ]);

    const reduced = await map.reduce<string[]>((acc, record) => {
      acc.push(`${record.key}:${record.value}`);
      return acc;
    }, []);
    expect(reduced).toEqual(["a:new-a", "m:new-m", "z:keep-z"]);
  });

  it("auto-flush snapshots the current pending set and leaves later writes for a subsequent auto-flush", async () => {
    const originalUpsert = NdjsonMap.prototype.upsert;
    const entered = deferred();
    const release = deferred();
    let callCount = 0;

    const upsertSpy = vi.spyOn(NdjsonMap.prototype, "upsert").mockImplementation(async function (this: NdjsonMap<string, string>, entries, signal) {
      callCount += 1;
      if (callCount === 1) {
        entered.resolve();
        await release.promise;
      }
      return originalUpsert.call(this, entries, signal);
    });

    const map = new LazyNdjsonMap<string, string>(codec, immediateAutoFlush, createSlot());

    map.upsert([{ key: "a", value: "alpha" }]);
    await entered.promise;

    // Write arrives while the first auto-flush is in progress — not included in its snapshot
    map.upsert([{ key: "b", value: "beta" }]);
    expect(upsertSpy.mock.calls[0]?.[0].map(({ key }: { key: string }) => key)).toEqual(["a"]);

    release.resolve();

    // Debounce re-triggers after the first auto-flush settles; wait for both to complete
    await vi.waitFor(() => expect(upsertSpy).toHaveBeenCalledTimes(2));

    // Second auto-flush picked up "b" as a separate snapshot
    expect(upsertSpy.mock.calls.map(([entries]) => entries.map(({ key }: { key: string }) => key))).toEqual([
      ["a"],
      ["b"],
    ]);
    expect(await collectRecords(map)).toEqual([
      { key: "a", value: "alpha" },
      { key: "b", value: "beta" },
    ]);
  });

  it("queues concurrent flushes and drains writes that arrive during a flush", async () => {
    const originalUpsert = NdjsonMap.prototype.upsert;
    const entered = deferred();
    const release = deferred();
    let callCount = 0;

    const upsertSpy = vi.spyOn(NdjsonMap.prototype, "upsert").mockImplementation(async function (this: NdjsonMap<string, string>, entries, signal) {
      callCount += 1;
      if (callCount === 1) {
        entered.resolve();
        await release.promise;
      }

      return originalUpsert.call(this, entries, signal);
    });

    const map = new LazyNdjsonMap<string, string>(codec, noAutoFlush, createSlot());
    map.upsert([{ key: "a", value: "alpha" }]);

    const firstFlush = map.flush();
    const secondFlush = map.flush();

    // Concurrent flushes queue independently (no coalescing)
    expect(secondFlush).not.toBe(firstFlush);

    await entered.promise;
    map.upsert([{ key: "b", value: "beta" }]);
    release.resolve();

    await Promise.all([firstFlush, secondFlush]);

    // First flush drained "a" then picked up "b"; second flush found pending empty
    expect(upsertSpy.mock.calls.map(([entries]) => entries.map(({ key }: { key: string }) => key))).toEqual([
      ["a"],
      ["b"],
    ]);
    expect(await collectRecords(map)).toEqual([
      { key: "a", value: "alpha" },
      { key: "b", value: "beta" },
    ]);
  });

  it("caps the debounce delay at the remaining maxDelay as the window fills", () => {
    const clock = installManualTimers();
    const map = new LazyNdjsonMap<string, string>(
      codec,
      { debounceMs: 100, maxDelayMs: 150, maxStalenessMs: Infinity },
      createSlot(),
    );

    map.upsert([{ key: "a", value: "alpha" }]);
    // debounce=100, maxDelayRemaining=150 → min(100,150)=100
    expect([...clock.timers.values()].map((t) => t.delay)).toEqual([100]);

    clock.setNow(80);
    map.upsert([{ key: "b", value: "beta" }]);
    // debounce=100, maxDelayRemaining=150-80=70 → min(100,70)=70
    expect([...clock.timers.values()].map((t) => t.delay)).toEqual([70]);
  });

  it("drops stale pending work when the timer fires late (simulating freeze/thaw)", () => {
    const clock = installManualTimers();
    const upsertSpy = vi.spyOn(NdjsonMap.prototype, "upsert");

    const map = new LazyNdjsonMap<string, string>(
      codec,
      { debounceMs: 100, maxDelayMs: 1_000, maxStalenessMs: 50 },
      createSlot(),
    );

    map.upsert([{ key: "a", value: "stale" }]);
    clock.setNow(1_000); // simulate long freeze
    clock.timers.values().next().value?.callback(); // fire the timer late

    expect(upsertSpy).not.toHaveBeenCalled();
  });

  it("drops stale queued work after an in-flight auto-flush settles", async () => {
    const clock = installManualTimers();
    const originalUpsert = NdjsonMap.prototype.upsert;
    const entered = deferred();
    const release = deferred();
    let callCount = 0;

    vi.spyOn(NdjsonMap.prototype, "upsert").mockImplementation(async function (this: NdjsonMap<string, string>, entries, signal) {
      callCount += 1;
      if (callCount === 1) {
        entered.resolve();
        await release.promise;
      }
      return originalUpsert.call(this, entries, signal);
    });

    const map = new LazyNdjsonMap<string, string>(
      codec,
      { debounceMs: 100, maxDelayMs: 500, maxStalenessMs: 50 },
      createSlot(),
    );

    // upsert "a" at t=0, then fire the timer to start the flush
    map.upsert([{ key: "a", value: "first" }]);
    clock.timers.values().next().value?.callback();
    await entered.promise;

    // Write arrives at t=10 while flush is running
    clock.setNow(10);
    map.upsert([{ key: "b", value: "stale" }]);

    // Simulate freeze: advance past staleness threshold (100 - 10 = 90 > 50)
    clock.setNow(100);
    release.resolve();

    // Let the finally callback and any subsequent microtasks settle
    await vi.waitFor(() => expect(callCount).toBe(1));

    // "b" was stale — no second auto-flush should have fired
    expect(callCount).toBe(1);
  });

  it("re-arms the timer after an in-flight auto-flush settles with fresh pending work", async () => {
    const originalUpsert = NdjsonMap.prototype.upsert;
    const entered = deferred();
    const release = deferred();
    let callCount = 0;

    const upsertSpy = vi.spyOn(NdjsonMap.prototype, "upsert").mockImplementation(async function (this: NdjsonMap<string, string>, entries, signal) {
      callCount += 1;
      if (callCount === 1) {
        entered.resolve();
        await release.promise;
      }
      return originalUpsert.call(this, entries, signal);
    });

    const map = new LazyNdjsonMap<string, string>(
      codec,
      { ...immediateAutoFlush, maxStalenessMs: Infinity },
      createSlot(),
    );

    map.upsert([{ key: "a", value: "alpha" }]);
    await entered.promise;

    // Write arrives while first auto-flush is running
    map.upsert([{ key: "b", value: "beta" }]);
    release.resolve();

    // Should re-arm and eventually flush "b"
    await vi.waitFor(() => expect(upsertSpy).toHaveBeenCalledTimes(2));

    expect(await collectRecords(map)).toEqual([
      { key: "a", value: "alpha" },
      { key: "b", value: "beta" },
    ]);
  });

  it("aborts an in-flight auto-flush before an explicit flush retries the same pending entries", async () => {
    const originalUpsert = NdjsonMap.prototype.upsert;
    const entered = deferred();
    let callCount = 0;

    const upsertSpy = vi.spyOn(NdjsonMap.prototype, "upsert").mockImplementation(async function (this: NdjsonMap<string, string>, entries, signal) {
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

      return originalUpsert.call(this, entries, signal);
    });

    const map = new LazyNdjsonMap<string, string>(codec, immediateAutoFlush, createSlot());
    map.upsert([{ key: "a", value: "alpha" }]);

    await entered.promise;
    await map.flush();

    expect(upsertSpy).toHaveBeenCalledTimes(2);
    expect((upsertSpy.mock.calls[0]?.[1] as AbortSignal | undefined)?.aborted).toBe(true);
    expect(await collectRecords(map)).toEqual([{ key: "a", value: "alpha" }]);
  });
});
