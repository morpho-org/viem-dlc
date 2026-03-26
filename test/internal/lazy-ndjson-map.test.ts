import { Buffer } from "buffer";
import { brotliCompressSync } from "zlib";

import { afterEach, describe, expect, it, vi } from "vitest";

import { type Codec, createSlot, type Entry, LazyNdjsonMap, NdjsonMap } from "../../src/internal/index.js";
import { parse, stringify } from "../../src/utils/json.js";

const codec: Codec<string> = {
  fromJson: (value) => parse<string>(value, "throw"),
  toJson: stringify,
};

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
    records.push(record);
  }
  return records;
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("LazyNdjsonMap", () => {
  it("merge-sorts pending writes with flushed data in sorted key order", async () => {
    const source = [serializeLine("x", "old-x"), serializeLine("y", "keep-y"), ""].join("\n");
    const map = new LazyNdjsonMap<string, string>(
      codec,
      { autoFlushThresholdBytes: Number.MAX_SAFE_INTEGER },
      createSlot(brotliCompressSync(Buffer.from(source))),
    );

    map.upsert({ key: "x", value: "new-x" });
    map.upsert({ key: "z", value: "tail-z" });

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
      { autoFlushThresholdBytes: Number.MAX_SAFE_INTEGER },
      createSlot(brotliCompressSync(Buffer.from(source))),
    );

    map.upsert({ key: "a", value: "new-a" });
    map.upsert({ key: "m", value: "new-m" });

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

  it("auto-flush snapshots the current pending set and leaves later writes pending for a later flush", async () => {
    const originalUpsert = NdjsonMap.prototype.upsert;
    const entered = deferred();
    const release = deferred();
    const completed = deferred();
    let callCount = 0;

    const upsertSpy = vi.spyOn(NdjsonMap.prototype, "upsert").mockImplementation(async function (entries, signal) {
      callCount += 1;
      if (callCount === 1) {
        entered.resolve();
        await release.promise;
      }

      const result = await originalUpsert.call(this, entries, signal);
      if (callCount === 1) completed.resolve();
      return result;
    });

    const map = new LazyNdjsonMap<string, string>(codec, { autoFlushThresholdBytes: 1 }, createSlot());

    map.upsert({ key: "a", value: "alpha" });
    await entered.promise;

    map.upsert({ key: "b", value: "beta" });
    release.resolve();
    await completed.promise;

    expect(upsertSpy).toHaveBeenCalledTimes(1);
    expect(await collectRecords(map)).toEqual([
      { key: "a", value: "alpha" },
      { key: "b", value: "beta" },
    ]);

    await map.flush();

    expect(upsertSpy).toHaveBeenCalledTimes(2);
    expect(upsertSpy.mock.calls.map(([entries]) => entries.map(({ key }) => key))).toEqual([["a"], ["b"]]);
  });

  it("shares explicit flushes, warns on writes during flush, and drains those writes in a later pass", async () => {
    const originalUpsert = NdjsonMap.prototype.upsert;
    const entered = deferred();
    const release = deferred();
    let callCount = 0;

    const upsertSpy = vi.spyOn(NdjsonMap.prototype, "upsert").mockImplementation(async function (entries, signal) {
      callCount += 1;
      if (callCount === 1) {
        entered.resolve();
        await release.promise;
      }

      return originalUpsert.call(this, entries, signal);
    });
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    const map = new LazyNdjsonMap<string, string>(
      codec,
      { autoFlushThresholdBytes: Number.MAX_SAFE_INTEGER },
      createSlot(),
    );
    map.upsert({ key: "a", value: "alpha" });

    const firstFlush = map.flush();
    const secondFlush = map.flush();

    expect(secondFlush).toBe(firstFlush);

    await entered.promise;
    map.upsert({ key: "b", value: "beta" });
    release.resolve();

    await firstFlush;

    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(String(warnSpy.mock.calls[0]?.[0])).toContain("Upserting key 'b' while explicit flush is in progress");
    expect(upsertSpy.mock.calls.map(([entries]) => entries.map(({ key }) => key))).toEqual([["a"], ["b"]]);
    expect(await collectRecords(map)).toEqual([
      { key: "a", value: "alpha" },
      { key: "b", value: "beta" },
    ]);
  });

  it("aborts an in-flight auto-flush before an explicit flush retries the same pending entries", async () => {
    const originalUpsert = NdjsonMap.prototype.upsert;
    const entered = deferred();
    let callCount = 0;

    const upsertSpy = vi.spyOn(NdjsonMap.prototype, "upsert").mockImplementation(async function (entries, signal) {
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

    const map = new LazyNdjsonMap<string, string>(codec, { autoFlushThresholdBytes: 1 }, createSlot());
    map.upsert({ key: "a", value: "alpha" });

    await entered.promise;
    await map.flush();

    expect(upsertSpy).toHaveBeenCalledTimes(2);
    expect((upsertSpy.mock.calls[0]?.[1] as AbortSignal | undefined)?.aborted).toBe(true);
    expect(await collectRecords(map)).toEqual([{ key: "a", value: "alpha" }]);
  });
});
