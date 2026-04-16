import { Buffer } from "buffer";
import { zstdCompressSync } from "zlib";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  type Codec,
  createSlot,
  type Entry,
  type LazyEntry,
  LinesBlobCompressed,
  NdjsonMap,
  type Slot,
} from "../../src/internal/index.js";
import { parse, stringify } from "../../src/utils/json.js";

const codec: Codec<string> = {
  fromJson: (value) => parse<string>(value, "throw"),
  toJson: stringify,
};

function serializeLine(key: string, value: string) {
  return `{"key":${JSON.stringify(key)},"value":${stringify(value)}}`;
}

function collectRecords<T, K extends string>(map: NdjsonMap<T, K>) {
  return map.reduce<Entry<T, K>[]>((acc, record) => {
    acc.push({ key: record.key, value: record.value });
    return acc;
  }, []);
}

async function collectRawLines(slot: Slot) {
  const blob = new LinesBlobCompressed(createSlot(slot.get()));
  const lines: string[] = [];
  for await (const line of blob.lines()) {
    lines.push(line);
  }
  return lines;
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("NdjsonMap", () => {
  it("does not parse stored rawValue until requested and caches the parsed value", async () => {
    const fromJson = vi.fn((value: string) => parse<string>(value, "throw"));
    const map = new NdjsonMap<string, string>(
      { fromJson, toJson: stringify },
      new LinesBlobCompressed(createSlot(zstdCompressSync(Buffer.from(`${serializeLine("a", "alpha")}\n`)))),
    );

    let record: LazyEntry<string> | undefined;
    await map.scan((r) => {
      record = r;
      return false;
    });

    expect(record).toBeDefined();
    expect(record!.rawValue).toBe(stringify("alpha"));
    expect(fromJson).not.toHaveBeenCalled();

    expect(record!.value).toBe("alpha");
    expect(record!.value).toBe("alpha");
    expect(fromJson).toHaveBeenCalledTimes(1);
  });

  it("skips malformed envelopes and invalid string-key tokens while still parsing valid tricky keys", async () => {
    const trickyKey = 'prefix ","value": suffix';
    const invalidEscapedKey = '{"key":"\\uZZZZ","value":"bad"}';
    const source = `\nnot-json\n{"key":1,"value":"bad"}\n${invalidEscapedKey}\n${serializeLine(trickyKey, "ok")}\n`;
    const compressed = zstdCompressSync(Buffer.from(source));
    const map = new NdjsonMap<string, string>(codec, new LinesBlobCompressed(createSlot(compressed)));

    expect(await collectRecords(map)).toEqual([{ key: trickyKey, value: "ok" }]);

    const keys = await map.reduce<string[]>((acc, record) => {
      acc.push(record.key);
      return acc;
    }, []);
    expect(keys).toEqual([trickyKey]);
  });

  it("merge-inserts new keys in sorted order, replaces existing keys in-place, and deduplicates", async () => {
    const source = [serializeLine("x", "old-x"), serializeLine("y", "keep-y"), serializeLine("z", "keep-z"), ""].join(
      "\n",
    );
    const map = new NdjsonMap<string, string>(
      codec,
      new LinesBlobCompressed(createSlot(zstdCompressSync(Buffer.from(source)))),
    );

    await map.upsert([
      { key: "x", value: "new-x" },
      { key: "a", value: "insert-a" },
      { key: "x", value: "new-x" },
    ]);

    expect(await collectRecords(map)).toEqual([
      { key: "a", value: "insert-a" },
      { key: "x", value: "new-x" },
      { key: "y", value: "keep-y" },
      { key: "z", value: "keep-z" },
    ]);
  });

  it("preserves a line with a malformed value during rewrite", async () => {
    const source = ['{"key":"a","value":oops}', serializeLine("b", "keep-b"), ""].join("\n");
    const slot = createSlot(zstdCompressSync(Buffer.from(source)));
    const map = new NdjsonMap<string, string>(codec, new LinesBlobCompressed(slot));

    await map.upsert([{ key: "c", value: "new-c" }]);

    expect(await collectRawLines(slot)).toEqual([
      '{"key":"a","value":oops}',
      serializeLine("b", "keep-b"),
      serializeLine("c", "new-c"),
    ]);
  });

  it("drops the corrupted suffix after a duplicate stored key is encountered", async () => {
    const source = [serializeLine("a", "old-a"), serializeLine("a", "stale-a"), serializeLine("b", "stale-b"), ""].join(
      "\n",
    );
    const slot = createSlot(zstdCompressSync(Buffer.from(source)));
    const map = new NdjsonMap<string, string>(codec, new LinesBlobCompressed(slot));
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    await map.upsert([
      { key: "a", value: "new-a" },
      { key: "c", value: "new-c" },
    ]);

    expect(await collectRawLines(slot)).toEqual([serializeLine("a", "new-a"), serializeLine("c", "new-c")]);
    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(String(warnSpy.mock.calls[0]?.[0])).toContain("Duplicate key in blob");
  });

  it("drops the corrupted suffix after an unsorted stored key is encountered", async () => {
    const source = [
      serializeLine("b", "keep-b"),
      serializeLine("a", "stale-a"),
      serializeLine("c", "stale-c"),
      "",
    ].join("\n");
    const slot = createSlot(zstdCompressSync(Buffer.from(source)));
    const map = new NdjsonMap<string, string>(codec, new LinesBlobCompressed(slot));
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    await map.upsert([{ key: "d", value: "new-d" }]);

    expect(await collectRawLines(slot)).toEqual([serializeLine("b", "keep-b"), serializeLine("d", "new-d")]);
    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(String(warnSpy.mock.calls[0]?.[0])).toContain("Unsorted key in blob");
  });

  describe("scan(extra) merge-sort", () => {
    it("interleaves extra entries before, between, and after stored keys and overrides collisions", async () => {
      const source = [serializeLine("c", "stored-c"), serializeLine("f", "stored-f"), ""].join("\n");
      const map = new NdjsonMap<string, string>(
        codec,
        new LinesBlobCompressed(createSlot(zstdCompressSync(Buffer.from(source)))),
      );

      const extra = new Map<string, string>([
        ["a", "extra-a"], // before all stored
        ["c", "extra-c"], // collision — extra wins
        ["z", "extra-z"], // after all stored
      ]);

      const result = await map.reduce<Entry<string, string>[]>(
        (acc, r) => {
          acc.push({ key: r.key, value: r.value });
          return acc;
        },
        [],
        extra,
      );

      expect(result).toEqual([
        { key: "a", value: "extra-a" },
        { key: "c", value: "extra-c" },
        { key: "f", value: "stored-f" },
        { key: "z", value: "extra-z" },
      ]);
    });

    it("returns stored entries unchanged when extra is undefined or empty", async () => {
      const source = [serializeLine("x", "val"), ""].join("\n");
      const map = new NdjsonMap<string, string>(
        codec,
        new LinesBlobCompressed(createSlot(zstdCompressSync(Buffer.from(source)))),
      );

      const withUndefined = await collectRecords(map);
      const withEmpty = await map.reduce<Entry<string, string>[]>(
        (acc, r) => {
          acc.push({ key: r.key, value: r.value });
          return acc;
        },
        [],
        new Map(),
      );

      expect(withUndefined).toEqual([{ key: "x", value: "val" }]);
      expect(withEmpty).toEqual(withUndefined);
    });
  });

  describe("scan(extra)", () => {
    it("merge-sorts extra entries and stops early when the visitor returns false", async () => {
      const source = [serializeLine("c", "stored-c"), serializeLine("f", "stored-f"), ""].join("\n");
      const map = new NdjsonMap<string, string>(
        codec,
        new LinesBlobCompressed(createSlot(zstdCompressSync(Buffer.from(source)))),
      );
      const extra = new Map<string, string>([
        ["a", "extra-a"],
        ["c", "extra-c"],
        ["z", "extra-z"],
      ]);

      const visited: string[] = [];
      await map.scan((record) => {
        visited.push(`${record.key}:${record.value}`);
        if (record.key === "c") return false;
      }, extra);

      expect(visited).toEqual(["a:extra-a", "c:extra-c"]);
    });

    it("stops early during trailing extras after all stored lines are consumed", async () => {
      const source = [serializeLine("a", "stored-a"), ""].join("\n");
      const map = new NdjsonMap<string, string>(
        codec,
        new LinesBlobCompressed(createSlot(zstdCompressSync(Buffer.from(source)))),
      );
      const extra = new Map<string, string>([
        ["x", "extra-x"],
        ["y", "extra-y"],
        ["z", "extra-z"],
      ]);

      const visited: string[] = [];
      await map.scan((record) => {
        visited.push(record.key);
        if (record.key === "y") return false;
      }, extra);

      expect(visited).toEqual(["a", "x", "y"]);
    });
  });

  it("folds through merged entries during rewrite in sorted key order", async () => {
    const source = [serializeLine("x", "old-x"), serializeLine("y", "keep-y"), ""].join("\n");
    const map = new NdjsonMap<string, string>(
      codec,
      new LinesBlobCompressed(createSlot(zstdCompressSync(Buffer.from(source)))),
    );

    const reduced = await map.upsertAndFold<string[]>(
      [
        { key: "x", value: "new-x" },
        { key: "z", value: "tail-z" },
      ],
      (acc, record) => {
        acc.push(`${record.key}:${record.value}`);
        return acc;
      },
      [],
    );

    expect(reduced).toEqual(["x:new-x", "y:keep-y", "z:tail-z"]);
    expect(await collectRecords(map)).toEqual([
      { key: "x", value: "new-x" },
      { key: "y", value: "keep-y" },
      { key: "z", value: "tail-z" },
    ]);
  });

  it("uses reduce directly when upsertAndFold receives no entries", async () => {
    const source = [serializeLine("a", "alpha"), serializeLine("b", "beta"), ""].join("\n");
    const map = new NdjsonMap<string, string>(
      codec,
      new LinesBlobCompressed(createSlot(zstdCompressSync(Buffer.from(source)))),
    );
    const rewriteSpy = vi.spyOn(LinesBlobCompressed.prototype, "rewrite");

    const reduced = await map.upsertAndFold<string[]>(
      [],
      (acc, record) => {
        acc.push(`${record.key}:${record.value}`);
        return acc;
      },
      [],
    );

    expect(reduced).toEqual(["a:alpha", "b:beta"]);
    expect(rewriteSpy).not.toHaveBeenCalled();
  });
});
