import { Buffer } from "buffer";
import { zstdCompressSync } from "zlib";

import { afterEach, describe, expect, it, vi } from "vitest";

import { CompressedLinesBlob, type Codec, createSlot, type Entry, NdjsonMap, type Slot } from "../../src/internal/index.js";
import { parse, stringify } from "../../src/utils/json.js";

const codec: Codec<string> = {
  fromJson: (value) => parse<string>(value, "throw"),
  toJson: stringify,
};

function serializeLine(key: string, value: string) {
  return `{"key":${JSON.stringify(key)},"value":${stringify(value)}}`;
}

async function collectRecords<T, K extends string>(map: NdjsonMap<T, K>) {
  const records: Entry<T, K>[] = [];
  for await (const record of map.records()) {
    records.push(record);
  }
  return records;
}

async function collectRawLines(slot: Slot) {
  const blob = new CompressedLinesBlob(createSlot(slot.get()));
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
  it("skips malformed envelopes and still parses keys containing the separator text", async () => {
    const trickyKey = 'prefix ","value": suffix';
    const source = `\nnot-json\n{"key":1,"value":"bad"}\n${serializeLine(trickyKey, "ok")}\n`;
    const compressed = zstdCompressSync(Buffer.from(source));
    const map = new NdjsonMap<string, string>(codec, createSlot(compressed));

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
    const map = new NdjsonMap<string, string>(codec, createSlot(zstdCompressSync(Buffer.from(source))));

    await map.upsert([
      { key: "x", value: "new-x" },
      { key: "a", value: "insert-a" },
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
    const map = new NdjsonMap<string, string>(codec, slot);

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
    const map = new NdjsonMap<string, string>(codec, slot);
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
    const map = new NdjsonMap<string, string>(codec, slot);
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    await map.upsert([{ key: "d", value: "new-d" }]);

    expect(await collectRawLines(slot)).toEqual([serializeLine("b", "keep-b"), serializeLine("d", "new-d")]);
    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(String(warnSpy.mock.calls[0]?.[0])).toContain("Unsorted key in blob");
  });
});
