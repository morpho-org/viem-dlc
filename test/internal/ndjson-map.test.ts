import { Buffer } from "buffer";
import { brotliCompressSync } from "zlib";

import { describe, expect, it } from "vitest";

import { BrotliLineBlob, type Codec, type Entry, NdjsonMap } from "../../src/internal/index.js";
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

async function collectRawLines(map: NdjsonMap<string>) {
  const blob = new BrotliLineBlob(map.toBase64());
  const lines: string[] = [];
  for await (const line of blob.lines()) {
    lines.push(line);
  }
  return lines;
}

describe("NdjsonMap", () => {
  it("skips malformed envelopes and still parses keys containing the separator text", async () => {
    const trickyKey = 'prefix ","value": suffix';
    const source = `\nnot-json\n{"key":1,"value":"bad"}\n${serializeLine(trickyKey, "ok")}\n`;
    const compressed = brotliCompressSync(Buffer.from(source)).toString("base64");
    const map = new NdjsonMap<string, string>(codec, compressed);

    expect(await collectRecords(map)).toEqual([{ key: trickyKey, value: "ok" }]);

    const keys = await map.reduce<string[]>((acc, record) => {
      acc.push(record.key);
      return acc;
    }, []);
    expect(keys).toEqual([trickyKey]);
  });

  it("replaces the first matching occurrence, drops later duplicates, and appends unmatched upserts", async () => {
    const source = [
      serializeLine("x", "old-1"),
      serializeLine("y", "keep-y"),
      serializeLine("x", "old-2"),
      serializeLine("z", "keep-z"),
      "",
    ].join("\n");
    const map = new NdjsonMap<string, string>(codec, brotliCompressSync(Buffer.from(source)));

    await map.upsert([
      { key: "x", value: "new-x" },
      { key: "a", value: "append-a" },
    ]);

    expect(await collectRecords(map)).toEqual([
      { key: "x", value: "new-x" },
      { key: "y", value: "keep-y" },
      { key: "z", value: "keep-z" },
      { key: "a", value: "append-a" },
    ]);
  });

  it("lets a malformed value claim a key during rewrite and shadow a later valid duplicate", async () => {
    const source = [
      '{"key":"a","value":oops}',
      serializeLine("a", "real-a"),
      serializeLine("b", "keep-b"),
      "",
    ].join("\n");
    const map = new NdjsonMap<string, string>(codec, brotliCompressSync(Buffer.from(source)));

    await map.upsert([{ key: "c", value: "new-c" }]);

    expect(await collectRawLines(map)).toEqual([
      '{"key":"a","value":oops}',
      serializeLine("b", "keep-b"),
      serializeLine("c", "new-c"),
    ]);
  });
});
