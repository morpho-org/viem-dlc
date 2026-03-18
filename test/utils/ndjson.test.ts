import { Buffer } from "node:buffer";
import { Transform } from "node:stream";
import { brotliCompressSync } from "node:zlib";

import { describe, expect, it, vi } from "vitest";

import { parse, stringify } from "../../src/utils/json.js";

const zlibMockState = vi.hoisted(() => {
  return {
    decompressedChunks: null as (Buffer | string)[] | null,
  };
});

vi.mock("node:zlib", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:zlib")>();

  return {
    ...actual,
    createBrotliDecompress: () => {
      if (zlibMockState.decompressedChunks === null) {
        return actual.createBrotliDecompress();
      }

      let emitted = false;
      return new Transform({
        transform(_chunk, _encoding, callback) {
          if (!emitted) {
            emitted = true;
            for (const part of zlibMockState.decompressedChunks ?? []) {
              this.push(part);
            }
          }
          callback();
        },
      });
    },
  };
});

import { NDJSON, type Codec, type Entry } from "../../src/utils/ndjson.js";

const codec: Codec<unknown> = { parse, stringify };

function serializeLine(key: string, value: unknown) {
  return `{"key":${JSON.stringify(key)},"value":${stringify(value)}}`;
}

async function collectRecords<T, K extends string>(ndjson: NDJSON<T, K>) {
  const records: Entry<T, K>[] = [];
  for await (const record of ndjson.records()) {
    records.push(record);
  }
  return records;
}

describe("NDJSON", () => {
  it("rejects malformed envelopes instead of parsing truncated values", () => {
    const ndjson = new NDJSON<number | string>(codec);
    const parseLine = (ndjson as { parseLine(line: string): Entry<number | string> | undefined }).parseLine.bind(ndjson);

    expect(parseLine('{"key":"a","value":123}')).toEqual({ key: "a", value: 123 });
    expect(parseLine('{"key":"a","value":123')).toBeUndefined();
    expect(parseLine('{"key":"a","value":123X}')).toBeUndefined();
    expect(parseLine('xxxxxxx"a","value":123}')).toBeUndefined();
    expect(parseLine('{"key":1,"value":123}')).toBeUndefined();
  });

  it("deduplicates duplicate keys on an empty blob with last-write-wins semantics", async () => {
    const ndjson = new NDJSON<string, string>({ parse, stringify });

    await ndjson.upsert([
      { key: "x", value: "first" },
      { key: "x", value: "second" },
    ]);

    expect(await collectRecords(ndjson)).toEqual([{ key: "x", value: "second" }]);
  });

  it("keeps only the first existing occurrence of a key when rewriting a blob", async () => {
    const source = `${serializeLine("x", "old1")}\n${serializeLine("x", "old2")}\n${serializeLine("y", "keep")}\n`;
    const ndjson = new NDJSON<string, string>({ parse, stringify }, brotliCompressSync(Buffer.from(source)));

    await ndjson.upsert([{ key: "x", value: "new" }]);

    expect(await collectRecords(ndjson)).toEqual([
      { key: "x", value: "new" },
      { key: "y", value: "keep" },
    ]);
  });

  it("does not let malformed existing lines shadow later valid records during upsert", async () => {
    const source = `xxxxxxx"a","value":oops\n${serializeLine("a", "real")}\n${serializeLine("b", "keep")}\n`;
    const ndjson = new NDJSON<string, string>({ parse, stringify }, brotliCompressSync(Buffer.from(source)));

    await ndjson.upsert([{ key: "c", value: "new" }]);

    expect(await collectRecords(ndjson)).toEqual([
      { key: "a", value: "real" },
      { key: "b", value: "keep" },
      { key: "c", value: "new" },
    ]);
  });

  it("preserves UTF-8 characters split across decompressor chunks", async () => {
    const emojiBytes = Buffer.from("😀", "utf8");
    zlibMockState.decompressedChunks = [
      Buffer.from('{"key":"smile","value":"'),
      emojiBytes.subarray(0, 2),
      emojiBytes.subarray(2),
      Buffer.from('"}\n'),
    ];

    try {
      const ndjson = new NDJSON<string, string>({ parse, stringify }, Buffer.from([0x00]));
      expect(await collectRecords(ndjson)).toEqual([{ key: "smile", value: "😀" }]);
    } finally {
      zlibMockState.decompressedChunks = null;
    }
  });
});
