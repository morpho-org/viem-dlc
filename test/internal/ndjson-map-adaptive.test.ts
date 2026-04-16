import { Buffer } from "buffer";
import { zstdCompressSync } from "zlib";

import { describe, expect, it } from "vitest";

import { type Codec, createSlot, type Entry } from "../../src/internal/index.js";
import { NdjsonMapAdaptive } from "../../src/internal/ndjson-map-adaptive.js";
import { parse, stringify } from "../../src/utils/json.js";

const codec: Codec<string> = {
  fromJson: (value) => parse<string>(value, "throw"),
  toJson: stringify,
};

const noAutoFlush = { debounceMs: 86_400_000, maxDelayMs: 86_400_000, thresholdBytes: 256 * 1024 };

const ZSTD_MAGIC = Buffer.from([0x28, 0xb5, 0x2f, 0xfd]);

function serializeLine(key: string, value: string) {
  return `{"key":${JSON.stringify(key)},"value":${stringify(value)}}`;
}

function collectRecords(map: NdjsonMapAdaptive<string, string>) {
  return map.reduce<Entry<string, string>[]>((acc, record) => {
    acc.push({ key: record.key, value: record.value });
    return acc;
  }, []);
}

describe("NdjsonMapAdaptive", () => {
  describe("mode selection at construction", () => {
    it("uses raw mode for an empty slot", () => {
      const map = new NdjsonMapAdaptive<string, string>(codec, createSlot(), noAutoFlush);
      expect(map.mode).toBe("raw");
    });

    it("uses raw mode for a small raw NDJSON slot below the threshold", () => {
      const slot = createSlot(Buffer.from(`${serializeLine("a", "alpha")}\n`));
      const map = new NdjsonMapAdaptive<string, string>(codec, slot, {
        ...noAutoFlush,
        thresholdBytes: 1024,
      });
      expect(map.mode).toBe("raw");
      expect(slot.get()[0]?.[0]).toBe(0x7b); // '{'
    });

    it("upgrades a large raw NDJSON slot to compressed during construction", () => {
      const lines: string[] = [];
      for (let i = 0; i < 200; i++) {
        lines.push(serializeLine(`k${i.toString().padStart(4, "0")}`, "x".repeat(50)));
      }
      const raw = Buffer.from(`${lines.join("\n")}\n`);
      const slot = createSlot(raw);
      expect(raw.length).toBeGreaterThan(2_000);

      const map = new NdjsonMapAdaptive<string, string>(codec, slot, {
        ...noAutoFlush,
        thresholdBytes: 1024,
      });

      expect(map.mode).toBe("compressed");
      const chunks = slot.get();
      expect(chunks.length).toBeGreaterThan(0);
      expect(chunks[0]!.subarray(0, 4).equals(ZSTD_MAGIC)).toBe(true);
    });

    it("keeps a compressed slot in compressed mode regardless of size", () => {
      const compressed = zstdCompressSync(Buffer.from(`${serializeLine("a", "alpha")}\n`));
      const slot = createSlot(compressed);

      const map = new NdjsonMapAdaptive<string, string>(codec, slot, {
        ...noAutoFlush,
        thresholdBytes: 10 * 1024 * 1024,
      });

      expect(map.mode).toBe("compressed");
      expect(slot.get()[0]!.subarray(0, 4).equals(ZSTD_MAGIC)).toBe(true);
    });
  });

  describe("data integrity across modes", () => {
    it("preserves entries when an existing raw slot is upgraded to compressed", async () => {
      const lines = [serializeLine("a", "alpha"), serializeLine("b", "beta"), serializeLine("c", "gamma")];
      const slot = createSlot(Buffer.from(`${lines.join("\n")}\n`));

      const map = new NdjsonMapAdaptive<string, string>(codec, slot, {
        ...noAutoFlush,
        thresholdBytes: 1, // force upgrade
      });

      expect(map.mode).toBe("compressed");
      expect(await collectRecords(map)).toEqual([
        { key: "a", value: "alpha" },
        { key: "b", value: "beta" },
        { key: "c", value: "gamma" },
      ]);
    });

    it("supports read-your-writes in raw mode", async () => {
      const map = new NdjsonMapAdaptive<string, string>(codec, createSlot(), noAutoFlush);
      expect(map.mode).toBe("raw");

      map.upsert([{ key: "b", value: "beta" }]);
      map.upsert([{ key: "a", value: "alpha" }]);

      expect(await collectRecords(map)).toEqual([
        { key: "a", value: "alpha" },
        { key: "b", value: "beta" },
      ]);
    });

    it("supports read-your-writes in compressed mode", async () => {
      const compressed = zstdCompressSync(Buffer.from(`${serializeLine("b", "beta")}\n`));
      const map = new NdjsonMapAdaptive<string, string>(codec, createSlot(compressed), noAutoFlush);
      expect(map.mode).toBe("compressed");

      map.upsert([{ key: "a", value: "alpha" }]);
      map.upsert([{ key: "c", value: "gamma" }]);

      expect(await collectRecords(map)).toEqual([
        { key: "a", value: "alpha" },
        { key: "b", value: "beta" },
        { key: "c", value: "gamma" },
      ]);
    });

    it("flushes raw mode entries to NDJSON in the slot", async () => {
      const slot = createSlot();
      const map = new NdjsonMapAdaptive<string, string>(codec, slot, noAutoFlush);

      map.upsert([
        { key: "a", value: "alpha" },
        { key: "b", value: "beta" },
      ]);
      await map.flush();

      const concatenated = Buffer.concat(slot.get()).toString("utf8");
      expect(concatenated).toBe(`${serializeLine("a", "alpha")}\n${serializeLine("b", "beta")}\n`);
    });

    it("preserves lex sort order across many upserts in raw mode", async () => {
      const map = new NdjsonMapAdaptive<string, string>(codec, createSlot(), noAutoFlush);

      const keys = ["zeta", "alpha", "mu", "beta", "yankee", "gamma", "delta"];
      map.upsert(keys.map((k) => ({ key: k, value: k.toUpperCase() })));

      const seen = await map.reduce<string[]>((acc, r) => {
        acc.push(r.key);
        return acc;
      }, []);

      expect(seen).toEqual(
        [...keys]
          .map((k) => JSON.stringify(k))
          .sort()
          .map((j) => JSON.parse(j) as string),
      );
    });
  });

  describe("flushAndFold parity", () => {
    it("works in raw mode", async () => {
      const map = new NdjsonMapAdaptive<string, string>(codec, createSlot(), noAutoFlush);
      map.upsert([
        { key: "a", value: "alpha" },
        { key: "b", value: "beta" },
      ]);

      const result = await map.flushAndFold<string[]>((acc, r) => {
        acc.push(`${r.key}:${r.value}`);
        return acc;
      }, []);

      expect(result).toEqual(["a:alpha", "b:beta"]);
      expect(await collectRecords(map)).toEqual([
        { key: "a", value: "alpha" },
        { key: "b", value: "beta" },
      ]);
    });
  });
});
