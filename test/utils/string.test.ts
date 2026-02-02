import { describe, expect, it } from "vitest";

import { shardString } from "../../src/utils/string.js";

describe("shardString", () => {
  describe("basic functionality", () => {
    it('returns [""] for empty string', () => {
      expect(shardString("", 10)).toEqual([""]);
    });

    it("returns single chunk when string fits within maxBytes", () => {
      expect(shardString("hello", 10)).toEqual(["hello"]);
    });

    it("returns single chunk for exact fit", () => {
      expect(shardString("hello", 5)).toEqual(["hello"]);
    });

    it("splits ASCII string into multiple chunks", () => {
      expect(shardString("hello world", 5)).toEqual(["hello", " worl", "d"]);
    });

    it("handles single character", () => {
      expect(shardString("a", 1)).toEqual(["a"]);
    });
  });

  describe("multi-byte UTF-8 characters", () => {
    it("handles 2-byte characters (Latin Extended, etc.)", () => {
      // 'ñ' is U+00F1, encoded as 2 bytes in UTF-8: 0xC3 0xB1
      expect(shardString("ñ", 2)).toEqual(["ñ"]);
      expect(shardString("ñ", 3)).toEqual(["ñ"]);
    });

    it("handles 3-byte characters (CJK, etc.)", () => {
      // '中' is U+4E2D, encoded as 3 bytes in UTF-8: 0xE4 0xB8 0xAD
      expect(shardString("中", 3)).toEqual(["中"]);
      expect(shardString("中", 4)).toEqual(["中"]);
    });

    it("handles 4-byte characters (emoji, rare CJK)", () => {
      // '😀' is U+1F600, encoded as 4 bytes in UTF-8
      expect(shardString("😀", 4)).toEqual(["😀"]);
      expect(shardString("😀", 5)).toEqual(["😀"]);
    });

    it("splits mixed ASCII and multi-byte correctly", () => {
      // 'a' = 1 byte, '中' = 3 bytes, 'b' = 1 byte
      // Total: 5 bytes
      expect(shardString("a中b", 5)).toEqual(["a中b"]);
      expect(shardString("a中b", 4)).toEqual(["a中", "b"]);
      expect(shardString("a中b", 3)).toEqual(["a", "中", "b"]);
    });

    it("does not split surrogate pairs", () => {
      // Emoji like '😀' is represented as a surrogate pair in JS strings (2 UTF-16 code units)
      // Should never be split mid-character
      // 'a' = 1 byte, '😀' = 4 bytes, 'b' = 1 byte
      expect(shardString("a😀b", 5)).toEqual(["a😀", "b"]); // 'a' + '😀' = 5 bytes fits in one chunk
      expect(shardString("a😀b", 4)).toEqual(["a", "😀", "b"]); // 'a' = 1, can't add 😀 (4), so new chunk
    });
  });

  describe("boundary conditions", () => {
    it("handles maxBytes = 1 with ASCII", () => {
      expect(shardString("abc", 1)).toEqual(["a", "b", "c"]);
    });

    it("creates chunk boundaries at code point boundaries", () => {
      // Mix of 1, 2, 3, and 4 byte characters
      const input = "aéã😀";
      // 'a' = 1 byte, 'é' = 2 bytes, 'ã' = 2 bytes, '😀' = 4 bytes
      // Total: 9 bytes
      expect(shardString(input, 9)).toEqual([input]);
      expect(shardString(input, 5)).toEqual(["aéã", "😀"]);
      expect(shardString(input, 4)).toEqual(["aé", "ã", "😀"]);
    });

    it("handles string with only multi-byte characters", () => {
      // '中文' = 6 bytes (3 + 3)
      expect(shardString("中文", 6)).toEqual(["中文"]);
      expect(shardString("中文", 3)).toEqual(["中", "文"]);
    });

    it("handles long strings", () => {
      const input = "a".repeat(100);
      const chunks = shardString(input, 10);
      expect(chunks.length).toBe(10);
      expect(chunks.every((chunk) => chunk.length === 10)).toBe(true);
      expect(chunks.join("")).toBe(input);
    });
  });

  describe("error handling", () => {
    it("throws for maxBytes = 0", () => {
      expect(() => shardString("test", 0)).toThrow("maxBytes must be a positive safe integer");
    });

    it("throws for negative maxBytes", () => {
      expect(() => shardString("test", -1)).toThrow("maxBytes must be a positive safe integer");
    });

    it("throws for non-integer maxBytes", () => {
      expect(() => shardString("test", 1.5)).toThrow("maxBytes must be a positive safe integer");
    });

    it("throws when single character exceeds maxBytes", () => {
      // '中' requires 3 bytes
      expect(() => shardString("中", 2)).toThrow("a single character requires 3 UTF-8 bytes, exceeds maxBytes=2");
    });

    it("throws when emoji exceeds maxBytes", () => {
      // '😀' requires 4 bytes
      expect(() => shardString("😀", 3)).toThrow("a single character requires 4 UTF-8 bytes, exceeds maxBytes=3");
    });
  });

  describe("UTF-8 encoding verification", () => {
    it("produces chunks that TextEncoder encodes within maxBytes", () => {
      const encoder = new TextEncoder();
      const input = "hello 中文 世界 😀🎉";
      const maxBytes = 10;

      const chunks = shardString(input, maxBytes);

      for (const chunk of chunks) {
        const encoded = encoder.encode(chunk);
        expect(encoded.length).toBeLessThanOrEqual(maxBytes);
      }

      // Verify all content is preserved
      expect(chunks.join("")).toBe(input);
    });

    it("maximizes chunk utilization", () => {
      const encoder = new TextEncoder();
      const input = "aaaa"; // 4 ASCII characters = 4 bytes

      const chunks = shardString(input, 2);

      expect(chunks).toEqual(["aa", "aa"]);
      // Each chunk should use exactly maxBytes
      for (const chunk of chunks) {
        expect(encoder.encode(chunk).length).toBe(2);
      }
    });
  });
});
