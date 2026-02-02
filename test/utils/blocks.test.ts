import { describe, expect, it } from "vitest";

import {
  divideBlockRange,
  halveBlockRange,
  isErrorCausedByBlockRange,
  resolveBlockNumber,
} from "../../src/utils/blocks.js";

describe("resolveBlockNumber", () => {
  it("returns bigint values unchanged", () => {
    expect(resolveBlockNumber(100n)).toBe(100n);
    expect(resolveBlockNumber(0n)).toBe(0n);
    expect(resolveBlockNumber(999999999999n)).toBe(999999999999n);
  });

  it("converts hex strings to bigint", () => {
    expect(resolveBlockNumber("0x64")).toBe(100n);
    expect(resolveBlockNumber("0x0")).toBe(0n);
    expect(resolveBlockNumber("0xf4240")).toBe(1000000n);
  });

  it('resolves "earliest" to 0n', () => {
    expect(resolveBlockNumber("earliest")).toBe(0n);
    expect(resolveBlockNumber("earliest", 12345n)).toBe(0n);
  });

  it('resolves "latest" to the provided latest value', () => {
    expect(resolveBlockNumber("latest", 12345n)).toBe(12345n);
    expect(resolveBlockNumber("latest", 0n)).toBe(0n);
  });

  it('resolves "latest" to 0n when no latest provided', () => {
    expect(resolveBlockNumber("latest")).toBe(0n);
  });

  it("throws for unsupported block tags", () => {
    expect(() => resolveBlockNumber("safe")).toThrow("unsupported block tag");
    expect(() => resolveBlockNumber("finalized")).toThrow("unsupported block tag");
    expect(() => resolveBlockNumber("pending")).toThrow("unsupported block tag");
  });
});

describe("divideBlockRange", () => {
  it("returns empty array when fromBlock > toBlock", () => {
    const ranges = divideBlockRange({ fromBlock: 100n, toBlock: 50n }, 10);
    expect(ranges).toEqual([]);
  });

  it("returns single chunk when range fits within maxBlockRange", () => {
    const ranges = divideBlockRange({ fromBlock: 0n, toBlock: 50n }, 100);
    expect(ranges).toEqual([{ fromBlock: 0n, toBlock: 50n }]);
  });

  it("returns single chunk for exact maxBlockRange size", () => {
    const ranges = divideBlockRange({ fromBlock: 0n, toBlock: 99n }, 100);
    expect(ranges).toEqual([{ fromBlock: 0n, toBlock: 99n }]);
  });

  it("splits range into multiple chunks", () => {
    const ranges = divideBlockRange({ fromBlock: 0n, toBlock: 249n }, 100);
    expect(ranges).toEqual([
      { fromBlock: 0n, toBlock: 99n },
      { fromBlock: 100n, toBlock: 199n },
      { fromBlock: 200n, toBlock: 249n },
    ]);
  });

  it("handles non-aligned start block", () => {
    const ranges = divideBlockRange({ fromBlock: 50n, toBlock: 249n }, 100);
    expect(ranges).toEqual([
      { fromBlock: 50n, toBlock: 149n },
      { fromBlock: 150n, toBlock: 249n },
    ]);
  });

  it("handles single block range", () => {
    const ranges = divideBlockRange({ fromBlock: 100n, toBlock: 100n }, 10);
    expect(ranges).toEqual([{ fromBlock: 100n, toBlock: 100n }]);
  });

  it("handles large block numbers", () => {
    const ranges = divideBlockRange({ fromBlock: 20000000n, toBlock: 20000099n }, 50);
    expect(ranges).toEqual([
      { fromBlock: 20000000n, toBlock: 20000049n },
      { fromBlock: 20000050n, toBlock: 20000099n },
    ]);
  });

  it("handles maxBlockRange of 1", () => {
    const ranges = divideBlockRange({ fromBlock: 0n, toBlock: 2n }, 1);
    expect(ranges).toEqual([
      { fromBlock: 0n, toBlock: 0n },
      { fromBlock: 1n, toBlock: 1n },
      { fromBlock: 2n, toBlock: 2n },
    ]);
  });

  describe("with alignTo", () => {
    it("aligns start block down to nearest multiple", () => {
      // Request starts at 10_003, should align down to 10_000
      const ranges = divideBlockRange({ fromBlock: 10_003n, toBlock: 19_999n }, 10_000, 10_000);
      expect(ranges[0]!.fromBlock).toBe(10_000n);
    });

    it("aligns end block up to nearest multiple minus 1", () => {
      // Request ends at 99_995, should align up to 99_999 (next multiple is 100_000)
      const ranges = divideBlockRange({ fromBlock: 90_000n, toBlock: 99_995n }, 10_000, 10_000);
      expect(ranges[ranges.length - 1]!.toBlock).toBe(99_999n);
    });

    it("extends range on both sides when unaligned", () => {
      // [10_003, 99_995] with align 10_000 → [10_000, 99_999]
      const ranges = divideBlockRange({ fromBlock: 10_003n, toBlock: 99_995n }, 10_000, 10_000);
      expect(ranges[0]!.fromBlock).toBe(10_000n);
      expect(ranges[ranges.length - 1]!.toBlock).toBe(99_999n);
    });

    it("produces aligned chunk boundaries", () => {
      const ranges = divideBlockRange({ fromBlock: 10_003n, toBlock: 39_995n }, 10_000, 10_000);
      expect(ranges).toEqual([
        { fromBlock: 10_000n, toBlock: 19_999n },
        { fromBlock: 20_000n, toBlock: 29_999n },
        { fromBlock: 30_000n, toBlock: 39_999n },
      ]);
    });

    it("does not extend already-aligned ranges", () => {
      const ranges = divideBlockRange({ fromBlock: 10_000n, toBlock: 29_999n }, 10_000, 10_000);
      expect(ranges).toEqual([
        { fromBlock: 10_000n, toBlock: 19_999n },
        { fromBlock: 20_000n, toBlock: 29_999n },
      ]);
    });

    it("handles range smaller than alignment", () => {
      // Small range [15_000, 15_500] aligns to [10_000, 19_999]
      const ranges = divideBlockRange({ fromBlock: 15_000n, toBlock: 15_500n }, 10_000, 10_000);
      expect(ranges).toEqual([{ fromBlock: 10_000n, toBlock: 19_999n }]);
    });

    it("handles alignment smaller than maxBlockRange", () => {
      // Align to 10_000 but maxBlockRange is 20_000
      const ranges = divideBlockRange({ fromBlock: 10_003n, toBlock: 39_995n }, 20_000, 10_000);
      expect(ranges).toEqual([
        { fromBlock: 10_000n, toBlock: 29_999n },
        { fromBlock: 30_000n, toBlock: 39_999n },
      ]);
    });

    it("handles alignment larger than maxBlockRange", () => {
      // Align to 100_000 but maxBlockRange is 10_000
      const ranges = divideBlockRange({ fromBlock: 50_000n, toBlock: 150_000n }, 10_000, 100_000);
      expect(ranges[0]!.fromBlock).toBe(0n); // Aligned down to 0
      expect(ranges[ranges.length - 1]!.toBlock).toBe(199_999n); // Aligned up to next 100k - 1
    });

    it("handles range starting at 0", () => {
      const ranges = divideBlockRange({ fromBlock: 0n, toBlock: 25_000n }, 10_000, 10_000);
      expect(ranges[0]!.fromBlock).toBe(0n);
      expect(ranges[ranges.length - 1]!.toBlock).toBe(29_999n);
    });
  });

  describe("with Infinity (unconstrained)", () => {
    it("returns single range for entire span", () => {
      const ranges = divideBlockRange({ fromBlock: 0n, toBlock: 1_000_000n }, Infinity);
      expect(ranges).toEqual([{ fromBlock: 0n, toBlock: 1_000_000n }]);
    });

    it("respects alignment with Infinity", () => {
      const ranges = divideBlockRange({ fromBlock: 10_003n, toBlock: 99_995n }, Infinity, 10_000);
      expect(ranges).toEqual([{ fromBlock: 10_000n, toBlock: 99_999n }]);
    });

    it("handles empty range with Infinity", () => {
      const ranges = divideBlockRange({ fromBlock: 100n, toBlock: 50n }, Infinity);
      expect(ranges).toEqual([]);
    });
  });
});

describe("halveBlockRange", () => {
  it("returns undefined for single block range", () => {
    const result = halveBlockRange({ fromBlock: 100n, toBlock: 100n });
    expect(result).toBeUndefined();
  });

  it("halves a 2-block range into two 1-block ranges", () => {
    const result = halveBlockRange({ fromBlock: 100n, toBlock: 101n });
    expect(result).toEqual([
      { fromBlock: 100n, toBlock: 100n },
      { fromBlock: 101n, toBlock: 101n },
    ]);
  });

  it("halves an even range correctly", () => {
    const result = halveBlockRange({ fromBlock: 0n, toBlock: 99n });
    expect(result).toEqual([
      { fromBlock: 0n, toBlock: 49n },
      { fromBlock: 50n, toBlock: 99n },
    ]);
  });

  it("halves an odd range correctly (second half gets extra block)", () => {
    const result = halveBlockRange({ fromBlock: 0n, toBlock: 100n });
    // 101 blocks total, first half gets 50, second gets 51
    expect(result).toEqual([
      { fromBlock: 0n, toBlock: 49n },
      { fromBlock: 50n, toBlock: 100n },
    ]);
  });

  it("handles large block numbers", () => {
    const result = halveBlockRange({ fromBlock: 20000000n, toBlock: 20000099n });
    expect(result).toEqual([
      { fromBlock: 20000000n, toBlock: 20000049n },
      { fromBlock: 20000050n, toBlock: 20000099n },
    ]);
  });
});

describe("isErrorCausedByBlockRange", () => {
  function createRpcError(code: number, message: string): Error {
    return Object.assign(new Error(message), { code });
  }

  describe("code-based detection", () => {
    it("detects -32602 errors", () => {
      const error = createRpcError(-32602, "Invalid params");
      expect(isErrorCausedByBlockRange(error)).toBe(true);
    });

    it("detects -32005 errors", () => {
      const error = createRpcError(-32005, "Limit exceeded");
      expect(isErrorCausedByBlockRange(error)).toBe(true);
    });

    it("detects -32012 errors", () => {
      const error = createRpcError(-32012, "Query limit");
      expect(isErrorCausedByBlockRange(error)).toBe(true);
    });

    it("detects -32000 errors", () => {
      const error = createRpcError(-32000, "Server error");
      expect(isErrorCausedByBlockRange(error)).toBe(true);
    });
  });

  describe("message-based detection", () => {
    it('detects "range exceeded" messages', () => {
      const error = createRpcError(-1, "block range exceeded limit");
      expect(isErrorCausedByBlockRange(error)).toBe(true);
    });

    it('detects "range too large" messages', () => {
      const error = createRpcError(-1, "range too large");
      expect(isErrorCausedByBlockRange(error)).toBe(true);
    });

    it('detects "exceed block" messages', () => {
      const error = createRpcError(-1, "query would exceed block limit");
      expect(isErrorCausedByBlockRange(error)).toBe(true);
    });

    it('detects "max block" messages', () => {
      const error = createRpcError(-1, "max block range is 10000");
      expect(isErrorCausedByBlockRange(error)).toBe(true);
    });

    it('detects "returned more than" messages (Alchemy/Infura style)', () => {
      const error = createRpcError(-1, "query returned more than 10000 results");
      expect(isErrorCausedByBlockRange(error)).toBe(true);
    });

    it('detects "response size" messages', () => {
      const error = createRpcError(-1, "response size exceeded");
      expect(isErrorCausedByBlockRange(error)).toBe(true);
    });
  });

  describe("non-matching errors", () => {
    it("returns false for unrelated error codes", () => {
      const error = createRpcError(-32601, "Method not found");
      expect(isErrorCausedByBlockRange(error)).toBe(false);
    });

    it("returns false for plain Error without code", () => {
      const error = new Error("Some random error");
      expect(isErrorCausedByBlockRange(error)).toBe(false);
    });

    it("returns false for non-Error values", () => {
      expect(isErrorCausedByBlockRange("string")).toBe(false);
      expect(isErrorCausedByBlockRange(null)).toBe(false);
    });
  });

  describe("real-world RPC error formats", () => {
    it("handles QuickNode style errors", () => {
      const error = createRpcError(-32602, "block range limit exceeded");
      expect(isErrorCausedByBlockRange(error)).toBe(true);
    });

    it("handles Alchemy style errors", () => {
      const error = createRpcError(-32005, "query returned more than 10000 results");
      expect(isErrorCausedByBlockRange(error)).toBe(true);
    });

    it("handles Infura style errors", () => {
      const error = createRpcError(-32005, "query returned more than 10000 results");
      expect(isErrorCausedByBlockRange(error)).toBe(true);
    });
  });
});
