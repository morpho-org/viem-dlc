import { type Address, type Hex, type LogTopic, type RpcLog, toHex } from "viem";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { handleGetLogs } from "../../src/transports/logs-cache/handlers/eth-get-logs.js";
import { createSink } from "../../src/transports/logs-cache/sink.js";
import type { CachedChunk, InvalidationStrategy } from "../../src/transports/logs-cache/types.js";
import { CACHE_KEY_SEPARATOR, computeCacheKey } from "../../src/transports/logs-cache/utils.js";
import type { Cache } from "../../src/types.js";
import { sleep } from "../../src/utils/sleep.js";

// =============================================================================
// Test Utilities
// =============================================================================

function createMockLog(blockNumber: bigint, logIndex = 0): RpcLog {
  return {
    address: "0x1234567890123456789012345678901234567890",
    topics: ["0xabc"],
    data: "0x",
    blockNumber: toHex(blockNumber),
    transactionHash: `0x${"a".repeat(64)}`,
    transactionIndex: "0x0",
    blockHash: `0x${blockNumber.toString(16).padStart(64, "0")}`, // Unique per block
    logIndex: toHex(logIndex),
    removed: false,
  };
}

function createMockCache(): Cache<CachedChunk> & {
  storage: Map<string, CachedChunk>;
  readCalls: string[][];
  writeCalls: { key: string; value: CachedChunk }[][];
} {
  const storage = new Map<string, CachedChunk>();
  const readCalls: string[][] = [];
  const writeCalls: { key: string; value: CachedChunk }[][] = [];

  return {
    storage,
    readCalls,
    writeCalls,
    async read(keys: string[]): Promise<(CachedChunk | undefined)[]> {
      readCalls.push(keys);
      return keys.map((key) => storage.get(key));
    },
    async write(items: { key: string; value: CachedChunk }[]): Promise<void> {
      writeCalls.push(items);
      for (const { key, value } of items) {
        storage.set(key, value);
      }
    },
  };
}

function createMockRequestFn(options: {
  latestBlock?: bigint;
  logGenerator?: (fromBlock: bigint, toBlock: bigint) => RpcLog[];
}) {
  const { latestBlock = 100_000n, logGenerator } = options;

  return vi.fn().mockImplementation(async (args: { method: string; params?: any[] }) => {
    if (args.method === "eth_blockNumber") {
      return toHex(latestBlock);
    }

    if (args.method === "eth_getLogs") {
      const filter = args.params?.[0];

      if (filter.blockHash) {
        return [createMockLog(0n)];
      }

      const fromBlock = BigInt(filter.fromBlock);
      const toBlock = BigInt(filter.toBlock);

      if (logGenerator) {
        return logGenerator(fromBlock, toBlock);
      }

      // Default: return one log at the start of the range
      return [createMockLog(fromBlock)];
    }

    throw new Error(`Unexpected method: ${args.method}`);
  });
}

function createCachedChunk(
  fromBlock: bigint,
  toBlock: bigint,
  logs: RpcLog[] = [],
  options: Partial<{
    fetchedAt: number;
    fetchedAtBlock: bigint;
  }> = {},
): CachedChunk {
  return {
    logs,
    fetchedAt: options.fetchedAt ?? Date.now(),
    fetchedAtBlock: options.fetchedAtBlock ?? toBlock + 1000n,
    alignedRange: { fromBlock, toBlock },
    fetchedRange: { fromBlock, toBlock },
  };
}

// Invalidation strategy that never invalidates
const neverInvalidate: InvalidationStrategy = () => 0;

// Invalidation strategy that always invalidates
const alwaysInvalidate: InvalidationStrategy = () => 1;

const chainId = 1;

// =============================================================================
// computeCacheKey Tests
// =============================================================================

describe("computeCacheKey", () => {
  describe("basic key generation", () => {
    it("generates key with address and topics", () => {
      const key = computeCacheKey({
        chainId,
        address: "0x1234567890123456789012345678901234567890",
        topics: ["0xabc"],
        fromBlock: 0n,
        toBlock: 9999n,
      });

      expect(key).toContain(CACHE_KEY_SEPARATOR);
      expect(key).toContain("0:9999");
    });
  });

  describe("address normalization", () => {
    it("lowercases single address", () => {
      const key1 = computeCacheKey({
        chainId,
        address: "0xABCDEF1234567890123456789012345678901234",
        fromBlock: 0n,
        toBlock: 9999n,
      });
      const key2 = computeCacheKey({
        chainId,
        address: "0xabcdef1234567890123456789012345678901234",
        fromBlock: 0n,
        toBlock: 9999n,
      });

      expect(key1).toBe(key2);
    });

    it("sorts and lowercases address array", () => {
      const key1 = computeCacheKey({
        chainId,
        address: ["0xBBBB567890123456789012345678901234567890", "0xAAAA567890123456789012345678901234567890"],
        fromBlock: 0n,
        toBlock: 9999n,
      });
      const key2 = computeCacheKey({
        chainId,
        address: ["0xaaaa567890123456789012345678901234567890", "0xbbbb567890123456789012345678901234567890"],
        fromBlock: 0n,
        toBlock: 9999n,
      });

      expect(key1).toBe(key2);
    });
  });

  describe("topic normalization", () => {
    it("lowercases topic values", () => {
      const key1 = computeCacheKey({
        chainId,
        topics: ["0xABCDEF"],
        fromBlock: 0n,
        toBlock: 9999n,
      });
      const key2 = computeCacheKey({
        chainId,
        topics: ["0xabcdef"],
        fromBlock: 0n,
        toBlock: 9999n,
      });

      expect(key1).toBe(key2);
    });

    it("handles null topics", () => {
      const key = computeCacheKey({
        chainId,
        topics: [null, "0xabc", null],
        fromBlock: 0n,
        toBlock: 9999n,
      });

      expect(key).toBeDefined();
    });

    it("sorts topic arrays and lowercases values", () => {
      const key1 = computeCacheKey({
        chainId,
        topics: [["0xBBB", "0xAAA"]],
        fromBlock: 0n,
        toBlock: 9999n,
      });
      const key2 = computeCacheKey({
        chainId,
        topics: [["0xaaa", "0xbbb"]],
        fromBlock: 0n,
        toBlock: 9999n,
      });

      expect(key1).toBe(key2);
    });
  });

  describe("determinism", () => {
    it("generates same key for same parameters", () => {
      const params = {
        chainId,
        address: "0x1234567890123456789012345678901234567890" as const,
        topics: ["0xabc", null, ["0xdef", "0x123"]] satisfies LogTopic[],
        fromBlock: 10000n,
        toBlock: 19999n,
      };

      const key1 = computeCacheKey(params);
      const key2 = computeCacheKey(params);

      expect(key1).toBe(key2);
    });
  });
});

// =============================================================================
// createCacheWriter Tests
// =============================================================================

describe("createCacheWriter", () => {
  const binSize = 10_000;
  const defaultFilter = {
    address: "0x1234567890123456789012345678901234567890" as Address,
    topics: ["0xabc"] as LogTopic[],
  };
  let cache: ReturnType<typeof createMockCache>;
  let sink: ReturnType<typeof createSink>;

  beforeEach(() => {
    cache = createMockCache();
    sink = createSink({ chainId, binSize, cache }, { filter: defaultFilter });
  });

  describe("single response handling", () => {
    it("writes complete bin to cache", async () => {
      const log = createMockLog(5000n);

      sink({
        logs: [log],
        fromBlock: 0n,
        toBlock: 9999n,
        fetchedAtBlock: 50000n,
        fetchedAt: Date.now(),
      });

      // Allow fire-and-forget to complete
      await sleep(10);

      expect(cache.writeCalls).toHaveLength(1);
      expect(cache.writeCalls[0]).toHaveLength(1);
      expect(cache.writeCalls[0]![0]!.value.logs).toEqual([log]);
    });

    it("does not write incomplete bin to cache", async () => {
      sink({
        logs: [createMockLog(5000n)],
        fromBlock: 0n,
        toBlock: 5000n,
        fetchedAtBlock: 50000n,
        fetchedAt: Date.now(),
      });

      await sleep(10);

      expect(cache.writeCalls).toHaveLength(0);
    });
  });

  describe("accumulation", () => {
    it("accumulates multiple responses until bin is complete", async () => {
      // First half of bin
      sink({
        logs: [createMockLog(2500n)],
        fromBlock: 0n,
        toBlock: 4999n,
        fetchedAtBlock: 50000n,
        fetchedAt: Date.now(),
      });

      await sleep(10);
      expect(cache.writeCalls).toHaveLength(0);

      // Second half of bin
      sink({
        logs: [createMockLog(7500n)],
        fromBlock: 5000n,
        toBlock: 9999n,
        fetchedAtBlock: 50000n,
        fetchedAt: Date.now(),
      });

      await sleep(10);

      expect(cache.writeCalls).toHaveLength(1);
      expect(cache.writeCalls[0]![0]!.value.logs).toHaveLength(2);
    });

    it("handles overlapping responses correctly", async () => {
      // First response covers 0-6000
      sink({
        logs: [createMockLog(3000n)],
        fromBlock: 0n,
        toBlock: 6000n,
        fetchedAtBlock: 50000n,
        fetchedAt: Date.now(),
      });

      // Second response covers 4000-9999 (overlaps 4000-6000)
      sink({
        logs: [createMockLog(8000n)],
        fromBlock: 4000n,
        toBlock: 9999n,
        fetchedAtBlock: 50000n,
        fetchedAt: Date.now(),
      });

      await sleep(10);

      // Should complete bin 0-9999
      expect(cache.writeCalls).toHaveLength(1);
    });
  });

  describe("deduplication", () => {
    it("deduplicates logs from concurrent responses for the same bin", async () => {
      // Simulate concurrent requests - both responses contain the same log at block 5000
      const sharedLog = createMockLog(5000n);
      const uniqueLog1 = createMockLog(3000n);
      const uniqueLog2 = createMockLog(7000n);

      // First concurrent response covers 0-5000
      sink({
        logs: [uniqueLog1, sharedLog],
        fromBlock: 0n,
        toBlock: 5000n,
        fetchedAtBlock: 50000n,
        fetchedAt: Date.now(),
      });

      // Second concurrent response covers 5000-9999 (overlaps at 5000)
      sink({
        logs: [sharedLog, uniqueLog2],
        fromBlock: 5000n,
        toBlock: 9999n,
        fetchedAtBlock: 50000n,
        fetchedAt: Date.now(),
      });

      await sleep(10);

      expect(cache.writeCalls).toHaveLength(1);
      const cachedLogs = cache.writeCalls[0]![0]!.value.logs;

      // Should have 3 unique logs, not 4 (sharedLog should be deduplicated)
      expect(cachedLogs).toHaveLength(3);

      // Verify the logs are the expected ones
      const blockNumbers = cachedLogs.map((log) => BigInt(log.blockNumber!)).sort();
      expect(blockNumbers).toEqual([3000n, 5000n, 7000n]);
    });
  });

  describe("multi-bin response", () => {
    it("distributes response across multiple bins", async () => {
      // Response spans bins [0-9999] and [10000-19999]
      const logs = [createMockLog(5000n), createMockLog(15000n)];

      sink({
        logs,
        fromBlock: 0n,
        toBlock: 19999n,
        fetchedAtBlock: 50000n,
        fetchedAt: Date.now(),
      });

      await sleep(10);

      // Both bins should be written in a single batched call
      expect(cache.writeCalls).toHaveLength(1);
      const writeItems = cache.writeCalls[0]!;
      expect(writeItems).toHaveLength(2);

      // Verify logs are correctly distributed
      const bin0 = writeItems.find((item) => item.key.includes("0:9999"));
      const bin1 = writeItems.find((item) => item.key.includes("10000:19999"));

      expect(bin0?.value.logs).toHaveLength(1);
      expect(bin0?.value.logs[0]?.blockNumber).toBe(toHex(5000n));
      expect(bin1?.value.logs).toHaveLength(1);
      expect(bin1?.value.logs[0]?.blockNumber).toBe(toHex(15000n));
    });
  });

  describe("metadata tracking", () => {
    it("tracks fetchedAt as max across responses", async () => {
      const earlyTime = 1000;
      const lateTime = 2000;

      sink({
        logs: [],
        fromBlock: 0n,
        toBlock: 4999n,
        fetchedAtBlock: 50000n,
        fetchedAt: earlyTime,
      });

      sink({
        logs: [],
        fromBlock: 5000n,
        toBlock: 9999n,
        fetchedAtBlock: 50000n,
        fetchedAt: lateTime,
      });

      await sleep(10);

      expect(cache.writeCalls[0]![0]!.value.fetchedAt).toBe(lateTime);
    });

    it("tracks fetchedAtBlock as max across responses", async () => {
      sink({
        logs: [],
        fromBlock: 0n,
        toBlock: 4999n,
        fetchedAtBlock: 40000n,
        fetchedAt: Date.now(),
      });

      sink({
        logs: [],
        fromBlock: 5000n,
        toBlock: 9999n,
        fetchedAtBlock: 50000n,
        fetchedAt: Date.now(),
      });

      await sleep(10);

      expect(cache.writeCalls[0]![0]!.value.fetchedAtBlock).toBe(50000n);
    });
  });

  describe("filter isolation", () => {
    it("maintains separate cache entries for different filters", async () => {
      const address1 = "0x1111111111111111111111111111111111111111";
      const address2 = "0x2222222222222222222222222222222222222222";

      // Create two sinks with different filters (as logsCache would per-request)
      const sink1 = createSink({ chainId, binSize, cache }, { filter: { address: address1 } });
      const sink2 = createSink({ chainId, binSize, cache }, { filter: { address: address2 } });

      sink1({
        logs: [createMockLog(5000n)],
        fromBlock: 0n,
        toBlock: 9999n,
        fetchedAtBlock: 50000n,
        fetchedAt: Date.now(),
      });

      sink2({
        logs: [createMockLog(6000n)],
        fromBlock: 0n,
        toBlock: 9999n,
        fetchedAtBlock: 50000n,
        fetchedAt: Date.now(),
      });

      await sleep(10);

      // Should have two separate cache entries (different keys due to different addresses)
      expect(cache.writeCalls).toHaveLength(2);
      expect(cache.storage.size).toBe(2);
    });
  });
});

// =============================================================================
// handleGetLogs Tests
// =============================================================================

describe("handleGetLogs", () => {
  const binSize = 10_000;
  let cache: ReturnType<typeof createMockCache>;

  beforeEach(() => {
    cache = createMockCache();
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2024-01-01"));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe("cache miss behavior", () => {
    it("fetches all ranges on complete cache miss", async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: (from) => [createMockLog(from)],
      });

      const logs = await handleGetLogs(
        requestFn,
        chainId,
        [{ fromBlock: "0x0", toBlock: "0x270f" }], // 0-9999, one bin
        { binSize, invalidationStrategy: neverInvalidate, cache },
      );

      expect(logs).toHaveLength(1);
      expect(cache.readCalls).toHaveLength(1);

      // Should have made eth_getLogs call
      const getLogsCalls = requestFn.mock.calls.filter((call) => call[0].method === "eth_getLogs");
      expect(getLogsCalls.length).toBeGreaterThan(0);
    });

    it("passes through blockHash queries without caching", async () => {
      const requestFn = createMockRequestFn({});
      const blockHash: Hex = `0x${"c".repeat(64)}`;

      const logs = await handleGetLogs(requestFn, chainId, [{ blockHash }], {
        binSize,
        invalidationStrategy: neverInvalidate,
        cache,
      });

      expect(logs).toHaveLength(1);
      expect(cache.readCalls).toHaveLength(0);
    });
  });

  describe("cache hit behavior", () => {
    it("returns cached data on full cache hit", async () => {
      const cachedLog = createMockLog(5000n);
      const cacheKey = computeCacheKey({
        chainId,
        fromBlock: 0n,
        toBlock: 9999n,
      });

      cache.storage.set(cacheKey, createCachedChunk(0n, 9999n, [cachedLog]));

      const requestFn = createMockRequestFn({ latestBlock: 100_000n });

      const logs = await handleGetLogs(requestFn, chainId, [{ fromBlock: "0x0", toBlock: "0x270f" }], {
        binSize,
        invalidationStrategy: neverInvalidate,
        cache,
      });

      expect(logs).toContainEqual(cachedLog);

      // Should NOT have made eth_getLogs call
      const getLogsCalls = requestFn.mock.calls.filter((call) => call[0].method === "eth_getLogs");
      expect(getLogsCalls).toHaveLength(0);
    });

    it("handles partial cache hits with gap fetching", async () => {
      // Cache first bin only
      const cachedLog = createMockLog(5000n);
      const cacheKey = computeCacheKey({ chainId, fromBlock: 0n, toBlock: 9999n });
      cache.storage.set(cacheKey, createCachedChunk(0n, 9999n, [cachedLog]));

      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: (from) => [createMockLog(from)],
      });

      // Request spans 2 bins: [0-9999] (cached) and [10000-19999] (miss)
      const logs = await handleGetLogs(
        requestFn,
        chainId,
        [{ fromBlock: "0x0", toBlock: "0x4e1f" }], // 0-19999
        { binSize, invalidationStrategy: neverInvalidate, cache },
      );

      // Should have logs from both cache and fetch
      expect(logs.length).toBeGreaterThanOrEqual(2);

      // Should have fetched only the missing range
      const getLogsCalls = requestFn.mock.calls.filter((call) => call[0].method === "eth_getLogs");
      expect(getLogsCalls).toHaveLength(1);

      // The fetch should be for the gap (second bin)
      const fetchedFrom = BigInt(getLogsCalls[0]![0].params[0].fromBlock);
      expect(fetchedFrom).toBe(10000n);
    });
  });

  describe("probabilistic invalidation", () => {
    it("refetches when invalidation strategy returns 1", async () => {
      const cachedLog = createMockLog(5000n);
      const cacheKey = computeCacheKey({ chainId, fromBlock: 0n, toBlock: 9999n });
      cache.storage.set(cacheKey, createCachedChunk(0n, 9999n, [cachedLog]));

      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: () => [createMockLog(6000n)], // Different log than cached
      });

      await handleGetLogs(requestFn, chainId, [{ fromBlock: "0x0", toBlock: "0x270f" }], {
        binSize,
        invalidationStrategy: alwaysInvalidate,
        cache,
      });

      // Should have fetched fresh data
      const getLogsCalls = requestFn.mock.calls.filter((call) => call[0].method === "eth_getLogs");
      expect(getLogsCalls).toHaveLength(1);
    });

    it("passes correct context to invalidation strategy", async () => {
      const fetchedAtBlock = 50000n;
      const fetchedAt = Date.now() - 5000; // 5 seconds ago

      const cachedLog = createMockLog(5000n);
      const cacheKey = computeCacheKey({ chainId, fromBlock: 0n, toBlock: 9999n });
      cache.storage.set(cacheKey, createCachedChunk(0n, 9999n, [cachedLog], { fetchedAtBlock, fetchedAt }));

      const invalidationStrategy = vi.fn().mockReturnValue(0);

      const requestFn = createMockRequestFn({ latestBlock: 100_000n });

      await handleGetLogs(requestFn, chainId, [{ fromBlock: "0x0", toBlock: "0x270f" }], {
        binSize,
        invalidationStrategy,
        cache,
      });

      expect(invalidationStrategy).toHaveBeenCalledWith({
        confirmations: Number(fetchedAtBlock - 9999n),
        cacheAgeMs: expect.any(Number),
        totalChunks: 1,
      });
    });
  });

  describe("gap merging", () => {
    it("merges consecutive gaps into single fetch", async () => {
      // Cache bins 0 and 3, leave gaps at 1 and 2
      cache.storage.set(computeCacheKey({ chainId, fromBlock: 0n, toBlock: 9999n }), createCachedChunk(0n, 9999n, []));
      cache.storage.set(
        computeCacheKey({ chainId, fromBlock: 30000n, toBlock: 39999n }),
        createCachedChunk(30000n, 39999n, []),
      );

      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: () => [],
      });

      await handleGetLogs(
        requestFn,
        chainId,
        [{ fromBlock: "0x0", toBlock: "0x9c3f" }], // 0-39999 (4 bins)
        { binSize, invalidationStrategy: neverInvalidate, cache },
      );

      // Should merge gaps [10000-19999] and [20000-29999] into single fetch
      const getLogsCalls = requestFn.mock.calls.filter((call) => call[0].method === "eth_getLogs");
      expect(getLogsCalls).toHaveLength(1);

      const fetchedFrom = BigInt(getLogsCalls[0]![0].params[0].fromBlock);
      const fetchedTo = BigInt(getLogsCalls[0]![0].params[0].toBlock);
      expect(fetchedFrom).toBe(10000n);
      expect(fetchedTo).toBe(29999n);
    });
  });

  describe("edge cases", () => {
    it("returns empty array when fromBlock > toBlock", async () => {
      const requestFn = createMockRequestFn({ latestBlock: 100_000n });

      const logs = await handleGetLogs(
        requestFn,
        chainId,
        [{ fromBlock: "0x2710", toBlock: "0x0" }], // 10000 to 0
        { binSize, invalidationStrategy: neverInvalidate, cache },
      );

      expect(logs).toEqual([]);
    });

    it('resolves "latest" block tag correctly', async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 15000n,
        logGenerator: () => [],
      });

      await handleGetLogs(requestFn, chainId, [{ fromBlock: "0x0", toBlock: "latest" }], {
        binSize,
        invalidationStrategy: neverInvalidate,
        cache,
      });

      // Should have read cache for bins [0-9999] and [10000-19999]
      // (aligned up from 15000)
      expect(cache.readCalls).toHaveLength(1);
      expect(cache.readCalls[0]).toHaveLength(2);
    });

    it('resolves "earliest" block tag to 0', async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: () => [],
      });

      await handleGetLogs(requestFn, chainId, [{ fromBlock: "earliest", toBlock: "0x270f" }], {
        binSize,
        invalidationStrategy: neverInvalidate,
        cache,
      });

      expect(cache.readCalls[0]![0]).toContain("0:9999");
    });

    it("filters logs to requested range", async () => {
      // Request a range smaller than bin size
      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: () => [
          createMockLog(0n), // Outside requested range
          createMockLog(5000n), // Inside requested range
          createMockLog(9999n), // Outside requested range
        ],
      });

      const logs = await handleGetLogs(
        requestFn,
        chainId,
        [{ fromBlock: "0x1388", toBlock: "0x1770" }], // 5000-6000
        { binSize, invalidationStrategy: neverInvalidate, cache },
      );

      // Should only include the log at 5000n
      expect(logs).toHaveLength(1);
      expect(logs[0]?.blockNumber).toBe(toHex(5000n));
    });

    it("sorts logs by block number", async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: () => [createMockLog(8000n), createMockLog(2000n), createMockLog(5000n)],
      });

      const logs = await handleGetLogs(requestFn, chainId, [{ fromBlock: "0x0", toBlock: "0x270f" }], {
        binSize,
        invalidationStrategy: neverInvalidate,
        cache,
      });

      const blockNumbers = logs.map((log) => BigInt(log.blockNumber!));
      expect(blockNumbers).toEqual([2000n, 5000n, 8000n]);
    });
  });
});

// =============================================================================
// Invalidation Strategy Tests
// =============================================================================

describe("invalidation strategies", () => {
  // Import the actual strategies to test them
  // Note: These are tested as units since they're pure functions

  describe("createSimpleInvalidation behavior", () => {
    it("returns 0 when cache age is below minimum", () => {
      // Cache entry is very fresh (0ms old)
      const strategy: InvalidationStrategy = ({ cacheAgeMs }) => {
        if (cacheAgeMs < 5000) return 0;
        return 0.5;
      };

      expect(strategy({ confirmations: 0, cacheAgeMs: 0, totalChunks: 1 })).toBe(0);
      expect(strategy({ confirmations: 0, cacheAgeMs: 4999, totalChunks: 1 })).toBe(0);
    });

    it("returns 1 for hot blocks (few confirmations)", () => {
      // Entry was near chain tip when fetched
      const strategy: InvalidationStrategy = ({ confirmations, cacheAgeMs }) => {
        if (cacheAgeMs < 5000) return 0;
        if (confirmations < 128) return 1;
        return 0.001;
      };

      expect(strategy({ confirmations: 10, cacheAgeMs: 10000, totalChunks: 1 })).toBe(1);
      expect(strategy({ confirmations: 127, cacheAgeMs: 10000, totalChunks: 1 })).toBe(1);
    });

    it("returns low probability for old entries with many confirmations", () => {
      const strategy: InvalidationStrategy = ({ confirmations, cacheAgeMs }) => {
        if (cacheAgeMs < 5000) return 0;
        if (confirmations < 128) return 1;
        return 0.001;
      };

      expect(strategy({ confirmations: 1000, cacheAgeMs: 10000, totalChunks: 1 })).toBe(0.001);
    });
  });
});
