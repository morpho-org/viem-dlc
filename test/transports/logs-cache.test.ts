import { type RpcLog, toHex } from "viem";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createSlot, LazyNdjsonMap } from "../../src/internal/index.js";
import type { Entry } from "../../src/internal/ndjson-map.js";
import { MemoryStore } from "../../src/stores/memory.js";
import { handleGetLogs } from "../../src/transports/logs-cache/eth-get-logs/handler.js";
import { createSink } from "../../src/transports/logs-cache/eth-get-logs/sink.js";
import type { CachedChunk, CachedLogs, CachedMetadata } from "../../src/transports/logs-cache/eth-get-logs/types.js";
import { keychain } from "../../src/transports/logs-cache/keychain.js";
import type { InvalidationStrategy } from "../../src/transports/logs-cache/types.js";
import { parse, stringify } from "../../src/utils/json.js";

// =============================================================================
// Test Utilities
// =============================================================================

const codec = { toJson: stringify, fromJson: parse } as const;
const chainId = 1;
const binSize = 10_000;
const neverInvalidate: InvalidationStrategy = () => 0;
const alwaysInvalidate: InvalidationStrategy = () => 1;

function createMockLog(blockNumber: bigint, logIndex = 0): RpcLog {
  return {
    address: "0x1234567890123456789012345678901234567890",
    topics: ["0xabc"],
    data: "0x",
    blockNumber: toHex(blockNumber),
    transactionHash: `0x${"a".repeat(64)}`,
    transactionIndex: "0x0",
    blockHash: `0x${blockNumber.toString(16).padStart(64, "0")}`,
    logIndex: toHex(logIndex),
    removed: false,
  };
}

function entryKey(fromBlock: bigint, toBlock: bigint) {
  return keychain.entryKey(chainId, "eth_getLogs", { fromBlock, toBlock });
}

function createNdjson() {
  const slot = createSlot();
  const ndjson = new LazyNdjsonMap<CachedChunk>(codec, { autoFlushThresholdBytes: Number.MAX_SAFE_INTEGER }, slot);
  return { ndjson, slot };
}

/** Pre-populate a store with metadata + logs entries for one or more bins. */
async function populateStore(
  store: MemoryStore,
  blobKey: string,
  bins: {
    fromBlock: bigint;
    toBlock: bigint;
    logs: RpcLog[];
    fetchedAt?: number;
    fetchedAtBlock?: bigint;
  }[],
) {
  let buffers = store.get(blobKey) ?? [];
  const ndjson = new LazyNdjsonMap<CachedChunk>(
    codec,
    { autoFlushThresholdBytes: Number.MAX_SAFE_INTEGER },
    { get: () => buffers, set: (v) => { buffers = v; store.set(blobKey, v); } },
  );

  for (const bin of bins) {
    const ek = entryKey(bin.fromBlock, bin.toBlock);
    ndjson.upsert([
      {
        key: ek.metadata,
        value: {
          __type: "metadata" as const,
          fetchedAt: bin.fetchedAt ?? Date.now(),
          fetchedAtBlock: bin.fetchedAtBlock ?? bin.toBlock + 1000n,
          alignedRange: { fromBlock: bin.fromBlock, toBlock: bin.toBlock },
          fetchedRange: { fromBlock: bin.fromBlock, toBlock: bin.toBlock },
        },
      },
      {
        key: ek.data,
        value: bin.logs as CachedLogs,
      },
    ]);
  }

  await ndjson.flush();
}

async function collectRecords(ndjson: LazyNdjsonMap<CachedChunk>) {
  const records: Entry<CachedChunk>[] = [];
  for await (const record of ndjson.records()) {
    records.push(record);
  }
  return records;
}

/**
 * Creates a mock requestFn that handles eth_blockNumber and eth_getLogs.
 * For eth_getLogs, it calls the onLogsResponse callback with generated logs.
 */
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
      const additional = args.params?.[2];
      const fromBlock = BigInt(filter.fromBlock);
      const toBlock = BigInt(filter.toBlock);

      const logs = logGenerator ? logGenerator(fromBlock, toBlock) : [createMockLog(fromBlock)];

      if (additional?.onLogsResponse) {
        additional.onLogsResponse({
          logs,
          fromBlock,
          toBlock,
          fetchedAtBlock: BigInt(additional.latestBlock),
          fetchedAt: Date.now(),
        });
      }

      return additional?.onLogsResponseOnly ? undefined : logs;
    }

    throw new Error(`Unexpected method: ${args.method}`);
  });
}

// =============================================================================
// Sink Tests
// =============================================================================

describe("createSink", () => {
  it("writes metadata + logs as a batch for complete bins", async () => {
    const { ndjson } = createNdjson();
    const sink = createSink({ chainId, binSize, ndjson });

    sink({
      logs: [createMockLog(5000n)],
      fromBlock: 0n,
      toBlock: 9999n,
      fetchedAtBlock: 50000n,
      fetchedAt: 1000,
    });

    await ndjson.flush();
    const records = await collectRecords(ndjson);

    const ek = entryKey(0n, 9999n);
    expect(records).toHaveLength(2);

    // Metadata entry
    expect(records[0]!.key).toBe(ek.metadata);
    expect((records[0]!.value as CachedMetadata).__type).toBe("metadata");
    expect((records[0]!.value as CachedMetadata).fetchedAt).toBe(1000);

    // Logs entry
    expect(records[1]!.key).toBe(ek.data);
    expect(records[1]!.value).toHaveLength(1);
    expect((records[1]!.value as CachedLogs)[0]!.blockNumber).toBe(toHex(5000n));
  });

  it("does not write incomplete bins", async () => {
    const { ndjson } = createNdjson();
    const sink = createSink({ chainId, binSize, ndjson });

    sink({
      logs: [createMockLog(5000n)],
      fromBlock: 0n,
      toBlock: 5000n, // Only covers half the bin [0, 9999]
      fetchedAtBlock: 50000n,
      fetchedAt: Date.now(),
    });

    await ndjson.flush();
    expect(await collectRecords(ndjson)).toHaveLength(0);
  });

  it("accumulates multiple responses until bin is complete", async () => {
    const { ndjson } = createNdjson();
    const sink = createSink({ chainId, binSize, ndjson });

    sink({
      logs: [createMockLog(2500n)],
      fromBlock: 0n,
      toBlock: 4999n,
      fetchedAtBlock: 50000n,
      fetchedAt: Date.now(),
    });

    // Still incomplete
    await ndjson.flush();
    expect(await collectRecords(ndjson)).toHaveLength(0);

    sink({
      logs: [createMockLog(7500n)],
      fromBlock: 5000n,
      toBlock: 9999n,
      fetchedAtBlock: 50000n,
      fetchedAt: Date.now(),
    });

    await ndjson.flush();
    const records = await collectRecords(ndjson);
    expect(records).toHaveLength(2);
    expect(records[1]!.value).toHaveLength(2);
  });

  it("sorts logs within bin by blockNumber then logIndex", async () => {
    const { ndjson } = createNdjson();
    const sink = createSink({ chainId, binSize, ndjson });

    // Logs arrive in reverse order
    sink({
      logs: [createMockLog(8000n, 1), createMockLog(8000n, 0), createMockLog(2000n, 0)],
      fromBlock: 0n,
      toBlock: 9999n,
      fetchedAtBlock: 50000n,
      fetchedAt: Date.now(),
    });

    await ndjson.flush();
    const records = await collectRecords(ndjson);
    const logs = records[1]!.value as CachedLogs;

    expect(logs.map((l) => [BigInt(l.blockNumber!), BigInt(l.logIndex!)])).toEqual([
      [2000n, 0n],
      [8000n, 0n],
      [8000n, 1n],
    ]);
  });

  it("distributes multi-bin responses across bins", async () => {
    const { ndjson } = createNdjson();
    const sink = createSink({ chainId, binSize, ndjson });

    sink({
      logs: [createMockLog(5000n), createMockLog(15000n)],
      fromBlock: 0n,
      toBlock: 19999n,
      fetchedAtBlock: 50000n,
      fetchedAt: Date.now(),
    });

    await ndjson.flush();
    const records = await collectRecords(ndjson);

    // 2 bins x 2 entries (metadata + logs) = 4
    // All 0: metadata keys sort before all 1: logs keys
    expect(records).toHaveLength(4);
    expect(records[0]!.key).toContain("0:");
    expect(records[1]!.key).toContain("0:");
    expect(records[2]!.key).toContain("1:");
    expect(records[3]!.key).toContain("1:");

    // Logs entries have correct data
    const bin0Logs = records[2]!.value as CachedLogs;
    expect(bin0Logs).toHaveLength(1);
    expect(bin0Logs[0]!.blockNumber).toBe(toHex(5000n));

    const bin1Logs = records[3]!.value as CachedLogs;
    expect(bin1Logs).toHaveLength(1);
    expect(bin1Logs[0]!.blockNumber).toBe(toHex(15000n));
  });

  it("tracks fetchedAt and fetchedAtBlock as max across responses", async () => {
    const { ndjson } = createNdjson();
    const sink = createSink({ chainId, binSize, ndjson });

    sink({
      logs: [],
      fromBlock: 0n,
      toBlock: 4999n,
      fetchedAtBlock: 40000n,
      fetchedAt: 1000,
    });

    sink({
      logs: [],
      fromBlock: 5000n,
      toBlock: 9999n,
      fetchedAtBlock: 50000n,
      fetchedAt: 2000,
    });

    await ndjson.flush();
    const records = await collectRecords(ndjson);
    const metadata = records[0]!.value as CachedMetadata;

    expect(metadata.fetchedAt).toBe(2000);
    expect(metadata.fetchedAtBlock).toBe(50000n);
  });
});

// =============================================================================
// Keychain Tests
// =============================================================================

describe("keychain", () => {
  it("zero-pads entry keys to 20 digits", () => {
    const key = entryKey(0n, 9999n);
    expect(key.metadata).toBe("0:00000000000000000000:00000000000000009999");
    expect(key.data).toBe("1:00000000000000000000:00000000000000009999");
  });

  it("produces lexicographically correct order across digit-length boundaries", () => {
    const key9k = entryKey(9000n, 9999n);
    const key10k = entryKey(10000n, 19999n);
    expect(key9k.data < key10k.data).toBe(true);
  });

  it("generates deterministic blob keys", () => {
    const req = {
      method: "eth_getLogs" as const,
      params: [{ address: "0x1234567890123456789012345678901234567890", topics: ["0xabc"] }] as any,
    };
    expect(keychain.blobKey(chainId, req)).toBe(keychain.blobKey(chainId, req));
  });
});

// =============================================================================
// handleGetLogs Tests
// =============================================================================

describe("handleGetLogs", () => {
  const blobKey = "test-blob-key";
  let store: MemoryStore;

  beforeEach(() => {
    store = new MemoryStore();
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2024-01-01"));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe("cache miss", () => {
    it("fetches all ranges on complete cache miss", async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: (from) => [createMockLog(from)],
      });

      const logs = await handleGetLogs(
        requestFn,
        chainId,
        [{ fromBlock: "0x0", toBlock: "0x270f" }], // 0-9999
        blobKey,
        { binSize, invalidationStrategy: neverInvalidate, store },
      );

      expect(logs).toHaveLength(1);

      const getLogsCalls = requestFn.mock.calls.filter((c: any) => c[0].method === "eth_getLogs");
      expect(getLogsCalls.length).toBeGreaterThan(0);
    });

    it("persists fetched data to store", async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: (from) => [createMockLog(from)],
      });

      await handleGetLogs(requestFn, chainId, [{ fromBlock: "0x0", toBlock: "0x270f" }], blobKey, {
        binSize,
        invalidationStrategy: neverInvalidate,
        store,
      });

      expect(store.get(blobKey)).not.toBeNull();
    });
  });

  describe("cache hit", () => {
    it("returns cached data without fetching", async () => {
      const cachedLog = createMockLog(5000n);

      await populateStore(store, blobKey, [{ fromBlock: 0n, toBlock: 9999n, logs: [cachedLog] }]);

      const requestFn = createMockRequestFn({ latestBlock: 100_000n });

      const logs = await handleGetLogs(requestFn, chainId, [{ fromBlock: "0x0", toBlock: "0x270f" }], blobKey, {
        binSize,
        invalidationStrategy: neverInvalidate,
        store,
      });

      expect(logs).toContainEqual(cachedLog);

      const getLogsCalls = requestFn.mock.calls.filter((c: any) => c[0].method === "eth_getLogs");
      expect(getLogsCalls).toHaveLength(0);
    });

    it("handles partial cache hits with gap fetching", async () => {
      await populateStore(store, blobKey, [
        { fromBlock: 0n, toBlock: 9999n, logs: [createMockLog(5000n)] },
      ]);

      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: (from) => [createMockLog(from)],
      });

      const logs = await handleGetLogs(
        requestFn,
        chainId,
        [{ fromBlock: "0x0", toBlock: "0x4e1f" }], // 0-19999
        blobKey,
        { binSize, invalidationStrategy: neverInvalidate, store },
      );

      expect(logs.length).toBeGreaterThanOrEqual(2);

      const getLogsCalls = requestFn.mock.calls.filter((c: any) => c[0].method === "eth_getLogs");
      expect(getLogsCalls).toHaveLength(1);
      expect(BigInt(getLogsCalls[0]![0].params[0].fromBlock)).toBe(10000n);
    });
  });

  describe("invalidation", () => {
    it("refetches when invalidation strategy returns 1", async () => {
      await populateStore(store, blobKey, [
        { fromBlock: 0n, toBlock: 9999n, logs: [createMockLog(5000n)] },
      ]);

      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: () => [createMockLog(6000n)],
      });

      await handleGetLogs(requestFn, chainId, [{ fromBlock: "0x0", toBlock: "0x270f" }], blobKey, {
        binSize,
        invalidationStrategy: alwaysInvalidate,
        store,
      });

      const getLogsCalls = requestFn.mock.calls.filter((c: any) => c[0].method === "eth_getLogs");
      expect(getLogsCalls).toHaveLength(1);
    });

    it("passes correct context to invalidation strategy", async () => {
      const fetchedAtBlock = 50000n;
      const fetchedAt = Date.now() - 5000;

      await populateStore(store, blobKey, [
        { fromBlock: 0n, toBlock: 9999n, logs: [], fetchedAt, fetchedAtBlock },
      ]);

      const strategy = vi.fn().mockReturnValue(0);
      const requestFn = createMockRequestFn({ latestBlock: 100_000n });

      await handleGetLogs(requestFn, chainId, [{ fromBlock: "0x0", toBlock: "0x270f" }], blobKey, {
        binSize,
        invalidationStrategy: strategy,
        store,
      });

      expect(strategy).toHaveBeenCalledWith({
        confirmations: Number(fetchedAtBlock - 9999n),
        cacheAgeMs: expect.any(Number),
        totalChunks: 1,
      });
    });
  });

  describe("gap merging", () => {
    it("merges consecutive gaps into single fetch", async () => {
      // Cache bins 0 and 3, leave gaps at 1 and 2
      await populateStore(store, blobKey, [
        { fromBlock: 0n, toBlock: 9999n, logs: [] },
        { fromBlock: 30000n, toBlock: 39999n, logs: [] },
      ]);

      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: () => [],
      });

      await handleGetLogs(
        requestFn,
        chainId,
        [{ fromBlock: "0x0", toBlock: "0x9c3f" }], // 0-39999
        blobKey,
        { binSize, invalidationStrategy: neverInvalidate, store },
      );

      const getLogsCalls = requestFn.mock.calls.filter((c: any) => c[0].method === "eth_getLogs");
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
        [{ fromBlock: "0x2710", toBlock: "0x0" }],
        blobKey,
        { binSize, invalidationStrategy: neverInvalidate, store },
      );

      expect(logs).toEqual([]);
    });

    it("throws on blockHash queries", async () => {
      const requestFn = createMockRequestFn({});

      await expect(
        handleGetLogs(requestFn, chainId, [{ blockHash: `0x${"c".repeat(64)}` }], blobKey, {
          binSize,
          invalidationStrategy: neverInvalidate,
          store,
        }),
      ).rejects.toThrow("blockHash");
    });

    it("filters logs to requested range", async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: () => [createMockLog(0n), createMockLog(5000n), createMockLog(9999n)],
      });

      const logs = await handleGetLogs(
        requestFn,
        chainId,
        [{ fromBlock: "0x1388", toBlock: "0x1770" }], // 5000-6000
        blobKey,
        { binSize, invalidationStrategy: neverInvalidate, store },
      );

      expect(logs).toHaveLength(1);
      expect(logs[0]!.blockNumber).toBe(toHex(5000n));
    });

    it("sorts logs by block number", async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: () => [createMockLog(8000n), createMockLog(2000n), createMockLog(5000n)],
      });

      const logs = await handleGetLogs(
        requestFn,
        chainId,
        [{ fromBlock: "0x0", toBlock: "0x270f" }],
        blobKey,
        { binSize, invalidationStrategy: neverInvalidate, store },
      );

      const blockNumbers = logs.map((log) => BigInt(log.blockNumber!));
      expect(blockNumbers).toEqual([2000n, 5000n, 8000n]);
    });
  });

  describe("reduce", () => {
    it("applies reduce callback to logs in order", async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: () => [createMockLog(8000n), createMockLog(2000n), createMockLog(5000n)],
      });

      const observed: bigint[] = [];
      const reduce = (acc: RpcLog[], log: RpcLog) => {
        observed.push(BigInt(log.blockNumber!));
        acc.push(log);
        return acc;
      };

      await handleGetLogs(
        requestFn,
        chainId,
        [{ fromBlock: "0x0", toBlock: "0x270f" }, { reduce }],
        blobKey,
        { binSize, invalidationStrategy: neverInvalidate, store },
      );

      // Reduce sees logs in sorted order (within-bin sort by sink)
      expect(observed).toEqual([2000n, 5000n, 8000n]);
    });
  });

  describe("error handling", () => {
    it("flushes partial data to store on fetch error", async () => {
      // Cache the middle bin so we get two non-contiguous gaps that can't be merged
      await populateStore(store, blobKey, [
        { fromBlock: 10000n, toBlock: 19999n, logs: [] },
      ]);

      let callCount = 0;
      const requestFn = vi.fn().mockImplementation(async (args: { method: string; params?: any[] }) => {
        if (args.method === "eth_blockNumber") return toHex(100_000n);
        if (args.method === "eth_getLogs") {
          callCount++;
          const filter = args.params?.[0];
          const additional = args.params?.[2];
          const fromBlock = BigInt(filter.fromBlock);
          const toBlock = BigInt(filter.toBlock);

          // Succeed for first gap, fail for second
          if (callCount === 1) {
            additional?.onLogsResponse?.({
              logs: [createMockLog(fromBlock)],
              fromBlock,
              toBlock,
              fetchedAtBlock: BigInt(additional.latestBlock),
              fetchedAt: Date.now(),
            });
            return undefined;
          }
          throw new Error("RPC failure");
        }
        throw new Error(`Unexpected: ${args.method}`);
      });

      await expect(
        handleGetLogs(
          requestFn,
          chainId,
          [{ fromBlock: "0x0", toBlock: "0x752f" }], // 0-29999 (3 bins, middle cached)
          blobKey,
          { binSize, invalidationStrategy: neverInvalidate, store },
        ),
      ).rejects.toThrow("Gap fetch failed");

      // Partial data should still be persisted
      expect(store.get(blobKey)).not.toBeNull();
    });
  });
});

// =============================================================================
// Invalidation Strategy Tests
// =============================================================================

describe("invalidation strategies", () => {
  it("returns 0 when cache age is below minimum", () => {
    const strategy: InvalidationStrategy = ({ cacheAgeMs }) => (cacheAgeMs < 5000 ? 0 : 0.5);

    expect(strategy({ confirmations: 0, cacheAgeMs: 0, totalChunks: 1 })).toBe(0);
    expect(strategy({ confirmations: 0, cacheAgeMs: 4999, totalChunks: 1 })).toBe(0);
  });

  it("returns 1 for hot blocks (few confirmations)", () => {
    const strategy: InvalidationStrategy = ({ confirmations, cacheAgeMs }) => {
      if (cacheAgeMs < 5000) return 0;
      return confirmations < 128 ? 1 : 0.001;
    };

    expect(strategy({ confirmations: 10, cacheAgeMs: 10000, totalChunks: 1 })).toBe(1);
    expect(strategy({ confirmations: 127, cacheAgeMs: 10000, totalChunks: 1 })).toBe(1);
  });

  it("returns low probability for deeply confirmed entries", () => {
    const strategy: InvalidationStrategy = ({ confirmations, cacheAgeMs }) => {
      if (cacheAgeMs < 5000) return 0;
      return confirmations < 128 ? 1 : 0.001;
    };

    expect(strategy({ confirmations: 1000, cacheAgeMs: 10000, totalChunks: 1 })).toBe(0.001);
  });
});
