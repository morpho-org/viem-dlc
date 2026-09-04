import { type RpcLog, toHex } from "viem";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createFacetId } from "../../../../src/observability.js";
import { MemoryStore } from "../../../../src/stores/memory.js";
import { handleEthGetLogs } from "../../../../src/transports/cache/eth-get-logs/handler.js";
import { keychain } from "../../../../src/transports/cache/keychain.js";
import { type CacheSchema, cacheTransportKey } from "../../../../src/transports/cache/schema.js";
import type { HandlerContext, InvalidationStrategy } from "../../../../src/transports/cache/types.js";
import type { EIP1193Parameters } from "../../../../src/types.js";
import { createCoalescingMutex } from "../../../../src/utils/coalescing-mutex.js";

import {
  alwaysInvalidate,
  binSize,
  chainId,
  createMockLog,
  createMockRequestFn,
  deferred,
  neverInvalidate,
  populateStore,
} from "./_helpers.js";

type EthGetLogsRequest = EIP1193Parameters<CacheSchema, "eth_getLogs">;

function getLogsCalls(requestFn: ReturnType<typeof vi.fn>) {
  return requestFn.mock.calls.filter((c) => c[0].method === "eth_getLogs");
}

function blockNumbers(logs: RpcLog[]) {
  return logs.map((log) => BigInt(log.blockNumber!));
}

function logsInRange(logs: RpcLog[]) {
  return (fromBlock: bigint, toBlock: bigint) =>
    logs.filter((log) => {
      const blockNumber = BigInt(log.blockNumber!);
      return fromBlock <= blockNumber && blockNumber <= toBlock;
    });
}

describe("handleEthGetLogs", () => {
  const { coalesce } = createCoalescingMutex();
  const blobKey = keychain.blobKey(chainId, { method: "eth_getLogs", params: [{}] } as unknown as EthGetLogsRequest)!;
  let store: MemoryStore;

  beforeEach(() => {
    store = new MemoryStore();
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2024-01-01"));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function callHandler(
    requestFn: ReturnType<typeof createMockRequestFn>,
    params: unknown[],
    invalidationStrategy: InvalidationStrategy,
  ) {
    return handleEthGetLogs(
      {
        binSize,
        invalidationStrategy,
        store,
        chainId,
        requestFn: requestFn as unknown as HandlerContext["requestFn"],
        coalesce,
        facetId: createFacetId(cacheTransportKey),
      },
      { method: "eth_getLogs", params } as unknown as EthGetLogsRequest,
    );
  }

  describe("cache miss", () => {
    it("fetches all ranges on complete cache miss", async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: (from) => [createMockLog(from)],
      });

      const logs = await callHandler(requestFn, [{ fromBlock: "0x0", toBlock: "0x270f" }], neverInvalidate);

      expect(logs).toHaveLength(1);
      expect(getLogsCalls(requestFn).length).toBeGreaterThan(0);
    });

    it("persists fetched data to store", async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: (from) => [createMockLog(from)],
      });

      await callHandler(requestFn, [{ fromBlock: "0x0", toBlock: "0x270f" }], neverInvalidate);

      expect(store.get(blobKey)).not.toBeNull();
    });

    it("returns logs from the trailing partial bin on cold miss", async () => {
      const logs = [createMockLog(9999n), createMockLog(10000n), createMockLog(10005n)];
      const requestFn = createMockRequestFn({
        latestBlock: 10005n,
        logGenerator: logsInRange(logs),
      });

      const result = await callHandler(requestFn, [{ fromBlock: toHex(9999n), toBlock: "latest" }], neverInvalidate);

      expect(blockNumbers(result)).toEqual([9999n, 10000n, 10005n]);
    });
  });

  describe("cache hit", () => {
    it("returns cached data without fetching", async () => {
      const cachedLog = createMockLog(5000n);

      await populateStore(store, blobKey, [{ fromBlock: 0n, toBlock: 9999n, logs: [cachedLog] }]);

      const requestFn = createMockRequestFn({ latestBlock: 100_000n });
      const logs = await callHandler(requestFn, [{ fromBlock: "0x0", toBlock: "0x270f" }], neverInvalidate);

      expect(logs).toContainEqual(cachedLog);
      expect(getLogsCalls(requestFn)).toHaveLength(0);
    });

    it("handles partial cache hits with gap fetching", async () => {
      await populateStore(store, blobKey, [{ fromBlock: 0n, toBlock: 9999n, logs: [createMockLog(5000n)] }]);

      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: (from) => [createMockLog(from)],
      });

      const logs = await callHandler(requestFn, [{ fromBlock: "0x0", toBlock: "0x4e1f" }], neverInvalidate);

      expect(logs.length).toBeGreaterThanOrEqual(2);
      expect(getLogsCalls(requestFn)).toHaveLength(1);
      expect(BigInt(getLogsCalls(requestFn)[0]![0].params[0].fromBlock)).toBe(10000n);
    });

    it("includes cached logs from a trailing partial bin on a repeat request", async () => {
      const logs = [createMockLog(9999n), createMockLog(10000n), createMockLog(10005n)];
      await populateStore(store, blobKey, [
        {
          fromBlock: 0n,
          toBlock: 9999n,
          logs: [logs[0]!],
          fetchedAtBlock: 10005n,
        },
        {
          fromBlock: 10000n,
          toBlock: 19999n,
          fetchedRange: { fromBlock: 10000n, toBlock: 10005n },
          logs: logs.slice(1),
          fetchedAtBlock: 10005n,
        },
      ]);

      const requestFn = createMockRequestFn({
        latestBlock: 10005n,
        logGenerator: logsInRange(logs),
      });

      const result = await callHandler(requestFn, [{ fromBlock: toHex(9999n), toBlock: "latest" }], neverInvalidate);

      expect(blockNumbers(result)).toEqual([9999n, 10000n, 10005n]);
    });
  });

  describe("invalidation", () => {
    it("refetches when invalidation strategy returns 1", async () => {
      await populateStore(store, blobKey, [{ fromBlock: 0n, toBlock: 9999n, logs: [createMockLog(5000n)] }]);

      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: () => [createMockLog(6000n)],
      });

      await callHandler(requestFn, [{ fromBlock: "0x0", toBlock: "0x270f" }], alwaysInvalidate);

      expect(getLogsCalls(requestFn)).toHaveLength(1);
    });

    it("passes correct context to invalidation strategy", async () => {
      const fetchedAtBlock = 50000n;
      const fetchedAt = Date.now() - 5000;

      await populateStore(store, blobKey, [{ fromBlock: 0n, toBlock: 9999n, logs: [], fetchedAt, fetchedAtBlock }]);

      const strategy = vi.fn().mockReturnValue(0);
      const requestFn = createMockRequestFn({ latestBlock: 100_000n });

      await callHandler(requestFn, [{ fromBlock: "0x0", toBlock: "0x270f" }], strategy);

      expect(strategy).toHaveBeenCalledWith({
        confirmations: Number(fetchedAtBlock - 9999n),
        cacheAgeMs: expect.any(Number),
        totalChunks: 1,
      });
    });

    it("refetches the trailing partial bin when latest advances", async () => {
      const logs = [
        createMockLog(9999n),
        createMockLog(10000n),
        createMockLog(10005n),
        createMockLog(10006n),
        createMockLog(10007n),
      ];
      await populateStore(store, blobKey, [
        {
          fromBlock: 0n,
          toBlock: 9999n,
          logs: [logs[0]!],
          fetchedAtBlock: 10005n,
        },
        {
          fromBlock: 10000n,
          toBlock: 19999n,
          fetchedRange: { fromBlock: 10000n, toBlock: 10005n },
          logs: logs.slice(1, 3),
          fetchedAtBlock: 10005n,
        },
      ]);

      const requestFn = createMockRequestFn({
        latestBlock: 10007n,
        logGenerator: logsInRange(logs),
      });

      const result = await callHandler(requestFn, [{ fromBlock: toHex(9999n), toBlock: "latest" }], neverInvalidate);

      expect(blockNumbers(result)).toEqual([9999n, 10000n, 10005n, 10006n, 10007n]);
      expect(getLogsCalls(requestFn)).toHaveLength(1);
      expect(BigInt(getLogsCalls(requestFn)[0]![0].params[0].fromBlock)).toBe(10000n);
    });
  });

  describe("gap merging", () => {
    it("merges consecutive gaps into single fetch", async () => {
      await populateStore(store, blobKey, [
        { fromBlock: 0n, toBlock: 9999n, logs: [] },
        { fromBlock: 30000n, toBlock: 39999n, logs: [] },
      ]);

      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: () => [],
      });

      await callHandler(requestFn, [{ fromBlock: "0x0", toBlock: "0x9c3f" }], neverInvalidate);

      expect(getLogsCalls(requestFn)).toHaveLength(1);
      expect(BigInt(getLogsCalls(requestFn)[0]![0].params[0].fromBlock)).toBe(10000n);
      expect(BigInt(getLogsCalls(requestFn)[0]![0].params[0].toBlock)).toBe(29999n);
    });
  });

  describe("edge cases", () => {
    it("returns empty array when fromBlock > toBlock", async () => {
      const requestFn = createMockRequestFn({ latestBlock: 100_000n });

      const logs = await callHandler(requestFn, [{ fromBlock: "0x2710", toBlock: "0x0" }], neverInvalidate);

      expect(logs).toEqual([]);
    });

    it("throws on blockHash queries", async () => {
      const requestFn = createMockRequestFn({});

      await expect(callHandler(requestFn, [{ blockHash: `0x${"c".repeat(64)}` }], neverInvalidate)).rejects.toThrow(
        "blockHash",
      );
    });

    it("filters logs to requested range", async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: () => [createMockLog(0n), createMockLog(5000n), createMockLog(9999n)],
      });

      const logs = await callHandler(requestFn, [{ fromBlock: "0x1388", toBlock: "0x1770" }], neverInvalidate);

      expect(logs).toHaveLength(1);
      expect(logs[0]!.blockNumber).toBe(toHex(5000n));
    });

    it("returns logs when the requested range lies entirely inside the trailing partial bin", async () => {
      const logs = [createMockLog(10000n), createMockLog(10003n), createMockLog(10005n)];
      const requestFn = createMockRequestFn({
        latestBlock: 10005n,
        logGenerator: logsInRange(logs),
      });

      const result = await callHandler(requestFn, [{ fromBlock: toHex(10003n), toBlock: "latest" }], neverInvalidate);

      expect(blockNumbers(result)).toEqual([10003n, 10005n]);
    });

    it("sorts logs by block number", async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: () => [createMockLog(8000n), createMockLog(2000n), createMockLog(5000n)],
      });

      const logs = await callHandler(requestFn, [{ fromBlock: "0x0", toBlock: "0x270f" }], neverInvalidate);

      expect(blockNumbers(logs)).toEqual([2000n, 5000n, 8000n]);
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

      await callHandler(requestFn, [{ fromBlock: "0x0", toBlock: "0x270f" }, { reduce }], neverInvalidate);

      expect(observed).toEqual([2000n, 5000n, 8000n]);
    });

    it("isolates reducer failures across coalesced participants", async () => {
      const requestStarted = deferred<void>();
      const releaseRequest = deferred<void>();
      const requestFn = vi.fn().mockImplementation(async (args) => {
        if (args.method === "eth_blockNumber") return toHex(100_000n);
        if (args.method === "eth_getLogs") {
          requestStarted.resolve();
          await releaseRequest.promise;

          const filter = args.params?.[0];
          const additional = args.params?.[2];
          const fromBlock = BigInt(filter.fromBlock);
          const toBlock = BigInt(filter.toBlock);
          const logs = [createMockLog(fromBlock)];

          additional?.onLogsResponse?.({
            logs,
            fromBlock,
            toBlock,
            fetchedAtBlock: BigInt(additional.latestBlock),
            fetchedAt: Date.now(),
          });

          return additional?.onLogsResponseOnly ? undefined : logs;
        }
        throw new Error(`Unexpected method: ${args.method}`);
      });

      const badReduce = () => {
        throw new Error("boom");
      };
      const goodReduce = (acc: RpcLog[], log: RpcLog) => {
        acc.push(log);
        return acc;
      };

      const leader = callHandler(requestFn, [{ fromBlock: "0x0", toBlock: "0xa" }], neverInvalidate);
      await requestStarted.promise;
      const badFollower = callHandler(
        requestFn,
        [{ fromBlock: "0x0", toBlock: "0xa" }, { reduce: badReduce }],
        neverInvalidate,
      );
      const goodFollower = callHandler(
        requestFn,
        [{ fromBlock: "0x0", toBlock: "0xa" }, { reduce: goodReduce }],
        neverInvalidate,
      );
      releaseRequest.resolve();

      const results = await Promise.allSettled([leader, badFollower, goodFollower]);

      expect(results[0]).toMatchObject({ status: "fulfilled" });
      expect(results[1]).toMatchObject({ status: "rejected" });
      expect(results[2]).toMatchObject({ status: "fulfilled" });
      expect((results[0] as PromiseFulfilledResult<RpcLog[]>).value).toHaveLength(1);
      expect((results[1] as PromiseRejectedResult).reason.message).toContain("boom");
      expect((results[2] as PromiseFulfilledResult<RpcLog[]>).value).toHaveLength(1);
      expect(getLogsCalls(requestFn)).toHaveLength(1);
    });
  });

  describe("search", () => {
    it("applies search per coalesced participant", async () => {
      const requestStarted = deferred<void>();
      const releaseRequest = deferred<void>();

      const requestFn = vi.fn().mockImplementation(async (args) => {
        if (args.method === "eth_blockNumber") {
          return toHex(100_000n);
        }

        if (args.method === "eth_getLogs") {
          requestStarted.resolve();
          await releaseRequest.promise;

          const filter = args.params?.[0];
          const additional = args.params?.[2];
          const fromBlock = BigInt(filter.fromBlock);
          const toBlock = BigInt(filter.toBlock);
          const logs = [{ ...createMockLog(fromBlock), data: "0xdead" }];

          additional?.onLogsResponse?.({
            logs,
            fromBlock,
            toBlock,
            fetchedAtBlock: BigInt(additional.latestBlock),
            fetchedAt: Date.now(),
          });

          return additional?.onLogsResponseOnly ? undefined : logs;
        }

        throw new Error(`Unexpected method: ${args.method}`);
      });

      const leader = callHandler(
        requestFn,
        [{ fromBlock: "0x0", toBlock: "0xa" }, { search: "beef" }],
        neverInvalidate,
      );
      await requestStarted.promise;
      const follower = callHandler(requestFn, [{ fromBlock: "0x0", toBlock: "0xa" }], neverInvalidate);
      releaseRequest.resolve();

      const [leaderLogs, followerLogs] = await Promise.all([leader, follower]);

      expect(leaderLogs).toEqual([]);
      expect(followerLogs).toHaveLength(1);
      expect(getLogsCalls(requestFn)).toHaveLength(1);
    });

    it("applies search per coalesced participant in the reverse direction", async () => {
      const requestStarted = deferred<void>();
      const releaseRequest = deferred<void>();

      const requestFn = vi.fn().mockImplementation(async (args) => {
        if (args.method === "eth_blockNumber") return toHex(100_000n);
        if (args.method === "eth_getLogs") {
          requestStarted.resolve();
          await releaseRequest.promise;

          const filter = args.params?.[0];
          const additional = args.params?.[2];
          const fromBlock = BigInt(filter.fromBlock);
          const toBlock = BigInt(filter.toBlock);
          const logs = [{ ...createMockLog(fromBlock), data: "0xdead" }];

          additional?.onLogsResponse?.({
            logs,
            fromBlock,
            toBlock,
            fetchedAtBlock: BigInt(additional.latestBlock),
            fetchedAt: Date.now(),
          });

          return additional?.onLogsResponseOnly ? undefined : logs;
        }
        throw new Error(`Unexpected method: ${args.method}`);
      });

      const leader = callHandler(requestFn, [{ fromBlock: "0x0", toBlock: "0xa" }], neverInvalidate);
      await requestStarted.promise;
      const follower = callHandler(
        requestFn,
        [{ fromBlock: "0x0", toBlock: "0xa" }, { search: "beef" }],
        neverInvalidate,
      );
      releaseRequest.resolve();

      const [leaderLogs, followerLogs] = await Promise.all([leader, follower]);

      expect(leaderLogs).toHaveLength(1);
      expect(followerLogs).toEqual([]);
      expect(getLogsCalls(requestFn)).toHaveLength(1);
    });

    it("documents that search is bin-level, not log-level", async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 100_000n,
        logGenerator: () => [
          { ...createMockLog(0n), data: "0xdead" },
          { ...createMockLog(1n), data: "0xbeef" },
        ],
      });

      const logs = await callHandler(
        requestFn,
        [{ fromBlock: "0x0", toBlock: "0x1" }, { search: "dead" }],
        neverInvalidate,
      );

      expect(logs).toHaveLength(2);
      expect(blockNumbers(logs)).toEqual([0n, 1n]);
    });

    it("rejects only the follower when its search regex is invalid", async () => {
      const requestStarted = deferred<void>();
      const releaseRequest = deferred<void>();

      const requestFn = vi.fn().mockImplementation(async (args) => {
        if (args.method === "eth_blockNumber") return toHex(100_000n);
        if (args.method === "eth_getLogs") {
          requestStarted.resolve();
          await releaseRequest.promise;

          const filter = args.params?.[0];
          const additional = args.params?.[2];
          const fromBlock = BigInt(filter.fromBlock);
          const toBlock = BigInt(filter.toBlock);
          const logs = [createMockLog(fromBlock)];

          additional?.onLogsResponse?.({
            logs,
            fromBlock,
            toBlock,
            fetchedAtBlock: BigInt(additional.latestBlock),
            fetchedAt: Date.now(),
          });

          return additional?.onLogsResponseOnly ? undefined : logs;
        }
        throw new Error(`Unexpected method: ${args.method}`);
      });

      const leader = callHandler(requestFn, [{ fromBlock: "0x0", toBlock: "0xa" }], neverInvalidate);
      await requestStarted.promise;
      const follower = callHandler(requestFn, [{ fromBlock: "0x0", toBlock: "0xa" }, { search: "(" }], neverInvalidate);
      releaseRequest.resolve();

      const results = await Promise.allSettled([leader, follower]);

      expect(results[0]).toMatchObject({ status: "fulfilled" });
      expect(results[1]).toMatchObject({ status: "rejected" });
      expect((results[0] as PromiseFulfilledResult<RpcLog[]>).value).toHaveLength(1);
      expect((results[1] as PromiseRejectedResult).reason.message).toContain("Invalid regular expression");
      expect(getLogsCalls(requestFn)).toHaveLength(1);
    });
  });

  describe("error handling", () => {
    it("flushes partial data to store on fetch error", async () => {
      await populateStore(store, blobKey, [{ fromBlock: 10000n, toBlock: 19999n, logs: [] }]);

      let callCount = 0;
      const requestFn = vi.fn().mockImplementation(async (args) => {
        if (args.method === "eth_blockNumber") return toHex(100_000n);
        if (args.method === "eth_getLogs") {
          callCount++;
          const filter = args.params?.[0];
          const additional = args.params?.[2];
          const fromBlock = BigInt(filter.fromBlock);
          const toBlock = BigInt(filter.toBlock);

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

      await expect(callHandler(requestFn, [{ fromBlock: "0x0", toBlock: "0x752f" }], neverInvalidate)).rejects.toThrow(
        "Gap fetch failed",
      );

      expect(store.get(blobKey)).not.toBeNull();
    });
  });
});
