import { type Hex, type LogTopic, type RpcLog, toHex } from "viem";
import { describe, expect, it, vi } from "vitest";

import { handleGetLogs } from "../../src/transports/logs-divider/handlers.js";
import type { LogsResponse } from "../../src/transports/logs-divider/types.js";
import { estimateUtf8Bytes } from "../../src/utils/json.js";
import { sleep } from "../../src/utils/sleep.js";

function createMockLog(blockNumber: bigint, logIndex = 0): RpcLog {
  return {
    address: "0x1234567890123456789012345678901234567890",
    topics: ["0xabc"],
    data: "0x",
    blockNumber: toHex(blockNumber),
    transactionHash: `0x${"a".repeat(64)}`,
    transactionIndex: "0x0",
    blockHash: `0x${"b".repeat(64)}`,
    logIndex: toHex(logIndex),
    removed: false,
  };
}

function createRangeError(code: number, message: string): Error {
  return Object.assign(new Error(message), { code });
}

function createMockRequestFn(options: {
  latestBlock?: bigint;
  logsPerRequest?: number;
  logGenerator?: (fromBlock: bigint, toBlock: bigint) => RpcLog[];
  failOnRange?: (fromBlock: bigint, toBlock: bigint) => Error | null;
}) {
  const { latestBlock = 1000n, logsPerRequest = 1, logGenerator, failOnRange } = options;

  return vi.fn().mockImplementation(async (args: { method: string; params?: any[] }) => {
    if (args.method === "eth_blockNumber") {
      return toHex(latestBlock);
    }

    if (args.method === "eth_getLogs") {
      const filter = args.params?.[0];

      // Handle blockHash queries (no fromBlock/toBlock)
      if (filter.blockHash) {
        const logs: RpcLog[] = [];
        for (let i = 0; i < logsPerRequest; i++) {
          logs.push(createMockLog(0n, i));
        }
        return logs;
      }

      const fromBlock = BigInt(filter.fromBlock);
      const toBlock = BigInt(filter.toBlock);

      // Check if we should fail for this range
      if (failOnRange) {
        const error = failOnRange(fromBlock, toBlock);
        if (error) throw error;
      }

      // Generate logs
      if (logGenerator) {
        return logGenerator(fromBlock, toBlock);
      }

      // Default: return logsPerRequest logs at fromBlock
      const logs: RpcLog[] = [];
      for (let i = 0; i < logsPerRequest; i++) {
        logs.push(createMockLog(fromBlock, i));
      }
      return logs;
    }

    throw new Error(`Unexpected method: ${args.method}`);
  });
}

const defaultConfig = {
  maxBlockRange: 100,
  maxConcurrentChunks: 5,
};

describe("handleGetLogs", () => {
  describe("basic functionality", () => {
    it("returns logs for a simple request within maxBlockRange", async () => {
      const requestFn = createMockRequestFn({ logsPerRequest: 2 });

      const logs = await handleGetLogs(requestFn, [{ fromBlock: "0x0", toBlock: "0x50" }], defaultConfig);

      expect(logs).toHaveLength(2);
      expect(requestFn).toHaveBeenCalledTimes(2); // eth_blockNumber + eth_getLogs
    });

    it("passes through blockHash queries unchanged", async () => {
      const requestFn = createMockRequestFn({ logsPerRequest: 1 });
      const blockHash: Hex = `0x${"a".repeat(64)}`;

      const logs = await handleGetLogs(requestFn, [{ blockHash }], defaultConfig);

      expect(logs).toHaveLength(1);
      // Should not call eth_blockNumber for blockHash queries
      expect(requestFn).toHaveBeenCalledTimes(1);
      expect(requestFn).toHaveBeenCalledWith({ method: "eth_getLogs", params: [{ blockHash }] }, { dedupe: true });
    });

    it("returns empty array when fromBlock > toBlock", async () => {
      const requestFn = createMockRequestFn({});

      const logs = await handleGetLogs(requestFn, [{ fromBlock: "0x100", toBlock: "0x50" }], defaultConfig);

      expect(logs).toEqual([]);
      // Should only call eth_blockNumber
      expect(requestFn).toHaveBeenCalledTimes(1);
    });

    it('resolves "latest" block tag correctly', async () => {
      const requestFn = createMockRequestFn({ latestBlock: 500n, logsPerRequest: 1 });

      await handleGetLogs(requestFn, [{ fromBlock: "0x0", toBlock: "latest" }], {
        ...defaultConfig,
        maxBlockRange: 1000,
      });

      // Should fetch from 0 to 500 (the latest block)
      const getLogsCall = requestFn.mock.calls.find((call: any[]) => call[0].method === "eth_getLogs");
      expect(getLogsCall?.[0].params[0].toBlock).toBe(toHex(500n));
    });

    it('resolves "earliest" block tag to 0', async () => {
      const requestFn = createMockRequestFn({ latestBlock: 500n, logsPerRequest: 1 });

      await handleGetLogs(requestFn, [{ fromBlock: "earliest", toBlock: "0x50" }], defaultConfig);

      const getLogsCall = requestFn.mock.calls.find((call: any[]) => call[0].method === "eth_getLogs");
      expect(getLogsCall?.[0].params[0].fromBlock).toBe(toHex(0n));
    });

    it("filters out logs above maxLogBytes and keeps logs exactly at the limit", async () => {
      const smallLog = createMockLog(0n, 0);
      const largeLog = { ...createMockLog(0n, 1), data: `0x${"a".repeat(1_000)}` as Hex };
      const maxLogBytes = estimateUtf8Bytes(smallLog);
      const requestFn = createMockRequestFn({ logGenerator: () => [smallLog, largeLog] });

      const logs = await handleGetLogs(requestFn, [{ fromBlock: "0x0", toBlock: "0x50" }], {
        ...defaultConfig,
        maxLogBytes,
      });

      expect(logs).toEqual([smallLog]);
      expect(estimateUtf8Bytes(largeLog)).toBeGreaterThan(maxLogBytes);
    });
  });

  describe("range division", () => {
    it("divides large ranges into chunks", async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 300n,
        logGenerator: (from) => [createMockLog(from)],
      });

      const logs = await handleGetLogs(
        requestFn,
        [{ fromBlock: "0x0", toBlock: "0x12b" }], // 0 to 299
        { ...defaultConfig, maxBlockRange: 100 },
      );

      // Should have 3 chunks: 0-99, 100-199, 200-299
      const getLogsCalls = requestFn.mock.calls.filter((call: any[]) => call[0].method === "eth_getLogs");
      expect(getLogsCalls).toHaveLength(3);
      expect(logs).toHaveLength(3); // One log per chunk
    });

    it("maintains result order across chunks", async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 300n,
        logGenerator: (from) => [createMockLog(from)],
      });

      const logs = await handleGetLogs(requestFn, [{ fromBlock: "0x0", toBlock: "0x12b" }], {
        ...defaultConfig,
        maxBlockRange: 100,
      });

      // Logs should be in block order
      const blockNumbers = logs.map((log) => BigInt(log.blockNumber!));
      expect(blockNumbers).toEqual([0n, 100n, 200n]);
    });

    it("preserves address and topics in divided requests", async () => {
      const requestFn = createMockRequestFn({ latestBlock: 200n });
      const address = "0x1234567890123456789012345678901234567890";
      const topics: LogTopic[] = [["0xabc"], null, ["0xdef"]];

      await handleGetLogs(
        requestFn,
        [{ fromBlock: "0x0", toBlock: "0xc7", address, topics }], // 0 to 199
        { ...defaultConfig, maxBlockRange: 100 },
      );

      const getLogsCalls = requestFn.mock.calls.filter((call: any[]) => call[0].method === "eth_getLogs");

      for (const call of getLogsCalls) {
        expect(call[0].params[0].address).toBe(address);
        expect(call[0].params[0].topics).toEqual(topics);
      }
    });
  });

  describe("concurrency", () => {
    it("respects maxConcurrentChunks limit", async () => {
      let maxConcurrent = 0;
      let currentConcurrent = 0;

      const requestFn = vi.fn().mockImplementation(async (args: any) => {
        if (args.method === "eth_blockNumber") {
          return toHex(500n);
        }

        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);

        // Simulate async work
        await sleep(10);

        currentConcurrent--;
        return [createMockLog(0n)];
      });

      await handleGetLogs(
        requestFn,
        [{ fromBlock: "0x0", toBlock: "0x1f3" }], // 0 to 499 = 5 chunks
        { ...defaultConfig, maxBlockRange: 100, maxConcurrentChunks: 2 },
      );

      expect(maxConcurrent).toBeLessThanOrEqual(2);
    });

    it("processes all chunks even with concurrency limit", async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 500n,
        logGenerator: (from) => [createMockLog(from)],
      });

      const logs = await handleGetLogs(
        requestFn,
        [{ fromBlock: "0x0", toBlock: "0x1f3" }], // 5 chunks
        { ...defaultConfig, maxBlockRange: 100, maxConcurrentChunks: 2 },
      );

      expect(logs).toHaveLength(5);
    });
  });

  describe("error handling and retry", () => {
    it("retries with halved range on block range error", async () => {
      let attempts = 0;
      const requestFn = vi.fn().mockImplementation(async (args: any) => {
        if (args.method === "eth_blockNumber") {
          return toHex(100n);
        }

        attempts++;
        const filter = args.params?.[0];
        const rangeSize = BigInt(filter.toBlock) - BigInt(filter.fromBlock) + 1n;

        // Fail if range > 50
        if (rangeSize > 50n) {
          throw createRangeError(-32005, "query returned more than 10000 results");
        }

        return [createMockLog(BigInt(filter.fromBlock))];
      });

      const logs = await handleGetLogs(
        requestFn,
        [{ fromBlock: "0x0", toBlock: "0x63" }], // 0 to 99 = 100 blocks
        { ...defaultConfig, maxBlockRange: 100 },
      );

      // Should have retried: first try fails, then two halves succeed
      expect(attempts).toBe(3);
      expect(logs).toHaveLength(2);
    });

    it("continues halving until success or single block", async () => {
      const requestFn = vi.fn().mockImplementation(async (args: any) => {
        if (args.method === "eth_blockNumber") {
          return toHex(10n);
        }

        const filter = args.params?.[0];
        const rangeSize = BigInt(filter.toBlock) - BigInt(filter.fromBlock) + 1n;

        // Fail if range > 2
        if (rangeSize > 2n) {
          throw createRangeError(-32000, "block range too large");
        }

        return [createMockLog(BigInt(filter.fromBlock))];
      });

      const logs = await handleGetLogs(
        requestFn,
        [{ fromBlock: "0x0", toBlock: "0x7" }], // 0 to 7 = 8 blocks
        { ...defaultConfig, maxBlockRange: 10 },
      );

      // Should halve multiple times: 8 -> 4,4 -> 2,2,2,2
      expect(logs.length).toBeGreaterThan(0);
    });

    it("propagates non-range errors immediately", async () => {
      const requestFn = vi.fn().mockImplementation(async (args: any) => {
        if (args.method === "eth_blockNumber") {
          return toHex(100n);
        }
        throw new Error("Network error");
      });

      await expect(handleGetLogs(requestFn, [{ fromBlock: "0x0", toBlock: "0x50" }], defaultConfig)).rejects.toThrow(
        "Network error",
      );
    });

    it("propagates error when single block still fails", async () => {
      const requestFn = vi.fn().mockImplementation(async (args: any) => {
        if (args.method === "eth_blockNumber") {
          return toHex(10n);
        }
        // Always fail with range error
        throw createRangeError(-32000, "block range error");
      });

      await expect(handleGetLogs(requestFn, [{ fromBlock: "0x0", toBlock: "0x0" }], defaultConfig)).rejects.toThrow(
        "block range error",
      );
    });
  });

  describe("logs response callback", () => {
    it("calls onLogsResponse for each successful sub-request", async () => {
      const logsResponses: LogsResponse[] = [];
      const requestFn = createMockRequestFn({
        latestBlock: 200n,
        logGenerator: (from) => [createMockLog(from)],
      });

      await handleGetLogs(
        requestFn,
        [
          { fromBlock: "0x0", toBlock: "0xc7" }, // 2 chunks
          { onLogsResponse: (response) => logsResponses.push(response) },
        ],
        {
          ...defaultConfig,
          maxBlockRange: 100,
        },
      );

      expect(logsResponses).toHaveLength(2);
      expect(logsResponses[0]!.fromBlock).toBe(0n);
      expect(logsResponses[0]!.toBlock).toBe(99n);
      expect(logsResponses[1]!.fromBlock).toBe(100n);
      expect(logsResponses[1]!.toBlock).toBe(199n);
    });

    it("passes filtered logs to onLogsResponse", async () => {
      const smallLog = createMockLog(0n, 0);
      const largeLog = { ...createMockLog(0n, 1), data: `0x${"b".repeat(1_000)}` as Hex };
      const logsResponses: LogsResponse[] = [];
      const requestFn = createMockRequestFn({ logGenerator: () => [smallLog, largeLog] });

      await handleGetLogs(requestFn, [{ fromBlock: "0x0", toBlock: "0x50" }, { onLogsResponse: (response) => logsResponses.push(response) }], {
        ...defaultConfig,
        maxLogBytes: estimateUtf8Bytes(smallLog),
      });

      expect(logsResponses).toHaveLength(1);
      expect(logsResponses[0]!.logs).toEqual([smallLog]);
    });

    it("calls callback for retried (halved) requests too", async () => {
      const logsResponses: LogsResponse[] = [];
      let firstAttempt = true;

      const requestFn = vi.fn().mockImplementation(async (args: any) => {
        if (args.method === "eth_blockNumber") {
          return toHex(100n);
        }

        const filter = args.params?.[0];
        const rangeSize = BigInt(filter.toBlock) - BigInt(filter.fromBlock) + 1n;

        // Fail first attempt only
        if (firstAttempt && rangeSize > 50n) {
          firstAttempt = false;
          throw createRangeError(-32005, "range too large");
        }

        return [createMockLog(BigInt(filter.fromBlock))];
      });

      await handleGetLogs(
        requestFn,
        [
          { fromBlock: "0x0", toBlock: "0x63" }, // 100 blocks -> halved to 2x50
          { onLogsResponse: (response) => logsResponses.push(response) },
        ],
        {
          ...defaultConfig,
          maxBlockRange: 100,
        },
      );

      // Should have 2 callbacks (one for each half after retry)
      expect(logsResponses).toHaveLength(2);
    });
  });

  describe("edge cases", () => {
    it("handles empty logs response", async () => {
      const requestFn = createMockRequestFn({
        logGenerator: () => [],
      });

      const logs = await handleGetLogs(requestFn, [{ fromBlock: "0x0", toBlock: "0x50" }], defaultConfig);

      expect(logs).toEqual([]);
    });

    it("handles single block range", async () => {
      const requestFn = createMockRequestFn({ logsPerRequest: 1 });

      const logs = await handleGetLogs(
        requestFn,
        [{ fromBlock: "0x64", toBlock: "0x64" }], // Just block 100
        defaultConfig,
      );

      expect(logs).toHaveLength(1);
    });

    it("handles range exactly equal to maxBlockRange", async () => {
      const requestFn = createMockRequestFn({
        logGenerator: (from) => [createMockLog(from)],
      });

      const logs = await handleGetLogs(
        requestFn,
        [{ fromBlock: "0x0", toBlock: "0x63" }], // Exactly 100 blocks
        { ...defaultConfig, maxBlockRange: 100 },
      );

      // Should be a single request, not divided
      const getLogsCalls = requestFn.mock.calls.filter((call: any[]) => call[0].method === "eth_getLogs");
      expect(getLogsCalls).toHaveLength(1);
      expect(logs).toHaveLength(1);
    });

    it("handles very large block ranges", async () => {
      const requestFn = createMockRequestFn({
        latestBlock: 1000000n,
        logGenerator: () => [],
      });

      const logs = await handleGetLogs(
        requestFn,
        [{ fromBlock: "0x0", toBlock: "0xf423f" }], // 0 to 999999
        { ...defaultConfig, maxBlockRange: 100000 },
      );

      // Should make 10 requests
      const getLogsCalls = requestFn.mock.calls.filter((call: any[]) => call[0].method === "eth_getLogs");
      expect(getLogsCalls).toHaveLength(10);
      expect(logs).toEqual([]);
    });
  });
});
