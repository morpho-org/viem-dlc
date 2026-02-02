import { type Hex, hexToBigInt, type RpcLog, toHex } from "viem";

import type { BlockRange, EIP1193PublicRequestFn, EthGetLogsParams } from "../../types.js";
import {
  divideBlockRange,
  halveBlockRange,
  isErrorCausedByBlockRange,
  isInBlockRange,
  resolveBlockNumber,
} from "../../utils/blocks.js";
import { min } from "../../utils/math.js";

import type { LogsDividerConfig, OnLogsResponse } from "./types.js";

/** Internal context passed through the processing pipeline */
interface ProcessContext {
  requestFn: EIP1193PublicRequestFn;
  onLogsResponse?: OnLogsResponse;
  baseFilter: Omit<EthGetLogsParams, "blockHash">;
  maxConcurrentChunks: number;
  latestBlockNumber: bigint;
}

/** Fetches logs for a single range with automatic retry and range halving on range-related failure. */
async function fetchRangeWithRetry(ctx: ProcessContext, range: BlockRange): Promise<RpcLog[]> {
  // Constrain toBlock to chain tip (range may span past it due to alignment)
  const constrainedRange: BlockRange = {
    fromBlock: range.fromBlock,
    toBlock: min(range.toBlock, ctx.latestBlockNumber),
  };

  // This happens when alignTo > maxBlockRange and alignment extends past latestBlockNumber.
  if (constrainedRange.fromBlock > constrainedRange.toBlock) {
    return [];
  }

  const filter = {
    ...ctx.baseFilter,
    fromBlock: toHex(constrainedRange.fromBlock),
    toBlock: toHex(constrainedRange.toBlock),
  };

  try {
    const logs = await ctx.requestFn(
      { method: "eth_getLogs", params: [filter] },
      // `retryCount: 0` so that we fail fast on block range errors
      { dedupe: true, retryCount: 0 },
    );

    // Success - invoke callback
    ctx.onLogsResponse?.({
      logs,
      filter,
      fromBlock: range.fromBlock,
      toBlock: range.toBlock,
      fetchedAtBlock: ctx.latestBlockNumber,
      fetchedAt: Date.now(),
    });

    return logs;
  } catch (error) {
    if (isErrorCausedByBlockRange(error)) {
      // Use constrainedRange to avoid halving into invalid ranges
      const halves = halveBlockRange(constrainedRange);

      if (halves) {
        // Recursively fetch both halves
        const logs = await Promise.all(halves.map((half) => fetchRangeWithRetry(ctx, half)));
        return logs.flat();
      }
    }

    // Add range context to non-range errors for easier debugging
    const rangeContext = `[fetchRangeWithRetry [${range.fromBlock}n, ${range.toBlock}n]]`;
    if (error instanceof Error) {
      error.message = `${rangeContext} ${error.message}`;
      throw error;
    }
    throw new Error(`${rangeContext} ${String(error)}`);
  }
}

/**
 * Processes multiple ranges with concurrency limit.
 * Uses a worker pool pattern to limit parallel requests.
 * Maintains result order despite concurrent execution.
 *
 * @dev This is useful even when composed with the `rateLimiter` transport.
 * Take ranges to be [A, B, ..., Z] -- if we request all in parallel, then the
 * *retries* for range A are queued after the *initial* requests for range Z.
 * This isn't a problem for `logsDivider`, since we need to fetch all ranges
 * anyway, but it can produce unexpected mental-model-overhead for `onLogsResponse`
 * consumers. Applying a concurrency limit ensures outer ranges are fully processed
 * roughly in order, even while inner ranges/retries fight for priority.
 */
async function processRangesWithConcurrency(ctx: ProcessContext, ranges: BlockRange[]): Promise<RpcLog[]> {
  if (ranges.length === 0) return [];

  const results: RpcLog[][] = new Array(ranges.length);
  let cursor = 0;

  async function worker() {
    while (cursor < ranges.length) {
      const index = cursor++;
      results[index] = await fetchRangeWithRetry(ctx, ranges[index]!);
    }
  }

  const workerCount = Math.min(ctx.maxConcurrentChunks, ranges.length);
  await Promise.all(Array.from({ length: workerCount }, worker));

  return results.flat();
}

/**
 * Main handler for eth_getLogs requests.
 * Divides large ranges, manages rate limiting, and handles retries.
 */
export async function handleGetLogs(
  requestFn: EIP1193PublicRequestFn,
  filter: EthGetLogsParams & { latestBlock?: Hex },
  config: Required<Omit<LogsDividerConfig, "alignTo">> & Pick<LogsDividerConfig, "alignTo">,
): Promise<RpcLog[]> {
  const maybeLatestBlockNumber = filter.latestBlock;
  delete filter.latestBlock; // make sure our extra param isn't passed to upstream RPCs

  // blockHash queries cannot be divided - pass through
  if (filter.blockHash) {
    return requestFn({ method: "eth_getLogs", params: [filter] }, { dedupe: true });
  }

  const latestBlockNumber = hexToBigInt(
    maybeLatestBlockNumber ?? (await requestFn({ method: "eth_blockNumber" }, { dedupe: true })),
  );

  // Resolve block tags to numbers
  const fromBlock = resolveBlockNumber(filter.fromBlock ?? "earliest", latestBlockNumber);
  const toBlock = min(resolveBlockNumber(filter.toBlock ?? "latest", latestBlockNumber), latestBlockNumber);

  if (fromBlock > toBlock) {
    return [];
  }

  const ctx: ProcessContext = {
    requestFn,
    onLogsResponse: config.onLogsResponse,
    baseFilter: filter,
    maxConcurrentChunks: config.maxConcurrentChunks,
    latestBlockNumber,
  };

  const range: BlockRange = { fromBlock, toBlock };

  // Divide into chunks and process with concurrency
  const ranges = divideBlockRange(range, config.maxBlockRange, config.alignTo);
  const logs = await processRangesWithConcurrency(ctx, ranges);

  // Filter out logs outside original range (if alignment extended the range)
  return logs.filter(isInBlockRange(range));
}
