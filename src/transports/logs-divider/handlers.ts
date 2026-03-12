import { type EIP1193RequestFn, hexToBigInt, type RpcLog, toHex } from "viem";

import type { BlockRange, EthGetLogsHashlessFilter, RpcSignature } from "../../types.js";
import {
  divideBlockRange,
  halveBlockRange,
  isErrorCausedByBlockRange,
  isInBlockRange,
  resolveBlockNumber,
} from "../../utils/blocks.js";
import { min } from "../../utils/math.js";
import type { RateLimiterSchema } from "../rate-limiter/schema.js";

import type { LogsDividerRpcSchema } from "./schema.js";
import type { LogsDividerConfig, OnLogsResponse } from "./types.js";

/** Internal context passed through the processing pipeline */
interface ProcessContext {
  requestFn: EIP1193RequestFn<RateLimiterSchema>;
  onLogsResponse?: OnLogsResponse;
  baseFilter: EthGetLogsHashlessFilter;
  latestBlockNumber: bigint;
}

/** Fetches logs for a single range with automatic retry and range halving on range-related failure. */
async function fetchRangeWithRetry(ctx: ProcessContext, range: BlockRange, priority?: number): Promise<RpcLog[]> {
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
      {
        method: "eth_getLogs",
        params: [filter, { __rateLimiter: true, priority }],
      },
      // `retryCount: 0` so that we fail fast on block range errors
      { dedupe: true, retryCount: 0 },
    );

    // Success - invoke callback
    ctx.onLogsResponse?.({
      logs,
      fromBlock: constrainedRange.fromBlock,
      toBlock: constrainedRange.toBlock,
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
        const logs = await Promise.all(halves.map((half) => fetchRangeWithRetry(ctx, half, priority)));
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
 * Main handler for eth_getLogs requests.
 * Divides large ranges, assigns chunk priorities, and handles retries.
 */
export async function handleGetLogs(
  requestFn: EIP1193RequestFn<RateLimiterSchema>,
  [filter, ...params]: RpcSignature<LogsDividerRpcSchema, "eth_getLogs">["Parameters"],
  config: LogsDividerConfig,
): Promise<RpcLog[]> {
  // blockHash queries cannot be divided - pass through
  if (filter.blockHash) {
    return requestFn({ method: "eth_getLogs", params: params[0] ? [filter, params[0]] : [filter] }, { dedupe: true });
  }

  // Get extra params
  const priority = params[0]?.priority ?? 0;
  const latestBlockNumber = hexToBigInt(
    params[1]?.latestBlock ?? (await requestFn({ method: "eth_blockNumber" }, { dedupe: true })),
  );

  // Resolve block tags to numbers
  const fromBlock = resolveBlockNumber(filter.fromBlock ?? "earliest", latestBlockNumber);
  const toBlock = min(resolveBlockNumber(filter.toBlock ?? "latest", latestBlockNumber), latestBlockNumber);

  if (fromBlock > toBlock) {
    return [];
  }

  const ctx: ProcessContext = {
    requestFn,
    onLogsResponse: params[1]?.onLogsResponse,
    baseFilter: filter,
    latestBlockNumber,
  };

  const range: BlockRange = { fromBlock, toBlock };
  const chunks = divideBlockRange(range, config.maxBlockRange, config.alignTo);
  const logs = await Promise.all(
    chunks.map(async (chunk, i) => {
      // Take chunks to be [A, B, ..., Z] -- if we make requests without specifying priority, the queue
      // is FIFO, so *retries* for chunk A are queued after the *initial* request for chunk Z. This isn't
      // a problem here, since we need to fetch all ranges anyway, but it can produce unexpected
      // mental-model-overhead for `onLogsResponse` consumers. By using the chunk index as the priority,
      // we ensure that *if we're rate/concurrency limited*, chunks are processed roughly in order.
      const result = await fetchRangeWithRetry(ctx, chunk, priority + i / chunks.length);
      // Filter out logs outside original range (in case alignment extended the range).
      // We do this per-chunk to avoid creating an extra copy of the final flattened array, which could be large.
      return result.filter(isInBlockRange(range));
    }),
  );

  return logs.flat();
}
