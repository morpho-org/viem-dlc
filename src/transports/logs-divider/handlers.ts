import { type EIP1193RequestFn, hexToBigInt, type RpcLog, toHex } from "viem";

import type { Observability } from "../../observability.js";
import type { BlockRange, EthGetLogsHashlessFilter, RpcSignature } from "../../types.js";
import { augment } from "../../utils/arrays.js";
import {
  classifyBlockRangeError,
  divideBlockRange,
  halveBlockRange,
  isInBlockRange,
  resolveBlockNumber,
} from "../../utils/blocks.js";
import { min } from "../../utils/math.js";
import type { RateLimiterSchema } from "../rate-limiter/schema.js";

import { type LogsDividerSchema, logsDividerTransportKey } from "./schema.js";
import type { LogsDividerConfig, OnLogsResponse } from "./types.js";

/** Internal context passed through the processing pipeline */
interface ProcessContext {
  requestFn: EIP1193RequestFn<RateLimiterSchema>;
  onLogsResponse?: OnLogsResponse;
  onLogsResponseOnly?: boolean;
  baseFilter: EthGetLogsHashlessFilter;
  latestBlockNumber: bigint;
  observability: Observability & {
    logsFetched: number;
    /** Accumulator for halving stats, surfaced as fields on the terminal wide event. */
    splits: {
      count: number;
      causes: { range: number; timeout: number };
      maxDepth: number;
    };
    /** Counts of leaf `requestFn` durations (success or failure), keyed by 100ms-bin lower bound in ms. */
    fetchDurationsMs: Record<number, number>;
  };
}

/** Fetches logs for a single range with automatic retry and range halving on range-related failure. */
async function fetchRangeWithRetry(
  ctx: ProcessContext,
  range: BlockRange,
  priority?: number,
  timeoutSplitsRemaining = 1,
  depth = 0,
): Promise<RpcLog[]> {
  if (depth > ctx.observability.splits.maxDepth) ctx.observability.splits.maxDepth = depth;

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
    let logs: RpcLog[];
    const t0 = performance.now();
    try {
      logs = await ctx.requestFn(
        {
          method: "eth_getLogs",
          params: [filter, { __rateLimiter: true, priority }],
        },
        // `retryCount: 0` so that we fail fast on block range errors
        { retryCount: 0 },
      );
    } finally {
      const bin = Math.floor((performance.now() - t0) / 100) * 100;
      ctx.observability.fetchDurationsMs[bin] = (ctx.observability.fetchDurationsMs[bin] ?? 0) + 1;
    }

    // Success - invoke callback
    ctx.onLogsResponse?.({
      logs,
      fromBlock: constrainedRange.fromBlock,
      toBlock: constrainedRange.toBlock,
      fetchedAtBlock: ctx.latestBlockNumber,
      fetchedAt: Date.now(),
    });
    ctx.observability.logsFetched += logs.length;

    return ctx.onLogsResponseOnly ? [] : logs;
  } catch (error) {
    const cause = classifyBlockRangeError(error);
    if (cause === "range" || (cause === "timeout" && timeoutSplitsRemaining > 0)) {
      // Use constrainedRange to avoid halving into invalid ranges
      const halves = halveBlockRange(constrainedRange);

      if (halves) {
        const nextBudget = cause === "timeout" ? timeoutSplitsRemaining - 1 : timeoutSplitsRemaining;
        ctx.observability.splits.count += 1;
        ctx.observability.splits.causes[cause] += 1;

        const logs = await Promise.all(
          halves.map((half) => fetchRangeWithRetry(ctx, half, priority, nextBudget, depth + 1)),
        );
        return ctx.onLogsResponseOnly ? [] : logs.flat();
      }
    }

    ctx.observability.logger
      ?.withMetadata({ from_block: Number(range.fromBlock), to_block: Number(range.toBlock) })
      .withError(error)
      .error("fetch failed");
    throw error;
  }
}

/**
 * Main handler for eth_getLogs requests.
 * Divides large ranges, assigns chunk priorities, and handles retries.
 */
export async function handleEthGetLogs(
  requestFn: EIP1193RequestFn<RateLimiterSchema>,
  [filter, ...params]: RpcSignature<LogsDividerSchema, "eth_getLogs">["Parameters"],
  config: LogsDividerConfig,
  observability: Observability = {},
): Promise<RpcLog[]> {
  // blockHash queries cannot be divided - pass through
  if (filter.blockHash) {
    return requestFn({ method: "eth_getLogs", params: params[0] ? [filter, params[0]] : [filter] });
  }

  // Get extra params
  const priority = params[0]?.priority ?? 0;
  const latestBlockNumber = hexToBigInt(params[1]?.latestBlock ?? (await requestFn({ method: "eth_blockNumber" })));

  // Resolve block tags to numbers
  const fromBlock = resolveBlockNumber(filter.fromBlock ?? "earliest", latestBlockNumber);
  const toBlock = min(resolveBlockNumber(filter.toBlock ?? "latest", latestBlockNumber), latestBlockNumber);

  if (fromBlock > toBlock) {
    return [];
  }

  const ctx: ProcessContext = {
    requestFn,
    onLogsResponse: params[1]?.onLogsResponse,
    onLogsResponseOnly: params[1]?.onLogsResponseOnly,
    baseFilter: filter,
    latestBlockNumber,
    observability: {
      ...observability,
      logsFetched: 0,
      splits: { count: 0, causes: { range: 0, timeout: 0 }, maxDepth: 0 },
      fetchDurationsMs: {},
    },
  };

  const range: BlockRange = { fromBlock, toBlock };
  const chunks = divideBlockRange(range, config.maxBlockRange, config.alignTo);

  const tag = `${logsDividerTransportKey}.${ctx.observability.counter}`;
  ctx.observability.logger?.withContext({
    [`${tag}.from_block`]: Number(fromBlock),
    [`${tag}.to_block`]: Number(toBlock),
    [`${tag}.latest_block`]: Number(latestBlockNumber),
    [`${tag}.nominal_ranges`]: chunks.length,
  });

  try {
    const logs = await augment(chunks).mapAsync(
      async (chunk, i) => {
        // Take chunks to be [A, B, ..., Z] -- if we make requests without specifying priority, the queue
        // is FIFO, so *retries* for chunk A are queued after the *initial* request for chunk Z. This isn't
        // a problem here, since we need to fetch all ranges anyway, but it can produce unexpected
        // mental-model-overhead for `onLogsResponse` consumers. By using the chunk index as the priority,
        // we ensure that *if we're rate/concurrency limited*, chunks are processed roughly in order.
        const result = await fetchRangeWithRetry(ctx, chunk, priority + i / chunks.length);
        // Filter out logs outside original range (in case alignment extended the range).
        // We do this per-chunk to avoid creating an extra copy of the final flattened array, which could be large.
        return result.filter(isInBlockRange(range));
      },
      // NOTE: Defensive upper bound to avoid flooding EventLoop. Request concurrency is managed by `rateLimiter`.
      { maxConcurrent: 1000 },
    );
    return logs.flat();
  } finally {
    ctx.observability.logger?.withContext({
      [`${tag}.logs_fetched`]: ctx.observability.logsFetched,
      [`${tag}.splits`]: ctx.observability.splits,
      [`${tag}.max_depth`]: ctx.observability.splits.maxDepth,
      [`${tag}.fetch_durations_ms`]: ctx.observability.fetchDurationsMs,
    });
  }
}
