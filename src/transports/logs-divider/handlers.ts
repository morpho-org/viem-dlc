import { type EIP1193RequestFn, hexToBigInt, type RpcLog, toHex } from "viem";

import { type Facet, type FacetId, getObservability } from "../../observability.js";
import type { BlockRange, EthGetLogsHashlessFilter, RpcSignature } from "../../types.js";
import { augment } from "../../utils/arrays.js";
import {
  classifyBlockRangeError,
  divideBlockRange,
  halveBlockRange,
  isInBlockRange,
  resolveBlockNumber,
} from "../../utils/blocks.js";
import { serializeError } from "../../utils/errors.js";
import { min } from "../../utils/math.js";
import type { RateLimiterSchema } from "../rate-limiter/schema.js";

import type { LogsDividerSchema } from "./schema.js";
import type { LogsDividerConfig, OnLogsResponse } from "./types.js";

/** Internal context passed through the processing pipeline */
interface ProcessContext {
  requestFn: EIP1193RequestFn<RateLimiterSchema>;
  onLogsResponse?: OnLogsResponse;
  onLogsResponseOnly?: boolean;
  baseFilter: EthGetLogsHashlessFilter;
  latestBlockNumber: bigint;
  facet?: Facet;
  stats: {
    logsFetched: number;
    /** Halving stats, surfaced as flat `splits_*` fields on the terminal wide event. */
    splits: { count: number; range: number; timeout: number; maxDepth: number };
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
  if (depth > ctx.stats.splits.maxDepth) ctx.stats.splits.maxDepth = depth;

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
      ctx.stats.fetchDurationsMs[bin] = (ctx.stats.fetchDurationsMs[bin] ?? 0) + 1;
    }

    // Success - invoke callback
    ctx.onLogsResponse?.({
      logs,
      fromBlock: constrainedRange.fromBlock,
      toBlock: constrainedRange.toBlock,
      fetchedAtBlock: ctx.latestBlockNumber,
      fetchedAt: Date.now(),
    });
    ctx.stats.logsFetched += logs.length;

    return ctx.onLogsResponseOnly ? [] : logs;
  } catch (error) {
    const cause = classifyBlockRangeError(error);
    if (cause === "range" || (cause === "timeout" && timeoutSplitsRemaining > 0)) {
      // Use constrainedRange to avoid halving into invalid ranges
      const halves = halveBlockRange(constrainedRange);

      if (halves) {
        const nextBudget = cause === "timeout" ? timeoutSplitsRemaining - 1 : timeoutSplitsRemaining;
        ctx.stats.splits.count += 1;
        ctx.stats.splits[cause] += 1;

        const logs = await Promise.all(
          halves.map((half) => fetchRangeWithRetry(ctx, half, priority, nextBudget, depth + 1)),
        );
        return ctx.onLogsResponseOnly ? [] : logs.flat();
      }
    }

    // Record on the wide event rather than emitting a separate log entry. `Promise.all`
    // upstream surfaces only the first rejection, so this bounded list is the only
    // record of sibling chunks that failed in parallel.
    ctx.facet?.push("failed_ranges", {
      from_block: Number(range.fromBlock),
      to_block: Number(range.toBlock),
      error: serializeError(error),
    });
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
  facetId: FacetId,
): Promise<RpcLog[]> {
  // blockHash queries cannot be divided - pass through
  if (filter.blockHash) {
    return requestFn({ method: "eth_getLogs", params: params[0] ? [filter, params[0]] : [filter] });
  }

  const facet = getObservability()?.facet(facetId);

  // Get extra params
  const priority = params[0]?.priority ?? 0;
  const latestBlockNumber = hexToBigInt(params[1]?.latestBlock ?? (await requestFn({ method: "eth_blockNumber" })));

  // Resolve block tags to numbers
  const fromBlock = resolveBlockNumber(filter.fromBlock ?? "earliest", latestBlockNumber);
  const toBlock = min(resolveBlockNumber(filter.toBlock ?? "latest", latestBlockNumber), latestBlockNumber);

  if (fromBlock > toBlock) {
    facet?.set({ short_circuit: "empty_range" });
    return [];
  }

  const ctx: ProcessContext = {
    requestFn,
    onLogsResponse: params[1]?.onLogsResponse,
    onLogsResponseOnly: params[1]?.onLogsResponseOnly,
    baseFilter: filter,
    latestBlockNumber,
    facet,
    stats: {
      logsFetched: 0,
      splits: { count: 0, range: 0, timeout: 0, maxDepth: 0 },
      fetchDurationsMs: {},
    },
  };

  const range: BlockRange = { fromBlock, toBlock };
  const chunks = divideBlockRange(range, config.maxBlockRange, config.alignTo);

  facet?.set({
    from_block: Number(fromBlock),
    to_block: Number(toBlock),
    latest_block: Number(latestBlockNumber),
    nominal_ranges: chunks.length,
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
    facet?.set({
      logs_fetched: ctx.stats.logsFetched,
      fetch_durations_ms: ctx.stats.fetchDurationsMs,
      splits_count: ctx.stats.splits.count,
      splits_range: ctx.stats.splits.range,
      splits_timeout: ctx.stats.splits.timeout,
      splits_max_depth: ctx.stats.splits.maxDepth,
    });
  }
}
