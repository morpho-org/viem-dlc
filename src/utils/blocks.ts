import { hexToBigInt, isHex, type RpcError, type RpcLog } from "viem";

import type { BlockNumberish, BlockRange } from "../types.js";

import { max } from "./math.js";

/** Resolves a `BlockNumberish` value to a `bigint`. */
export function resolveBlockNumber(b: BlockNumberish, latest = 0n): bigint {
  if (typeof b === "bigint") return b;

  if (isHex(b)) return hexToBigInt(b);

  switch (b) {
    case "earliest":
      return 0n;
    case "latest":
      return latest;
    case "safe":
    case "finalized":
    case "pending":
    default:
      throw new Error(`Attempted to resolve unsupported block tag: '${b}'`);
  }
}

/** Returns a filter function that keeps logs in the range [fromBlock, toBlock]. */
export function isInBlockRange(range: BlockRange) {
  return (log: RpcLog): boolean => {
    if (!log.blockNumber) return false;
    const blockNumber = hexToBigInt(log.blockNumber);
    return range.fromBlock <= blockNumber && blockNumber <= range.toBlock;
  };
}

/**
 * Divides `range` into chunks of at most `maxBlockRange` and returns them in ascending order.
 *
 * @param maxBlockRange Maximum blocks per chunk. Use `Infinity` for unconstrained (no splitting).
 * @param alignTo Optional alignment boundary. When set, chunk boundaries are aligned to
 *   multiples of this value. This may extend chunks slightly beyond the original range
 *   (e.g., [10_003, 99_995] with alignTo=10_000 becomes chunks aligned to [10_000, ...] and [..., 100_000]).
 *   Useful for cache hit optimization.
 */
export function divideBlockRange(range: BlockRange, maxBlockRange: number, alignTo?: number): BlockRange[] {
  if (range.fromBlock > range.toBlock) return [];

  const alignment = alignTo ? BigInt(alignTo) : undefined;

  // Align the starting block down to alignment boundary
  const alignedStart = alignment ? (range.fromBlock / alignment) * alignment : range.fromBlock;

  // Align the ending block up to alignment boundary
  const alignedEnd = alignment ? (range.toBlock / alignment + 1n) * alignment - 1n : range.toBlock;

  // Handle unconstrained case (Infinity) - return single range
  if (!Number.isFinite(maxBlockRange)) {
    return [{ fromBlock: alignedStart, toBlock: alignedEnd }];
  }

  const ranges: BlockRange[] = [];
  const step = BigInt(maxBlockRange);
  let current = alignedStart;

  while (current <= alignedEnd) {
    const chunkEnd = current + step - 1n;
    ranges.push({
      fromBlock: current,
      toBlock: chunkEnd > alignedEnd ? alignedEnd : chunkEnd,
    });
    current = chunkEnd + 1n;
  }

  return ranges;
}

/**
 * Merges an array of `BlockRange`s, combining those which are consecutive or overlapping.
 * Returns a new array of non-overlapping `BlockRange`s sorted by `fromBlock`.
 */
export function mergeBlockRanges(ranges: BlockRange[]): BlockRange[] {
  if (ranges.length === 0) return [];

  // Sort by fromBlock ascending
  const sorted = [...ranges].sort((a, b) => {
    if (a.fromBlock < b.fromBlock) return -1;
    if (a.fromBlock > b.fromBlock) return 1;
    return 0;
  });

  const merged: BlockRange[] = [];
  let current = { ...sorted[0]! };

  for (let i = 1; i < sorted.length; i++) {
    const next = sorted[i]!;

    // Check if current and next are consecutive or overlapping
    // Consecutive: current.toBlock + 1n === next.fromBlock
    // Overlapping: current.toBlock >= next.fromBlock
    if (current.toBlock + 1n >= next.fromBlock) {
      // Merge: extend current to include next
      current.toBlock = max(current.toBlock, next.toBlock);
    } else {
      // No overlap, push current and start new
      merged.push(current);
      current = { ...next };
    }
  }

  // Push the last range
  merged.push(current);

  return merged;
}

/** Halves `range` and returns [firstHalf, secondHalf] (or `undefined` if span is 1 block). */
export function halveBlockRange(range: BlockRange): [BlockRange, BlockRange] | undefined {
  const size = range.toBlock - range.fromBlock + 1n;

  if (size <= 1n) {
    // Cannot halve a single block
    return undefined;
  }

  const mid = range.fromBlock + size / 2n - 1n;
  return [
    { fromBlock: range.fromBlock, toBlock: mid },
    { fromBlock: mid + 1n, toBlock: range.toBlock },
  ];
}

/** Determines if an error indicates the block range was too large. */
export function isErrorCausedByBlockRange(error: unknown): boolean {
  if (!isRpcError(error)) return false;

  const code = error.code;
  const message = error.message || "";

  for (const pattern of BLOCK_RANGE_ERROR_PATTERNS) {
    if (pattern.code !== undefined && pattern.code !== code) continue;
    if (pattern.message !== undefined && !pattern.message.test(message)) continue;

    return true;
  }

  return false;
}

export function isRpcError(error: unknown): error is RpcError {
  return error instanceof Error && "code" in error && "message" in error;
}

/**
 * Error patterns that indicate "block range too large" from various RPC providers.
 * Patterns are checked in order - more specific matches first.
 */
const BLOCK_RANGE_ERROR_PATTERNS: { code?: number; message?: RegExp }[] = [
  { code: -32602 },
  { code: -32005 },
  { code: -32012 },
  { code: -32000 },
  { message: /range.*exceed/i },
  { message: /range.*too/i },
  { message: /exceed.*block/i },
  { message: /max.*block/i },
  { message: /blocks/i },
  { message: /returned more than/i },
  { message: /response size/i },
];
