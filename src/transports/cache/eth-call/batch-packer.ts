export type BatchRange = readonly [start: number, end: number];

/**
 * Exact greedy batch packer. Walks `perMissBytes` left-to-right, grouping consecutive
 * misses into batches whose total wire bytes (`overheadBytes + sum of element bytes`)
 * stay within `maxBytes`. Always includes at least one miss per batch, so a single
 * oversized element still makes progress.
 *
 * Callers derive `overheadBytes` and `perMissBytes` from the actual wire format, so
 * unlike a proportional heuristic this never produces a batch larger than `maxBytes`
 * (unless a single miss already exceeds it).
 *
 * Returns `[[0, n]]` (a single batch) when splitting is disabled (`maxBytes` unset
 * or non-positive).
 */
export function packByCalldataBytes(
  perMissBytes: readonly number[],
  overheadBytes: number,
  maxBytes: number | undefined,
): BatchRange[] {
  const n = perMissBytes.length;
  if (n === 0) return [];
  if (!maxBytes || maxBytes <= 0) return [[0, n]];

  const ranges: BatchRange[] = [];
  let i = 0;
  while (i < n) {
    let batchBytes = overheadBytes + perMissBytes[i]!;
    let j = i + 1;
    while (j < n && batchBytes + perMissBytes[j]! <= maxBytes) {
      batchBytes += perMissBytes[j]!;
      j++;
    }
    ranges.push([i, j]);
    i = j;
  }
  return ranges;
}
