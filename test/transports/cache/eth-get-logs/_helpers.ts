import { type RpcLog, toHex } from "viem";
import { type Mock, vi } from "vitest";

import { createSlot, LazyNdjsonMap } from "../../../../src/internal/index.js";
import type { Entry } from "../../../../src/internal/ndjson-map.js";
import type { MemoryStore } from "../../../../src/stores/memory.js";
import type { CachedChunk, CachedLogs, CachedMetadata } from "../../../../src/transports/cache/eth-get-logs/types.js";
import { keychain } from "../../../../src/transports/cache/keychain.js";
import type { InvalidationStrategy } from "../../../../src/transports/cache/types.js";
import type { BlockRange } from "../../../../src/types.js";
import { parse, stringify } from "../../../../src/utils/json.js";

export const codec = { toJson: stringify, fromJson: parse } as const;
export const chainId = 1;
export const binSize = 10_000;
export const neverInvalidate: InvalidationStrategy = () => 0;
export const alwaysInvalidate: InvalidationStrategy = () => 1;

export function createMockLog(blockNumber: bigint, logIndex = 0): RpcLog {
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

export function entryKey(fromBlock: bigint, toBlock: bigint) {
  return keychain.entryKey(chainId, "eth_getLogs", { fromBlock, toBlock });
}

export function createNdjson() {
  const slot = createSlot();
  const ndjson = new LazyNdjsonMap<CachedChunk>(codec, { autoFlushThresholdBytes: Number.MAX_SAFE_INTEGER }, slot);
  return { ndjson, slot };
}

export function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

type StoredBin = {
  fromBlock: bigint;
  toBlock: bigint;
  logs: RpcLog[];
  alignedRange?: BlockRange;
  fetchedRange?: BlockRange;
  fetchedAt?: number;
  fetchedAtBlock?: bigint;
};

/** Pre-populate a store with metadata + logs entries for one or more bins. */
export async function populateStore(store: MemoryStore, blobKey: string, bins: StoredBin[]) {
  let buffers = store.get(blobKey) ?? [];
  const ndjson = new LazyNdjsonMap<CachedChunk>(
    codec,
    { autoFlushThresholdBytes: Number.MAX_SAFE_INTEGER },
    {
      get: () => buffers,
      set: (v) => {
        buffers = v;
        store.set(blobKey, v);
      },
    },
  );

  for (const bin of bins) {
    const alignedRange = bin.alignedRange ?? { fromBlock: bin.fromBlock, toBlock: bin.toBlock };
    const fetchedRange = bin.fetchedRange ?? alignedRange;
    const ek = entryKey(alignedRange.fromBlock, alignedRange.toBlock);
    ndjson.upsert([
      {
        key: ek.metadata,
        value: {
          __type: "metadata" as const,
          fetchedAt: bin.fetchedAt ?? Date.now(),
          fetchedAtBlock: bin.fetchedAtBlock ?? fetchedRange.toBlock + 1000n,
          alignedRange,
          fetchedRange,
        } satisfies CachedMetadata,
      },
      {
        key: ek.data,
        value: bin.logs as CachedLogs,
      },
    ]);
  }

  await ndjson.flush();
}

export async function collectRecords(ndjson: LazyNdjsonMap<CachedChunk>) {
  const records: Entry<CachedChunk>[] = [];
  for await (const record of ndjson.records()) {
    records.push({ key: record.key, value: record.value });
  }
  return records;
}

/**
 * Creates a mock requestFn that emulates the `logsDivider` callback contract.
 * For `eth_getLogs`, the callback range is constrained to `latestBlock`.
 */
type MockFilter = { fromBlock: string; toBlock: string };
type MockAdditional = {
  latestBlock?: string;
  onLogsResponse?: (resp: {
    logs: RpcLog[];
    fromBlock: bigint;
    toBlock: bigint;
    fetchedAtBlock: bigint;
    fetchedAt: number;
  }) => void;
  onLogsResponseOnly?: boolean;
};

export function createMockRequestFn(options: {
  latestBlock?: bigint;
  logGenerator?: (fromBlock: bigint, toBlock: bigint) => RpcLog[];
}): Mock {
  const { latestBlock = 100_000n, logGenerator } = options;

  return vi.fn().mockImplementation(async (args: { method: string; params?: unknown[] }) => {
    if (args.method === "eth_blockNumber") {
      return toHex(latestBlock);
    }

    if (args.method === "eth_getLogs") {
      const filter = args.params?.[0] as MockFilter;
      const additional = args.params?.[2] as MockAdditional | undefined;
      const fromBlock = BigInt(filter.fromBlock);
      const requestedToBlock = BigInt(filter.toBlock);
      const constrainedToBlock = additional?.latestBlock
        ? BigInt(additional.latestBlock) < requestedToBlock
          ? BigInt(additional.latestBlock)
          : requestedToBlock
        : requestedToBlock;

      const logs = logGenerator ? logGenerator(fromBlock, constrainedToBlock) : [createMockLog(fromBlock)];

      if (additional?.onLogsResponse) {
        additional.onLogsResponse({
          logs,
          fromBlock,
          toBlock: constrainedToBlock,
          fetchedAtBlock: BigInt(additional.latestBlock!),
          fetchedAt: Date.now(),
        });
      }

      return additional?.onLogsResponseOnly ? undefined : logs;
    }

    throw new Error(`Unexpected method: ${args.method}`);
  });
}
