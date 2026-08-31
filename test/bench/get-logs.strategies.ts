import { fileURLToPath } from "url";

import { createPublicClient, http, parseAbiItem } from "viem";
import { base } from "viem/chains";

import { getLogs2 } from "../../dist/actions/index.js";
import { HierarchicalStore, LruStore, NodeFsStore } from "../../dist/stores/index.js";
import { cache, createSimpleInvalidation } from "../../dist/transports/cache/index.js";

/** Morpho Blue on Base; the same query examples/02 and /03 make. */
const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb" as const;
const MORPHO_DEPLOYMENT_BLOCK = 13_977_148n;
const SAMPLE_BORROWER = "0x6aBB9Aeb93e9aF04c8D9FD2b84BF6ca8EDC3eAf2" as const;
const borrowEvent = parseAbiItem(
  "event Borrow(bytes32 indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares)",
);

export const FIXTURES_DIR = fileURLToPath(new URL("./fixtures/", import.meta.url));

export const STRATEGIES = ["fetch-all-then-filter", "reduce", "search-and-reduce"] as const;
export type Strategy = (typeof STRATEGIES)[number];

/**
 * Fixed bounds keep the fixture fully warm across runs; `toBlock` must stay below the chain head
 * or the final bin is refetched every run. What's measured is dominated by cache-side work — each
 * request still makes one `eth_blockNumber` preflight, identical across strategies.
 * `BENCH_FROM_BLOCK` narrows the range for quick local runs.
 */
export const query = {
  address: MORPHO,
  event: borrowEvent,
  strict: true,
  fromBlock: process.env.BENCH_FROM_BLOCK ? BigInt(process.env.BENCH_FROM_BLOCK) : MORPHO_DEPLOYMENT_BLOCK,
  toBlock: 50_000_000n,
} as const;

export function createBenchClient(rpcUrl: string) {
  const store = new HierarchicalStore([
    new LruStore({ maxBytes: 1 << 30 }),
    new NodeFsStore({ directory: FIXTURES_DIR }),
  ]);
  const transport = cache(http(rpcUrl), [
    { binSize: 10_000, store, invalidationStrategy: createSimpleInvalidation(), gasLimit: 30_000_000 },
    { maxBlockRange: 100_000 },
    { retryCount: 3, retryDelay: 1_000, blockTimestamp: false },
    { maxBytes: 8_192 },
    { maxRequestsPerSecond: 10, maxBurstRequests: 5, maxConcurrentRequests: 5 },
  ]);
  return { store, client: createPublicClient({ chain: base, transport }) };
}

export type BenchClient = ReturnType<typeof createBenchClient>["client"];

export function runStrategy(client: BenchClient, strategy: Strategy) {
  const isTarget = (log: { args: { onBehalf?: string } }) => log.args.onBehalf === SAMPLE_BORROWER;
  switch (strategy) {
    case "fetch-all-then-filter":
      return getLogs2(client, query).then((all) => all.filter(isTarget));
    case "reduce":
      return getLogs2(client, {
        ...query,
        reduce: (acc, log) => {
          if (isTarget(log)) acc.push(log);
          return acc;
        },
      });
    case "search-and-reduce":
      return getLogs2(client, {
        ...query,
        search: SAMPLE_BORROWER.slice(2),
        reduce: (acc, log) => {
          if (isTarget(log)) acc.push(log);
          return acc;
        },
      });
  }
}
