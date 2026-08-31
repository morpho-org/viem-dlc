/**
 * `getLogs2` with `search` and `reduce`: narrow a globally-shared cache down to one account
 * without either putting the account in the RPC filter (which would fragment the cache) or
 * materializing every log in memory.
 *
 * Three strategies, same answer:
 *   1. fetch everything, then filter        — simplest; memory ∝ all logs
 *   2. `reduce`                              — folds per bin; memory ∝ matches
 *   3. `search` + `reduce`                   — also skips JSON.parse on bins that can't match
 */
import { getLogs2 } from "@morpho-org/viem-dlc/actions";
import { HierarchicalStore, LruStore, NodeFsStore } from "@morpho-org/viem-dlc/stores";
import { cache, createSimpleInvalidation } from "@morpho-org/viem-dlc/transports/cache";
import { createPublicClient, http, parseAbiItem } from "viem";
import { base } from "viem/chains";

const rpcUrl = process.env.RPC_URL;
if (!rpcUrl) throw new Error("Set RPC_URL (see examples/README.md)");

const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb" as const;
const MORPHO_DEPLOYMENT_BLOCK = 13_977_148n;
const SAMPLE_BORROWER = "0x6aBB9Aeb93e9aF04c8D9FD2b84BF6ca8EDC3eAf2" as const;
const borrowEvent = parseAbiItem(
  "event Borrow(bytes32 indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares)",
);

async function measure<T>(label: string, fn: () => Promise<T>): Promise<T> {
  const start = performance.now();
  const result = await fn();
  console.log(`  ${label.padEnd(28)} ${(performance.now() - start).toFixed(0).padStart(6)} ms`);
  return result;
}

const store = new HierarchicalStore([
  new LruStore({ maxBytes: 100_000_000 }),
  new NodeFsStore({ directory: ".cache/examples" }),
]);

const transport = cache(http(rpcUrl), [
  { binSize: 10_000, store, invalidationStrategy: createSimpleInvalidation(), gasLimit: 30_000_000 },
  { maxBlockRange: 100_000 },
  { retryCount: 3, retryDelay: 1_000, blockTimestamp: false },
  { maxBytes: 8_192 },
  { maxRequestsPerSecond: 10, maxBurstRequests: 5, maxConcurrentRequests: 5 },
]);

const client = createPublicClient({ chain: base, transport });

const query = {
  address: MORPHO,
  event: borrowEvent,
  strict: true,
  fromBlock: MORPHO_DEPLOYMENT_BLOCK,
  toBlock: "latest",
} as const;

await measure("warm the cache", () => getLogs2(client, query));

const filtered = await measure("1. fetch all, then filter", async () => {
  const all = await getLogs2(client, query);
  return all.filter((log) => log.args.onBehalf === SAMPLE_BORROWER);
});

const reduced = await measure("2. reduce", () =>
  getLogs2(client, {
    ...query,
    reduce: (acc, log) => {
      if (log.args.onBehalf === SAMPLE_BORROWER) acc.push(log);
      return acc;
    },
  }),
);

// `onBehalf` is indexed, so it sits in `topics` as zero-padded lowercase hex; the address body is
// the substring that can appear in a matching bin's raw JSON.
const searched = await measure("3. search + reduce", () =>
  getLogs2(client, {
    ...query,
    search: SAMPLE_BORROWER.slice(2),
    reduce: (acc, log) => {
      if (log.args.onBehalf === SAMPLE_BORROWER) acc.push(log);
      return acc;
    },
  }),
);

console.log(`\nBorrow logs for ${SAMPLE_BORROWER}: ${filtered.length} / ${reduced.length} / ${searched.length}`);

await store.flush();
