/**
 * `cache`: the all-in-one transport. Every `eth_getLogs` is aligned to `binSize` bins and stored,
 * so the second run of the same query is served from the store instead of the RPC.
 *
 * The store is layered: an in-process LRU in front of a directory on disk, so the warm run
 * survives process restarts too.
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
  fromBlock: MORPHO_DEPLOYMENT_BLOCK,
  toBlock: "latest",
} as const;

const cold = await measure("cold (RPC)", () => getLogs2(client, query));
const warm = await measure("warm (store)", () => getLogs2(client, query));

console.log(`\n${cold.length} Borrow logs; warm run returned ${warm.length}`);
console.log(
  `first: block ${cold[0]?.blockNumber}, args ${JSON.stringify(cold[0]?.args, (_, v) => (typeof v === "bigint" ? v.toString() : v))}`,
);

await store.flush();
