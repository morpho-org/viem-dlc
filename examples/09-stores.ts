/**
 * Stores compose. Reads fall through a `HierarchicalStore` top to bottom and writes fan out;
 * `TtlStore` bounds how long a warm tier may mask a fresher remote; `CompressedStore` shrinks
 * what lands on disk. The remote tiers below are optional and only attach when their env is set.
 */
import type { Store } from "@morpho-org/viem-dlc";
import { getLogs2 } from "@morpho-org/viem-dlc/actions";
import { CompressedStore, HierarchicalStore, LruStore, NodeFsStore, TtlStore } from "@morpho-org/viem-dlc/stores";
import { cache, createSimpleInvalidation } from "@morpho-org/viem-dlc/transports/cache";
import { createPublicClient, http, parseAbiItem } from "viem";
import { getBlockNumber } from "viem/actions";
import { base } from "viem/chains";

const rpcUrl = process.env.RPC_URL;
if (!rpcUrl) throw new Error("Set RPC_URL (see examples/README.md)");

const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb" as const;
const borrowEvent = parseAbiItem(
  "event Borrow(bytes32 indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares)",
);

const tiers: Store[] = [
  new TtlStore(new LruStore({ maxBytes: 100_000_000 }), { ttlMs: 60_000 }),
  new CompressedStore(new NodeFsStore({ directory: ".cache/examples", prefix: "compressed" })),
];

if (process.env.UPSTASH_REDIS_REST_URL && process.env.UPSTASH_REDIS_REST_TOKEN) {
  const { createOptimizedUpstashStore } = await import("@morpho-org/viem-dlc/stores/upstash");
  tiers.push(createOptimizedUpstashStore({ maxRequestBytes: 1_000_000 }));
  console.log("attached Upstash tier");
}

if (process.env.BLOB_READ_WRITE_TOKEN) {
  const { createOptimizedVercelStore } = await import("@morpho-org/viem-dlc/stores/vercel");
  tiers.push(createOptimizedVercelStore({ prefix: "viem-dlc-examples" }));
  console.log("attached Vercel Blob tier");
}

const store = new HierarchicalStore(tiers, { populateOnMiss: true });

// Stores are plain key → Buffer[] maps; the transport is just one client of that interface.
await store.set("hello", [Buffer.from("world")]);
console.log(`store.get("hello") → ${(await store.get("hello"))?.map(String).join("")}`);
await store.delete("hello");

const transport = cache(http(rpcUrl), [
  { binSize: 10_000, store, invalidationStrategy: createSimpleInvalidation(), gasLimit: 30_000_000 },
  { maxBlockRange: 100_000 },
  { retryCount: 3, retryDelay: 1_000, blockTimestamp: false },
  { maxBytes: 8_192 },
  { maxRequestsPerSecond: 10, maxBurstRequests: 5, maxConcurrentRequests: 5 },
]);

const client = createPublicClient({ chain: base, transport });
const toBlock = await getBlockNumber(client);
const logs = await getLogs2(client, { address: MORPHO, event: borrowEvent, fromBlock: toBlock - 50_000n, toBlock });
console.log(`${logs.length} Borrow logs written through ${tiers.length} tiers`);

// Flush before exit so debounced remote tiers finish their writes.
await store.flush();
