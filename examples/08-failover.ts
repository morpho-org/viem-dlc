/**
 * `failover`: front two providers with per-provider limits. Each branch is a full `cache` stack
 * built once, so rate limiters and coalescing state persist across requests (unlike viem's
 * `fallback`). The shared store means partial progress on branch A is visible to branch B.
 *
 * Set `RPC_URL_FALLBACK` to a second provider; it defaults to `RPC_URL` so the example still runs.
 */
import { getLogs2 } from "@morpho-org/viem-dlc/actions";
import { LruStore } from "@morpho-org/viem-dlc/stores";
import { defaultShouldThrow, failover } from "@morpho-org/viem-dlc/transports";
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

const store = new LruStore({ maxBytes: 100_000_000 });
const shared = { binSize: 10_000, store, invalidationStrategy: createSimpleInvalidation() };
const tail = [
  { retryCount: 2, retryDelay: 500, blockTimestamp: false },
  { maxBytes: 8_192 },
  { maxRequestsPerSecond: 10, maxBurstRequests: 5, maxConcurrentRequests: 5 },
] as const;

const transport = failover(
  [
    cache(http(rpcUrl), [{ ...shared, gasLimit: 30_000_000 }, { maxBlockRange: 100_000 }, ...tail]),
    cache(http(process.env.RPC_URL_FALLBACK ?? rpcUrl), [
      { ...shared, gasLimit: 50_000_000 },
      { maxBlockRange: 10_000 },
      ...tail,
    ]),
  ],
  {
    // Auth/billing failures won't get better on retry, but also shouldn't try the next provider.
    shouldThrow: (err) => defaultShouldThrow(err) || [401, 402, 403].includes((err as { status?: number }).status ?? 0),
  },
);

const client = createPublicClient({ chain: base, transport });

const toBlock = await getBlockNumber(client);
const logs = await getLogs2(client, {
  address: MORPHO,
  event: borrowEvent,
  fromBlock: toBlock - 100_000n,
  toBlock,
});

console.log(`${logs.length} Borrow logs over the last 100 000 blocks`);
