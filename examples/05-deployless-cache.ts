/**
 * `cache` + `readLens({ batch, cache })`: the same lens read as 04, but each element's result is
 * cached under `blobKey` for `ttl` ms. Repeat elements are served from the store; only novel
 * elements go upstream. `delta` desynchronizes refreshes so many keys populated together don't
 * all expire in the same instant.
 */
import { MAX_INITCODE_SIZE, readLens } from "@morpho-org/viem-dlc/actions";
import { LruStore } from "@morpho-org/viem-dlc/stores";
import { cache, createSimpleInvalidation } from "@morpho-org/viem-dlc/transports/cache";
import { sol } from "soltag";
import {
  type Address,
  type Chain,
  type Client,
  createPublicClient,
  type Hex,
  http,
  parseAbiItem,
  type Transport,
} from "viem";
import { getBlockNumber, getLogs } from "viem/actions";
import { base } from "viem/chains";

const rpcUrl = process.env.RPC_URL;
if (!rpcUrl) throw new Error("Set RPC_URL (see examples/README.md)");

const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb" as const;
const borrowEvent = parseAbiItem(
  "event Borrow(bytes32 indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares)",
);

async function measure<T>(label: string, fn: () => Promise<T>): Promise<T> {
  const start = performance.now();
  const result = await fn();
  console.log(`  ${label.padEnd(28)} ${(performance.now() - start).toFixed(0).padStart(6)} ms`);
  return result;
}

/** Distinct `(market, borrower)` pairs seen in `Borrow` events over a block range. */
async function discoverPositions(client: Client<Transport, Chain>, range: { fromBlock: bigint; toBlock: bigint }) {
  const logs = await getLogs(client, { address: MORPHO, event: borrowEvent, strict: true, ...range });
  const seen = new Map<string, { id: Hex; user: Address }>();
  for (const { args } of logs) seen.set(`${args.id}:${args.onBehalf}`, { id: args.id, user: args.onBehalf });
  return [...seen.values()];
}

const IMorpho = `
  interface IMorpho {
    struct Position { uint256 supplyShares; uint128 borrowShares; uint128 collateral; }
    function position(bytes32 id, address user) external view returns (Position memory);
  }
`;

// A lens is one function over one element; the transport calls it once per element in its own
// frame and paginates. Nothing here knows about batching.
const positionsLens = sol("MorphoPositionsLens")`
  pragma solidity ^0.8.24;
  ${IMorpho}
  contract MorphoPositionsLens {
    IMorpho constant MORPHO = IMorpho(0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb);
    struct Input { bytes32 id; address user; }

    function positionOf(Input calldata x) external view returns (IMorpho.Position memory) {
      return MORPHO.position(x.id, x.user);
    }
  }
`;

const transport = cache(http(rpcUrl), [
  {
    binSize: 10_000,
    store: new LruStore({ maxBytes: 100_000_000 }),
    invalidationStrategy: createSimpleInvalidation(),
  },
  { maxBlockRange: 100_000 },
  { retryCount: 3, retryDelay: 1_000, blockTimestamp: false },
  { maxBytes: 8_192 },
  { maxRequestsPerSecond: 10, maxBurstRequests: 5, maxConcurrentRequests: 5 },
]);

const client = createPublicClient({ chain: base, transport });

const toBlock = await getBlockNumber(client);
const inputs = await discoverPositions(client, { fromBlock: toBlock - 20_000n, toBlock });
console.log(`${inputs.length} distinct (market, borrower) pairs in the last 20 000 blocks\n`);

const cachedPositions = (keys: typeof inputs) =>
  readLens(client, {
    ...positionsLens.with(),
    functionName: "positionOf",
    args: keys,
    batch: { batchSize: MAX_INITCODE_SIZE },
    cache: { blobKey: "morpho-positions", ttl: 300_000, delta: 10_000 },
  });

await measure("cold", () => cachedPositions(inputs));
await measure("warm (same elements)", () => cachedPositions(inputs));
await measure("half novel", () => cachedPositions(inputs.slice(inputs.length / 2)));
await measure("warm again", () => cachedPositions(inputs));
