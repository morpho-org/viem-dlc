/**
 * `deployless` + `readLens`: read thousands of positions through a lens contract that is never
 * deployed. The lens is one per-item view function written inline with soltag; `readLens` splits
 * the input array across upstream `eth_call`s and the envelope calls the lens once per element in
 * its own frame.
 *
 * Two figures size the opening wave, and nothing else is tuned: the provider's cap, stated once on
 * the transport as `gasLimit`, and the lens's own cost, stated as `batch.gas`. Both come from a run
 * under observability (10-observability): `gas_limit_observed`, then `fixed_gas`, `item_gas_avg`
 * and `item_gas_stddev`. Every later chunk is sized from what the pages actually reported, so a
 * wrong figure costs a round trip, never a result.
 */
import { MAX_INITCODE_SIZE, readLens } from "@morpho-org/viem-dlc/actions";
import { deployless, logsDivider } from "@morpho-org/viem-dlc/transports";
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

const client = createPublicClient({
  chain: base,
  // `logsDivider` is here only so discovery survives whatever `eth_getLogs` range the provider
  // allows (see 01); `deployless` is the transport this example is about.
  transport: deployless(
    logsDivider(http(rpcUrl), [
      { maxBlockRange: 10_000 },
      { retryCount: 3, retryDelay: 1_000, blockTimestamp: false },
      { maxBytes: 8_192 },
      { maxRequestsPerSecond: 10, maxConcurrentRequests: 5 },
    ]),
    { gasLimit: 600_000_000 },
  ),
});

const toBlock = await getBlockNumber(client);
const inputs = await discoverPositions(client, { fromBlock: toBlock - 9_000n, toBlock });
console.log(`${inputs.length} distinct (market, borrower) pairs in the last 9 000 blocks\n`);

const { results: positions, skipped } = await measure("positionOf × inputs", () =>
  readLens(client, {
    ...positionsLens.with(),
    functionName: "positionOf",
    args: inputs,
    // Delivered as initcode (the default), so a chunk is capped at `MAX_INITCODE_SIZE` bytes —
    // ~690 pairs at 64 B each, while this frame's gas would pay for ~80 000. When bytes bind that
    // far ahead of gas, `envelope: "override"` lifts the cap entirely; 06 does exactly that.
    batch: { batchSize: MAX_INITCODE_SIZE, gas: { fixed: 242_000, item: { avg: 7_300, stddev: 150 } } },
  }),
);

const borrowing = positions.filter((p) => p.borrowShares > 0n).length;
console.log(
  `\n${positions.length} positions returned (${skipped.length} skipped), ${borrowing} with outstanding borrow shares`,
);
