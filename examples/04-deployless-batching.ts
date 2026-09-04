/**
 * `deployless` + `readLens`: read thousands of positions through a lens contract that is never
 * deployed. The lens is one per-item view function written inline with soltag; `readLens` splits
 * the input array across upstream `eth_call`s under a byte budget, and the envelope calls the lens
 * once per element in its own frame. No gas budget exists anywhere: the envelope reports how far
 * it got, so a chunk adapts to whatever gas the node grants.
 */
import { MAX_INITCODE_SIZE, readLens } from "@morpho-org/viem-dlc/actions";
import { deployless } from "@morpho-org/viem-dlc/transports";
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
  transport: deployless(http(rpcUrl)),
});

const toBlock = await getBlockNumber(client);
const inputs = await discoverPositions(client, { fromBlock: toBlock - 9_000n, toBlock });
console.log(`${inputs.length} distinct (market, borrower) pairs in the last 9 000 blocks\n`);

const { results: positions, skipped } = await measure("positionOf × inputs", () =>
  readLens(client, {
    ...positionsLens.with(),
    functionName: "positionOf",
    args: inputs,
    batch: { batchSize: MAX_INITCODE_SIZE },
  }),
);

const borrowing = positions.filter((p) => p.borrowShares > 0n).length;
console.log(
  `\n${positions.length} positions returned (${skipped.length} skipped), ${borrowing} with outstanding borrow shares`,
);
