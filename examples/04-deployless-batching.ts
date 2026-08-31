/**
 * `deployless` + `policy({ batch })`: read thousands of positions through a lens contract that is
 * never deployed. The lens is written inline with soltag; `policy` tells the transport how to
 * split the input array across upstream `eth_call`s under a byte budget and a gas budget.
 */
import { MAX_INITCODE_SIZE, policy } from "@morpho-org/viem-dlc/actions";
import { deployless } from "@morpho-org/viem-dlc/transports";
import { sol } from "soltag";
import {
  type Address,
  type Chain,
  type Client,
  createPublicClient,
  getAbiItem,
  type Hex,
  http,
  parseAbiItem,
  type Transport,
} from "viem";
import { getBlockNumber, getLogs, readContract } from "viem/actions";
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

// One dynamic-array input, one dynamic-array output, same length and order — the shape `policy` requires.
const positionsLens = sol("MorphoPositionsLens")`
  pragma solidity ^0.8.24;
  ${IMorpho}
  contract MorphoPositionsLens {
    IMorpho constant MORPHO = IMorpho(0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb);
    struct Input { bytes32 id; address user; }

    function positions(Input[] calldata inputs) external view returns (IMorpho.Position[] memory out) {
      out = new IMorpho.Position[](inputs.length);
      for (uint256 i = 0; i < inputs.length; i++) {
        out[i] = MORPHO.position(inputs[i].id, inputs[i].user);
      }
    }
  }
`;

const client = createPublicClient({
  chain: base,
  transport: deployless(http(rpcUrl), { gasLimit: 30_000_000 }),
});

const toBlock = await getBlockNumber(client);
const inputs = await discoverPositions(client, { fromBlock: toBlock - 9_000n, toBlock });
console.log(`${inputs.length} distinct (market, borrower) pairs in the last 9 000 blocks\n`);

const positions = await measure("positions(inputs)", () =>
  readContract(client, {
    ...positionsLens.with(),
    functionName: "positions",
    args: [inputs],
    stateOverride: [
      policy({
        abi: getAbiItem({ abi: positionsLens.abi, name: "positions" }),
        // eth_estimateGas on Base fits ~336k + 27k·N for this lens (the constant is mostly the
        // counterfactual deploy); padded ~25%.
        batch: { batchSize: MAX_INITCODE_SIZE, gas: { constant: 350_000, linear: 35_000, quadratic: 0 } },
      }),
    ],
  }),
);

const borrowing = positions.filter((p) => p.borrowShares > 0n).length;
console.log(`\n${positions.length} positions returned, ${borrowing} with outstanding borrow shares`);
