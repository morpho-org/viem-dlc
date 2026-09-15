/**
 * `withLogging`: every outermost request inside the scope emits one "concluded" wide event, with
 * flat fields contributed by each transport it crossed (`viem-dlc-cache.*`, `viem-dlc-logs-divider.*`,
 * …). Any `Logger`-shaped object works; here it's a real LogLayer writing to the console.
 *
 * The second scope is where the numbers 04-07 state come from: a lens read reports `gas_limit_observed`,
 * `fixed_gas`, `item_gas_avg` and `item_gas_stddev`, which go back verbatim into the transport's
 * `gasLimit` and the read's `batch.gas`. Run it without stating them, take the figures over a
 * representative window, paste them in.
 */
import { withLogging } from "@morpho-org/viem-dlc";
import { getLogs2, readLens } from "@morpho-org/viem-dlc/actions";
import { LruStore } from "@morpho-org/viem-dlc/stores";
import { cache, createSimpleInvalidation } from "@morpho-org/viem-dlc/transports/cache";
import { ConsoleTransport, LogLayer } from "loglayer";
import { sol } from "soltag";
import { createPublicClient, http, parseAbiItem } from "viem";
import { getBlockNumber } from "viem/actions";
import { base } from "viem/chains";

const rpcUrl = process.env.RPC_URL;
if (!rpcUrl) throw new Error("Set RPC_URL (see examples/README.md)");

const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb" as const;
const borrowEvent = parseAbiItem(
  "event Borrow(bytes32 indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares)",
);

const positionsLens = sol("MorphoPositionsLens")`
  pragma solidity ^0.8.24;
  interface IMorpho {
    struct Position { uint256 supplyShares; uint128 borrowShares; uint128 collateral; }
    function position(bytes32 id, address user) external view returns (Position memory);
  }
  contract MorphoPositionsLens {
    IMorpho constant MORPHO = IMorpho(0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb);
    struct Input { bytes32 id; address user; }

    function positionOf(Input calldata x) external view returns (IMorpho.Position memory) {
      return MORPHO.position(x.id, x.user);
    }
  }
`;

const logger = new LogLayer({
  transport: new ConsoleTransport({ logger: console, messageField: "msg", levelField: "level", stringify: true }),
});

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

const logs = await withLogging(
  () => getLogs2(client, { address: MORPHO, event: borrowEvent, strict: true, fromBlock: toBlock - 50_000n, toBlock }),
  { logger, service: "examples", example: "10-observability" },
);

console.log(`\n${logs.length} Borrow logs; the wide event above describes the call that fetched them\n`);

const inputs = [
  ...new Map(logs.map((l) => [`${l.args.id}:${l.args.onBehalf}`, { id: l.args.id, user: l.args.onBehalf }])).values(),
].slice(0, 500);

const { results } = await withLogging(
  () => readLens(client, { ...positionsLens.with(), functionName: "positionOf", args: inputs }),
  { logger, service: "examples", example: "10-observability" },
);

console.log(
  `\n${results.length} positions; the *_gas fields on that second event are what 04-07 state as ` +
    `\`gasLimit\` and \`batch.gas\``,
);
