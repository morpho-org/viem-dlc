/**
 * `withLogging`: every outermost request inside the scope emits one "concluded" wide event, with
 * flat fields contributed by each transport it crossed (`viem-dlc-cache.*`, `viem-dlc-logs-divider.*`,
 * …). Any `Logger`-shaped object works; here it's a real LogLayer writing to the console.
 */
import { withLogging } from "@morpho-org/viem-dlc";
import { getLogs2 } from "@morpho-org/viem-dlc/actions";
import { LruStore } from "@morpho-org/viem-dlc/stores";
import { cache, createSimpleInvalidation } from "@morpho-org/viem-dlc/transports/cache";
import { ConsoleTransport, LogLayer } from "loglayer";
import { createPublicClient, http, parseAbiItem } from "viem";
import { getBlockNumber } from "viem/actions";
import { base } from "viem/chains";

const rpcUrl = process.env.RPC_URL;
if (!rpcUrl) throw new Error("Set RPC_URL (see examples/README.md)");

const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb" as const;
const borrowEvent = parseAbiItem(
  "event Borrow(bytes32 indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares)",
);

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
  () => getLogs2(client, { address: MORPHO, event: borrowEvent, fromBlock: toBlock - 50_000n, toBlock }),
  { logger, service: "examples", example: "09-observability" },
);

console.log(`\n${logs.length} Borrow logs; the wide event above describes the call that fetched them`);
