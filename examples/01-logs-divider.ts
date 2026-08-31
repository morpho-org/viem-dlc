/**
 * `logsDivider`: split one large `eth_getLogs` into aligned chunks with retry, rate limiting,
 * and a progress callback — on top of a plain `http` transport, no cache involved.
 */
import { type LogsDividerSchema, logsDivider } from "@morpho-org/viem-dlc/transports";
import { createPublicClient, encodeEventTopics, http, numberToHex, parseAbiItem, rpcSchema } from "viem";
import { getBlockNumber } from "viem/actions";
import { base } from "viem/chains";

const rpcUrl = process.env.RPC_URL;
if (!rpcUrl) throw new Error("Set RPC_URL (see examples/README.md)");

const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb" as const;
const borrowEvent = parseAbiItem(
  "event Borrow(bytes32 indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares)",
);

const transport = logsDivider(http(rpcUrl), [
  { maxBlockRange: 10_000, alignTo: 10_000 },
  { retryCount: 3, retryDelay: 1_000, blockTimestamp: false },
  { maxBytes: 8_192 },
  { maxRequestsPerSecond: 10, maxConcurrentRequests: 5 },
]);

// `rpcSchema` types `client.request` with the divider's extended `eth_getLogs` parameters.
const client = createPublicClient({ chain: base, transport, rpcSchema: rpcSchema<LogsDividerSchema>() });

const toBlock = await getBlockNumber(client);
const fromBlock = toBlock - 100_000n;

const logs = await client.request({
  method: "eth_getLogs",
  params: [
    {
      address: MORPHO,
      topics: encodeEventTopics({ abi: [borrowEvent] }),
      fromBlock: numberToHex(fromBlock),
      toBlock: numberToHex(toBlock),
    },
    undefined,
    {
      onLogsResponse: ({ logs, fromBlock, toBlock }) => {
        console.log(`  chunk ${fromBlock}..${toBlock}: ${logs.length} logs`);
      },
    },
  ],
});

console.log(`\n${logs.length} Borrow logs over ${toBlock - fromBlock} blocks`);
