import { logsDivider } from "@morpho-org/viem-dlc/transports";
import { createPublicClient, encodeEventTopics, numberToHex, parseAbiItem, rpcSchema } from "viem";
import { getBlockNumber } from "viem/actions";

/** @type {`0x${string}`} */
const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb";
const borrowEvent = parseAbiItem(
  "event Borrow(bytes32 indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares)",
);

/**
 * The same range the previous tab couldn't ask for, through `logsDivider`.
 *
 * `maxBlockRange` is a ceiling, not a promise: a chunk that comes back 413 or times out is halved
 * and retried, so an endpoint stricter than your guess costs a round trip rather than the request.
 * `alignTo` snaps chunk boundaries to fixed multiples, which is what lets the cache in the next
 * section reuse them.
 *
 * `onLogsResponse` is the divider's own schema extension. It fires per chunk as each lands, which is
 * why the feed below fills while the request is still in flight.
 *
 * @type {import("../../src/tutorials/types.js").Tab<{ settings: { blocks: string; maxBlockRange: string } }>}
 */
const run = async ({ transport, chain, settings, log }) => {
  const client = createPublicClient({
    chain,
    rpcSchema: rpcSchema(),
    transport: logsDivider(transport, [
      { maxBlockRange: Number(settings.maxBlockRange), alignTo: Number(settings.maxBlockRange) },
      { retryCount: 3, retryDelay: 1_000, blockTimestamp: false },
      { maxBytes: 8_192 },
      { maxRequestsPerSecond: 10, maxConcurrentRequests: 5 },
    ]),
  });

  const toBlock = await getBlockNumber(client);
  const fromBlock = toBlock - BigInt(settings.blocks);

  let chunks = 0;
  let logs = 0;
  const started = performance.now();

  const all = await client.request({
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
        /** @param {{ logs: unknown[]; fromBlock: bigint; toBlock: bigint }} response */
        onLogsResponse: ({ logs: chunk, fromBlock: from, toBlock: to }) => {
          chunks += 1;
          logs += chunk.length;
          log(`chunk ${chunks} (${from}..${to}): ${chunk.length} logs, ${logs} so far`);
        },
      },
    ],
  });

  return {
    summary: {
      logs: all.length,
      chunks,
      blocks: Number(settings.blocks),
      elapsed: `${(performance.now() - started).toFixed(0)} ms`,
    },
  };
};

export default run;
