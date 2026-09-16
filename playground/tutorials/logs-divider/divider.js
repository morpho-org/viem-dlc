import { logsDivider } from "@morpho-org/viem-dlc/transports";
import { createPublicClient, encodeEventTopics, numberToHex, parseAbiItem, rpcSchema } from "viem";
import { getBlockNumber } from "viem/actions";

/** @type {`0x${string}`} */
const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb";
const borrowEvent = parseAbiItem(
  "event Borrow(bytes32 indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares)",
);

/**
 * `logsDivider` splits one wide `eth_getLogs` into aligned chunks, retries what fails, and drops
 * oversized logs. No cache and no store are involved — this is the transport on its own.
 *
 * The third request parameter is the divider's own schema extension: `onLogsResponse` fires once
 * per chunk as it lands, which is why the progress below appears while the request is still in
 * flight rather than all at once at the end.
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

  return { summary: { logs: all.length, chunks, blocks: Number(settings.blocks) } };
};

export default run;
