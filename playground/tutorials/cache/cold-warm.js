import { getLogs2 } from "@morpho-org/viem-dlc/actions";
import { LruStore } from "@morpho-org/viem-dlc/stores";
import { cache, createSimpleInvalidation } from "@morpho-org/viem-dlc/transports/cache";
import { createPublicClient, parseAbiItem } from "viem";

/** @type {`0x${string}`} */
const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb";
const borrowEvent = parseAbiItem(
  "event Borrow(bytes32 indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares)",
);

/**
 * The all-in-one `cache` transport, run twice over the same range.
 *
 * `binSize` sets cache granularity and the divider's `alignTo` follows it, so requests land on bin
 * boundaries and the second pass is served entirely from the store. Only the `eth_blockNumber`
 * preflight goes out on the warm pass.
 *
 * The store is an in-memory `LruStore`; a reload starts cold again. `NodeFsStore` would persist it
 * outside a browser.
 *
 * @type {import("../../src/tutorials/types.js").Tab<import("../../src/tutorials/shared.js").RangeContext>}
 */
const run = async ({ transport, chain, settings, range, log }) => {
  const client = createPublicClient({
    chain,
    transport: cache(transport, [
      {
        binSize: Number(settings.binSize),
        store: new LruStore({ maxBytes: 100_000_000 }),
        invalidationStrategy: createSimpleInvalidation(),
      },
      // Base's public endpoint rejects anything wider with a 2,000-range error (as HTTP 413);
      // the divider would otherwise halve its way down to a workable size.
      { maxBlockRange: 2_000 },
      { retryCount: 3, retryDelay: 1_000, blockTimestamp: false },
      { maxBytes: 8_192 },
      { maxRequestsPerSecond: 8, maxBurstRequests: 2, maxConcurrentRequests: 3 },
    ]),
  });

  const query = { address: MORPHO, event: borrowEvent, strict: true, ...range };

  log("cold pass — nothing in the store yet");
  const coldStart = performance.now();
  const cold = await getLogs2(client, query);
  const coldMs = performance.now() - coldStart;

  log(`warm pass — ${cold.length} logs now binned in the store`);
  const warmStart = performance.now();
  const warm = await getLogs2(client, query);
  const warmMs = performance.now() - warmStart;

  return {
    summary: {
      logs: cold.length,
      cold: `${coldMs.toFixed(0)} ms`,
      warm: `${warmMs.toFixed(0)} ms`,
      speedup: `${(coldMs / warmMs).toFixed(1)}x`,
      agree: cold.length === warm.length ? "yes" : "NO",
    },
  };
};

export default run;
