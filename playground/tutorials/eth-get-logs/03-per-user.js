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
 * The obvious cache: ask for one borrower's logs, and let the store keep them.
 *
 * The blob key is `hash(address, topics)`, and `args` puts the borrower into `topics`. So every
 * borrower gets a blob of their own, warm only for whoever paid to fill it. One store is shared by
 * everything here, exactly as a server would hold one — it doesn't help, because the keys differ.
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
      { maxBlockRange: 2_000 },
      { retryCount: 3, retryDelay: 1_000, blockTimestamp: false },
      { maxBytes: 8_192 },
      { maxRequestsPerSecond: 8, maxBurstRequests: 2, maxConcurrentRequests: 3 },
    ]),
  });

  // One broad pass, only to find two borrowers who actually appear in this range.
  const all = await getLogs2(client, { address: MORPHO, event: borrowEvent, strict: true, ...range });
  if (all.length === 0) throw new Error("No Borrow logs in this range — widen it.");

  const counts = new Map();
  for (const entry of all) counts.set(entry.args.onBehalf, (counts.get(entry.args.onBehalf) ?? 0) + 1);
  const ranked = [...counts.entries()].sort((a, b) => b[1] - a[1]);
  const [first, second] = [ranked[0]?.[0], ranked[1]?.[0]];
  if (!first || !second) throw new Error("Need two distinct borrowers in this range — widen it.");

  /** @param {`0x${string}`} user */
  const forUser = async (user) => {
    const started = performance.now();
    const logs = await getLogs2(client, {
      address: MORPHO,
      event: borrowEvent,
      // This is the line that makes the blob private to `user`.
      args: { onBehalf: user },
      strict: true,
      ...range,
    });
    const ms = performance.now() - started;
    log(`${user}: ${logs.length} logs in ${ms.toFixed(0)} ms`);
    return { count: logs.length, ms };
  };

  const a = await forUser(first);
  const b = await forUser(second);

  return {
    summary: {
      "borrower A": `${a.ms.toFixed(0)} ms`,
      "borrower B": `${b.ms.toFixed(0)} ms`,
      "A logs": a.count,
      "B logs": b.count,
    },
  };
};

export default run;
