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
 * The same two borrowers, served from one blob.
 *
 * Drop the borrower from `topics` and every borrower shares a key. The first request fills the
 * range; every borrower after that is a read of bytes you already hold, narrowed by `search` and
 * `reduce` rather than by another round trip.
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

  // One key for everyone: address and event, no borrower.
  const query = { address: MORPHO, event: borrowEvent, strict: true, ...range };

  const filled = performance.now();
  const all = await getLogs2(client, query);
  log(`filled the range: ${all.length} logs in ${(performance.now() - filled).toFixed(0)} ms`);
  if (all.length === 0) throw new Error("No Borrow logs in this range; widen it.");

  const counts = new Map();
  for (const entry of all) counts.set(entry.args.onBehalf, (counts.get(entry.args.onBehalf) ?? 0) + 1);
  const ranked = [...counts.entries()].sort((a, b) => b[1] - a[1]);
  const [first, second] = [ranked[0]?.[0], ranked[1]?.[0]];
  if (!first || !second) throw new Error("Need two distinct borrowers in this range; widen it.");

  /** @param {`0x${string}`} user */
  const forUser = async (user) => {
    const started = performance.now();
    const logs = await getLogs2(client, {
      ...query,
      // Matched against each bin's raw NDJSON before anything is parsed.
      search: user.slice(2).toLowerCase(),
      reduce: (acc, entry) => {
        if (entry.args.onBehalf === user) acc.push(entry);
        return acc;
      },
    });
    const ms = performance.now() - started;
    log(`${user}: ${logs.length} logs in ${ms.toFixed(0)} ms, no upstream request`);
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
