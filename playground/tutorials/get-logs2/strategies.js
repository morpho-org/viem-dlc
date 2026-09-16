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
 * Three ways to narrow one shared cache to a single account, over an identical warm range.
 *
 * 1. fetch everything, then filter — decodes every log, holds them all
 * 2. `reduce` — folds per bin as logs decode, so memory tracks matches rather than the result set
 * 3. `search` + `reduce` — `search` is a regex matched against raw NDJSON *before* parsing, so bins
 *    that cannot contain the address never pay for `JSON.parse` at all
 *
 * All three must return the same count. The gap between them is parsing, not network: the cache is
 * warm for every pass.
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

  log("warming the cache…");
  const all = await getLogs2(client, query);
  if (all.length === 0) throw new Error("No Borrow logs in this range — widen it.");

  // The busiest borrower in the range. Picking a one-off would make all three strategies look
  // identical — the point is the work each avoids, which only shows when there is work to avoid.
  const counts = new Map();
  for (const entry of all) counts.set(entry.args.onBehalf, (counts.get(entry.args.onBehalf) ?? 0) + 1);
  const [target, appearances] = [...counts.entries()].sort((a, b) => b[1] - a[1])[0] ?? [];
  if (!target) throw new Error("No borrower found in this range.");
  log(`busiest borrower ${target} appears ${appearances} times in ${all.length} logs`);

  /**
   * @param {string} label
   * @param {() => Promise<number>} fn
   */
  const time = async (label, fn) => {
    const started = performance.now();
    const count = await fn();
    const ms = performance.now() - started;
    log(`${label}: ${count} matches in ${ms.toFixed(0)} ms`);
    return { count, ms };
  };

  const filtered = await time(
    "filter",
    async () => (await getLogs2(client, query)).filter((l) => l.args.onBehalf === target).length,
  );

  const reduced = await time("reduce", async () => {
    const matches = await getLogs2(client, {
      ...query,
      reduce: (acc, log) => {
        if (log.args.onBehalf === target) acc.push(log);
        return acc;
      },
    });
    return matches.length;
  });

  const searched = await time("search + reduce", async () => {
    const matches = await getLogs2(client, {
      ...query,
      search: target.slice(2).toLowerCase(),
      reduce: (acc, log) => {
        if (log.args.onBehalf === target) acc.push(log);
        return acc;
      },
    });
    return matches.length;
  });

  return {
    summary: {
      scanned: all.length,
      matches: filtered.count,
      filter: `${filtered.ms.toFixed(0)} ms`,
      reduce: `${reduced.ms.toFixed(0)} ms`,
      search: `${searched.ms.toFixed(0)} ms`,
      agree: filtered.count === reduced.count && reduced.count === searched.count ? "yes" : "NO",
    },
  };
};

export default run;
