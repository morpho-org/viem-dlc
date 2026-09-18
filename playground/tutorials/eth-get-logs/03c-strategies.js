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
 * Four passes over one warm blob. The network is out of the picture: every pass reads the same
 * cached bytes, so what differs is work done in this process.
 *
 * `reduce` runs inside the transport as each bin decodes, so only the accumulator survives the pass.
 * `search` is matched against a bin's raw NDJSON *before* it is parsed, so a bin that cannot contain
 * the borrower is never decoded. `parsed` counts the logs each pass actually handed to JavaScript.
 *
 * The last two passes use the same code against different borrowers, which is the whole point:
 * `search` saves exactly as much as your target is rare.
 *
 * @type {import("../../src/tutorials/types.js").Tab<import("../../src/tutorials/shared.js").RangeContext>}
 */
const run = async ({ transport, chain, settings, range, log }) => {
  const binSize = BigInt(settings.binSize);
  const client = createPublicClient({
    chain,
    transport: cache(transport, [
      {
        binSize: Number(binSize),
        store: new LruStore({ maxBytes: 100_000_000 }),
        invalidationStrategy: createSimpleInvalidation(),
      },
      { maxBlockRange: 2_000 },
      { retryCount: 3, retryDelay: 1_000, blockTimestamp: false },
      { maxBytes: 8_192 },
      { maxRequestsPerSecond: 8, maxBurstRequests: 2, maxConcurrentRequests: 3 },
    ]),
  });

  const query = { address: MORPHO, event: borrowEvent, strict: true, ...range };

  log("warming the range");
  const all = await getLogs2(client, query);
  if (all.length === 0) throw new Error("No Borrow logs in this range — widen it.");

  const counts = new Map();
  for (const entry of all) counts.set(entry.args.onBehalf, (counts.get(entry.args.onBehalf) ?? 0) + 1);
  const ranked = [...counts.entries()].sort((a, b) => b[1] - a[1]);
  const busiest = ranked[0]?.[0];
  const rarest = ranked[ranked.length - 1]?.[0];
  if (!busiest || !rarest) throw new Error("No borrowers in this range.");

  /** @param {string} who */
  const binsHolding = (who) =>
    new Set(all.filter((l) => l.args.onBehalf === who).map((l) => l.blockNumber / binSize)).size;
  const bins = new Set(all.map((l) => l.blockNumber / binSize)).size;
  log(`${all.length} logs in ${bins} bins`);
  log(`busiest borrower sits in ${binsHolding(busiest)} bins, rarest in ${binsHolding(rarest)}`);

  /**
   * @param {string} label
   * @param {() => Promise<{ matches: number; parsed: number }>} fn
   */
  const time = async (label, fn) => {
    const started = performance.now();
    const out = await fn();
    const ms = performance.now() - started;
    log(`${label}: ${out.matches} matches, ${out.parsed} parsed, ${ms.toFixed(0)} ms`);
    return { ...out, ms };
  };

  /** @param {string} who @param {boolean} useSearch */
  const narrow = async (who, useSearch) => {
    let parsed = 0;
    const matches = await getLogs2(client, {
      ...query,
      ...(useSearch ? { search: who.slice(2).toLowerCase() } : {}),
      reduce: (acc, entry) => {
        parsed += 1;
        if (entry.args.onBehalf === who) acc.push(entry);
        return acc;
      },
    });
    return { matches: matches.length, parsed };
  };

  const filtered = await time("filter", async () => {
    const everything = await getLogs2(client, query);
    return { matches: everything.filter((l) => l.args.onBehalf === busiest).length, parsed: everything.length };
  });
  const reduced = await time("reduce", () => narrow(busiest, false));
  const searchBusy = await time("search, common target", () => narrow(busiest, true));
  const searchRare = await time("search, rare target", () => narrow(rarest, true));

  return {
    summary: {
      "logs in range": all.length,
      "filter parsed": filtered.parsed,
      "reduce parsed": reduced.parsed,
      "search parsed (common)": searchBusy.parsed,
      "search parsed (rare)": searchRare.parsed,
      agree: filtered.matches === reduced.matches && reduced.matches === searchBusy.matches ? "yes" : "NO",
    },
  };
};

export default run;
