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
 * The five layers, stated once.
 *
 * Drop `maxRequestsPerSecond` and the elapsed time rises while the request count doesn't move: the
 * limiter changes when requests leave, never how many.
 *
 * `blockTimestamp` is the enricher's backfill. Base is an OP-stack chain and already returns
 * `blockTimestamp` on every log, so turning it on costs nothing here. The summary reports whether
 * the endpoint supplied them. On a chain that doesn't, the enricher fetches one block per distinct
 * block that carried a log, which is the number below.
 *
 * @type {import("../../src/tutorials/types.js").Tab<import("../../src/tutorials/shared.js").RangeContext & { settings: { rps: string; blockTimestamp: string; maxBytes: string } }>}
 */
const run = async ({ transport, chain, settings, range, log }) => {
  const client = createPublicClient({
    chain,
    transport: cache(transport, [
      // 1. cache — the store, its granularity, and when an entry stops being trusted.
      {
        binSize: Number(settings.binSize),
        store: new LruStore({ maxBytes: 100_000_000 }),
        invalidationStrategy: createSimpleInvalidation(),
      },
      // 2. logsDivider — how wide a chunk may be before it is split, and what happens when one fails.
      { maxBlockRange: 2_000 },
      // 3. logsEnricher — retries, and whether each log carries its block timestamp.
      {
        retryCount: 3,
        retryDelay: 1_000,
        blockTimestamp: settings.blockTimestamp === "true",
      },
      // 4. logsSieve — the per-log byte ceiling; anything larger is dropped rather than carried.
      { maxBytes: Number(settings.maxBytes) },
      // 5. rateLimiter — what this provider will tolerate, stated once for every call beneath it.
      { maxRequestsPerSecond: Number(settings.rps), maxBurstRequests: 2, maxConcurrentRequests: 3 },
    ]),
  });

  const started = performance.now();
  const logs = await getLogs2(client, { address: MORPHO, event: borrowEvent, strict: true, ...range });
  const ms = performance.now() - started;

  const withTimestamp = logs.filter((entry) => entry.blockTimestamp !== undefined).length;
  const blocks = new Set(logs.map((entry) => entry.blockNumber)).size;
  log(`${logs.length} logs across ${blocks} distinct blocks; ${withTimestamp} carry a timestamp`);
  log(
    withTimestamp === logs.length
      ? "this endpoint returns timestamps itself, so the enricher has nothing to backfill"
      : `the enricher would fetch ${blocks} blocks to backfill the rest`,
  );

  return {
    summary: {
      logs: logs.length,
      "distinct blocks": blocks,
      "timestamps already present": withTimestamp,
      elapsed: `${ms.toFixed(0)} ms`,
    },
  };
};

export default run;
