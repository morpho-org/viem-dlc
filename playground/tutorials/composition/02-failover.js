import { getLogs2 } from "@morpho-org/viem-dlc/actions";
import { LruStore } from "@morpho-org/viem-dlc/stores";
import { defaultShouldThrow, failover } from "@morpho-org/viem-dlc/transports";
import { cache, createSimpleInvalidation } from "@morpho-org/viem-dlc/transports/cache";
import { createPublicClient, http, parseAbiItem } from "viem";

/** @type {`0x${string}`} */
const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb";
const borrowEvent = parseAbiItem(
  "event Borrow(bytes32 indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares)",
);

/**
 * Two providers, one store, different limits.
 *
 * The first branch points at an endpoint that does not exist, standing in for a provider that is
 * down. The second is the endpoint configured above. Each branch is a **complete `cache` stack
 * built once**, so its rate limiter, its coalescing state and its knowledge of its own block-range
 * limit persist across requests — which is what viem's `fallback` cannot do, because it only
 * dispatches between bare transports.
 *
 * The store is shared, so whatever branch A managed to fetch before it failed is already visible to
 * branch B. Read `succeeded_index` in the table: 0 means the primary served it, 1 means it fell
 * through.
 *
 * @type {import("../../src/tutorials/types.js").Tab<import("../../src/tutorials/shared.js").RangeContext & { settings: { primaryUrl: string } }>}
 */
const run = async ({ transport, chain, settings, range, log }) => {
  // One store behind both branches. Progress made through either is progress for both.
  const store = new LruStore({ maxBytes: 100_000_000 });
  const shared = {
    binSize: Number(settings.binSize),
    store,
    invalidationStrategy: createSimpleInvalidation(),
  };
  const tail = /** @type {const} */ ([
    { retryCount: 2, retryDelay: 500, blockTimestamp: false },
    { maxBytes: 8_192 },
    { maxRequestsPerSecond: 8, maxBurstRequests: 2, maxConcurrentRequests: 3 },
  ]);

  const client = createPublicClient({
    chain,
    transport: failover(
      [
        // Branch A: a provider that is not answering. Its limits are still stated here, because
        // they are facts about that provider rather than about this call.
        cache(http(settings.primaryUrl, { retryCount: 0, timeout: 4_000 }), [
          shared,
          { maxBlockRange: 100_000 },
          ...tail,
        ]),
        // Branch B: the endpoint from the box above, which allows far narrower ranges.
        cache(transport, [shared, { maxBlockRange: 2_000 }, ...tail]),
      ],
      {
        // An auth or billing failure will not get better on the next provider either, so let it
        // through instead of burning the fallback on it.
        shouldThrow: (error) =>
          defaultShouldThrow(error) || [401, 402, 403].includes(/** @type {{status?: number}} */ (error).status ?? 0),
      },
    ),
  });

  log(`primary: ${settings.primaryUrl}`);
  const started = performance.now();
  const logs = await getLogs2(client, { address: MORPHO, event: borrowEvent, strict: true, ...range });
  const ms = performance.now() - started;
  log(`${logs.length} logs in ${ms.toFixed(0)} ms`);

  return { summary: { logs: logs.length, elapsed: `${ms.toFixed(0)} ms` } };
};

export default run;
