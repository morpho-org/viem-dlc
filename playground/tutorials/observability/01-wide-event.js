import { withLogging } from "@morpho-org/viem-dlc";
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
 * Nothing is emitted until you ask. Outside a `withLogging` scope the library is silent; inside it,
 * every outermost transport call produces exactly one event when it concludes.
 *
 * `logger` is any object of the right shape. This one is a dozen lines and keeps what it is given;
 * a real LogLayer satisfies the same interface without adaptation.
 *
 * @type {import("../../src/tutorials/types.js").Tab<import("../../src/tutorials/shared.js").RangeContext>}
 */
const run = async ({ transport, chain, settings, range, log }) => {
  /** @type {{ message: string; fields: Record<string, unknown> }[]} */
  const events = [];

  /**
   * Collects instead of printing. This is the entire `Logger` interface.
   *
   * @param {Record<string, unknown>} context
   * @returns {import("@morpho-org/viem-dlc").Logger}
   */
  const collect = (context) => {
    /** @param {Record<string, unknown>} extra */
    const emit =
      (extra) =>
      /** @param {string} [message] */
      (message) => {
        events.push({ message: message ?? "", fields: { ...context, ...extra } });
      };
    /** @param {Record<string, unknown>} extra @returns {import("@morpho-org/viem-dlc").LogBuilder} */
    const builder = (extra) => ({
      withMetadata: (metadata) => builder({ ...extra, ...metadata }),
      withError: (error) => builder({ ...extra, error: `${error}` }),
      info: emit(extra),
      warn: emit(extra),
      error: emit(extra),
    });
    return {
      child: () => collect({ ...context }),
      withContext: (added) => collect({ ...context, ...added }),
      metadataOnly: (metadata) => events.push({ message: "", fields: { ...context, ...metadata } }),
      ...builder({}),
    };
  };

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

  const query = { address: MORPHO, event: borrowEvent, strict: true, ...range };

  // Outside the scope: no events, whatever the transports do.
  await getLogs2(client, query);
  log(`before withLogging: ${events.length} events`);

  // Inside it: one event per outermost call. `service` and anything else here is stamped on each.
  const logs = await withLogging(() => getLogs2(client, query), {
    logger: collect({}),
    service: "playground",
    tutorial: "observability",
  });
  log(`inside withLogging: ${events.length} event for ${logs.length} logs`);

  const fields = events.at(-1)?.fields ?? {};
  const namespaces = [...new Set(Object.keys(fields).flatMap((k) => (k.includes(".") ? [k.split(".")[0]] : [])))];
  for (const namespace of namespaces) log(`contributed by ${namespace}`);

  return {
    summary: {
      "events outside": 0,
      "events inside": events.length,
      "fields on the event": Object.keys(fields).length,
      "transports that contributed": namespaces.length,
    },
  };
};

export default run;
