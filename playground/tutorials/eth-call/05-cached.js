import { readLens } from "@morpho-org/viem-dlc/actions";
import { LruStore } from "@morpho-org/viem-dlc/stores";
import { cache, createSimpleInvalidation } from "@morpho-org/viem-dlc/transports/cache";
import { createPublicClient } from "viem";

/**
 * The same lens read, through `cache` instead of `deployless`.
 *
 * `cache` handles `eth_call` as well as `eth_getLogs`, and for a lens read the unit of caching is
 * the **element**: each one's result is stored under `blobKey` for `ttl` milliseconds. A repeat
 * element is answered from the store and never reaches the envelope, so only novel elements cost a
 * request. `delta` spreads expiries so a blob filled in one burst doesn't expire in one burst.
 *
 * Four passes: half the corpus cold, the same half again, then the whole corpus — half of which has
 * never been seen — and finally the whole corpus warm. Watch `elements_fetched` against
 * `elements_requested`: on the third pass only the new half goes upstream.
 *
 * @type {import("../../src/tutorials/types.js").Tab<import("../../src/tutorials/eth-call.js").EthCallContext>}
 */
const run = async ({ transport, chain, settings, lens, vaults, log }) => {
  const client = createPublicClient({
    chain,
    transport: cache(transport, [
      {
        binSize: 10_000,
        store: new LruStore({ maxBytes: 100_000_000 }),
        invalidationStrategy: createSimpleInvalidation(),
      },
      { maxBlockRange: 2_000 },
      { retryCount: 3, retryDelay: 1_000, blockTimestamp: false },
      { maxBytes: 8_192 },
      { maxRequestsPerSecond: 8, maxBurstRequests: 2, maxConcurrentRequests: 3 },
    ]),
  });

  const count = Number(settings.vaults);
  const inputs = vaults.slice(0, count).map((vault) => ({ vault, grief: 0n }));

  /** @param {typeof inputs} args @param {string} label */
  const read = async (args, label) => {
    const started = performance.now();
    const { results } = await readLens(client, {
      ...lens.with(),
      functionName: "snapshotOf",
      args,
      cache: { blobKey: "vault-snapshots", ttl: 300_000, delta: 10_000 },
    });
    const ms = performance.now() - started;
    log(`${label}: ${results.length} of ${args.length} in ${ms.toFixed(0)} ms`);
    return ms;
  };

  // Fill with half the corpus first, so the third pass has genuinely novel elements to fetch.
  const half = inputs.slice(0, Math.floor(inputs.length / 2));

  const cold = await read(half, "cold, nothing cached");
  const warm = await read(half, "warm, same elements");
  const mixed = await read(inputs, "half of these are new");
  const again = await read(inputs, "warm again");

  return {
    summary: {
      cold: `${cold.toFixed(0)} ms`,
      warm: `${warm.toFixed(0)} ms`,
      "half new": `${mixed.toFixed(0)} ms`,
      "warm again": `${again.toFixed(0)} ms`,
      speedup: `${(cold / warm).toFixed(1)}x`,
    },
  };
};

export default run;
