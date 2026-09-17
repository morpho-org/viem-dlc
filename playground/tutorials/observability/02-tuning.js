import { withLogging } from "@morpho-org/viem-dlc";
import { readLens } from "@morpho-org/viem-dlc/actions";
import { deployless } from "@morpho-org/viem-dlc/transports";
import { createPublicClient } from "viem";

/**
 * The loop the `eth_call` tutorial opens, closed.
 *
 * Run the lens without stating a single figure. The event that comes back carries what the frame
 * cost — and those are exactly the numbers `gasLimit` and `batch.gas` want. This prints the config
 * to paste.
 *
 * Take them over a representative window. They are properties of the lens and the provider, not of
 * one request, and the only thing a wrong value costs is a round trip.
 *
 * @type {import("../../src/tutorials/types.js").Tab<import("../../src/tutorials/observability.js").ObservabilityContext>}
 */
const run = async ({ transport, chain, settings, lens, vaults, log }) => {
  /** @type {Record<string, unknown>[]} */
  const events = [];

  /**
   * The same dozen lines as the previous step, keeping only the fields.
   *
   * @param {Record<string, unknown>} context
   * @returns {import("@morpho-org/viem-dlc").Logger}
   */
  const sink = (context) => {
    /** @param {Record<string, unknown>} extra */
    const push = (extra) => () => {
      events.push({ ...context, ...extra });
    };
    /** @param {Record<string, unknown>} extra @returns {import("@morpho-org/viem-dlc").LogBuilder} */
    const builder = (extra) => ({
      withMetadata: (metadata) => builder({ ...extra, ...metadata }),
      withError: (error) => builder({ ...extra, error: `${error}` }),
      info: push(extra),
      warn: push(extra),
      error: push(extra),
    });
    return {
      child: () => sink({ ...context }),
      withContext: (added) => sink({ ...context, ...added }),
      metadataOnly: (metadata) => events.push({ ...context, ...metadata }),
      ...builder({}),
    };
  };

  const client = createPublicClient({ chain, transport: deployless(transport) });
  const inputs = vaults.slice(0, Number(settings.vaults)).map((vault) => ({ vault, grief: 0n }));

  const { results } = await withLogging(
    () => readLens(client, { ...lens.with(), functionName: "snapshotOf", args: inputs }),
    { logger: sink({}), service: "playground" },
  );

  const fields = Object.assign({}, ...events);
  /** @param {string} name */
  const facet = (name) => {
    const key = Object.keys(fields).find((k) => k === name || k.endsWith(`.${name}`));
    return key === undefined ? undefined : Math.round(Number(fields[key]));
  };

  const gasLimit = facet("gas_limit_observed");
  const fixed = facet("fixed_gas");
  const avg = facet("item_gas_avg");
  const stddev = facet("item_gas_stddev");

  log(`measured over ${results.length} elements:`);
  log(`  deployless(http(url), { gasLimit: ${gasLimit} })`);
  log(`  batch: { gas: { fixed: ${fixed}, item: { avg: ${avg}, stddev: ${stddev} } } }`);

  // The decision rule for `envelope: "override"`, straight off the same event.
  const requested = facet("elements_requested");
  const batches = facet("nominal_batches");
  const affordable = gasLimit && fixed && avg ? Math.floor((gasLimit - fixed) / avg) : undefined;
  const perChunk = requested && batches ? Math.round(requested / batches) : undefined;
  if (affordable && perChunk) {
    log(`gas would pay for ~${affordable} elements per frame; this request packed ${perChunk} per chunk`);
    // Only meaningful once the request is large enough to fill a chunk: below that, the packing is
    // bounded by what you asked for rather than by bytes, and the ratio says nothing.
    if (batches === 1 && perChunk < 600) log("too few elements to tell what binds — raise the count");
    else log(affordable > perChunk * 2 ? "bytes bind — consider envelope: 'override'" : "gas binds — initcode is fine");
  }

  return {
    summary: {
      gasLimit: gasLimit ?? "n/a",
      fixed_gas: fixed ?? "n/a",
      item_gas_avg: avg ?? "n/a",
      item_gas_stddev: stddev ?? "n/a",
      "gas affords": affordable ?? "n/a",
      "elements per chunk": perChunk ?? "n/a",
    },
  };
};

export default run;
