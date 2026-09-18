import { readLens } from "@morpho-org/viem-dlc/actions";
import { deployless } from "@morpho-org/viem-dlc/transports";
import { createPublicClient } from "viem";

/**
 * The same read with every hint the previous run reported, copied straight off its wide event:
 * `fixed_gas`, `item_gas_avg` and `item_gas_stddev` into `batch.gas`, `gas_limit_observed` into the
 * transport's `gasLimit`.
 *
 * Expect the same request count. The hints size the opening wave, and the opening wave here is
 * bounded by bytes rather than by gas, so there is nothing for them to improve. The hints are an
 * optimization you may skip. A wrong one costs a round trip.
 *
 * @type {import("../../src/tutorials/types.js").Tab<import("../../src/tutorials/eth-call.js").EthCallContext>}
 */
const run = async ({ transport, chain, settings, lens, vaults, log }) => {
  const client = createPublicClient({
    chain,
    // `gas_limit_observed` from the previous run: the cap Base actually granted.
    transport: deployless(transport, { gasLimit: 600_000_000 }),
  });

  const count = Number(settings.vaults);
  const grief = BigInt(settings.grief);
  const inputs = vaults.slice(0, count).map((vault, i) => ({ vault, grief: i === 0 ? grief : 0n }));

  const { results, skipped } = await readLens(client, {
    ...lens.with(),
    functionName: "snapshotOf",
    args: inputs,
    batch: {
      // Measured, not guessed. Read them off the table under the previous tab.
      gas: { fixed: 288_653, item: { avg: 103_090, stddev: 86_160 } },
    },
  });

  log(`${results.length} served, ${skipped.length} skipped`);

  return {
    summary: {
      served: results.length,
      skipped: skipped.length,
    },
  };
};

export default run;
