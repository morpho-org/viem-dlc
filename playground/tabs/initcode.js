import { readLens } from "@morpho-org/viem-dlc/actions";

/**
 * Delivery as initcode — the default. Each chunk is EIP-3860 initcode, so it is capped at 49 152
 * bytes, about 690 pairs at 64 B each, however much gas the frame has. Watch `chunks_initcode`
 * climb with the element count while `nominal_batches` stays at 1.
 *
 * @type {import("../src/tab.js").Tab}
 */
const run = async ({ client, lens, inputs, log }) => {
  log(`reading ${inputs.length} positions as initcode`);

  const { results, skipped } = await readLens(client, {
    ...lens.with(),
    functionName: "positionOf",
    args: inputs,
    batch: {
      // Measured under `withLogging`; see examples/10-observability.ts.
      gas: { fixed: 242_000, item: { avg: 7_300, stddev: 150 } },
    },
  });

  const borrowing = results.filter((p) => p.borrowShares > 0n).length;
  log(`${borrowing} of ${results.length} have outstanding borrow shares`);

  return { results, skipped };
};

export default run;
