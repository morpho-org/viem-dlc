import { readLens } from "@morpho-org/viem-dlc/actions";

/**
 * Delivery by state override. The envelope is placed at a fixed address through `eth_call`'s
 * state-override parameter instead of being created from initcode, so EIP-3860's byte cap does not
 * apply and the frame's gas becomes the only bound — the same elements collapse into far fewer,
 * far larger chunks. A provider that ignores overrides is detected on the opening wave and the
 * request finishes as initcode, counted in `override_fallbacks_unsupported`.
 *
 * @type {import("../src/tab.js").Tab}
 */
const run = async ({ client, lens, inputs, log }) => {
  log(`reading ${inputs.length} positions through a state override`);

  const { results, skipped } = await readLens(client, {
    ...lens.with(),
    functionName: "positionOf",
    args: inputs,
    batch: {
      envelope: "override",
      gas: { fixed: 242_000, item: { avg: 7_300, stddev: 150 } },
    },
  });

  const borrowing = results.filter((p) => p.borrowShares > 0n).length;
  log(`${borrowing} of ${results.length} have outstanding borrow shares`);

  return { results, skipped };
};

export default run;
