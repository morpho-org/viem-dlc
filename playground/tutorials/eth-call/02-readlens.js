import { readLens } from "@morpho-org/viem-dlc/actions";
import { deployless } from "@morpho-org/viem-dlc/transports";
import { createPublicClient } from "viem";

/**
 * The same vaults, the same lens, the same grief element — through `deployless` instead.
 *
 * The envelope calls `snapshotOf` once per element in its own frame. A frame that runs out of gas
 * reports how far it got, the transport re-packs what it did not reach, and an element that dies
 * even when it runs alone comes back in `skipped`. Nothing here is tuned: there is no batch size,
 * no gas figure, and no retry policy in this file.
 *
 * @type {import("../../src/tutorials/types.js").Tab<import("../../src/tutorials/eth-call.js").EthCallContext>}
 */
const run = async ({ transport, chain, settings, lens, vaults, log }) => {
  const client = createPublicClient({ chain, transport: deployless(transport) });

  const count = Number(settings.vaults);
  const grief = BigInt(settings.grief);
  const inputs = vaults.slice(0, count).map((vault, i) => ({ vault, grief: i === 0 ? grief : 0n }));

  log(`${inputs.length} vaults through a lens`);

  const { results, skipped } = await readLens(client, {
    ...lens.with(),
    functionName: "snapshotOf",
    args: inputs,
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
