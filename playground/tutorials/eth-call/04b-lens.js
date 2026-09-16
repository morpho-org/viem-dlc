import { readLens } from "@morpho-org/viem-dlc/actions";
import { deployless } from "@morpho-org/viem-dlc/transports";
import { createPublicClient } from "viem";

/**
 * The same three reads, expressed where the dependency lives. Inside `snapshotOf` the vault, its
 * asset and that asset's decimals are three ordinary Solidity calls in one frame, so the three
 * rounds collapse into one.
 *
 * Open the lens above and read it: the waterfall is still there, written as the straight line it
 * always was.
 *
 * @type {import("../../src/tutorials/types.js").Tab<import("../../src/tutorials/eth-call.js").EthCallContext>}
 */
const run = async ({ transport, chain, settings, lens, vaults, log }) => {
  const client = createPublicClient({ chain, transport: deployless(transport) });

  const count = Number(settings.vaults);
  const inputs = vaults.slice(0, count).map((vault) => ({ vault, grief: 0n }));

  const { results, skipped } = await readLens(client, {
    ...lens.with(),
    functionName: "snapshotOf",
    args: inputs,
  });

  log(`${results.length} snapshots, each carrying totalAssets, asset and decimals`);

  return {
    summary: {
      served: results.length,
      skipped: skipped.length,
      rounds: 1,
    },
  };
};

export default run;
