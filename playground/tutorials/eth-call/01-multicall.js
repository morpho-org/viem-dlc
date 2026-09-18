import { createPublicClient } from "viem";
import { multicall } from "viem/actions";

/**
 * Multicall3 over the same lens the next step uses, placed at a fixed address by state override so
 * both sides run identical code.
 *
 * `batchSize` is viem's only lever, and it counts bytes. Leave it at the 1024-byte default and you
 * pay one request per handful of vaults. Raise it and the requests collapse, until one batch costs
 * more gas than the node will spend. That batch returns nothing.
 *
 * @type {import("../../src/tutorials/types.js").Tab<import("../../src/tutorials/eth-call.js").EthCallContext>}
 */
const run = async ({ transport, chain, settings, lens, vaults, log }) => {
  const client = createPublicClient({ chain, transport });

  const count = Number(settings.vaults);
  const grief = BigInt(settings.grief);
  // One vault carries the extra cost. Which one does not matter.
  const inputs = vaults.slice(0, count).map((vault, i) => ({ vault, grief: i === 0 ? grief : 0n }));

  log(`${inputs.length} vaults through Multicall3 at batchSize=${settings.batchSize}`);

  const results = await multicall(client, {
    contracts: inputs.map((input) => ({
      address: lens.stateOverride.address,
      abi: lens.abi,
      functionName: "snapshotOf",
      args: [input],
    })),
    // The default. `true` catches a revert in one element; it does not catch one element spending
    // the frame, because gas is shared and the failure lands on whoever comes after.
    allowFailure: true,
    batchSize: Number(settings.batchSize),
    stateOverride: [lens.stateOverride],
  });

  const served = results.filter((result) => result.status === "success");
  log(`${served.length} of ${results.length} elements came back`);

  return {
    summary: {
      served: served.length,
      lost: results.length - served.length,
    },
  };
};

export default run;
