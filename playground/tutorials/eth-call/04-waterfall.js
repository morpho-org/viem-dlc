import { createPublicClient, parseAbi } from "viem";
import { multicall } from "viem/actions";

/**
 * The snapshot without a lens. Each read depends on the one before it, so the rounds are sequential
 * no matter how wide you batch: you cannot ask for a vault's asset decimals until you know its
 * asset, and you cannot know its asset until you've called the vault.
 *
 * Batching makes each round cheap. It does nothing about the number of rounds, because the
 * dependency is in your data, not in your transport.
 *
 * @type {import("../../src/tutorials/types.js").Tab<import("../../src/tutorials/eth-call.js").EthCallContext>}
 */
const run = async ({ transport, chain, settings, vaults, log }) => {
  const client = createPublicClient({ chain, transport });

  const abi = parseAbi([
    "function totalAssets() view returns (uint256)",
    "function asset() view returns (address)",
    "function decimals() view returns (uint8)",
  ]);

  const count = Number(settings.vaults);
  const addresses = vaults.slice(0, count);
  /**
   * @param {readonly `0x${string}`[]} targets
   * @param {"totalAssets" | "asset" | "decimals"} functionName
   */
  const read = (targets, functionName) =>
    multicall(client, {
      contracts: targets.map((address) => ({ address, abi, functionName })),
      allowFailure: true,
    });

  let rounds = 0;

  log("round 1: totalAssets");
  const totals = await read(addresses, "totalAssets");
  rounds += 1;

  log("round 2: asset");
  const assets = await read(addresses, "asset");
  rounds += 1;

  log("round 3: decimals, on the assets round 2 discovered");
  const known = /** @type {`0x${string}`[]} */ (
    assets.flatMap((entry) => (entry.status === "success" ? [entry.result] : []))
  );
  const decimals = await read(known, "decimals");
  rounds += 1;

  const served = totals.filter((entry) => entry.status === "success").length;
  log(`${served} snapshots in ${rounds} dependent rounds`);

  return {
    summary: {
      served,
      rounds,
      "decimals read": decimals.filter((entry) => entry.status === "success").length,
    },
  };
};

export default run;
