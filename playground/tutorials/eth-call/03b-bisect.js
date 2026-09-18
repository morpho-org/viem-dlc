import { createPublicClient } from "viem";
import { multicall } from "viem/actions";

/**
 * What you write once you've been bitten: raise `batchSize` for the request count, then halve any
 * batch that comes back empty and retry it, down to single elements.
 *
 * It works. It also costs a failed request for every split, and the splits are pure waste, since the
 * node spent real time on each one before giving up. Compare its request count with the previous
 * section's, remembering that the public endpoint's rate limit sets the wall clock here, so the
 * request count is the figure worth reading.
 *
 * @type {import("../../src/tutorials/types.js").Tab<import("../../src/tutorials/eth-call.js").EthCallContext>}
 */
const run = async ({ transport, chain, settings, lens, vaults, log }) => {
  const client = createPublicClient({ chain, transport });

  const count = Number(settings.vaults);
  const grief = BigInt(settings.grief);
  const inputs = vaults.slice(0, count).map((vault, i) => ({ vault, grief: i === 0 ? grief : 0n }));

  /** @param {{ vault: `0x${string}`; grief: bigint }} input */
  const contractFor = (input) => ({
    address: lens.stateOverride.address,
    abi: lens.abi,
    functionName: "snapshotOf",
    args: [input],
  });

  let splits = 0;

  /**
   * Returns one entry per element: the snapshot, or `undefined` where even a lone call failed.
   *
   * @param {{ vault: `0x${string}`; grief: bigint }[]} range
   * @returns {Promise<(unknown | undefined)[]>}
   */
  const fetchRange = async (range) => {
    try {
      const out = await multicall(client, {
        contracts: range.map(contractFor),
        allowFailure: true,
        // One batch per call, so a failure localizes to exactly this range.
        batchSize: 0,
        stateOverride: [lens.stateOverride],
      });
      // A whole-batch gas failure and a single reverting element look identical from here, so the
      // only safe reading of "any failure" is "retry this range smaller".
      if (out.every((entry) => entry.status === "success")) return out.map((entry) => entry.result);
      if (range.length === 1) return [undefined];
    } catch {
      if (range.length === 1) return [undefined];
    }

    splits += 1;
    const middle = Math.ceil(range.length / 2);
    const [left, right] = await Promise.all([fetchRange(range.slice(0, middle)), fetchRange(range.slice(middle))]);
    return [...left, ...right];
  };

  log(`${inputs.length} vaults, bisecting on failure`);
  const results = await fetchRange(inputs);
  const served = results.filter((entry) => entry !== undefined);
  log(`${served.length} served after ${splits} splits`);

  return {
    summary: {
      served: served.length,
      lost: results.length - served.length,
      splits,
    },
  };
};

export default run;
