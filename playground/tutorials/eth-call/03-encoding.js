import { arrayifiedAbi } from "@morpho-org/viem-dlc/actions";
import { encodeFunctionData, getAbiItem } from "viem";

/**
 * No network. Encode the same read both ways and compare.
 *
 * Multicall treats every element as its own call: one `encodeFunctionData` each, wrapped in an
 * `(address, bool, bytes)` tuple each. A lens read encodes the whole element array once, against the
 * array-shaped fragment `arrayifiedAbi` derives from the per-item function. Decoding is the mirror
 * image and is left out here, because measuring it honestly needs real return data.
 *
 * @type {import("../../src/tutorials/types.js").Tab<import("../../src/tutorials/eth-call.js").EthCallContext>}
 */
const run = async ({ settings, lens, vaults, log }) => {
  const count = Number(settings.vaults);
  const inputs = vaults.slice(0, count).map((vault) => ({ vault, grief: 0n }));

  const item = /** @type {import("viem").AbiFunction} */ (getAbiItem({ abi: lens.abi, name: "snapshotOf" }));
  const page = arrayifiedAbi(item);

  const aggregate3 = /** @type {const} */ ({
    type: "function",
    name: "aggregate3",
    stateMutability: "payable",
    inputs: [
      {
        type: "tuple[]",
        name: "calls",
        components: [
          { type: "address", name: "target" },
          { type: "bool", name: "allowFailure" },
          { type: "bytes", name: "callData" },
        ],
      },
    ],
    outputs: [
      {
        type: "tuple[]",
        components: [
          { type: "bool", name: "success" },
          { type: "bytes", name: "returnData" },
        ],
      },
    ],
  });

  const started = performance.now();
  const calls = inputs.map((input) => ({
    target: lens.stateOverride.address,
    allowFailure: true,
    callData: encodeFunctionData({ abi: [item], functionName: "snapshotOf", args: [input] }),
  }));
  const multicallData = encodeFunctionData({ abi: [aggregate3], functionName: "aggregate3", args: [calls] });
  const multicallMs = performance.now() - started;

  const lensStarted = performance.now();
  const lensData = encodeFunctionData({ abi: [page], functionName: "snapshotOf", args: [inputs] });
  const lensMs = performance.now() - lensStarted;

  /** @param {string} hex */
  const bytes = (hex) => (hex.length - 2) / 2;
  log(`multicall ${bytes(multicallData).toLocaleString()} B, lens ${bytes(lensData).toLocaleString()} B`);

  return {
    summary: {
      "multicall ms": multicallMs.toFixed(1),
      "lens ms": lensMs.toFixed(1),
      "multicall B/element": Math.round(bytes(multicallData) / inputs.length),
      "lens B/element": Math.round(bytes(lensData) / inputs.length),
    },
  };
};

export default run;
