import { getDeploymentBlockNumber } from "@morpho-org/viem-dlc/actions";
import { createPublicClient } from "viem";
import { getBlockNumber } from "viem/actions";

/** @type {`0x${string}`} */
const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb";

/**
 * Where does the history start?
 *
 * `getDeploymentBlockNumber` binary-searches `eth_getCode` for the block a contract first appeared
 * at, in about log2(head) requests. Use it as the `fromBlock` of a full-history read instead of
 * pasting a number that is only correct for one contract on one chain.
 *
 * @type {import("../../src/tutorials/types.js").Tab<{}>}
 */
const run = async ({ transport, chain, log }) => {
  const client = createPublicClient({ chain, transport });

  const head = await getBlockNumber(client);
  const started = performance.now();
  const deployed = await getDeploymentBlockNumber(client, { address: MORPHO });
  const ms = performance.now() - started;
  // `null` means the address holds no code at the head block.
  if (deployed === null) throw new Error("No code at that address on this chain.");

  log(`Morpho Blue first appears at block ${deployed}; the head is ${head}`);

  return {
    summary: {
      "deployed at": Number(deployed),
      "blocks of history": Number(head - deployed),
      elapsed: `${ms.toFixed(0)} ms`,
    },
  };
};

export default run;
