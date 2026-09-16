import { logsDivider } from "@morpho-org/viem-dlc/transports";
import type { InlineContract } from "soltag";
import { createPublicClient, parseAbiItem } from "viem";
import { getBlockNumber, getLogs } from "viem/actions";

import initcode from "../../tabs/deployless-initcode.js?raw";
import override from "../../tabs/deployless-override.js?raw";
import solidity from "../../tabs/positions.sol?raw";

import { BORROW_EVENT, MORPHO, type Pair } from "./shared.js";
import type { Example, TabContext } from "./types.js";

/** Context the deployless scripts receive. */
export type DeploylessContext = {
  inputs: readonly Pair[];
  lens: InlineContract<"MorphoPositionsLens">;
  settings: { elements: string; gasLimit: string };
};

let discovered: Pair[] | undefined;

export const deploylessExample: Example = {
  id: "deployless",
  title: "deployless",
  blurb:
    "Reads Morpho positions through a lens contract that is never deployed. Both panes are editable: " +
    "the Solidity recompiles in your browser when you change it, and the script is the program that runs. " +
    "Switch tabs to compare deliveries — `override` lifts the 49 152-byte initcode cap, so the same " +
    "elements collapse into fewer, larger chunks.",
  controls: [
    { id: "elements", label: "ELEMENTS", value: "2000", type: "number" },
    { id: "gasLimit", label: "TRANSPORT GASLIMIT", value: "600000000", type: "number" },
  ],
  solidity,
  scripts: [
    { id: "initcode", title: "INITCODE", source: initcode },
    { id: "override", title: "OVERRIDE", source: override },
  ],

  async prepare(context: TabContext) {
    if (!discovered?.length) {
      context.log("discovering (market, borrower) pairs…");

      const client = createPublicClient({
        chain: context.chain,
        // Only so discovery survives the provider's `eth_getLogs` range limit.
        transport: logsDivider(context.transport as never, [
          { maxBlockRange: 2_000 },
          { retryCount: 3, retryDelay: 1_000, blockTimestamp: false },
          { maxBytes: 8_192 },
          {},
        ]) as never,
      });

      const toBlock = await getBlockNumber(client);
      const logs = await getLogs(client, {
        address: MORPHO,
        event: parseAbiItem(BORROW_EVENT),
        strict: true,
        fromBlock: toBlock - 20_000n,
        toBlock,
      });

      const seen = new Map<string, Pair>();
      for (const { args } of logs) seen.set(`${args.id}:${args.onBehalf}`, { id: args.id, user: args.onBehalf });
      discovered = [...seen.values()];
    }

    if (!discovered.length) throw new Error("No Borrow events in the last 20 000 blocks — try another endpoint.");

    // Repeated to reach the requested count: every element is a real position, so packing and gas
    // figures are real; only input distinctness is synthetic.
    const count = Number(context.settings.elements);
    return { inputs: Array.from({ length: count }, (_, i) => discovered![i % discovered!.length]!) };
  },
};
