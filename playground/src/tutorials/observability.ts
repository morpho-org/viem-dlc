import type { InlineContract } from "soltag";
import type { Address } from "viem";

import { fetchCorpus } from "../../tutorials/eth-call/corpus.js";
import solidity from "../../tutorials/eth-call/vault-snapshot.sol?raw";
import intro from "../../tutorials/observability/00-intro.md?raw";
import wideEventScript from "../../tutorials/observability/01-wide-event.js?raw";
import wideEventProse from "../../tutorials/observability/01-wide-event.md?raw";
import tuningScript from "../../tutorials/observability/02-tuning.js?raw";
import tuningProse from "../../tutorials/observability/02-tuning.md?raw";

import { alignedRange } from "./shared.js";
import type { TabContext, Tutorial } from "./types.js";

/** Context the observability scripts receive: the log range and the `eth_call` tutorial's corpus. */
export type ObservabilityContext = {
  vaults: readonly Address[];
  lens: InlineContract;
  settings: { blocks: string; binSize: string; vaults: string };
};

const LENS = { name: "VaultSnapshotLens", source: solidity };

export const observabilityTutorial: Tutorial = {
  id: "observability",
  title: "observability",
  blurb:
    "Where every number the other tutorials quote comes from: one wide event per call, emitted " +
    "only inside a scope you open, through a logger you supply.",
  controls: [
    { id: "blocks", label: "blocks back", value: "20000", type: "number" },
    { id: "binSize", label: "bin size", value: "2000", type: "number" },
    { id: "vaults", label: "vaults (for the lens read)", value: "60", type: "number" },
  ],
  sections: [
    { heading: "One event per call", prose: intro },
    {
      heading: "Silent until you ask",
      prose: wideEventProse,
      step: {
        id: "wide-event",
        scripts: [{ id: "wide-event", title: "inside and outside a scope", source: wideEventScript }],
      },
    },
    {
      heading: "Reading the numbers back",
      prose: tuningProse,
      step: {
        id: "tuning",
        solidity: LENS,
        scripts: [{ id: "tuning", title: "measure, then paste", source: tuningScript }],
      },
    },
  ],

  async prepare(context: TabContext) {
    const [{ range }, { vaults }] = await Promise.all([alignedRange(context), fetchCorpus(120)]);
    return { range, vaults };
  },
};
