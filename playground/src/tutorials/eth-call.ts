import type { InlineContract } from "soltag";
import type { Address } from "viem";

import intro from "../../tutorials/eth-call/00-intro.md?raw";
import multicallScript from "../../tutorials/eth-call/01-multicall.js?raw";
import sharedFrame from "../../tutorials/eth-call/01-shared-frame.md?raw";
import oneFrame from "../../tutorials/eth-call/02-one-frame.md?raw";
import readLensScript from "../../tutorials/eth-call/02-readlens.js?raw";
import readLensHintedScript from "../../tutorials/eth-call/02b-readlens-hinted.js?raw";
import readLensOverrideScript from "../../tutorials/eth-call/02c-readlens-override.js?raw";
import readLensCompressScript from "../../tutorials/eth-call/02d-readlens-compress.js?raw";
import cost from "../../tutorials/eth-call/03-cost.md?raw";
import encodingScript from "../../tutorials/eth-call/03-encoding.js?raw";
import bisectScript from "../../tutorials/eth-call/03b-bisect.js?raw";
import lensesProse from "../../tutorials/eth-call/04-lenses.md?raw";
import waterfallScript from "../../tutorials/eth-call/04-waterfall.js?raw";
import lensScript from "../../tutorials/eth-call/04b-lens.js?raw";
import cacheProse from "../../tutorials/eth-call/05-cache.md?raw";
import cachedScript from "../../tutorials/eth-call/05-cached.js?raw";
import { fetchCorpus } from "../../tutorials/eth-call/corpus.js";
import solidity from "../../tutorials/eth-call/vault-snapshot.sol?raw";

import type { TabContext, Tutorial } from "./types.js";

/** Context the `eth_call` scripts receive. */
export type EthCallContext = {
  vaults: readonly Address[];
  corpusLive: boolean;
  lens: InlineContract;
  settings: { vaults: string; grief: string; batchSize: string };
};

/** Above the largest corpus any control can ask for, so `prepare` stays stable across settings. */
const CORPUS_SIZE = 300;

const LENS = { name: "VaultSnapshotLens", source: solidity };

export const ethCallTutorial: Tutorial = {
  id: "eth-call",
  title: "Multicall without compromise",
  blurb:
    "Read one value from every Morpho vault on Base, first with Multicall3 and then with a lens. " +
    "The controls are shared by every step, so each comparison changes the transport and nothing else.",
  controls: [
    { id: "vaults", label: "vaults", value: "120", type: "number" },
    { id: "grief", label: "grief rounds (on one vault)", value: "2000000", type: "number" },
  ],
  sections: [
    { heading: "The read", prose: intro },
    {
      heading: "Multicall shares one gas frame",
      prose: sharedFrame,
      step: {
        id: "multicall",
        controls: [{ id: "batchSize", label: "viem batchSize (bytes)", value: "1024", type: "number" }],
        solidity: LENS,
        scripts: [{ id: "multicall", title: "multicall", source: multicallScript }],
      },
    },
    {
      heading: "One frame per element",
      prose: oneFrame,
      step: {
        id: "readlens",
        solidity: LENS,
        scripts: [
          { id: "plain", title: "no hints", source: readLensScript },
          { id: "hinted", title: "with gas hints", source: readLensHintedScript },
          { id: "override", title: "override delivery", source: readLensOverrideScript },
          { id: "compress", title: "compressed calldata", source: readLensCompressScript },
        ],
      },
    },
    {
      heading: "The lens read is also cheaper",
      prose: cost,
      step: {
        id: "cost",
        solidity: LENS,
        scripts: [
          { id: "encoding", title: "encoding only", source: encodingScript },
          { id: "bisect", title: "multicall + bisect", source: bisectScript },
        ],
      },
    },
    {
      heading: "What a lens unlocks",
      prose: lensesProse,
      step: {
        id: "waterfall",
        solidity: LENS,
        scripts: [
          { id: "waterfall", title: "client-side waterfall", source: waterfallScript },
          { id: "lens", title: "one lens call", source: lensScript },
        ],
      },
    },
    {
      heading: "Caching elements, not calls",
      prose: cacheProse,
      step: {
        id: "cached",
        solidity: LENS,
        scripts: [{ id: "cached", title: "cold / warm / half novel", source: cachedScript }],
      },
    },
  ],

  async prepare(context: TabContext) {
    context.log("fetching the vault corpus");
    const { vaults, live } = await fetchCorpus(CORPUS_SIZE);
    context.log(
      `corpus: ${vaults.length} vaults available, ${live ? "live from the Morpho API" : "from the pinned snapshot"}`,
    );
    return { vaults, corpusLive: live };
  },
};
