import intro from "../../tutorials/eth-get-logs/00-intro.md?raw";
import dividerProse from "../../tutorials/eth-get-logs/01-divider.md?raw";
import plainScript from "../../tutorials/eth-get-logs/01-plain.js?raw";
import dividerScript from "../../tutorials/eth-get-logs/01b-divider.js?raw";
import cacheProse from "../../tutorials/eth-get-logs/02-cache.md?raw";
import coldWarmScript from "../../tutorials/eth-get-logs/02-cold-warm.js?raw";
import narrowProse from "../../tutorials/eth-get-logs/03-narrow.md?raw";
import perUserScript from "../../tutorials/eth-get-logs/03-per-user.js?raw";
import broadScript from "../../tutorials/eth-get-logs/03b-broad.js?raw";
import strategiesScript from "../../tutorials/eth-get-logs/03c-strategies.js?raw";
import strategiesProse from "../../tutorials/eth-get-logs/04-strategies.md?raw";

import { alignedRange } from "./shared.js";
import type { Tutorial } from "./types.js";

export const ethGetLogsTutorial: Tutorial = {
  id: "eth-get-logs",
  title: "eth_getLogs",
  blurb:
    "Read Morpho Blue's Borrow history on Base, from one call that no endpoint will answer to a " +
    "shared cache that serves every borrower without another request.",
  controls: [
    { id: "blocks", label: "blocks back", value: "100000", type: "number" },
    { id: "binSize", label: "bin size", value: "2000", type: "number" },
  ],
  sections: [
    { heading: "37 million blocks, one call", prose: intro },
    {
      heading: "Find the limit, then stop caring about it",
      prose: dividerProse,
      step: {
        id: "divider",
        controls: [{ id: "maxBlockRange", label: "maxBlockRange", value: "2000", type: "number" }],
        scripts: [
          { id: "plain", title: "plain getLogs", source: plainScript },
          { id: "divider", title: "logsDivider", source: dividerScript },
        ],
      },
    },
    {
      heading: "Finalized blocks never change",
      prose: cacheProse,
      step: { id: "cache", scripts: [{ id: "cold-warm", title: "cold vs warm", source: coldWarmScript }] },
    },
    {
      heading: "Cache broad, narrow after",
      prose: narrowProse,
      step: {
        id: "narrow",
        scripts: [
          { id: "per-user", title: "one blob per borrower", source: perUserScript },
          { id: "broad", title: "one blob for everyone", source: broadScript },
        ],
      },
    },
    {
      heading: "Three ways to read a warm blob",
      prose: strategiesProse,
      step: {
        id: "strategies",
        scripts: [{ id: "strategies", title: "filter / reduce / search", source: strategiesScript }],
      },
    },
  ],
  prepare: alignedRange,
};
