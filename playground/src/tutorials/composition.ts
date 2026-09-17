import intro from "../../tutorials/composition/00-intro.md?raw";
import layersScript from "../../tutorials/composition/01-layers.js?raw";
import failoverScript from "../../tutorials/composition/02-failover.js?raw";
import failoverProse from "../../tutorials/composition/02-failover.md?raw";
import storesScript from "../../tutorials/composition/03-stores.js?raw";
import storesProse from "../../tutorials/composition/03-stores.md?raw";

import { alignedRange } from "./shared.js";
import type { Tutorial } from "./types.js";

export const compositionTutorial: Tutorial = {
  id: "composition",
  title: "The transport stack",
  blurb:
    "The five layers behind cache(...), what each one costs, and how to state a provider's limits " +
    "once instead of at every call site.",
  controls: [
    { id: "blocks", label: "blocks back", value: "20000", type: "number" },
    { id: "binSize", label: "bin size", value: "2000", type: "number" },
  ],
  sections: [
    {
      heading: "Five layers, in order",
      prose: intro,
      step: {
        id: "layers",
        controls: [
          { id: "rps", label: "maxRequestsPerSecond", value: "8", type: "number" },
          { id: "blockTimestamp", label: "blockTimestamp (true / false)", value: "false" },
          { id: "maxBytes", label: "logsSieve maxBytes", value: "8192", type: "number" },
        ],
        scripts: [{ id: "layers", title: "one layer at a time", source: layersScript }],
      },
    },
    {
      heading: "One provider's limits are not another's",
      prose: failoverProse,
      step: {
        id: "failover",
        controls: [
          { id: "primaryUrl", label: "primary endpoint (deliberately unreachable)", value: "https://base.invalid" },
        ],
        scripts: [{ id: "failover", title: "primary down, fallback serves", source: failoverScript }],
      },
    },
    {
      heading: "Stores are four methods",
      prose: storesProse,
      step: {
        id: "stores",
        controls: [{ id: "hotBytes", label: "hot tier maxBytes", value: "4000000", type: "number" }],
        scripts: [{ id: "stores", title: "counting the tiers", source: storesScript }],
      },
    },
  ],
  prepare: alignedRange,
};
