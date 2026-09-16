import source from "../../tutorials/cache/cold-warm.js?raw";

import { alignedRange } from "./shared.js";
import type { Tutorial } from "./types.js";

export const cacheTutorial: Tutorial = {
  id: "cache",
  title: "cache",
  blurb:
    "The all-in-one `cache` transport over the same range twice. `binSize` sets cache granularity and the " +
    "divider aligns to it, so the warm pass is served entirely from the store — only the `eth_blockNumber` " +
    "preflight goes out. The store is in-memory, so a reload starts cold again.",
  controls: [
    { id: "blocks", label: "BLOCKS BACK", value: "50000", type: "number" },
    { id: "binSize", label: "BIN SIZE", value: "10000", type: "number" },
  ],
  sections: [
    {
      heading: "",
      prose: "",
      step: { id: "cold-warm", scripts: [{ id: "cold-warm", title: "COLD VS WARM", source }] },
    },
  ],
  prepare: alignedRange,
};
