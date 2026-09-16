import source from "../../tabs/search-reduce.js?raw";

import { alignedRange } from "./shared.js";
import type { Example } from "./types.js";

export const searchReduceExample: Example = {
  id: "search-reduce",
  title: "getLogs2",
  blurb:
    "Three ways to narrow one warm cache to a single account: decode everything then filter, fold with " +
    "`reduce` as logs decode, or put a `search` regex against the raw NDJSON ahead of `JSON.parse` so " +
    "bins that cannot match are never parsed. All three must agree.\n\n" +
    "Widen the range to see them diverge: over a few thousand logs the three are within noise, and what " +
    "`reduce` and `search` save first is peak memory rather than time — memory is what test/bench asserts, " +
    "and it is not something this page can show you.",
  controls: [
    { id: "blocks", label: "BLOCKS BACK", value: "100000", type: "number" },
    { id: "binSize", label: "BIN SIZE", value: "10000", type: "number" },
  ],
  scripts: [{ id: "compare", title: "FILTER / REDUCE / SEARCH", source }],
  prepare: alignedRange,
};
