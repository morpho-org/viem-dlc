import source from "../../tabs/logs-divider.js?raw";

import type { Example } from "./types.js";

export const logsDividerExample: Example = {
  id: "logs-divider",
  title: "logsDivider",
  blurb:
    "Splits one wide `eth_getLogs` into aligned chunks, retrying what fails and dropping oversized logs. " +
    "No cache and no store — this is the transport on its own. `onLogsResponse` reports each chunk as it " +
    "lands, so the progress below appears while the request is still in flight. Base's public endpoint caps a single `eth_getLogs` at 2 000 blocks — raise MAX BLOCK RANGE against an endpoint that allows more and watch the chunk count fall.",
  controls: [
    { id: "blocks", label: "BLOCKS BACK", value: "20000", type: "number" },
    { id: "maxBlockRange", label: "MAX BLOCK RANGE", value: "2000", type: "number" },
  ],
  scripts: [{ id: "divider", title: "DIVIDER", source }],
};
