import type { Hex, PublicRpcSchema } from "viem";

import type { SafelyExtendRpcSchema } from "../../types.js";

import type { OnLogsResponse } from "./types.js";

export type LogsDividerRpcSchema = SafelyExtendRpcSchema<
  PublicRpcSchema,
  [
    {
      Method: "eth_getLogs";
      AdditionalParameters: [
        {
          /** The return value of `eth_blockNumber`, if known. If omitted, it will be fetched. */
          latestBlock: Hex;
          /** Optional callback receiving logs results for each sub-request */
          onLogsResponse?: OnLogsResponse;
        },
      ];
    },
  ]
>;
