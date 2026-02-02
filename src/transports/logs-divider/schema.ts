import type { Hex, PublicRpcSchema } from "viem";

import type { SafelyExtendRpcSchema } from "../../types.js";

export type LogsDividerRpcSchema = SafelyExtendRpcSchema<
  PublicRpcSchema,
  [
    {
      Method: "eth_getLogs";
      AdditionalParameters: [
        {
          /** The return value of `eth_blockNumber`, if known. If omitted, it will be fetched. */
          latestBlock: Hex;
        },
      ];
    },
  ]
>;
