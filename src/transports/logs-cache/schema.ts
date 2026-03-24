import type { PublicRpcSchema as Base, RpcLog } from "viem";

import type { EIP1193Parameters, SafelyExtendRpcSchema } from "../../types.js";

export type LogsCacheRpcSchema = SafelyExtendRpcSchema<
  Base,
  [
    {
      Method: "eth_call";
      AdditionalParameters: [{ blobKey: string }];
    },
    {
      Method: "eth_getLogs";
      AdditionalParameters: [
        {
          /** @dev This offers no ordering guarantees. */
          reduce<T>(fn: (acc: T, v: RpcLog) => T, init: T): T;
        },
      ];
    },
  ]
>;

export const cachedMethods = [
  "eth_call",
  "eth_getLogs",
] as const satisfies EIP1193Parameters<LogsCacheRpcSchema>["method"][];

export type CachedMethod = (typeof cachedMethods)[number];
