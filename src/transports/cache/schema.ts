import type { PublicRpcSchema as Base, RpcLog } from "viem";

import type { EIP1193Parameters, SafelyExtendRpcSchema } from "../../types.js";

export type EthGetLogsReduce = (logs: RpcLog[], log: RpcLog) => RpcLog[];

export type CacheRpcSchema = SafelyExtendRpcSchema<
  Base,
  [
    {
      /**
       * Caches `eth_call` results per sub-call, with Multicall3 `aggregate3` detection.
       *
       * Assumptions:
       * - Cached callees are pure view functions whose return value depends only on
       *   `to`, `data`, `block`, and state/block overrides. Fields like `from`, `value`,
       *   and `gas` are excluded from cache identity because they have different semantics
       *   in direct vs. multicall contexts (`msg.sender` is the Multicall3 contract for
       *   sub-calls, not the original `from`).
       * - Re-fetching only missed sub-calls changes the aggregate3 execution context
       *   (e.g. remaining gas budget), so gas-sensitive sub-call behavior may diverge
       *   from a full-batch call. This is acceptable for typical view-function workloads.
       */
      Method: "eth_call";
      AdditionalParameters: [{
        /** Specifies which entry of the `Store` holds these calls. Blob is extended by new results, not replaced. */
        blobKey: string;
        /** Maximum age (ms) of a cached entry before it is considered stale and re-fetched. */
        ttl: number;
      }];
    },
    {
      Method: "eth_getLogs";
      AdditionalParameters: [
        {
          /** @dev Receives logs in order. */
          reduce?: EthGetLogsReduce;
        },
      ];
    },
  ]
>;

export const cachedMethods = [
  "eth_call",
  "eth_getLogs",
] as const satisfies EIP1193Parameters<CacheRpcSchema>["method"][];

export type CachedMethod = (typeof cachedMethods)[number];
