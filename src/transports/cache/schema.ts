import type { PublicRpcSchema as Base, RpcLog } from "viem";

import type { EIP1193Parameters, SafelyExtendRpcSchema } from "../../types.js";

export type EthGetLogsReduce = (logs: RpcLog[], log: RpcLog) => RpcLog[];

export type CacheSchema = SafelyExtendRpcSchema<
  Base,
  [
    {
      Method: "eth_getLogs";
      AdditionalParameters: [
        {
          /**
           * Pre-filter applied to the raw JSON of each batch of logs (logs that were emitted in a given block range,
           * of size `binSize`) **before** being passed to `reduce` or pushed onto the full array. This avoids the
           * cost of `JSON.parse` on large log arrays, with net speedup depending on your target's sparsity.
           *
           * Effectively regex, but passed as a plain string to survive RPC parameter normalization.
           * Matching is case-insensitive.
           */
          search?: string;
          /**
           * Folds over logs that match `search`, in order. This can help consolidate memory when you
           * don't need to materialize the full array (e.g., running total of balances).
           */
          reduce?: EthGetLogsReduce;
        },
      ];
    },
  ]
>;

export const cachedMethods = ["eth_call", "eth_getLogs"] as const satisfies EIP1193Parameters<CacheSchema>["method"][];

export type CachedMethod = (typeof cachedMethods)[number];
