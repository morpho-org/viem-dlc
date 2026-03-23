import type { Address, Hex } from "viem";

import type { EIP1193Parameters } from "../../types.js";
import { deepTransform, deepTransformOptions as dt } from "../../utils/objects.js";
import { pick } from "../../utils/pick.js";
import type { Tuple } from "../../utils/tuples.js";

import type { CachedMethod, LogsCacheRpcSchema } from "./schema.js";

/*//////////////////////////////////////////////////////////////
                    METHOD-SPECIFIC HELPERS
//////////////////////////////////////////////////////////////*/

const EthGetLogs = {
  /**
   * - if `x` is `Replacement` or an empty-array, returns `Replacement`
   * - elif `x` is `T[]` of length 1, returns `transform(x[0])`
   * - elif `x` is `T[]` of length > 1, returns `x.map(transform)`
   * - elif `x` is `T`, returns `transform(x)`
   */
  normalizeFilterArray<T extends {}, Replacement extends null | undefined>(
    x: T[] | T | Replacement,
    replacement: Replacement,
    transform?: (_: T) => T,
  ): T[] | T | Replacement {
    if (Array.isArray(x)) {
      switch (x.length) {
        case 0:
          return replacement;
        case 1:
          return transform ? transform(x[0]) : x[0];
        default:
          return transform ? x.map(transform) : x;
      }
    }
    return x && transform ? transform(x) : x;
  },

  // [] → undefined
  // 0xABC → 0xabc
  // [0xABC] → 0xabc
  // [0xABC, ...] → [0xabc, ...]
  normalizeFilterAddresses(address: Address[] | Address | undefined) {
    return EthGetLogs.normalizeFilterArray(address, undefined, (x) => x.toLowerCase() as Address);
  },

  // [[], ...] → [null, ...]
  // [0xABC, ...] → [0xabc, ...]
  // [[0xABC], ...] → [0xabc, ...]
  // [[0xABC, ...], ...] → [[0xabc, ...], ...]
  // [] → undefined
  // [null, null, null, null] → undefined
  normalizeFilterTopics(topics: (Hex[] | Hex | null)[] | undefined) {
    topics = topics?.map((topic) => EthGetLogs.normalizeFilterArray(topic, null, (x) => x.toLowerCase() as Hex));
    return topics?.every((topic) => topic === null) ? undefined : topics;
  },
};

/*//////////////////////////////////////////////////////////////
                          TUPLE HELPER
//////////////////////////////////////////////////////////////*/

type TupleIndexKeys<T extends Tuple> = T extends unknown ? Extract<keyof T, `${number}`> : never;
type ElementAt<T, K> = T extends unknown ? (K extends keyof T ? T[K] : undefined) : never;
type TupleNormalizers<T extends Tuple> = Partial<{
  [K in TupleIndexKeys<T>]: (input: ElementAt<T, K>) => ElementAt<T, K>;
}>;
function normalizeTuple<const T extends Tuple>(tuple: T, normalizers: TupleNormalizers<T>): T {
  const out = [...tuple] as { -readonly [K in keyof T]: T[K] };

  for (const k in normalizers) {
    const key = k as TupleIndexKeys<T>;
    const fn = normalizers[key];

    // Important for union tuples with differing lengths:
    // only normalize indices that actually exist on runtime tuple.
    if (fn && key in tuple) {
      out[key] = fn(tuple[key] as ElementAt<T, typeof key>);
    }
  }

  return out as T;
}

/*//////////////////////////////////////////////////////////////
                              MAIN
//////////////////////////////////////////////////////////////*/

/** Normalizes EIP1193 request parameters; should be called before request deduplication. */
export function normalize(args: EIP1193Parameters<LogsCacheRpcSchema>) {
  args = deepTransform(args, { ...dt.sortKeys, ...dt.lowercase, ...dt.deleteUndefined });

  switch (args.method) {
    case "eth_call":
      return {
        method: args.method,
        params: normalizeTuple(args.params, {
          0: (transaction) => pick(transaction, ["data", "from", "gas", "nonce", "to", "value"]),
          3: (_blockOverrides) => undefined,
        }),
      };
    case "eth_getLogs":
      return {
        method: args.method,
        params: normalizeTuple(args.params, {
          0: (filter) => ({
            ...filter,
            address: EthGetLogs.normalizeFilterAddresses(filter.address),
            topics: EthGetLogs.normalizeFilterTopics(filter.topics),
          }),
        }),
      };
    default: {
      const _: never = args.method as Extract<typeof args.method, CachedMethod>;
      return args;
    }
  }
}
