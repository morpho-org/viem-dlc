import type {
  EIP1193Parameters as _EIP1193Parameters,
  BlockNumber,
  BlockTag,
  EIP1193RequestFn,
  Hex,
  MaybePromise,
  Prettify,
  PublicRpcSchema,
  RpcSchema,
} from "viem";

import type { Concat, ElementwiseUnionUnion, Inits, Tuple } from "./utils/tuples.js";

/*//////////////////////////////////////////////////////////////
                            HELPERS
//////////////////////////////////////////////////////////////*/

/**
 * Alternative to the viem type of the same name that allows you to (optionally)
 * select parameters for a specific `Method`.
 */
export type EIP1193Parameters<T extends RpcSchema, Method extends T[number]["Method"] = T[number]["Method"]> = Extract<
  _EIP1193Parameters<T>,
  { method: Method }
>;

/**
 * Union of method signatures found in `T` that match `Method`.
 * `Method` is `string` by default, which matches all signatures.
 */
export type RpcSignature<T extends RpcSchema = RpcSchema, Method extends T[number]["Method"] = string> = Extract<
  T[number],
  { Method: Method }
>;

/**
 * Extended entry that defines `AdditionalParameters` for a single `Method`.
 * The method must exist in `T` with tuple-like `Parameters`.
 */
export type RpcSignatureExtension<T extends RpcSchema> = Extract<
  T[number],
  { Parameters: Tuple }
>["Method"] extends infer M
  ? {
      Method: M;
      AdditionalParameters: Tuple;
    }
  : never;

/**
 * Union of method names in `T` whose `Parameters` are tuple-like (and thus extendable).
 */
export type ExtendableRpcSignatures<T extends RpcSchema> = Extract<T[number], { Parameters: Tuple }>;

/**
 * Extended entries that define `AdditionalParameters` for a set of `Method`s.
 * Each method must exist in `T` with tuple-like `Parameters`.
 */
export type RpcSchemaExtension<T extends RpcSchema> = readonly RpcSignatureExtension<T>[];

/** Extracts members of `Extension` whose `Method` field includes `M` (supports union `Method`s). */
type MatchExtension<Extension, M> = Extension extends { Method: infer EM } ? (M extends EM ? Extension : never) : never;

type DeriveParameters<P, AdditionalP extends Tuple> = Exclude<P, undefined> extends infer Base // Infer non-optional part of `P`
  ? [Base] extends [never]
    ? P // undefined-ish, leave as-is
    : [Base] extends [Tuple]
      ? Concat<ElementwiseUnionUnion<Base>, Inits<AdditionalP>> | Extract<P, undefined>
      : P // not tuple-ish, leave as-is
  : never;

/**
 * Extends `T[K]["Parameters"]` to include `AdditionalParameters` (if any exist for `T[K]["Method"]`)
 * as an optional suffix. Preserves named tuple labels.
 */
type DeriveRpcSignature<
  T extends RpcSchema,
  K extends keyof T,
  Extension extends RpcSignatureExtension<T>,
> = T[K] extends { Method: infer M; Parameters?: infer P } // Try to infer Method and Parameters types for `T[K]`. If unable, leave `T[K]` as-is.
  ? // If `Extension` lacks Method `M`, leave `T[K]` as-is. Otherwise derive extended parameters.
    [MatchExtension<Extension, M>] extends [never]
    ? T[K]
    : Omit<T[K], "Parameters"> & {
        Parameters: DeriveParameters<P, MatchExtension<Extension, M>["AdditionalParameters"]>;
      }
  : T[K];

/**
 * Derives a new `RpcSchema` that appends the `AdditionalParameters` of `Extension` as optional elements
 * of `T`'s `Parameters`.
 *
 * This allows you to extend a base `RpcSchema` (e.g., `PublicRpcSchema`) without jeapardizing existing,
 * expected functionality, in contrast to a naive approach that could overwrite method(s) with entirely
 * different parameters.
 *
 * @example
 * // To add cache-related args to `eth_call`:
 * export type CacheRpcSchema = SafelyExtendRpcSchema<
 *   PublicRpcSchema,
 *   [{ Method: 'eth_call'; AdditionalParameters: [{ cacheKeys: string[] }] }]
 * >
 *
 * const req = async (args: EIP1193Parameters<CacheRpcSchema>) => {
 *   if (args.method === 'eth_call') {
 *     // Existing parameters are unchanged:
 *     const a = args.params[0] // type: ExactPartial<RpcTransactionRequest>
 *     const b = args.params[1] // type: `0x${string}` | BlockTag | RpcBlockIdentifier | undefined
 *     const c = args.params[2] // type: RpcStateOverride | undefined
 *     const d = args.params[3] // type: Rpc | undefined
 *     // `AdditionalParameters` are appended at the end:
 *     const e = args.params[4] // type: { cacheKeys: string[]; } | undefined
 *   }
 *   // ...
 * }
 */
export type SafelyExtendRpcSchema<T extends RpcSchema, Extension extends RpcSchemaExtension<T>> = {
  readonly [K in keyof T]: Prettify<DeriveRpcSignature<T, K, Extension[number]>>;
};

/*//////////////////////////////////////////////////////////////
                              TYPES
//////////////////////////////////////////////////////////////*/

/**
 * All methods are best-effort and MUST NOT throw. Stores should be robust to gaps
 * in wall clock time (e.g., freeze/thaw cycles in serverless function environments).
 *
 * @dev `flush` is expected to resolve after pending work is complete. The definition of
 * "pending work" may be Store-specific.
 */
export interface Store {
  get(key: string): MaybePromise<string | null>;
  set(key: string, value: string): MaybePromise<void>;
  delete(key: string): MaybePromise<void>;
  flush(): MaybePromise<void>;
}

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface Cache<T extends {}> {
  read(keys: string[]): Promise<(T | undefined)[]>;
  write(items: { key: string; value: T }[]): Promise<void>;
}

export type BlockNumberish = BlockNumber | BlockTag | Hex;

export type BlockRange = { fromBlock: bigint; toBlock: bigint };

export type EIP1193RequestOptions = Parameters<EIP1193RequestFn>["1"];

export type EthGetLogsFilter = RpcSignature<PublicRpcSchema, "eth_getLogs">["Parameters"][0];

export type EthGetLogsHashlessFilter = Omit<EthGetLogsFilter, "blockHash">;
