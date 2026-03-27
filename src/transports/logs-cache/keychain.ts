import type { RpcSchema } from "viem";

import type { BlockRange, EIP1193Parameters } from "../../types.js";
import { cyrb64Hash } from "../../utils/hash.js";
import { pick } from "../../utils/pick.js";

import type { CachedMethod, LogsCacheRpcSchema } from "./schema.js";

/**
 * Creates a keychain with proper typing for the `Schema` and `Methods`.
 *
 * @dev Curried generic factory -- works around TypeScript's lack of partial type argument inference.
 * The outer call `createKeychain<Schema, Methods>()` explicitly fixes the schema-level types,
 * while the inner call `(fns)` lets TS infer `Fns` from the implementation object. Without
 * this split, callers would have to either supply *all* type params explicitly or rely on
 * inference for *all* of them; currying lets us pin the schema and infer the rest.
 */
function createKeychain<Schema extends RpcSchema, Methods extends Schema[number]["Method"]>() {
  return <
    Fns extends {
      [M in Methods]: {
        blobKey: (chainId: number, req: EIP1193Parameters<Schema, M>) => `${number}:${M}:${string}` | null;
        // biome-ignore lint/suspicious/noExplicitAny: necessary to infer types
        entryKey: (chainId: number, method: M, inputs: any) => Record<string, string>;
      };
    },
  >(
    fns: Fns,
  ) => ({
    /** Identifies which key of the `Store` will hold data for this `req`. MUST be used exactly, no prefix/suffix. */
    blobKey<M extends Methods>(chainId: number, req: EIP1193Parameters<Schema, M>): ReturnType<Fns[M]["blobKey"]> {
      return fns[req.method].blobKey(chainId, req) as ReturnType<Fns[M]["blobKey"]>;
    },
    /** Identifies the subkeys *within* a blob that are relevant to `inputs`. Semantics vary by `method`. */
    entryKey<M extends Methods>(
      chainId: number,
      method: M,
      inputs: Parameters<Fns[M]["entryKey"]>[2],
    ): ReturnType<Fns[M]["entryKey"]> {
      return fns[method].entryKey(chainId, method, inputs) as ReturnType<Fns[M]["entryKey"]>;
    },
  });
}

function hash(obj: unknown) {
  return cyrb64Hash(JSON.stringify(obj));
}

export const keychain = createKeychain<LogsCacheRpcSchema, CachedMethod>()({
  eth_call: {
    blobKey(chainId, req) {
      const custom = req.params[4]?.blobKey;
      return custom ? `${chainId}:${req.method}:${hash(custom)}` : null;
    },
    entryKey(_chainId, _method, inputs: Record<string, unknown>) {
      return { data: `${0}:${hash(inputs)}` as const };
    },
  },
  eth_getLogs: {
    blobKey(chainId, req) {
      const suffix = hash(pick(req.params[0], ["address", "topics"]));
      return `${chainId}:${req.method}:${suffix}`;
    },
    entryKey(_chainId, _method, inputs: BlockRange) {
      const fromBlock = inputs.fromBlock.toString().padStart(20, "0");
      const toBlock = inputs.toBlock.toString().padStart(20, "0");
      return {
        metadata: `${0}:${fromBlock}:${toBlock}` as const,
        data: `${1}:${fromBlock}:${toBlock}` as const,
      };
    },
  },
});
