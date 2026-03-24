import type { Hex, RpcSchema } from "viem";

import type { BlockRange, EIP1193Parameters } from "../../types.js";
import { cyrb64Hash } from "../../utils/hash.js";
import { pick } from "../../utils/pick.js";

import type { CachedMethod, LogsCacheRpcSchema } from "./schema.js";

function createKeychain<Schema extends RpcSchema, Methods extends Schema[number]["Method"]>() {
  return <
    Fns extends {
      [M in Methods]: {
        blobKey: (chainId: number, args: EIP1193Parameters<Schema, M>) => `${number}:${M}:${string}` | null;
        // biome-ignore lint/suspicious/noExplicitAny: necessary to infer types
        entryKey: (chainId: number, method: M, inputs: any) => string;
      };
    },
  >(
    fns: Fns,
  ) => ({
    blobKey<M extends Methods>(chainId: number, args: EIP1193Parameters<Schema, M>): ReturnType<Fns[M]["blobKey"]> {
      return fns[args.method].blobKey(chainId, args) as ReturnType<Fns[M]["blobKey"]>;
    },
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
    blobKey(chainId, args) {
      const custom = args.params[4]?.blobKey
      return custom ? `${chainId}:${args.method}:${hash(custom)}` : null;
    },
    entryKey(_chainId, _method, inputs: { block: "latest", data: Hex }) {
      return `${inputs.block}:${inputs.data}` as const;
    },
  },
  eth_getLogs: {
    blobKey(chainId, args) {
      const suffix = hash(pick(args.params[0], ["address", "topics"]));
      return `${chainId}:${args.method}:${suffix}`;
    },
    entryKey(_chainId, _method, inputs: BlockRange) {
      return `${inputs.fromBlock}:${inputs.toBlock}` as const;
    },
  },
});
