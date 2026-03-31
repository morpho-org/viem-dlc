import { custom, type EIP1193RequestFn, type PublicRpcSchema, type Transport } from "viem";

import type { EIP1193Parameters, SafelyExtendedRpcSchema } from "../../types.js";
import { estimateUtf8Bytes } from "../../utils/json.js";

import type { LogsSieveConfig } from "./types.js";

export type * from "./types.js";

type Base = SafelyExtendedRpcSchema<PublicRpcSchema>;

/**
 * Creates a transport wrapper that filters oversized `eth_getLogs` entries.
 *
 * Logs whose UTF-8 encoded JSON representation exceeds `maxBytes` are silently
 * dropped from the response.
 */
export function logsSieve<T extends Base>(
  baseTransportFn: Transport<string, unknown, EIP1193RequestFn<T>>,
  [{ maxBytes }]: [LogsSieveConfig],
  // biome-ignore lint/suspicious/noExplicitAny: this `any` matches the underlying viem type's default
): Transport<"custom", Record<string, any>, EIP1193RequestFn<T>> {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1) {
    throw new Error(`[logsSieve] maxBytes must be a safe integer >= 1 (got ${maxBytes})`);
  }

  return (params) => {
    const requestFn = baseTransportFn(params).request as EIP1193RequestFn<Base>;

    const request = async (args: EIP1193Parameters<T>) => {
      if (args.method !== "eth_getLogs") {
        return requestFn(args, { dedupe: true });
      }

      const logs = await requestFn(args as EIP1193Parameters<Base, "eth_getLogs">, { dedupe: true });
      return logs.filter((log) => estimateUtf8Bytes(log) <= maxBytes);
    };

    return custom({ request })(params);
  };
}
