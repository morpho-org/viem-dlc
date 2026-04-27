import { createTransport, type EIP1193RequestFn, type PublicRpcSchema, type Transport } from "viem";

import type { EIP1193Parameters, SafelyExtendedRpcSchema } from "../../types.js";
import { estimateUtf8Bytes } from "../../utils/json.js";

import type { LogsSieveConfig } from "./types.js";

export type * from "./types.js";

type Base = SafelyExtendedRpcSchema<PublicRpcSchema>;

export const logsSieveTransportKey = "viem-dlc-logs-sieve" as const;

/**
 * Creates a transport wrapper that filters oversized `eth_getLogs` entries.
 *
 * Logs whose UTF-8 encoded JSON representation exceeds `maxBytes` are silently
 * dropped from the response.
 */
export function logsSieve<T extends Base>(
  baseTransportFn: Transport<string, unknown, EIP1193RequestFn<T>>,
  [{ maxBytes }]: [LogsSieveConfig],
): Transport<typeof logsSieveTransportKey, unknown, EIP1193RequestFn<T>> {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1) {
    throw new Error(`[logsSieve] maxBytes must be a safe integer >= 1 (got ${maxBytes})`);
  }

  return (params) => {
    const requestFn = baseTransportFn(params).request as EIP1193RequestFn<Base>;

    const request = async (args: EIP1193Parameters<T>) => {
      if (args.method !== "eth_getLogs") {
        return requestFn(args);
      }

      const logs = await requestFn(args as EIP1193Parameters<Base, "eth_getLogs">);
      return logs.filter((log) => estimateUtf8Bytes(log) <= maxBytes);
    };

    return createTransport({
      key: logsSieveTransportKey,
      name: "[viem-dlc] logs-sieve",
      request: request as EIP1193RequestFn,
      retryCount: params.retryCount,
      timeout: params.timeout,
      type: logsSieveTransportKey,
    });
  };
}
