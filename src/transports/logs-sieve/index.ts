import { custom, type EIP1193RequestFn, type PublicRpcSchema, type Transport } from "viem";

import type { EIP1193Parameters, EIP1193RequestOptions } from "../../types.js";
import { estimateUtf8Bytes } from "../../utils/json.js";

import type { LogsSieveSchema } from "./schema.js";
import type { LogsSieveConfig } from "./types.js";

export type * from "./schema.js";
export type * from "./types.js";

/**
 * Creates a transport wrapper that filters oversized `eth_getLogs` entries.
 *
 * Logs whose UTF-8 encoded JSON representation exceeds `maxBytes` are silently
 * dropped from the response.
 */
export function logsSieve(
  baseTransportFn: Transport<string, unknown, EIP1193RequestFn<PublicRpcSchema>>,
  [{ maxBytes }]: [LogsSieveConfig],
  // biome-ignore lint/suspicious/noExplicitAny: this `any` matches the underlying viem type's default
): Transport<"custom", Record<string, any>, EIP1193RequestFn<LogsSieveSchema>> {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1) {
    throw new Error(`[logsSieve] maxBytes must be a safe integer > 1 (got ${maxBytes})`);
  }

  return (params) => {
    const transport = baseTransportFn(params);

    const request = async (args: EIP1193Parameters<LogsSieveSchema>, options?: EIP1193RequestOptions) => {
      if (args.method !== "eth_getLogs") {
        return transport.request(args, options);
      }

      const logs = await transport.request(args, options);
      return logs.filter((log) => estimateUtf8Bytes(log) <= maxBytes);
    };

    return custom({ request })(params);
  };
}
