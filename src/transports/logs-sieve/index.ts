import { createTransport, type EIP1193RequestFn, type PublicRpcSchema, type Transport } from "viem";

import { createFacetId, getObservability, observe } from "../../observability.js";
import type { EIP1193Parameters, SafelyExtendedRpcSchema } from "../../types.js";
import { isRevertExpected } from "../../utils/deployless/codec.envelope.js";
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

  const facetId = createFacetId(logsSieveTransportKey);

  return (params) => {
    const requestFn = baseTransportFn(params).request as EIP1193RequestFn<Base>;

    const request = async (args: EIP1193Parameters<T>) => {
      if (args.method !== "eth_getLogs") {
        return requestFn(args, isRevertExpected(args) ? { retryCount: 0 } : undefined);
      }

      // Crossed once per chunk under a divider fan-out, so `add`/`stat` accumulate
      // per-call totals on this transport's slot.
      const facet = getObservability()?.facet(facetId);
      const logs = await requestFn(args as EIP1193Parameters<Base, "eth_getLogs">);
      const kept = logs.filter((log) => {
        const bytes = estimateUtf8Bytes(log);
        if (bytes <= maxBytes) return true;
        // Sizes of the dropped logs, so `maxBytes` can be tuned against what it rejects.
        facet?.stat("dropped_log_bytes", bytes);
        return false;
      });

      if (kept.length < logs.length) {
        facet?.add("logs_dropped", logs.length - kept.length);
      }

      return kept;
    };

    return createTransport({
      key: logsSieveTransportKey,
      name: "[viem-dlc] logs-sieve",
      request: observe(request, facetId, params.chain?.id) as EIP1193RequestFn,
      retryCount: 0,
      type: logsSieveTransportKey,
    });
  };
}
