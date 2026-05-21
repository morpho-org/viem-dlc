import { createTransport, type EIP1193RequestFn, type Hex, type PublicRpcSchema, type Transport } from "viem";

import { type Observability, observe } from "../../observability.js";
import type { EIP1193Parameters, SafelyExtendedRpcSchema } from "../../types.js";
import { isRevertExpected } from "../../utils/deployless/codec.envelope.js";

import type { LogsEnricherConfig } from "./types.js";

export type * from "./types.js";

type Base = SafelyExtendedRpcSchema<PublicRpcSchema>;

export const logsEnricherTransportKey = "viem-dlc-logs-enricher" as const;

/** Creates a transport wrapper that enriches `eth_getLogs` responses. */
export function logsEnricher<T extends Base>(
  baseTransportFn: Transport<string, unknown, EIP1193RequestFn<T>>,
  [{ retryCount, retryDelay, blockTimestamp }]: [LogsEnricherConfig],
): Transport<typeof logsEnricherTransportKey, unknown, EIP1193RequestFn<T>> {
  return (params) => {
    const requestFn = baseTransportFn(params).request as EIP1193RequestFn<Base>;

    const request = async (args: EIP1193Parameters<T>, observability?: Observability) => {
      if (args.method !== "eth_getLogs") {
        return requestFn(args, isRevertExpected(args) ? { retryCount: 0 } : undefined);
      }

      const logs = await requestFn(args as EIP1193Parameters<Base, "eth_getLogs">);

      if (!blockTimestamp) return logs;

      // Collect unique block numbers that need timestamps
      const blockNumbers = new Set<Hex>();
      for (const log of logs) {
        if (log.blockTimestamp === undefined && log.blockNumber !== null) {
          blockNumbers.add(log.blockNumber);
        }
      }

      if (blockNumbers.size === 0) return logs;

      // Fetch block headers in parallel
      const timestamps = new Map<Hex, Hex | null>();
      await Promise.all(
        [...blockNumbers].map(async (blockNumber) => {
          const block = await requestFn(
            { method: "eth_getBlockByNumber", params: [blockNumber, false] },
            { retryCount, retryDelay },
          );
          timestamps.set(blockNumber, block !== null ? block.timestamp : null);
        }),
      );

      // Enrich logs, dropping any whose block was reorged away
      const enriched = logs.reduce<typeof logs>((acc, log) => {
        if (log.blockTimestamp !== undefined || log.blockNumber === null) {
          acc.push(log);
          return acc;
        }
        const ts = timestamps.get(log.blockNumber);
        if (ts) {
          acc.push({ ...log, blockTimestamp: ts });
          return acc;
        }
        return acc;
      }, []);

      if (observability) {
        const tag = `${logsEnricherTransportKey}.${observability.counter}`;
        observability.logger?.withContext({
          [`${tag}.blocks_fetched`]: blockNumbers.size,
          [`${tag}.logs_dropped`]: logs.length - enriched.length,
        });
      }

      return enriched;
    };

    return createTransport({
      key: logsEnricherTransportKey,
      name: "[viem-dlc] logs-enricher",
      request: observe(request) as EIP1193RequestFn,
      retryCount: 0,
      type: logsEnricherTransportKey,
    });
  };
}
