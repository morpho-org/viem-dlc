import { custom, type EIP1193RequestFn, type Hex, type PublicRpcSchema, type Transport } from "viem";

import type { EIP1193Parameters } from "../../types.js";

import type { LogsEnricherSchema } from "./schema.js";
import type { LogsEnricherConfig } from "./types.js";

export type * from "./schema.js";
export type * from "./types.js";

/** Creates a transport wrapper that enriches `eth_getLogs` responses. */
export function logsEnricher(
  baseTransportFn: Transport<string, unknown, EIP1193RequestFn<PublicRpcSchema>>,
  [{ retryCount, retryDelay, blockTimestamp }]: [LogsEnricherConfig],
  // biome-ignore lint/suspicious/noExplicitAny: this `any` matches the underlying viem type's default
): Transport<"custom", Record<string, any>, EIP1193RequestFn<LogsEnricherSchema>> {
  return (params) => {
    const transport = baseTransportFn(params);

    const request = async (args: EIP1193Parameters<LogsEnricherSchema>) => {
      if (args.method !== "eth_getLogs") {
        return transport.request(args, { dedupe: true });
      }

      const logs = await transport.request(args, { dedupe: true });

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
          const block = await transport.request(
            { method: "eth_getBlockByNumber", params: [blockNumber, false] },
            { dedupe: true, retryCount, retryDelay },
          );
          timestamps.set(blockNumber, block !== null ? block.timestamp : null);
        }),
      );

      // Enrich logs, dropping any whose block was reorged away
      return logs.reduce<typeof logs>((acc, log) => {
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
    };

    return custom({ request })(params);
  };
}
