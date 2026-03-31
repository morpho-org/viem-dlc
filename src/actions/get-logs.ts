import type {
  AbiEvent,
  Account,
  Address,
  BlockNumber,
  BlockTag,
  Chain,
  Client,
  Log,
  LogTopic,
  MaybeAbiEventName,
  MaybeExtractEventArgsFromAbi,
  RpcLog,
  Transport,
} from "viem";
import { type EncodeEventTopicsParameters, encodeEventTopics, formatLog, numberToHex, parseEventLogs } from "viem";

import type { CacheSchema, EthGetLogsReduce } from "../transports/cache/schema.js";

export type GetLogs2Parameters<
  abiEvent extends AbiEvent | undefined = undefined,
  abiEvents extends readonly AbiEvent[] | readonly unknown[] | undefined = abiEvent extends AbiEvent
    ? [abiEvent]
    : undefined,
  strict extends boolean | undefined = undefined,
  fromBlock extends BlockNumber | BlockTag | undefined = undefined,
  toBlock extends BlockNumber | BlockTag | undefined = undefined,
  //
  _eventName extends string | undefined = MaybeAbiEventName<abiEvent>,
  _pending extends boolean = (fromBlock extends "pending" ? true : false) | (toBlock extends "pending" ? true : false),
  _log = Log<bigint, number, _pending, abiEvent, strict, abiEvents, _eventName>,
> = {
  /** Address or list of addresses from which logs originated */
  address?: Address | Address[] | undefined;
  /** Block number or tag after which to include logs */
  fromBlock?: fromBlock | BlockNumber | BlockTag | undefined;
  /** Block number or tag before which to include logs */
  toBlock?: toBlock | BlockNumber | BlockTag | undefined;
  /**
   * Pre-filter applied to the raw JSON of each cached batch of logs **before** parsing.
   * Effectively regex (case-insensitive). Matching is on the raw NDJSON, so use hex-encoded
   * values (e.g. address fragments, topic prefixes).
   */
  search?: string | undefined;
  /**
   * Folds over decoded logs that pass `search`, in order. Runs in streaming fashion inside
   * the cache handler (per-bin, per-log), so memory stays proportional to the accumulator,
   * not the full result set.
   *
   * Each log is decoded (formatted + ABI-parsed when `event`/`events` is provided) before
   * being passed to this callback — no need to decode manually.
   */
  reduce?: ((acc: _log[], log: _log) => _log[]) | undefined;
} & (
  | {
      event: abiEvent;
      events?: undefined;
      args?: MaybeExtractEventArgsFromAbi<abiEvents, _eventName> | undefined;
      /**
       * Whether or not the logs must match the indexed/non-indexed arguments on `event`.
       * @default false
       */
      strict?: strict | undefined;
    }
  | {
      event?: undefined;
      events: abiEvents;
      args?: undefined;
      strict?: strict | undefined;
    }
  | {
      event?: undefined;
      events?: undefined;
      args?: undefined;
      strict?: undefined;
    }
);

export type GetLogs2ReturnType<
  abiEvent extends AbiEvent | undefined = undefined,
  abiEvents extends readonly AbiEvent[] | readonly unknown[] | undefined = abiEvent extends AbiEvent
    ? [abiEvent]
    : undefined,
  strict extends boolean | undefined = undefined,
  fromBlock extends BlockNumber | BlockTag | undefined = undefined,
  toBlock extends BlockNumber | BlockTag | undefined = undefined,
  //
  _eventName extends string | undefined = MaybeAbiEventName<abiEvent>,
  _pending extends boolean = (fromBlock extends "pending" ? true : false) | (toBlock extends "pending" ? true : false),
> = Log<bigint, number, _pending, abiEvent, strict, abiEvents, _eventName>[];

/**
 * Returns a list of event logs matching the provided parameters, with support for
 * cache-layer `search` pre-filtering and streaming `reduce`.
 *
 * Mirrors viem's {@link https://viem.sh/docs/actions/public/getLogs getLogs} but
 * requires a client whose transport uses the `cache()` wrapper (i.e. whose
 * `rpcSchema` is `CacheSchema`).
 *
 * @example
 * import { parseAbiItem } from 'viem'
 * import { getLogs2 } from '@morpho-org/viem-dlc'
 *
 * const logs = await getLogs2(client, {
 *   address: '0x...',
 *   event: parseAbiItem('event Transfer(address indexed, address indexed, uint256)'),
 *   fromBlock: 18_000_000n,
 *   toBlock: 19_000_000n,
 *   search: 'deadbeef',
 *   reduce: (acc, log) => {
 *     acc.push(log)    // log.args is already decoded
 *     return acc
 *   },
 * })
 */
export async function getLogs2<
  chain extends Chain | undefined,
  const abiEvent extends AbiEvent | undefined = undefined,
  const abiEvents extends readonly AbiEvent[] | readonly unknown[] | undefined = abiEvent extends AbiEvent
    ? [abiEvent]
    : undefined,
  strict extends boolean | undefined = undefined,
  fromBlock extends BlockNumber | BlockTag | undefined = undefined,
  toBlock extends BlockNumber | BlockTag | undefined = undefined,
>(
  client: Client<Transport, chain, Account | undefined, CacheSchema>,
  {
    address,
    fromBlock,
    toBlock,
    event,
    events: events_,
    args,
    strict: strict_,
    search,
    reduce,
  }: GetLogs2Parameters<abiEvent, abiEvents, strict, fromBlock, toBlock>,
): Promise<GetLogs2ReturnType<abiEvent, abiEvents, strict, fromBlock, toBlock>> {
  type _Return = GetLogs2ReturnType<abiEvent, abiEvents, strict, fromBlock, toBlock>;

  const strict = strict_ ?? false;
  const events = events_ ?? (event ? [event] : undefined);

  let topics: LogTopic[] = [];
  if (events) {
    const encoded = (events as AbiEvent[]).flatMap((event) =>
      encodeEventTopics({
        abi: [event],
        eventName: (event as AbiEvent).name,
        args: events_ ? undefined : args,
      } as EncodeEventTopicsParameters),
    );
    topics = [encoded as LogTopic];
    if (event) topics = topics[0] as LogTopic[];
  }

  // Widen the generic block params to concrete RPC types and pin blockHash so TS
  // can resolve the fromBlock/toBlock branch of the schema's discriminated union.
  type RpcBlock = `0x${string}` | BlockTag | undefined;
  const rpcFromBlock = (typeof fromBlock === "bigint" ? numberToHex(fromBlock) : fromBlock) as RpcBlock;
  const rpcToBlock = (typeof toBlock === "bigint" ? numberToHex(toBlock) : toBlock) as RpcBlock;

  const filter = { address, topics, blockHash: undefined, fromBlock: rpcFromBlock, toBlock: rpcToBlock };

  if (reduce) {
    // Wrap user's reduce in a transport-level reduce that decodes each log before
    // passing it to the callback. The closure tracks the decoded accumulator; the
    // transport-level accumulator stays empty to keep memory low.
    let acc = [] as _Return;

    const transportReduce: EthGetLogsReduce = (_rpcAcc, rpcLog) => {
      const formatted = formatLog(rpcLog);
      if (events) {
        const parsed = parseEventLogs({
          abi: events,
          args,
          logs: [formatted],
          strict,
        });
        if (parsed.length > 0) {
          // biome-ignore lint/suspicious/noExplicitAny: viem is complicated
          acc = (reduce as any)(acc, parsed[0]!) as _Return;
        }
      } else {
        // biome-ignore lint/suspicious/noExplicitAny: viem is complicated
        acc = (reduce as any)(acc, formatted) as _Return;
      }
      return []; // don't accumulate at transport level
    };

    await client.request({
      method: "eth_getLogs",
      params: [filter, { search, reduce: transportReduce }],
    });

    return acc;
  }

  // No reduce — standard viem getLogs flow.
  // Return‐type narrowing doesn't survive the CacheSchema mapped type, so we assert.
  const logs = (await client.request({
    method: "eth_getLogs",
    params: [filter, { search }],
  })) as RpcLog[];

  const formattedLogs = logs.map((log) => formatLog(log));
  if (!events) return formattedLogs as _Return;

  return parseEventLogs({
    abi: events,
    args,
    logs: formattedLogs,
    strict,
  }) as unknown as _Return;
}
