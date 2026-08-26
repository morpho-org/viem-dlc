/// <reference types="node" />
import { randomBytes } from "crypto";

import type { Redis } from "@upstash/redis";

import { measureUtf8Bytes, shardString } from "../utils/strings.js";

/** Upstash's per-record cap (Free/PAYG). Values whose stored elements sum past it are dropped. */
const MAX_RECORD_BYTES = 100 * 1024 * 1024;

/*//////////////////////////////////////////////////////////////
                            SHARD CODEC
//////////////////////////////////////////////////////////////*/

/**
 * Stored element layout, with `D = decimalDigits(k)` and `i`, `k` zero-padded to `D`:
 * ```
 * head          wid|i|k|<base64>    i = 0
 * continuation  wid|i|<base64>      0 < i < k
 * ```
 * Every element carries its own index, so within one `wid` index ⇒ bytes is a function and
 * readers can pin every position to its unique original shard ({@link decodeContinuationShard}).
 */
export type ShardHead = { wid: string; k: number; D: number; payload: string };

const WID_HEX_CHARS = 16;
const HEAD_PREFIX_BYTES_SANS_DIGITS = WID_HEX_CHARS + "|".length * 3;
const CONTINUATION_PREFIX_BYTES_SANS_DIGITS = WID_HEX_CHARS + "|".length * 2;
const HEAD_RE = /^([0-9a-f]{16})\|(\d+)\|(\d+)\|/;

export type ShardPlan = {
  k: number;
  D: number;
  headCapacity: number;
  continuationCapacity: number;
  storedBytes: number;
};

/**
 * Decides how many shards a base64 payload needs under the complete-element bound `shardBytes`.
 * Digits only grow with `k`, so recomputing `k` at the smaller capacity until `D` stops
 * increasing terminates at the fixed point. `null` when `shardBytes` cannot hold a header.
 */
export function planShards(payloadLength: number, shardBytes: number): ShardPlan | null {
  let D = 1;
  for (;;) {
    const headCapacity = shardBytes - HEAD_PREFIX_BYTES_SANS_DIGITS - 2 * D;
    const continuationCapacity = shardBytes - CONTINUATION_PREFIX_BYTES_SANS_DIGITS - D;
    if (headCapacity <= 0 || continuationCapacity <= 0) return null;

    const k = payloadLength <= headCapacity ? 1 : 1 + Math.ceil((payloadLength - headCapacity) / continuationCapacity);
    const digits = String(k).length;
    if (digits > D) {
      D = digits;
      continue;
    }

    const prefixBytes = HEAD_PREFIX_BYTES_SANS_DIGITS + 2 * D + (k - 1) * (CONTINUATION_PREFIX_BYTES_SANS_DIGITS + D);
    return { k, D, headCapacity, continuationCapacity, storedBytes: payloadLength + prefixBytes };
  }
}

/** Encodes `value` into stored elements under a fresh `wid`; `null` if it cannot be stored. */
export function encodeShards(value: Buffer[], shardBytes: number): string[] | null {
  const payload = Buffer.concat(value).toString("base64");
  const plan = planShards(payload.length, shardBytes);
  if (plan === null || plan.storedBytes > MAX_RECORD_BYTES) return null;

  const { k, D, headCapacity, continuationCapacity } = plan;
  const wid = randomBytes(WID_HEX_CHARS / 2).toString("hex");
  const pad = (n: number) => String(n).padStart(D, "0");

  const shards = [`${wid}|${pad(0)}|${pad(k)}|${payload.slice(0, headCapacity)}`];
  if (k > 1) {
    const continuations = shardString(payload.slice(headCapacity), continuationCapacity);
    for (let j = 0; j < continuations.length; j++) shards.push(`${wid}|${pad(j + 1)}|${continuations[j]}`);
  }
  return shards;
}

export function decodeHeadShard(element: string): ShardHead | null {
  const match = HEAD_RE.exec(element);
  if (match === null) return null;

  const [prefix, wid, index, count] = match as unknown as [string, string, string, string];
  const D = count.length;
  const k = Number(count);
  if (index.length !== D || Number(index) !== 0 || k < 1) return null;

  return { wid, k, D, payload: element.slice(prefix.length) };
}

/** Returns the payload iff `element` is shard `index` of the value whose head is `head`. */
export function decodeContinuationShard(element: string, head: ShardHead, index: number): string | null {
  const prefix = `${head.wid}|${String(index).padStart(head.D, "0")}|`;
  return element.startsWith(prefix) ? element.slice(prefix.length) : null;
}

/*//////////////////////////////////////////////////////////////
                              SCRIPTS
//////////////////////////////////////////////////////////////*/

/**
 * Replaces `KEYS[1]` with `ARGV[2..]` atomically, expiring at the absolute `ARGV[1]` ms deadline
 * (`''` = persist). Replay-idempotent: a retried request re-asserts the same deadline.
 * Returns {@link ScriptResult}: 1 written, 0 dead-on-arrival (old value untouched), -3 bad arguments.
 */
export const WRITE_DIRECT_SCRIPT = `#!lua flags=allow-key-locking
local deadline = nil
if ARGV[1] ~= '' then
  deadline = tonumber(ARGV[1])
  if not deadline or deadline ~= math.floor(deadline) then
    return -3
  end

  local now = redis.call('TIME')
  local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

  if deadline <= now_ms then
    return 0
  end
end

redis.call('UNLINK', KEYS[1])
redis.call('RPUSH', KEYS[1], unpack(ARGV, 2))

if deadline then
  redis.call('PEXPIREAT', KEYS[1], deadline)
end

return 1
`;
export const WRITE_DIRECT_SHA = "20751b6a21acda37f3c04f492697200de6d95e25";

/**
 * Publishes staging `KEYS[1]` to live `KEYS[2]` iff staging has exactly `ARGV[2]` elements headed
 * by `ARGV[1]`, stamping the `ARGV[3]` ms deadline (`''` = persist) so `RENAME` carries it.
 * Returns {@link ScriptResult}: 1 published, 2 already live, 0 dead-on-arrival, -1 invalid
 * staging, -2 foreign live value, -3 bad arguments. Only 1 mutates live.
 */
export const PUBLISH_SCRIPT = `#!lua flags=allow-key-locking
local expected_head = ARGV[1]
local k = tonumber(ARGV[2])

if not k or k < 1 or k ~= math.floor(k) then
  return -3
end

local deadline = nil
if ARGV[3] ~= '' then
  deadline = tonumber(ARGV[3])
  if not deadline or deadline ~= math.floor(deadline) then
    return -3
  end

  local now = redis.call('TIME')
  local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

  if deadline <= now_ms then
    redis.call('UNLINK', KEYS[1])
    return 0
  end
end

local staging_head = redis.call('LINDEX', KEYS[1], 0)
if staging_head == expected_head
    and redis.call('LLEN', KEYS[1]) == k then
  if deadline then
    redis.call('PEXPIREAT', KEYS[1], deadline)
  else
    redis.call('PERSIST', KEYS[1])
  end

  redis.call('RENAME', KEYS[1], KEYS[2])
  return 1
end

local live_head = redis.call('LINDEX', KEYS[2], 0)
if live_head == expected_head
    and redis.call('LLEN', KEYS[2]) == k then
  return 2
end

if live_head then
  return -2
end

return -1
`;
export const PUBLISH_SHA = "e40a4cd7e0021bdf7cceca7d3e2b216248076261";

export enum ScriptResult {
  Written = 1,
  AlreadyLive = 2,
  DeadOnArrival = 0,
  InvalidStaging = -1,
  ForeignLiveValue = -2,
  BadArguments = -3,
}

/** Log level and message per {@link ScriptResult}; `null` is success. */
export const SCRIPT_OUTCOMES: Record<ScriptResult, ["info" | "warn", string] | null> = {
  [ScriptResult.Written]: null,
  [ScriptResult.AlreadyLive]: null,
  [ScriptResult.DeadOnArrival]: ["info", "deadline already passed; dropped"],
  [ScriptResult.InvalidStaging]: ["warn", "staging invalid; dropped"],
  [ScriptResult.ForeignLiveValue]: ["info", "lost to a concurrent writer"],
  [ScriptResult.BadArguments]: ["warn", "bad script arguments (bug)"],
};

export const isNoScript = (error: string | undefined) => error?.startsWith("NOSCRIPT") === true;

/*//////////////////////////////////////////////////////////////
                         HTTP BODY FRAMING
//////////////////////////////////////////////////////////////*/

/**
 * Commands are built as the exact arrays `@upstash/redis` serializes (`JSON.stringify` of an
 * array of commands), so byte measurement here equals the bytes on the wire.
 */
export type Command = (string | number)[];
export type CommandResult = { result: unknown; error?: string };

const JSON_BRACKETS_BYTES = "[]".length;
const JSON_SEPARATOR_BYTES = ",".length;

export const measureCommand = (command: Command) => measureUtf8Bytes(JSON.stringify(command));

/** Exact serialized size of a pipeline body carrying commands of the given sizes. */
export const measurePipeline = (commandBytes: number[]) =>
  JSON_BRACKETS_BYTES + commandBytes.reduce((sum, n) => sum + n, 0) + (commandBytes.length - 1) * JSON_SEPARATOR_BYTES;

/** One pipeline slot: a command, its exact request size, the list elements its response may carry, and who asked. */
export type Slot<T> = { command: Command; requestBytes: number; elements: number; tag: T };

export function toSlot<T>(command: Command, tag: T, elements = 0): Slot<T> {
  return { command, requestBytes: measureCommand(command), elements, tag };
}

/**
 * Greedy next-fit packing into pipelines whose exact request body stays ≤ `maxRequestBytes` and
 * whose responses carry at most `maxElements` list elements. A slot that cannot fit even alone is
 * sent alone; the provider's rejection surfaces through the caller's per-slot error handling.
 */
export function packPipelines<T>(slots: Slot<T>[], maxRequestBytes: number, maxElements = Infinity): Slot<T>[][] {
  const pipelines: Slot<T>[][] = [];

  let current: Slot<T>[] = [];
  let requestBytes = JSON_BRACKETS_BYTES;
  let elements = 0;
  const flush = () => {
    if (current.length > 0) pipelines.push(current);
    current = [];
    requestBytes = JSON_BRACKETS_BYTES;
    elements = 0;
  };

  for (const slot of slots) {
    const separator = current.length > 0 ? JSON_SEPARATOR_BYTES : 0;
    const fits =
      requestBytes + separator + slot.requestBytes <= maxRequestBytes && elements + slot.elements <= maxElements;

    if (!fits) flush();

    requestBytes += (current.length > 0 ? JSON_SEPARATOR_BYTES : 0) + slot.requestBytes;
    elements += slot.elements;
    current.push(slot);
  }
  flush();

  return pipelines;
}

export type Pipeline = ReturnType<Redis["pipeline"]>;

/** Replays a measured {@link Command} through the client's typed builders, which serialize to the same array. */
export function appendCommand(pipeline: Pipeline, command: Command) {
  const [name, ...args] = command;
  switch (name) {
    case "evalsha": {
      const [sha, keyCount, ...rest] = args as [string, number, ...string[]];
      pipeline.evalsha(sha, rest.slice(0, keyCount), rest.slice(keyCount));
      return;
    }
    case "lrange":
      pipeline.lrange(args[0] as string, args[1] as number, args[2] as number);
      return;
    case "rpush":
      pipeline.rpush(args[0] as string, ...(args.slice(1) as string[]));
      return;
    case "expire":
      pipeline.expire(args[0] as string, args[1] as number);
      return;
    case "unlink":
      pipeline.unlink(args[0] as string);
      return;
    default:
      throw new Error(`[UpstashStore] unsupported command ${name}`);
  }
}
