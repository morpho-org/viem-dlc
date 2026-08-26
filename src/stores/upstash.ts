/// <reference types="node" />
import { randomUUID } from "crypto";

import { Redis, type RedisConfigNodejs } from "@upstash/redis";

import type { Logger } from "../observability.js";
import type { Store } from "../types.js";
import { createInFlightBarrier } from "../utils/in-flight.js";

import { HierarchicalStore } from "./hierarchical.js";
import { LruStore } from "./lru.js";
import { ThrottledStore } from "./throttled.js";
import {
  appendCommand,
  type Command,
  type CommandResult,
  decodeContinuationShard,
  decodeHeadShard,
  encodeShards,
  isNoScript,
  measureCommand,
  measurePipeline,
  PUBLISH_SCRIPT,
  PUBLISH_SHA,
  packPipelines,
  planShards,
  SCRIPT_OUTCOMES,
  type ScriptResult,
  type ShardHead,
  type Slot,
  toSlot,
  WRITE_DIRECT_SCRIPT,
  WRITE_DIRECT_SHA,
} from "./upstash.internal.js";

export type UpstashStoreOptions = {
  /**
   * Bound on the exact serialized body of every HTTP request the store issues. A request that cannot
   * be split under it (a single shard plus its key and framing) is sent anyway and fails as a logged
   * per-key error, so keep this comfortably above `shardBytes`.
   */
  maxRequestBytes: number;
  /**
   * Provisions HTTP responses: each read request asks for at most `floor(maxResponseBytes / shardBytes)`
   * list elements. JSON framing is not modeled, so set this below the provider's limit with headroom.
   * Default: 10 MiB.
   */
  maxResponseBytes?: number;
  /** Bound on each stored list element, header included. Default: 64 KiB. */
  shardBytes?: number;
  /** Seconds. Omit for persistent values. */
  ttl?: number;
  redis?: Omit<RedisConfigNodejs, "automaticDeserialization">;
  /** Optional logger for non-request-bound emissions (e.g. background I/O errors). */
  logger?: Logger;
};

/*//////////////////////////////////////////////////////////////
                              LIMITS
//////////////////////////////////////////////////////////////*/

const DEFAULT_SHARD_BYTES = 64 * 1024;
const DEFAULT_MAX_RESPONSE_BYTES = 10 * 1024 * 1024;
/** Writer-liveness bound on staging keys; refreshed on every push. */
const STAGING_TTL_SEC = 60;
const READ_ATTEMPTS = 2;

/*//////////////////////////////////////////////////////////////
                               STORE
//////////////////////////////////////////////////////////////*/

type StagedWrite = { batches: Command[][]; publish: Command };

/**
 * A store that uses Upstash Redis for robust storage and retrieval of large, blob-like data.
 *
 * Every value is a Redis LIST of ≤ `shardBytes` self-indexed shards (`ShardHead`). Small
 * values are written atomically by `WRITE_DIRECT_SCRIPT`, many per HTTP request; large
 * values stage at a per-writer `tmp:<key>:<uuid>` list (60 s TTL, refreshed per push) and go
 * live through `PUBLISH_SCRIPT`. Readers verify wid-uniformity and index contiguity, so a
 * reader observes either the old value or one complete new one — never a splice.
 *
 * Reads and writes are best-effort and never throw; a torn read retries, then reports a miss.
 */
export class UpstashStore implements Store {
  private readonly options: UpstashStoreOptions;
  private readonly redis: Redis;
  private readonly inFlight = createInFlightBarrier();
  private readonly shardBytes: number;
  private readonly maxRequestBytes: number;
  /** List elements provisioned per read request; also the stage-2 page length. */
  private readonly elementsPerResponse: number;

  constructor(options: UpstashStoreOptions) {
    const fail = (message: string): never => {
      const err = new Error(`[UpstashStore] ${message}`);
      options.logger?.withMetadata({ class: UpstashStore.name, method: "constructor" }).withError(err).error();
      throw err;
    };

    const shardBytes = options.shardBytes ?? DEFAULT_SHARD_BYTES;
    const maxResponseBytes = options.maxResponseBytes ?? DEFAULT_MAX_RESPONSE_BYTES;
    const elementsPerResponse = Math.floor(maxResponseBytes / shardBytes);

    if (!Number.isSafeInteger(shardBytes) || planShards(0, shardBytes) === null) {
      fail(`shardBytes must be a safe integer large enough to hold a shard header (got ${options.shardBytes})`);
    }
    if (!Number.isSafeInteger(options.maxRequestBytes) || options.maxRequestBytes <= 0) {
      fail(`maxRequestBytes must be a positive safe integer (got ${options.maxRequestBytes})`);
    }
    if (!Number.isSafeInteger(maxResponseBytes) || elementsPerResponse < 1) {
      fail(`maxResponseBytes must be a safe integer >= shardBytes (got ${options.maxResponseBytes})`);
    }
    if (options.ttl !== undefined && (!Number.isSafeInteger(options.ttl) || options.ttl <= 0)) {
      fail(`ttl must be a positive safe integer (got ${options.ttl})`);
    }

    this.options = options;
    this.shardBytes = shardBytes;
    this.maxRequestBytes = options.maxRequestBytes;
    this.elementsPerResponse = elementsPerResponse;

    const clientOptions = {
      automaticDeserialization: false,
      responseEncoding: false,
      enableAutoPipelining: false,
    } as const;
    this.redis = options.redis ? new Redis({ ...options.redis, ...clientOptions }) : Redis.fromEnv(clientOptions);
  }

  private log(method: string, metadata: Record<string, unknown> = {}) {
    return this.options.logger?.withMetadata({ class: UpstashStore.name, method, ...metadata });
  }

  /*//////////////////////////////////////////////////////////////
                              TRANSPORT
  //////////////////////////////////////////////////////////////*/

  /** One HTTP request. Per-command errors are returned in their slot, never thrown. */
  private async execPipeline(commands: Command[], { atomic = false } = {}): Promise<CommandResult[]> {
    const pipeline = atomic ? this.redis.multi() : this.redis.pipeline();
    for (const command of commands) appendCommand(pipeline, command);
    return (await pipeline.exec({ keepErrors: true })) as CommandResult[];
  }

  /** Runs `EVALSHA` commands for `script`, reloading it once and reissuing only the `NOSCRIPT` slots. */
  private async execScript(commands: Command[], script: string): Promise<CommandResult[]> {
    const results = await this.execPipeline(commands);
    const missing = results.flatMap((r, i) => (isNoScript(r.error) ? [i] : []));
    if (missing.length === 0) return results;

    await this.redis.scriptLoad(script);
    const reissued = await this.execPipeline(missing.map((i) => commands[i]!));
    missing.forEach((i, j) => {
      results[i] = reissued[j]!;
    });
    return results;
  }

  /*//////////////////////////////////////////////////////////////
                                 READ
  //////////////////////////////////////////////////////////////*/

  async mget(keys: readonly string[]): Promise<(Buffer[] | null)[]> {
    const values = new Map<string, Buffer[] | null>();

    let pending = [...new Set(keys)];
    for (let attempt = 0; attempt < READ_ATTEMPTS && pending.length > 0; attempt++) {
      try {
        pending = await this._mget(pending, values);
      } catch (err) {
        this.log("mget", { attempt })?.withError(err).warn("read failed");
        pending = pending.filter((key) => !values.has(key));
      }
    }

    for (const key of pending) {
      this.log("mget", { key })?.warn("exhausted retries");
    }

    return keys.map((key) => values.get(key) ?? null);
  }

  /**
   * One read attempt. Resolved keys land in `values`; keys whose list changed underneath
   * (or whose request failed) are returned for retry.
   */
  private async _mget(keys: string[], values: Map<string, Buffer[] | null>): Promise<string[]> {
    const retry = new Set<string>();

    type PartialRead = { head: ShardHead; payloads: string[] };
    const partialReads = new Map<string, PartialRead>();

    const headSlots = keys.map((key) => toSlot(["lrange", key, 0, 0], key, 1));
    for (const pipeline of packPipelines(headSlots, this.maxRequestBytes, this.elementsPerResponse)) {
      const results = await this.execPipeline(pipeline.map((slot) => slot.command));

      pipeline.forEach(({ tag: key }, i) => {
        const { result, error } = results[i]!;
        if (error !== undefined) {
          this.log("mget", { key, error })?.warn("lrange failed");
          retry.add(key);
          return;
        }

        const elements = result as string[];
        if (elements.length === 0) {
          values.set(key, null);
          return;
        }

        const head = decodeHeadShard(elements[0]!);
        if (head === null) {
          this.log("mget", { key })?.info("unrecognized head; treating as miss");
          values.set(key, null);
        } else if (head.k === 1) {
          values.set(key, [Buffer.from(head.payload, "base64")]);
        } else {
          partialReads.set(key, { head, payloads: [head.payload] });
        }
      });
    }

    type Page = { key: string; from: number; to: number };
    const pageSlots: Slot<Page>[] = [];
    for (const [key, { head }] of partialReads) {
      for (let from = 1; from < head.k; from += this.elementsPerResponse) {
        const to = Math.min(head.k - 1, from + this.elementsPerResponse - 1);
        pageSlots.push(toSlot(["lrange", key, from, to], { key, from, to }, to - from + 1));
      }
    }

    for (const pipeline of packPipelines(pageSlots, this.maxRequestBytes, this.elementsPerResponse)) {
      const results = await this.execPipeline(pipeline.map((slot) => slot.command));

      pipeline.forEach(({ tag: { key, from, to } }, i) => {
        if (retry.has(key)) return;

        const { result, error } = results[i]!;
        const { head, payloads } = partialReads.get(key)!;
        const elements = error === undefined ? (result as string[]) : [];

        let intact = elements.length === to - from + 1;
        for (let j = 0; intact && j < elements.length; j++) {
          const payload = decodeContinuationShard(elements[j]!, head, from + j);
          if (payload === null) intact = false;
          else payloads.push(payload);
        }

        if (!intact) {
          this.log("mget", { key, from, to, error })?.info("torn read; list replaced mid-read");
          retry.add(key);
        }
      });
    }

    for (const [key, { payloads }] of partialReads) {
      if (!retry.has(key)) values.set(key, [Buffer.from(payloads.join(""), "base64")]);
    }

    return [...retry];
  }

  /*//////////////////////////////////////////////////////////////
                                WRITE
  //////////////////////////////////////////////////////////////*/

  async mset(entries: readonly (readonly [key: string, value: Buffer[]])[]): Promise<void> {
    if (entries.length === 0) return;
    try {
      await this.inFlight.track(this._mset(new Map(entries)));
    } catch (err) {
      this.log("mset")?.withError(err).warn("mset failed");
    }
  }

  /** Routes each entry to the direct or staged path by the exact size of its `WRITE_DIRECT` request. */
  private async _mset(entries: Map<string, Buffer[]>) {
    const deadlineArg = this.options.ttl === undefined ? "" : String(Date.now() + this.options.ttl * 1000);

    const directSlots: Slot<string>[] = [];
    const stagedWrites: (readonly [keys: string[], write: Promise<void>])[] = [];

    for (const [key, value] of entries) {
      const shards = encodeShards(value, this.shardBytes);
      if (shards === null) {
        this.log("mset", { key })?.warn("value exceeds record cap; dropped");
        continue;
      }

      const slot = toSlot(["evalsha", WRITE_DIRECT_SHA, 1, key, deadlineArg, ...shards], key);
      if (measurePipeline([slot.requestBytes]) <= this.maxRequestBytes) {
        directSlots.push(slot);
      } else {
        stagedWrites.push([[key], this.writeStaged(key, value, shards, deadlineArg)]);
      }
    }

    const pipelines = packPipelines(directSlots, this.maxRequestBytes);
    const writes = [
      ...pipelines.map((p) => [p.map((slot) => slot.tag), this.writeDirect(p)] as const),
      ...stagedWrites,
    ];
    const outcomes = await Promise.allSettled(writes.map(([, promise]) => promise));
    outcomes.forEach((outcome, i) => {
      if (outcome.status === "rejected") {
        this.log("mset", { keys: writes[i]![0] })?.withError(outcome.reason).warn("write failed");
      }
    });
  }

  private async writeDirect(slots: Slot<string>[]) {
    const results = await this.execScript(
      slots.map((slot) => slot.command),
      WRITE_DIRECT_SCRIPT,
    );
    slots.forEach(({ tag: key }, i) => {
      this.logScriptResult(key, results[i]!);
    });
  }

  private async writeStaged(key: string, value: Buffer[], shards: string[], deadlineArg: string) {
    let staged = this.planStagedWrite(key, shards, deadlineArg);

    // Before PUBLISH is issued, failed staging may restart once under a fresh wid and staging key.
    try {
      await this.stage(staged.batches);
    } catch (err) {
      this.log("mset", { key })?.withError(err).info("staging failed; restarting with a fresh staging key");
      staged = this.planStagedWrite(key, encodeShards(value, this.shardBytes)!, deadlineArg);
      await this.stage(staged.batches);
    }

    // Once PUBLISH is issued, only the same publish may be retried — never a restage.
    let results: CommandResult[];
    try {
      results = await this.execScript([staged.publish], PUBLISH_SCRIPT);
    } catch (err) {
      this.log("mset", { key })?.withError(err).info("publish transport failure; retrying same publish");
      results = await this.execScript([staged.publish], PUBLISH_SCRIPT);
    }
    this.logScriptResult(key, results[0]!);
  }

  /** Splits `shards` into request-sized `MULTI` batches (`RPUSH` + `EXPIRE`) against a fresh staging key, plus `PUBLISH`. */
  private planStagedWrite(key: string, shards: string[], deadlineArg: string): StagedWrite {
    const stagingKey = `tmp:${key}:${randomUUID()}`;
    const expire: Command = ["expire", stagingKey, STAGING_TTL_SEC];
    const measureBatch = (push: Command) => measurePipeline([measureCommand(push), measureCommand(expire)]);

    const batches: Command[][] = [];
    let push: Command = ["rpush", stagingKey];
    for (const shard of shards) {
      const grown = [...push, shard];
      const isEmpty = push.length === 2;
      if (!isEmpty && measureBatch(grown) > this.maxRequestBytes) {
        batches.push([push, expire]);
        push = ["rpush", stagingKey, shard];
      } else {
        push = grown;
      }
    }
    batches.push([push, expire]);

    const publish: Command = ["evalsha", PUBLISH_SHA, 2, stagingKey, key, shards[0]!, shards.length, deadlineArg];
    return { batches, publish };
  }

  private async stage(batches: Command[][]) {
    for (const batch of batches) {
      const results = await this.execPipeline(batch, { atomic: true });
      const failed = results.find((r) => r.error !== undefined);
      if (failed) throw new Error(failed.error);
    }
  }

  private logScriptResult(key: string, { result, error }: CommandResult) {
    const log = this.log("mset", { key, result, error });
    if (error !== undefined) return log?.warn("script failed");
    const outcome = SCRIPT_OUTCOMES[result as ScriptResult];
    if (outcome === undefined) return log?.error("unexpected script result (bug)");
    if (outcome !== null) log?.[outcome[0]](outcome[1]);
  }

  /*//////////////////////////////////////////////////////////////
                                DELETE
  //////////////////////////////////////////////////////////////*/

  async mdelete(keys: readonly string[]): Promise<void> {
    if (keys.length === 0) return;
    try {
      await this.inFlight.track(this._mdelete([...new Set(keys)]));
    } catch (err) {
      this.log("mdelete")?.withError(err).warn("mdelete failed");
    }
  }

  private async _mdelete(keys: string[]) {
    const slots = keys.map((key) => toSlot(["unlink", key], key));
    await Promise.all(
      packPipelines(slots, this.maxRequestBytes).map(async (pipeline) => {
        const results = await this.execPipeline(pipeline.map((slot) => slot.command));
        pipeline.forEach(({ tag: key }, i) => {
          const { error } = results[i]!;
          if (error !== undefined) this.log("mdelete", { key, error })?.warn("unlink failed");
        });
      }),
    );
  }

  async flush() {
    try {
      await this.inFlight.flush();
    } catch (err) {
      this.log("flush")?.withError(err).warn("flush failed");
    }
  }

  /*//////////////////////////////////////////////////////////////
                          SINGULAR ADAPTERS
  //////////////////////////////////////////////////////////////*/

  /** @deprecated Use {@link UpstashStore.mget}. */
  get(key: string): Promise<Buffer[] | null> {
    return this.mget([key]).then((values) => values[0]!);
  }

  /** @deprecated Use {@link UpstashStore.mset}. */
  set(key: string, value: Buffer[]): Promise<void> {
    return this.mset([[key, value]]);
  }

  /** @deprecated Use {@link UpstashStore.mdelete}. */
  delete(key: string): Promise<void> {
    return this.mdelete([key]);
  }
}

export function createOptimizedUpstashStore(options: UpstashStoreOptions) {
  const remote = new UpstashStore(options);

  // 10k commands/sec → 3-6 commands/write (or more for high shard count) → 3+ concurrent instances ≅ 300 writes/sec
  const maxWritesPerSecond = 300;
  // 100 commands/(10ms Upstash job bucket) → 3-6 commands/write → 3+ concurrent instances ≅ 3 writes
  const maxWritesBurst = 3;

  // We coalesce writes per key and rate-limit remote persistence.
  return new HierarchicalStore(
    [
      new LruStore({ maxBytes: 1 << 30, logger: options.logger }), // 1 GB
      new ThrottledStore(remote, {
        maxStalenessMs: 60_000, // defend against serverless freeze/thaw cycles
        maxWritesBurst,
        maxWritesPerSecond,
        maxConcurrent: Infinity,
        logger: options.logger,
      }),
    ],
    { populateOnMiss: true, logger: options.logger },
  );
}
