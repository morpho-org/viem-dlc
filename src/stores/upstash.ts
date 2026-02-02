/// <reference types="node" />
import { randomBytes, randomUUID } from "crypto";

import { Redis, type RedisConfigNodejs } from "@upstash/redis";

import type { Store } from "../types.js";
import { omit } from "../utils/omit.js";
import { shardString } from "../utils/string.js";

import { CompressedStore } from "./compressed.js";
import { DebouncedStore } from "./debounced.js";
import { HierarchicalStore } from "./hierarchical.js";
import { MemoryStore } from "./memory.js";

export type UpstashStoreOptions = {
  maxRequestBytes: number;
  ttl?: number;
  redis?: Omit<RedisConfigNodejs, "automaticDeserialization">;
};

class WriteId {
  static readonly BYTES_OF_RANDOMNESS = 8;
  static readonly LENGTH = 1 + WriteId.BYTES_OF_RANDOMNESS * 2; // hex chars + separator
  static readonly SEPARATOR = "|";

  readonly id: string;

  constructor() {
    this.id = randomBytes(WriteId.BYTES_OF_RANDOMNESS).toString("hex");
  }

  pack(shard: string): string {
    return `${this.id}${WriteId.SEPARATOR}${shard}`;
  }

  static unpack(shardWithId: string): [string, string] {
    const re = new RegExp(`^[0-9A-Fa-f]{${WriteId.LENGTH - 1}}\\${WriteId.SEPARATOR}`);
    if (!re.test(shardWithId)) {
      return ["0".repeat(WriteId.LENGTH - 1), shardWithId]; // legacy
    }
    const sepIdx = shardWithId.indexOf(WriteId.SEPARATOR);
    return [shardWithId.slice(0, sepIdx), shardWithId.slice(sepIdx + 1)];
  }
}

/** Lua script: Returns {length, firstShardOrNil} to avoid 1 roundtrip. */
const SMART_READ_SCRIPT = `
local len = redis.call('LLEN', KEYS[1])
if len == 0 then
  return {0, nil}
else
  return {len, redis.call('LINDEX', KEYS[1], 0)}
end
`;

/**
 * A store that uses Upstash Redis for robust storage and retrieval of large, blob-like data.
 *
 * - Stores all strings as arrays, sharding into multiple indices when they grow too large
 * - Robust under concurrency -- writes are atomic, and reads fail safely if non-atomicity is detected
 * - Respects\* `maxRequestBytes` for HTTP requests and responses
 *
 * \* _Measures values only. Does not include Redis commands, headers, and Upstash specifics,
 *    so you should configure `maxRequestBytes` with some headroom (~1kb)_
 */
export class UpstashStore implements Store {
  private readonly options: UpstashStoreOptions;
  private readonly redis: Redis;

  constructor(options: UpstashStoreOptions) {
    if (!Number.isSafeInteger(options.maxRequestBytes) || options.maxRequestBytes! <= WriteId.LENGTH) {
      throw new Error(
        `[UpstashStore] maxRequestBytes must be a safe integer > ${WriteId.LENGTH} (got ${options.maxRequestBytes})`,
      );
    }

    if (options.ttl !== undefined && (!Number.isSafeInteger(options.ttl) || options.ttl! <= 0)) {
      throw new Error(`[UpstashStore] ttl must be a positive safe integer (got ${options.ttl})`);
    }

    this.options = options;
    this.redis = options.redis
      ? new Redis({ ...options.redis, automaticDeserialization: false })
      : Redis.fromEnv({ automaticDeserialization: false });
  }

  private async _get(key: string): Promise<{ value: string | null; motivatesRetry: boolean }> {
    // Read array length and first shard in one request. If length is 0, shard is null.
    const [len, shard0WithId] = await this.redis.evalRo<[], [number, string | null]>(SMART_READ_SCRIPT, [key], []);

    // If length is 0 / shard is null (always co-occur but both are here for type checker), key doesn't exist.
    if (len === 0 || shard0WithId === null) {
      return { value: null, motivatesRetry: false };
    }

    const [writeId0, shard0] = WriteId.unpack(shard0WithId);

    // Fetch remaining shards individually. This is necessary since shards are near request/response size limit.
    const shards: string[] = [shard0];
    for (let i = 1; i < len; i++) {
      // NOTE: We don't `Promise.all` these because we expect values to be large enough to be bandwidth-constrained.
      const shardWithId = (await this.redis.lindex(key, i)) as string | null;

      // If shard is null, array must've been shortened after our initial read (non-atomic inconsistency).
      if (shardWithId === null) {
        return { value: null, motivatesRetry: true };
      }

      const [writeId, shard] = WriteId.unpack(shardWithId);

      // If writeId doesn't match, array must've been overwritten after our initial read (non-atomic inconsistency).
      if (writeId !== writeId0) {
        return { value: null, motivatesRetry: true };
      }

      shards.push(shard);
    }

    return { value: shards.join(""), motivatesRetry: false };
  }

  async get(key: string, maxRetries = 2): Promise<string | null> {
    // Allow retries in cases of network error or non-atomic inconsistency.
    for (let i = 0; i < maxRetries; i++) {
      try {
        const { value, motivatesRetry } = await this._get(key);
        if (!motivatesRetry) {
          return value;
        }
      } catch {
        /* empty */
      }
    }

    return null;
  }

  async set(key: string, value: string) {
    // Split `value` into shard(s), each no bigger than `maxRequestBytes - WriteId.LENGTH`.
    const shards = shardString(value, this.options.maxRequestBytes - WriteId.LENGTH);
    const hasMultipleChunks = shards.length > 1;

    // Write directly to `key` if there's only one shard, otherwise build tmp value for atomicity.
    const opKey = hasMultipleChunks ? `tmp:${key}:${randomUUID()}` : key;
    const writeId = new WriteId();

    // Begin multi tx (atomic). A single shard is completed in 1 tx (skip if block); N shards need N+1 txs.
    let tx = this.redis.multi();
    tx.unlink(opKey);
    tx.rpush(opKey, writeId.pack(shards[0]!));

    if (hasMultipleChunks) {
      // Set safety TTL on tmp key (auto-cleanup if process crashes)
      tx.expire(opKey, 60);
      await tx.exec();

      // Push remaining shards individually. This is necessary since shards are near request/response size limit.
      for (let i = 1; i < shards.length; i++) {
        await this.redis.rpush(opKey, writeId.pack(shards[i]!));
      }

      // Begin new multi tx
      tx = this.redis.multi();
      tx.rename(opKey, key);
    }

    if (this.options.ttl) {
      tx.expire(key, this.options.ttl);
    } else {
      tx.persist(key);
    }

    await tx.exec();
  }

  async delete(key: string) {
    await this.redis.unlink(key);
  }
}

export function createOptimizedUpstashStore(options: UpstashStoreOptions & { maxWritesPerSecond: number }) {
  const remote = new UpstashStore(omit(options, ["maxWritesPerSecond"]));

  // 10k commands/sec → 3-6 commands/write (or more for high shard count) → 3+ concurrent instances ≅ 300 writes/sec
  const maxWritesPerSecond = 300;

  // We use DebouncedStore to coalesce writes and reduce load, while still respecting rate limits.
  // debounceMs=500 gives good coalescing without too much lag.
  // maxStalenessMs=2000 ensures we don't hold data too long.
  return new HierarchicalStore([
    new MemoryStore(),
    new DebouncedStore(new CompressedStore(remote), {
      debounceMs: 500,
      maxStalenessMs: 2000,
      maxWritesBurst: maxWritesPerSecond,
      maxWritesPerSecond,
      onWriteError: (key, err) => console.error(`[UpstashStore] Write error for key ${key}:`, err),
    }),
  ]);
}
