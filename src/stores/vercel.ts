/// <reference types="node" />
import { createHash } from "crypto";

import { del, get, put } from "@vercel/blob";

import type { Logger } from "../observability.js";
import type { Store } from "../types.js";
import { createInFlightBarrier } from "../utils/in-flight.js";

import { HierarchicalStore } from "./hierarchical.js";
import { LruStore } from "./lru.js";
import { ThrottledStore } from "./throttled.js";

/** Vercel recommends multipart uploads for payloads larger than 100 MB. */
const MULTIPART_THRESHOLD_BYTES = 100 * 1024 * 1024;

export type VercelStoreOptions = {
  /**
   * Read-write token. Defaults to `process.env.BLOB_READ_WRITE_TOKEN` when
   * omitted (the SDK reads it automatically on Vercel deployments).
   */
  token?: string;
  /**
   * Optional pathname prefix applied to every key, used to namespace one
   * physical Blob store across multiple consumers. Leading/trailing slashes
   * are normalized.
   */
  prefix?: string;
  /**
   * CDN `Cache-Control` max-age (seconds) applied on each write. Must be >= 60.
   * Defaults to the SDK default (~1 month) when omitted.
   *
   * Only meaningful when `useCdnCache` is `true` -- otherwise reads bypass the
   * CDN cache regardless.
   */
  cacheControlMaxAge?: number;
  /**
   * Whether to read through Vercel's CDN cache. Defaults to `false`.
   *
   * - `false` (default): every read fetches directly from origin storage,
   *   guaranteeing read-after-write consistency. Each fall-through read counts
   *   as one Simple Operation regardless of recency.
   * - `true`: reads may be served from the CDN cache. Cheaper for hot keys,
   *   but overwrites and deletes take **up to ~60 seconds** to propagate.
   *
   * The default favors correctness; use `true` only if your workload tolerates
   * eventual consistency in exchange for fewer Simple Operations.
   */
  useCdnCache?: boolean;
  /** Optional logger for non-request-bound emissions (e.g. background I/O errors). */
  logger?: Logger;
};

/**
 * A store that uses **private** Vercel Blob storage for robust storage and
 * retrieval of large, blob-like data.
 *
 * - Each value is written as a single blob via `put()` (atomic, no sharding).
 * - Payloads larger than 100 MB transparently switch to multipart uploads.
 * - Best-effort: methods catch errors and warn rather than throw, per the
 *   `Store` contract.
 *
 * ## Caching and consistency
 *
 * Vercel Blob serves reads through a CDN cache that sits between this store
 * and the underlying object storage, with two consequences worth knowing:
 *
 * 1. **Up-to-60-second propagation on overwrites/deletes** _when reading
 *    through the CDN cache._ Per Vercel's documentation, after a `set` that
 *    overwrites an existing key (or after a `delete`), other readers may
 *    continue to see the previous value for up to ~60 seconds.
 *
 * 2. **`VercelStore` defaults to bypassing the CDN cache on reads**
 *    (`useCdnCache: false`, which passes `useCache: false` to the SDK's
 *    `get()` -- a private-blob-only feature). This guarantees read-after-write
 *    consistency at the cost of one Simple Operation per fall-through read.
 *    Set `useCdnCache: true` to opt back into the cheaper-but-eventually-
 *    consistent behavior.
 *
 * 3. **Cost model.** Each `set` is one Advanced Operation (multipart writes
 *    count as `1 + N + 1`); each `del` is also an Advanced Operation but is
 *    free of charge. Each non-cached read (the default) is one Simple
 *    Operation. Per-store rate limits are 15/s advanced, 20/s simple on
 *    Hobby (75/s and 120/s on Pro). The default throttle in
 *    `createOptimizedVercelStore` matches the Hobby advanced-op cap.
 *
 * 4. **Browser caching does not apply** to this store -- reads happen
 *    server-side via the SDK and blob URLs are never exposed to browsers.
 *
 * Treat `VercelStore` as a slow, cost-bounded persistence tier; the
 * `createOptimizedVercelStore` factory fronts it with `LruStore` so hot reads
 * never hit the network at all.
 */
export class VercelStore implements Store {
  private readonly options: VercelStoreOptions;
  private readonly inFlight = createInFlightBarrier();
  private readonly prefix: string;
  private readonly useCache: boolean;

  constructor(options: VercelStoreOptions = {}) {
    if (
      options.cacheControlMaxAge !== undefined &&
      (!Number.isFinite(options.cacheControlMaxAge) || options.cacheControlMaxAge < 60)
    ) {
      const err = new Error(`cacheControlMaxAge must be >= 60 seconds (got ${options.cacheControlMaxAge})`);
      options.logger?.withMetadata({ class: VercelStore.name, method: "constructor" }).withError(err).error();
      throw err;
    }

    this.options = options;
    this.prefix = (options.prefix ?? "").replace(/^\/+|\/+$/g, "");
    this.useCache = options.useCdnCache ?? false;
  }

  private resolvePathname(key: string): string {
    // sha256 hex keeps the pathname in a safe [0-9a-f] subset (Vercel's backend chokes on
    // some %-encoded bytes), fixed-length well under the 950-char pathname cap, and
    // collision-free at any realistic key count.
    const encoded = createHash("sha256").update(key).digest("hex");
    return this.prefix ? `${this.prefix}/${encoded}` : encoded;
  }

  private get tokenOption(): { token?: string } {
    return this.options.token ? { token: this.options.token } : {};
  }

  private async _get(key: string): Promise<Buffer[] | null> {
    const result = await get(this.resolvePathname(key), {
      access: "private",
      useCache: this.useCache,
      ...this.tokenOption,
    });
    if (result === null || result.stream === null) return null;

    const chunks: Uint8Array[] = [];
    const reader = result.stream.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        if (value) chunks.push(value);
      }
    } finally {
      reader.releaseLock();
    }
    return [Buffer.concat(chunks)];
  }

  async get(key: string): Promise<Buffer[] | null> {
    try {
      return await this._get(key);
    } catch (err) {
      this.options.logger?.withMetadata({ class: VercelStore.name, method: "get", key }).withError(err).warn("get failed");
      return null;
    }
  }

  private async _set(key: string, value: Buffer[]): Promise<void> {
    const body = Buffer.concat(value);
    await put(this.resolvePathname(key), body, {
      access: "private",
      allowOverwrite: true,
      addRandomSuffix: false,
      // Auto-enable multipart for payloads > 100 MB per Vercel's recommendation.
      // The SDK handles part splitting and reassembly transparently.
      multipart: body.byteLength > MULTIPART_THRESHOLD_BYTES,
      ...(this.options.cacheControlMaxAge !== undefined ? { cacheControlMaxAge: this.options.cacheControlMaxAge } : {}),
      ...this.tokenOption,
    });
  }

  async set(key: string, value: Buffer[]): Promise<void> {
    try {
      await this.inFlight.track(this._set(key, value));
    } catch (err) {
      this.options.logger?.withMetadata({ class: VercelStore.name, method: "set", key }).withError(err).warn("set failed");
    }
  }

  async delete(key: string): Promise<void> {
    try {
      await this.inFlight.track(Promise.resolve(del(this.resolvePathname(key), this.tokenOption)));
    } catch (err) {
      this.options.logger?.withMetadata({ class: VercelStore.name, method: "delete", key }).withError(err).warn("delete failed");
    }
  }

  async flush(): Promise<void> {
    try {
      await this.inFlight.flush();
    } catch (err) {
      this.options.logger?.withMetadata({ class: VercelStore.name, method: "flush" }).withError(err).warn("flush failed");
    }
  }
}

export function createOptimizedVercelStore(options: VercelStoreOptions = {}) {
  const remote = new VercelStore(options);

  // Vercel Blob's documented per-store advanced-operation rate limit is 15/s
  // on Hobby, 75/s on Pro, 125/s on Enterprise. Default to the Hobby cap.
  // Multipart uploads count as multiple advanced ops (start + N parts + end);
  // the LruStore tier above absorbs the bulk of cross-write traffic so the
  // remote tier rarely saturates this limit in practice.
  const maxWritesPerSecond = 15;
  const maxWritesBurst = 5;

  // The in-memory tier provides read-your-own-writes within a single process
  // and shields the remote tier from network/operation cost on hot keys.
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
