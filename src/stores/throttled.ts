import type { MaybePromise } from "viem";
import { withTimeout } from "viem";

import type { Logger } from "../observability.js";
import type { Store } from "../types.js";
import { createRateLimit, RateLimitGateError } from "../utils/with-rate-limit.js";

type PendingOp = { kind: "set"; value: Buffer[] } | { kind: "delete" };

export type ThrottledStoreOptions = {
  /** Maximum number of writes that can be initiated concurrently from resting state. */
  maxWritesBurst: number;
  /** Writes per second (token refill rate). Use `Infinity` to disable rate limiting. */
  maxWritesPerSecond: number;
  /** Maximum number of concurrent in-flight writes to the underlying store. */
  maxConcurrent: number;
  /** Drop queued writes older than this (ms). Defends against serverless freeze/thaw. Default: `Infinity`. */
  maxStalenessMs: number;
  /** Optional: handle write errors (default: ignore) -- MUST NOT THROW. */
  onWriteError?: (key: string, err: unknown, durationMs: number) => void;
  /**
   * Optional logger for non-request-bound emissions (e.g. background flush boundaries).
   * Per-`set`/`get`/`delete` events are read from the ambient ALS scope via `getCurrentLog()`
   * — this field is only needed for events that fire outside any caller's async scope.
   */
  logger?: Logger;
};

/**
 * A store that rate-limits, concurrency-limits, and coalesces writes to an underlying store.
 *
 * - `get` is passed through without throttling.
 * - `set` and `delete` are coalesced per-key: only the latest op is sent upstream.
 * - Writes are admitted by a token-bucket rate limiter with bounded concurrency.
 * - `flush` snapshots current key:version pairs, waits for each to be written
 *   (or intentionally discarded). A newer version satisfies the snapshotted one.
 *   Writes added after a key is satisfied do not extend the flush.
 */
export class ThrottledStore implements Store {
  private readonly rateLimiter: ReturnType<typeof createRateLimit>;
  private readonly maxStalenessMs: number;
  private readonly onWriteError?: (key: string, err: unknown, durationMs: number) => void;
  protected readonly logger?: Logger;

  /** Latest pending op per key. Written at call time, read lazily at admission time, deleted at completion. */
  private readonly pending = new Map<string, { op: PendingOp; version: number; lastUpdatedAt: number }>();
  /** Keys with a job queued or in-flight. Prevents duplicate jobs per key. */
  private readonly active = new Set<string>();
  /** Flush boundaries waiting for specific key:version pairs to settle. */
  private readonly flushBoundaries: { resolve: () => void; keys: Map<string, number> }[] = [];

  constructor(
    private readonly store: Store,
    opts: ThrottledStoreOptions,
  ) {
    this.rateLimiter = createRateLimit(opts.maxWritesBurst, opts.maxWritesPerSecond, opts.maxConcurrent);
    this.maxStalenessMs = opts.maxStalenessMs;
    this.onWriteError = opts.onWriteError;
    this.logger = opts.logger;
  }

  get(key: string) {
    return this.store.get(key);
  }

  set(key: string, value: Buffer[]) {
    const prev = this.pending.get(key);
    this.pending.set(key, {
      op: { kind: "set", value },
      version: (prev?.version ?? 0) + 1,
      lastUpdatedAt: Date.now(),
    });
    return this.ensureQueued(key);
  }

  delete(key: string) {
    const prev = this.pending.get(key);
    this.pending.set(key, {
      op: { kind: "delete" },
      version: (prev?.version ?? 0) + 1,
      lastUpdatedAt: Date.now(),
    });
    return this.ensureQueued(key);
  }

  async flush() {
    const snapshot = new Map([...this.pending.entries()].map(([key, { version }]) => [key, version]));
    if (snapshot.size > 0) {
      await new Promise<void>((resolve) => {
        this.flushBoundaries.push({ resolve, keys: snapshot });
      });
    }
    await this.store.flush();
  }

  private ensureQueued(key: string): MaybePromise<void> {
    if (this.active.has(key)) return;
    this.active.add(key);

    return this.rateLimiter
      .withRateLimit(
        async () => {
          const entry = this.pending.get(key);
          if (entry === undefined) return;

          const t0 = Date.now();
          try {
            await withTimeout(
              async () => (entry.op.kind === "set" ? this.store.set(key, entry.op.value) : this.store.delete(key)),
              {
                errorInstance: new Error(`[ThrottledStore] upstream ${entry.op.kind} timed out for key "${key}"`),
                timeout: 10_000,
              },
            );
          } catch (err) {
            this.logger
              ?.withMetadata({
                class: ThrottledStore.name,
                method: "ensureQueued",
                key,
                kind: entry.op.kind,
                duration_ms: Date.now() - t0,
              })
              .withError(err)
              .warn("upstream write failed");
            this.onWriteError?.(key, err, Date.now() - t0);
          }

          this.resolveFlushBoundaries(key, entry.version);
          if (this.pending.get(key)?.version === entry.version) {
            this.pending.delete(key);
          }
        },
        {
          gate: () => {
            const entry = this.pending.get(key);
            if (entry === undefined) return false;

            const isValid = Date.now() - entry.lastUpdatedAt <= this.maxStalenessMs;
            if (!isValid) {
              this.logger
                ?.withMetadata({
                  class: ThrottledStore.name,
                  method: "ensureQueued",
                  key,
                  kind: entry.op.kind,
                  age_ms: Date.now() - entry.lastUpdatedAt,
                })
                .info("dropped stale pending op");
              this.pending.delete(key);
              this.resolveFlushBoundaries(key, entry.version);
            }

            return isValid;
          },
        },
      )
      .catch((err) => {
        if (err instanceof RateLimitGateError) return;
        throw err;
      })
      .finally(() => {
        this.active.delete(key);
        if (this.pending.has(key)) this.ensureQueued(key);
      });
  }

  private resolveFlushBoundaries(key: string, version: number): void {
    for (let i = this.flushBoundaries.length - 1; i >= 0; i--) {
      const boundary = this.flushBoundaries[i]!;
      const target = boundary.keys.get(key);
      if (target === undefined || version < target) continue;

      boundary.keys.delete(key);
      if (boundary.keys.size === 0) {
        boundary.resolve();
        this.flushBoundaries.splice(i, 1);
      }
    }
  }
}
