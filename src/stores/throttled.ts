import { type MaybePromise, withTimeout } from "viem";

import type { Logger } from "../observability.js";
import type { ProvenanceAwareStore, Store, StoreRead } from "../types.js";
import { createRateLimit, RateLimitGateError } from "../utils/with-rate-limit.js";

type PendingOp = { kind: "set"; value: Buffer[] } | { kind: "delete" };

function isThenable<T>(value: MaybePromise<T>): value is Promise<T> {
  return typeof (value as { then?: unknown } | null | undefined)?.then === "function";
}

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
 * - `get` is not throttled. It serves the pending `set`/`delete` still eligible for admission, else
 *   the one being attempted upstream, so a read reflects a write this store can still make. Once an
 *   op settles — persisted, failed, or discarded as stale — reads revert to the underlying store.
 *   Such a read is `provisional` via {@link ProvenanceAwareStore.getWithProvenance}.
 * - `set` and `delete` record the op and return immediately, so callers are never
 *   blocked by the rate limiter. {@link ThrottledStore.flush} is the only durability
 *   barrier; write failures surface through `onWriteError`, never as a rejection.
 * - `set` and `delete` are coalesced per-key: only the latest op is sent upstream.
 * - Writes are admitted by a token-bucket rate limiter with bounded concurrency.
 * - `flush` snapshots current key:version pairs, waits for each to be written
 *   (or intentionally discarded). A newer version satisfies the snapshotted one.
 *   Writes added after a key is satisfied do not extend the flush.
 */
export class ThrottledStore implements ProvenanceAwareStore {
  private readonly rateLimiter: ReturnType<typeof createRateLimit>;
  private readonly maxStalenessMs: number;
  private readonly onWriteError?: (key: string, err: unknown, durationMs: number) => void;
  protected readonly logger?: Logger;

  /** Latest pending op per key. Written at call time, read lazily at admission time, deleted at completion. */
  private readonly pending = new Map<string, { op: PendingOp; version: number; lastUpdatedAt: number }>();
  /** Keys with a job queued or in-flight. Prevents duplicate jobs per key. */
  private readonly active = new Set<string>();
  /** The op being attempted upstream, per key. Outlives its `pending` entry when a newer version lands. */
  private readonly attempting = new Map<string, PendingOp>();
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
    const read = this.getWithProvenance(key);
    return isThenable(read) ? read.then(({ value }) => value) : read.value;
  }

  getWithProvenance(key: string): MaybePromise<StoreRead> {
    // Serve an op only while it can still reach the upstream store — the latest pending one inside the
    // gate's staleness window, else whichever is being attempted. Either way the value is unpersisted,
    // so it is provisional: see {@link StoreRead} for what a tier above must not do with it.
    const entry = this.pending.get(key);
    if (entry !== undefined && Date.now() - entry.lastUpdatedAt <= this.maxStalenessMs) {
      return { value: entry.op.kind === "set" ? entry.op.value : null, provisional: true };
    }
    const attempted = this.attempting.get(key);
    if (attempted !== undefined) {
      return { value: attempted.kind === "set" ? attempted.value : null, provisional: true };
    }
    const stored = this.store.get(key);
    return isThenable(stored)
      ? stored.then((value) => ({ value, provisional: false }))
      : { value: stored, provisional: false };
  }

  set(key: string, value: Buffer[]): void {
    const prev = this.pending.get(key);
    this.pending.set(key, {
      op: { kind: "set", value },
      version: (prev?.version ?? 0) + 1,
      lastUpdatedAt: Date.now(),
    });
    this.ensureQueued(key);
  }

  delete(key: string): void {
    const prev = this.pending.get(key);
    this.pending.set(key, {
      op: { kind: "delete" },
      version: (prev?.version ?? 0) + 1,
      lastUpdatedAt: Date.now(),
    });
    this.ensureQueued(key);
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

  private ensureQueued(key: string): void {
    if (this.active.has(key)) return;
    this.active.add(key);

    const queuedAt = Date.now();

    void this.rateLimiter
      .withRateLimit(
        async () => {
          const entry = this.pending.get(key);
          if (entry === undefined) return;

          const t0 = Date.now();
          this.attempting.set(key, entry.op);
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
          } finally {
            this.attempting.delete(key);
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
        // `RateLimitGateError` is the expected discard path (op went stale, see `gate` above).
        // Anything else is an admission failure -- report it rather than rejecting a caller
        // that has already moved on.
        if (err instanceof RateLimitGateError) return;
        this.onWriteError?.(key, err, Date.now() - queuedAt);
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
