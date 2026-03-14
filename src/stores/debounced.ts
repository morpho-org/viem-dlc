import { withTimeout } from "viem";

import type { Store } from "../types.js";
import { sleep } from "../utils/sleep.js";
import { createTokenBucket, type TokenBucket, timeUntilToken, tryConsume } from "../utils/with-rate-limit.js";

type PendingOp = { kind: "set"; value: string } | { kind: "delete" };

type PendingEntry = {
  op: PendingOp;
  firstQueuedAt: number;
  lastQueuedAt: number;
  version: number;
};

export type DebouncedStoreOptions = {
  /** Trigger flush no earlier than `lastQueuedAt + debounceMs`. */
  debounceMs: number;
  /** Trigger flush no later than `firstQueuedAt + maxStalenessMs` (best-effort). */
  maxStalenessMs: number;
  /** The maximum number of writes that can be initiated concurrently from resting state. */
  maxWritesBurst: number;
  /**
   * Rate limit for underlying store writes.
   * - Use `Infinity` to disable rate limiting (flush as fast as possible).
   * - Use `0` to allow only 1 write (initial token), then stop.
   */
  maxWritesPerSecond: number;
  /** Optional: handle write errors (default: log/ignore). */
  onWriteError?: (key: string, err: unknown) => void;
};

function dueAt(e: PendingEntry, opts: Pick<DebouncedStoreOptions, "debounceMs" | "maxStalenessMs">) {
  const debounceDue = e.lastQueuedAt + opts.debounceMs;
  const stalenessDue = e.firstQueuedAt + opts.maxStalenessMs;
  return Math.min(debounceDue, stalenessDue);
}

function applyUpstream(store: Store, key: string, op: PendingOp): Promise<void> {
  const fn = async () => (op.kind === "set" ? store.set(key, op.value) : store.delete(key));
  // NOTE: It's important for DebouncedStore.pump that this doesn't hang.
  return withTimeout(fn, {
    errorInstance: new Error(`[DebouncedStore] upstream ${op.kind} timed out for key "${key}"`),
    timeout: 10_000,
  });
}

/**
 * A store that debounces writes to an underlying store.
 * - Coalesces multiple writes to the same key.
 * - Respects `debounceMs` (wait for quiet period) and `maxStalenessMs` (force flush).
 * - Rate limits writes to the underlying store using a token bucket.
 * - Inlines rate limiting: prioritizes the most urgent keys when tokens are available.
 */
export class DebouncedStore implements Store {
  private readonly buffered = new Map<string, PendingEntry>();
  private readonly inFlight = new Map<string, Promise<void>>();

  private readonly bucket: TokenBucket;
  private readonly onWriteError: (key: string, err: unknown) => void;

  private closed = false;
  private pumpPromise: Promise<void> | null = null;

  constructor(
    private readonly store: Store,
    private readonly opts: DebouncedStoreOptions,
  ) {
    this.bucket = createTokenBucket(opts.maxWritesBurst, opts.maxWritesPerSecond);
    this.onWriteError = opts.onWriteError ?? (() => {});
  }

  /*//////////////////////////////////////////////////////////////
                                PUBLIC
  //////////////////////////////////////////////////////////////*/

  /**
   * Gets `key` from the underlying store -- no awareness of buffered writes.
   *
   * @dev Use `new HierarchicalStore([new MemoryStore(), new DebouncedStore(...)])` if you want
   * read-your-writes guarantee.
   */
  async get(key: string) {
    return this.store.get(key);
  }

  /** Schedules `key` to be set on the underlying store, respecting the configured `DebouncedStoreOptions`. */
  async set(key: string, value: string) {
    if (this.closed) throw new Error("[DebouncedStore] Store is closed");
    this.buffer(key, { kind: "set", value });
    void this.pump();
  }

  /** Schedules `key` to be deleted from the underlying store, respecting the configured `DebouncedStoreOptions`. */
  async delete(key: string) {
    if (this.closed) throw new Error("[DebouncedStore] Store is closed");
    this.buffer(key, { kind: "delete" });
    void this.pump();
  }

  /**
   * Waits for all writes to complete.
   * - If not closed: drains buffered entries and waits for in-flight.
   * - If closed: only waits for in-flight (buffered entries are abandoned).
   */
  async flush() {
    // Wait for all buffered entries to be pumped to in-flight.
    await this.pump();
    // Wait for in-flight promises to resolve (they handle their own errors, so no .allSettled necessary).
    await Promise.all([...this.inFlight.values()]);
    // Wait for underlying store to do its own flush.
    await this.store.flush();
  }

  /** Prevents further `buffered` entries from being moved to in-flight. */
  close(): void {
    this.closed = true;
  }

  /*//////////////////////////////////////////////////////////////
                              PRIVATE
  //////////////////////////////////////////////////////////////*/

  /** Buffers `op` for `key`, to be applied upstream later on. */
  private buffer(key: string, op: PendingOp) {
    const now = Date.now();
    const prev = this.buffered.get(key);

    this.buffered.set(key, {
      op,
      firstQueuedAt: prev?.firstQueuedAt ?? now,
      lastQueuedAt: now,
      version: (prev?.version ?? 0) + 1,
    });
  }

  /** Moves ops from `buffered` to `inFlight`, one at a time, respecting rate limit. */
  private async pump(): Promise<void> {
    if (!this.pumpPromise) {
      this.pumpPromise = this._pump().finally(() => {
        this.pumpPromise = null;
      });
    }

    return this.pumpPromise;
  }

  /** Guarded inner method -- only to be called within `pump`. */
  private async _pump(): Promise<void> {
    while (this.buffered.size > 0 && !this.closed) {
      // Respect rate limit
      const waitTime = timeUntilToken(this.bucket);
      if (waitTime === Infinity) {
        break;
      }
      if (waitTime > 0) {
        await sleep(waitTime);
        continue;
      }

      const key = this.pickNextKey();
      if (!key) {
        // No keys due? Sleep briefly.
        await sleep(10);
        continue;
      }

      // Consume 1 token
      if (!tryConsume(this.bucket)) {
        // At least 1 token should always be available since we waited, but we're defensive here nonetheless.
        continue;
      }

      void this.flushSnapshot(key);
    }
  }

  /** Pick the buffered entry that is longest overdue, where due date is defined in `dueAt` fn. */
  private pickNextKey(): string | undefined {
    const now = Date.now();
    let bestKey: string | undefined;
    let bestDueAt = Infinity;

    for (const [key, entry] of this.buffered) {
      if (this.inFlight.has(key)) continue;

      const when = dueAt(entry, this.opts);

      if (when > now) continue;

      if (when < bestDueAt) {
        bestDueAt = when;
        bestKey = key;
      }
    }

    return bestKey;
  }

  /** Take the buffered entry for `key` and apply it upstream, marking as in-flight while waiting for promise. */
  private async flushSnapshot(key: string): Promise<void> {
    const entry = this.buffered.get(key);
    if (!entry) return;

    const snap = { version: entry.version, op: entry.op };

    // NOTE: pump handles token consumption; we don't need `withRateLimit`
    const promise = applyUpstream(this.store, key, snap.op)
      .then(() => {
        // Read most recent entry for this key
        const cur = this.buffered.get(key);
        if (cur && cur.version === snap.version) {
          // If the version we applied upstream matches the _current_ version, terminate it.
          this.buffered.delete(key);
        } else if (cur) {
          // If what we applied upstream was a _previous_ version, bump `firstQueuedAt` to
          // reflect successful write, i.e. timelines reset for the latest version.
          // NOTE: This isn't perfectly correct, as multiple versions could've been written
          // _between_ what was applied upstream and what is current, but this approximation
          // is reasonable given quick upstream roundtrips.
          cur.firstQueuedAt = cur.lastQueuedAt;
        }
      })
      .catch((err) => {
        // Read most recent entry for this key
        const cur = this.buffered.get(key);
        if (cur && cur.version === snap.version) {
          // If the version that errored matches the _current_ version, terminate it.
          // User is expected to handle retries at a higher level.
          this.buffered.delete(key);
        }
        this.onWriteError(key, err);
      })
      .finally(() => {
        this.inFlight.delete(key);
      });

    this.inFlight.set(key, promise);

    return promise;
  }
}
