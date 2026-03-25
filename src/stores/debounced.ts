import { withTimeout } from "viem";

import type { Store } from "../types.js";
import { sleep } from "../utils/sleep.js";
import { createTokenBucket, type TokenBucket, timeUntilToken, tryConsume } from "../utils/with-rate-limit.js";

type PendingOp = { kind: "set"; value: Buffer[] } | { kind: "delete" };

type PendingEntry = {
  op: PendingOp;
  firstQueuedAt: number;
  lastQueuedAt: number;
  version: number;
};

export type DebouncedStoreOptions = {
  /** Trigger writes no earlier than `lastQueuedAt + debounceMs`. */
  debounceMs: number;
  /** Trigger writes no later than `firstQueuedAt + maxDelayMs` (best-effort). */
  maxDelayMs: number;
  /** Eliminate pending writes older than `maxStalenessMs`. */
  maxStalenessMs: number;
  /** The maximum number of writes that can be initiated concurrently from resting state. */
  maxWritesBurst: number;
  /**
   * Rate limit for underlying store writes.
   * - Use `Infinity` to disable rate limiting (write as fast as possible).
   * - Use `0` to allow only `maxWritesBurst` writes (initial tokens), then stop. WARNING: CAUSES `flush` TO HANG!
   */
  maxWritesPerSecond: number;
  /** Optional: handle write errors (default: ignore) -- MUST NOT THROW. */
  onWriteError?: (key: string, err: unknown, durationMs: number) => void;
};

function deadlineFor(e: PendingEntry, opts: Pick<DebouncedStoreOptions, "debounceMs" | "maxDelayMs">) {
  const debounceDeadline = e.lastQueuedAt + opts.debounceMs;
  const maxDelayDeadline = e.firstQueuedAt + opts.maxDelayMs;
  return Math.min(debounceDeadline, maxDelayDeadline);
}

function applyUpstream(store: Store, key: string, op: PendingOp): Promise<void> {
  const fn = async () => (op.kind === "set" ? store.set(key, op.value) : store.delete(key));
  // NOTE: It's important for DebouncedStore.pump that this doesn't hang.
  return withTimeout(fn, {
    errorInstance: new Error(`[DebouncedStore] upstream ${op.kind} timed out for key "${key}"`),
    timeout: 10_000,
  });
}

type FlushBoundary = { resolve: () => void; keys: Map<string, PendingEntry["version"]> };

/**
 * A store that debounces writes to an underlying store.
 * - Coalesces multiple writes to the same key.
 * - Respects `debounceMs` (wait for quiet period) and `maxDelayMs` (force write).
 * - Rate limits writes to the underlying store using a token bucket.
 * - Inlines rate limiting: prioritizes the most urgent keys when tokens are available.
 *
 * @dev All timing is best-effort and may be off by up to 10ms (pump polling interval).
 */
export class DebouncedStore implements Store {
  /** Helper for managing `flush` boundaries. */
  private static Flushes = class {
    /** The resolve callback and snapshotted key:version map for each flush ID. */
    public readonly boundaries = new Map<number, FlushBoundary>();
    /** Counter for generating flush IDs */
    private counter = 0;

    /** Captures a new `boundary` for processing. */
    capture(boundary: FlushBoundary) {
      this.boundaries.set(this.counter++, boundary);
    }

    /** Keys blocking earlier flushes have higher priority than those blocking later ones. 0 if non-blocking. */
    getPriorityOf(key: string) {
      let highestPriority = 0;

      for (const [id, boundary] of this.boundaries) {
        if (!boundary.keys.has(key)) continue;

        const priority = this.counter - id;
        if (priority > highestPriority) {
          highestPriority = priority;
        }
      }

      return highestPriority;
    }

    /** For each active flush, marks `key` as complete & does cleanup if no keys remain. */
    resolve(key: string, version: PendingEntry["version"]) {
      for (const [id, boundary] of this.boundaries) {
        if (version < (boundary.keys.get(key) ?? Infinity)) continue;

        boundary.keys.delete(key);
        if (boundary.keys.size === 0) {
          boundary.resolve();
          this.boundaries.delete(id);
        }
      }
    }
  };

  private readonly buffered = new Map<string, PendingEntry>();
  private readonly inFlight = new Map<string, Promise<void>>();

  private readonly bucket: TokenBucket;
  private readonly onWriteError?: (key: string, err: unknown, durationMs: number) => void;

  private flushes = new DebouncedStore.Flushes();
  private pumpPromise: Promise<void> | null = null;

  constructor(
    private readonly store: Store,
    private readonly opts: DebouncedStoreOptions,
  ) {
    this.bucket = createTokenBucket(opts.maxWritesBurst, opts.maxWritesPerSecond);
    this.onWriteError = opts.onWriteError;
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

  /** Schedules `key` to be set on the underlying store. */
  async set(key: string, value: Buffer[]) {
    this.buffer(key, { kind: "set", value });
    void this.pump();
  }

  /** Schedules `key` to be deleted from the underlying store. */
  async delete(key: string) {
    this.buffer(key, { kind: "delete" });
    void this.pump();
  }

  /**
   * Snapshots buffered ops, schedules them to be executed (if not already in-flight) as quickly as rate-limit
   * allows (bypassing debounce logic), and waits for them to complete.
   *
   * @dev `await debouncedStore.flush()` DOES NOT imply that all data has been persisted -- only that we
   * **tried** to persist it (or dropped it after `maxStalenessMs`). The underlying store is allowed to error.
   *
   * @dev If you call `set`/`delete` on a snapshotted key mid-flush, that op *might* be included in the flush,
   * but we offer no guarantee -- the previously-buffered op may have already been flushed, in which case the
   * incoming one is subject to standard (non-flushing) debounce behavior.
   */
  async flush() {
    if (this.buffered.size > 0) {
      const keys = new Map([...this.buffered.entries()].map(([key, { version }]) => [key, version]));
      // wait for every key:version in `keys` to be processed
      await new Promise<void>((resolve) => {
        this.flushes.capture({ resolve, keys });
        void this.pump();
      });
    }

    // wait for underlying store **after** our work has been flushed
    await this.store.flush();
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
    while (this.buffered.size > 0) {
      const key = this.pickNextKey();
      if (!key) {
        // No keys due? Sleep briefly.
        await sleep(10);
        continue;
      }

      // Respect rate limit
      const waitTime = timeUntilToken(this.bucket);
      if (waitTime === Infinity) {
        break;
      }
      if (waitTime > 0) {
        await sleep(waitTime);
        continue;
      }

      // Consume 1 token
      if (!tryConsume(this.bucket)) {
        // At least 1 token should always be available since we waited, but we're defensive here nonetheless.
        continue;
      }

      this.writeSnapshot(key);
    }
  }

  /**
   * Finds the buffered entry with the earliest deadline, considering only **past** deadlines (`deadline <= now`)
   * unless the entry is part of a flush, in which case `debounceMs` is ignored and work is prioritized
   * (even if deadline is in the future).
   */
  private pickNextKey(): string | undefined {
    const now = Date.now();

    let next: {
      key?: string;
      deadline: number;
      priority: number;
    } = { deadline: Infinity, priority: 0 };

    for (const [key, entry] of this.buffered) {
      if (this.inFlight.has(key)) continue;

      // Drop entries whose last-queued value is too stale to be worth writing.
      if (now - entry.lastQueuedAt > this.opts.maxStalenessMs) {
        this.buffered.delete(key);
        this.flushes.resolve(key, entry.version);
        continue;
      }

      const deadline = deadlineFor(entry, this.opts);
      const priority = this.flushes.getPriorityOf(key);
      const isValidWork = priority > 0 || deadline <= now;

      if (isValidWork && (priority > next.priority || (priority === next.priority && deadline < next.deadline))) {
        next = { key, deadline, priority };
      }
    }

    return next.key;
  }

  /** Take the buffered entry for `key` and apply it upstream, marking as in-flight while waiting for promise. */
  private writeSnapshot(key: string): void {
    const entry = this.buffered.get(key);
    if (!entry) return;

    const snap = { ...entry };
    const t0 = Date.now();

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
        this.onWriteError?.(key, err, Date.now() - t0);
      })
      .finally(() => {
        this.inFlight.delete(key);
        this.flushes.resolve(key, snap.version);
      });

    this.inFlight.set(key, promise);
  }
}
