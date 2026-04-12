import type { Slot } from "./compressed-lines-blob.js";
import { type Codec, type Entry, type LazyEntry, NdjsonMap } from "./ndjson-map.js";

/** Allow the process to exit even if this timer is still pending. */
const unref = (t: ReturnType<typeof setTimeout>) => {
  if (typeof t === "object" && t !== null && "unref" in t && typeof t.unref === "function") {
    t.unref();
  }
};

type AutoFlush =
  | { phase: "armed"; timer: ReturnType<typeof setTimeout>; since: number }
  | { phase: "running"; ac: AbortController };

/**
 * Lazy wrapper around {@link NdjsonMap} that buffers upserts and defers the
 * decompress/recompress cycle until an auto-flush timer fires or the caller
 * explicitly requests serialization via {@link flush} or {@link flushAndFold}.
 *
 * Read-side APIs provide read-your-writes semantics by merge-sorting pending
 * entries with the underlying compressed data:
 *
 * - {@link scan} is the fused visitor for hot paths and early-exit scans.
 * - {@link reduce} builds on {@link scan} for full folds.
 *
 * @dev Each instance expects to own its `slot`, i.e., no other entity should
 * cause `slot` to mutate or return different data.
 */
export class LazyNdjsonMap<T, K extends string = string> {
  private readonly inner: NdjsonMap<T, K>;

  /** Pending entries keyed by the original key */
  private pending = new Map<K, T>();

  /** Serial queue for all blob writes (auto-flush, flush, flushAndFold). */
  private queue: Promise<void> = Promise.resolve();

  /** Auto-flush state machine: undefined → armed → running → undefined. */
  private auto: AutoFlush | undefined;
  /** Timestamp of the most recent upsert (for staleness detection). */
  private lastPokedAt = 0;

  constructor(
    codec: Codec<T>,
    slot: Slot,
    private readonly opts: { debounceMs: number; maxDelayMs: number; maxStalenessMs: number },
  ) {
    this.inner = new NdjsonMap<T, K>(codec, slot);
  }

  /*//////////////////////////////////////////////////////////////
                                PUBLIC
  //////////////////////////////////////////////////////////////*/

  /**
   * Buffer entries for a deferred upsert. Duplicate keys within the pending
   * buffer are collapsed (last write wins).
   *
   * All entries in a batch are added to `pending` atomically before the
   * auto-flush is triggered, so batched entries are guaranteed to be flushed
   * together.
   */
  upsert(entries: Entry<T, K>[]): void {
    if (entries.length === 0) return;

    for (const entry of entries) {
      this.pending.set(entry.key, entry.value);
    }

    this.poke();
  }

  /**
   * Flush all pending entries into the underlying compressed blob. Cancels
   * any pending auto-flush timers and aborts any in-progress auto-flush,
   * then enqueues a drain loop that runs until `pending` is empty.
   */
  flush(): Promise<void> {
    this.cancelAutoFlush();
    return this.enqueue(async () => {
      while (this.pending.size > 0) {
        await this.drainOnce();
      }
    });
  }

  /**
   * Flush the pending snapshot captured at call time and fold through every
   * entry (existing + that snapshot) in sorted key order during the rewrite
   * pass. Returns the fold accumulator.
   *
   * When there are no pending entries, degenerates to a pure {@link reduce}
   * (no rewrite). Concurrent calls are serialized through the queue. Writes
   * that arrive while the flush/fold is in flight are left pending for a
   * later flush; they are not included in the current fold result.
   *
   * The fold callback only **observes** entries — it does not control what
   * gets written to the blob. Both reduce and filter are special cases of fold.
   */
  async flushAndFold<Acc>(fn: (acc: Acc, record: LazyEntry<T, K>) => Acc, init: Acc): Promise<Acc> {
    if (this.pending.size === 0) {
      return this.reduce(fn, init);
    }

    this.cancelAutoFlush();
    let result!: Acc;

    await this.enqueue(async () => {
      const [entries, cleanup] = this.takePendingSnapshot();
      result = await this.inner.upsertAndFold(entries, fn, init);
      cleanup();
    });

    return result;
  }

  /**
   * Fold every entry (flushed + pending) through `fn` in sorted key order.
   *
   * Implemented on top of {@link scan}, so it shares the fused read path.
   */
  reduce<Acc>(fn: (acc: Acc, record: LazyEntry<T, K>) => Acc, init: Acc): Promise<Acc> {
    return this.inner.reduce(fn, init, new Map(this.pending));
  }

  /**
   * Preferred hot read API.
   *
   * Visit each entry (flushed + pending) in sorted key order.
   * Return `false` from `fn` to stop the scan early.
   */
  scan(fn: (record: LazyEntry<T, K>) => boolean | undefined): Promise<void> {
    return this.inner.scan(fn, new Map(this.pending));
  }

  /*//////////////////////////////////////////////////////////////
                              PRIVATE
  //////////////////////////////////////////////////////////////*/

  /** Snapshot current pending entries and return them as an array + a cleanup function that removes flushed keys. */
  private takePendingSnapshot(): [entries: Entry<T, K>[], cleanup: () => void] {
    const snapshot = new Map(this.pending);
    const entries: Entry<T, K>[] = [];
    for (const [key, value] of snapshot) {
      entries.push({ key, value });
    }
    return [
      entries,
      () => {
        for (const [key, value] of snapshot) {
          if (this.pending.get(key) === value) {
            this.pending.delete(key);
          }
        }
      },
    ];
  }

  /** Update staleness timestamp and arm the auto-flush timer (unless an auto-flush is already in flight). */
  private poke() {
    this.lastPokedAt = Date.now();
    if (this.auto?.phase === "running") return; // will re-poke on settle

    const now = this.lastPokedAt;
    const since = this.auto?.phase === "armed" ? this.auto.since : now;
    if (this.auto?.phase === "armed") clearTimeout(this.auto.timer);

    const cap = Math.max(0, this.opts.maxDelayMs - (now - since));
    const timer = setTimeout(() => this.fireAutoFlush(), Math.min(this.opts.debounceMs, cap));
    unref(timer);
    this.auto = { phase: "armed", timer, since };
  }

  /** Timer callback: enqueue a drain loop if the pending data is still fresh. */
  private fireAutoFlush() {
    this.auto = undefined;
    if (Date.now() - this.lastPokedAt > this.opts.maxStalenessMs) return;

    const ac = new AbortController();
    this.auto = { phase: "running", ac };

    this.enqueue(async () => {
      while (this.pending.size > 0 && !ac.signal.aborted) {
        await this.drainOnce(ac.signal);
      }
    })
      .catch(() => {}) // auto-flush errors are silently dropped
      .finally(() => {
        if (this.auto?.phase !== "running" || this.auto.ac !== ac) return; // cancelled
        this.auto = undefined;
        if (this.pending.size > 0 && Date.now() - this.lastPokedAt <= this.opts.maxStalenessMs) {
          this.poke();
        }
      });
  }

  /** Cancel any pending or in-flight auto-flush. Used by explicit flush/flushAndFold. */
  private cancelAutoFlush() {
    if (!this.auto) return;
    if (this.auto.phase === "armed") clearTimeout(this.auto.timer);
    else this.auto.ac.abort();
    this.auto = undefined;
  }

  /** Append `work` to the serial queue. The queue swallows errors internally to stay live. */
  private enqueue(work: () => Promise<void>): Promise<void> {
    const p = this.queue.then(work);
    this.queue = p.catch(() => {});
    return p;
  }

  /**
   * Snapshot current pending entries, upsert them into the compressed blob,
   * then remove any that weren't overwritten during the flush.
   */
  private async drainOnce(signal?: AbortSignal): Promise<void> {
    if (this.pending.size === 0) return;
    const [entries, cleanup] = this.takePendingSnapshot();
    await this.inner.upsert(entries, signal);
    cleanup();
  }
}
