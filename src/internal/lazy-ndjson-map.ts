import { type DebouncedFn, debounce } from "../utils/debounce.js";

import type { Slot } from "./compressed-lines-blob.js";
import {
  type Codec,
  type Entry,
  type LazyEntry,
  lazyEntry,
  NdjsonMap,
  sortEntriesByRawKey,
  toRawKey,
} from "./ndjson-map.js";

/** No-op codec for pre-stringified values. */
const identity: Codec<string> = {
  fromJson: (s) => s,
  toJson: (s) => s,
};

/**
 * Lazy wrapper around {@link NdjsonMap} that buffers upserts and defers the
 * decompress/recompress cycle as long as possible.
 *
 * Pending entries are stringified eagerly (so their value byte size can be tracked)
 * but the actual upsert into the underlying compressed blob is deferred until:
 * - (a) the debounce/maxDelay timer fires (auto-flush), or
 * - (b) the caller requests serialization via {@link flush} or {@link flushAndFold}
 *
 * {@link records} and {@link reduce} provide read-your-writes semantics by
 * snapshotting pending entries at call time and merge-sorting them with
 * underlying data. Pending entries that update existing keys take precedence,
 * and new pending keys are interleaved at their sorted position.
 *
 * Auto-flush is best-effort: each upsert resets a debounce timer; the auto-flush
 * fires once the timer expires or `maxDelayMs` is reached (whichever comes first).
 * It snapshots the current pending entries, so writes that arrive during that pass
 * remain pending until a later auto-flush or an explicit {@link flush}/{@link flushAndFold}
 * call. Auto-flush errors are silently dropped — they surface only through
 * {@link flush} or {@link flushAndFold}.
 *
 * All blob writes (auto-flush, {@link flush}, {@link flushAndFold}) are serialized
 * through an internal queue. Explicit operations cancel any pending auto-flush
 * timers and abort any in-progress auto-flush before enqueueing their own work.
 *
 * Aborting is safe: the underlying blob is unchanged on abort, and all entries
 * from the aborted flush remain in `pending` for the next attempt.
 *
 * @dev Unlike {@link Store.flush}, which snapshots pending work and returns
 * after a single attempt, this flush drains until `pending` is empty. That
 * means continuous writes during a flush will keep it alive indefinitely.
 *
 * The underlying `NdjsonMap` uses a no-op codec since values are already
 * stringified when they enter the pending buffer.
 *
 * @dev IMPORTANT: Each instance expects to own its `slot`, i.e., no other entity
 * should cause `slot` to mutate or return different data.
 */
export class LazyNdjsonMap<T, K extends string = string> {
  private readonly inner: NdjsonMap<string, K>;
  private readonly codec: Codec<T>;
  private readonly debouncedFlush: DebouncedFn<[]>;

  /** Pre-stringified pending entries keyed by the original key */
  private pending = new Map<K, string>();

  /** Serial queue for all blob writes (auto-flush, flush, flushAndFold). */
  private queue: Promise<void> = Promise.resolve();

  constructor(
    codec: Codec<T>,
    options: { debounceMs: number; maxDelayMs: number; maxStalenessMs: number },
    slot: Slot,
  ) {
    this.codec = codec;
    this.inner = new NdjsonMap<string, K>(identity, slot);
    this.debouncedFlush = debounce((signal: AbortSignal) => this.enqueue(() => this.drainOnce(signal)), {
      debounceMs: options.debounceMs,
      maxDelayMs: options.maxDelayMs,
      maxStalenessMs: options.maxStalenessMs,
    });
  }

  /*//////////////////////////////////////////////////////////////
                                PUBLIC
  //////////////////////////////////////////////////////////////*/

  /**
   * Buffer entries for a deferred upsert. Values are stringified immediately
   * so their byte cost is tracked; duplicate keys within the pending buffer
   * are collapsed (last write wins).
   *
   * All entries in a batch are added to `pending` atomically before the
   * auto-flush is triggered, so batched entries are guaranteed to be flushed
   * together.
   */
  upsert(entries: Entry<T, K>[]): void {
    for (const entry of entries) {
      const rawValue = this.codec.toJson(entry.value);
      this.pending.set(entry.key, rawValue);
    }

    this.debouncedFlush();
  }

  /**
   * Flush all pending entries into the underlying compressed blob. Cancels
   * any pending auto-flush timers and aborts any in-progress auto-flush,
   * then enqueues a drain loop that runs until `pending` is empty.
   */
  flush(): Promise<void> {
    this.debouncedFlush.cancel();
    return this.enqueue(async () => {
      while (this.pending.size > 0) {
        await this.drainOnce();
      }
    });
  }

  /**
   * Flush all pending entries and fold through every entry (existing + pending)
   * in sorted key order during the rewrite pass. Returns the fold accumulator.
   *
   * When there are no pending entries, degenerates to a pure {@link reduce}
   * (no rewrite). Concurrent calls are serialized through the queue.
   *
   * The fold callback only **observes** entries — it does not control what
   * gets written to the blob. Both reduce and filter are special cases of fold.
   */
  async flushAndFold<Acc>(fn: (acc: Acc, record: LazyEntry<T, K>) => Acc, init: Acc): Promise<Acc> {
    if (this.pending.size === 0) {
      return this.reduce(fn, init);
    }

    this.debouncedFlush.cancel();
    let result!: Acc;

    await this.enqueue(async () => {
      // Snapshot pending entries
      const snapshot = new Map(this.pending);
      const entries: Entry<string, K>[] = [];
      for (const [key, rawValue] of snapshot) {
        entries.push({ key, value: rawValue });
      }

      // Flush + fold in a single decompression pass
      const codec = this.codec;
      result = await this.inner.upsertAndFold(
        entries,
        (acc: Acc, entry: Entry<string, K>) => fn(acc, lazyEntry(entry.key, entry.value, codec.fromJson)),
        init,
      );

      // Clean up flushed entries (same as drainOnce)
      for (const [key, rawValue] of snapshot) {
        if (this.pending.get(key) === rawValue) {
          this.pending.delete(key);
        }
      }

      // Drain any remaining pending entries (defensive, for concurrent writes)
      while (this.pending.size > 0) {
        await this.drainOnce();
      }
    });

    return result;
  }

  /** Stream-decompress and fold every entry (flushed + pending) through `fn`, in sorted key order. */
  async reduce<Acc>(fn: (acc: Acc, record: LazyEntry<T, K>) => Acc, init: Acc): Promise<Acc> {
    let acc = init;
    for await (const record of this.records()) {
      acc = fn(acc, record);
    }
    return acc;
  }

  /** Async generator that yields each entry (flushed + pending) in sorted key order. */
  async *records(): AsyncGenerator<LazyEntry<T, K>, void, void> {
    // TODO: micro-optimization: streamline 3 layers of async generators in this stack
    const pendingSnapshot = new Map(this.pending);
    const sorted = sortEntriesByRawKey(pendingSnapshot);
    const codec = this.codec;
    let idx = 0;

    for await (const record of this.inner.records()) {
      // Merge-insert: yield sorted pending entries that belong before this key
      const rawKey = toRawKey(record.key);
      while (idx < sorted.length && sorted[idx]![0] < rawKey) {
        const [, key, rawValue] = sorted[idx++]!;
        yield lazyEntry(key, rawValue, codec.fromJson);
      }

      if (pendingSnapshot.has(record.key)) continue;
      yield lazyEntry(record.key, record.value, codec.fromJson);
    }

    while (idx < sorted.length) {
      const [, key, rawValue] = sorted[idx++]!;
      yield lazyEntry(key, rawValue, codec.fromJson);
    }
  }

  /*//////////////////////////////////////////////////////////////
                              PRIVATE
  //////////////////////////////////////////////////////////////*/

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

    const snapshot = new Map(this.pending);
    const entries: Entry<string, K>[] = [];
    for (const [key, rawValue] of snapshot) {
      entries.push({ key, value: rawValue });
    }

    await this.inner.upsert(entries, signal);

    for (const [key, rawValue] of snapshot) {
      if (this.pending.get(key) === rawValue) {
        this.pending.delete(key);
      }
    }
  }
}
