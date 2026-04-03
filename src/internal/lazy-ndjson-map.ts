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
 * {@link records} and {@link reduce} provide read-your-writes semantics by
 * merge-sorting pending entries with the underlying compressed data.
 *
 * @dev Each instance expects to own its `slot`, i.e., no other entity should
 * cause `slot` to mutate or return different data.
 */
export class LazyNdjsonMap<T, K extends string = string> {
  private readonly inner: NdjsonMap<string, K>;
  private readonly codec: Codec<T>;
  private readonly opts: { debounceMs: number; maxDelayMs: number; maxStalenessMs: number };

  /** Pre-stringified pending entries keyed by the original key */
  private pending = new Map<K, string>();

  /** Serial queue for all blob writes (auto-flush, flush, flushAndFold). */
  private queue: Promise<void> = Promise.resolve();

  /** Auto-flush state machine: undefined → armed → running → undefined. */
  private auto: AutoFlush | undefined;
  /** Timestamp of the most recent upsert (for staleness detection). */
  private lastPokedAt = 0;

  constructor(
    codec: Codec<T>,
    options: { debounceMs: number; maxDelayMs: number; maxStalenessMs: number },
    slot: Slot,
  ) {
    this.codec = codec;
    this.opts = options;
    this.inner = new NdjsonMap<string, K>(identity, slot);
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

    this.cancelAutoFlush();
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

  /** Timer callback: enqueue a single drain pass if the pending data is still fresh. */
  private fireAutoFlush() {
    this.auto = undefined;
    if (Date.now() - this.lastPokedAt > this.opts.maxStalenessMs) return;

    const ac = new AbortController();
    this.auto = { phase: "running", ac };

    this.enqueue(() => this.drainOnce(ac.signal))
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
