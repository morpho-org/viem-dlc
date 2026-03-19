import { measureUtf8Bytes } from "../utils/strings.js";

import { type Codec, type Entry, NdjsonMap } from "./ndjson-map.js";

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
 * - (a) accumulated pending value bytes exceed `autoFlushThresholdBytes` (auto-flush), or
 * - (b) the caller requests serialization via {@link flush} or {@link toBase64}
 *
 * {@link records} and {@link reduce} provide read-your-writes semantics by
 * snapshotting pending entries at call time and overlaying them on flushed
 * data. Pending entries that update existing keys suppress the flushed
 * version; new pending keys appear after all flushed entries.
 *
 * Auto-flush is best-effort: if pending bytes cross the threshold while no
 * flush is active, a background flush starts. It snapshots the current pending
 * entries, so writes that arrive during that pass remain pending until a later
 * auto-flush or an explicit {@link flush}/{@link toBase64} call.
 *
 * User-requested flushes ({@link flush}/{@link toBase64}) are shared: if one is
 * already running, later callers await the same promise. Otherwise an explicit
 * flush aborts any in-progress auto-flush and keeps flushing until `pending` is
 * empty.
 *
 * Aborting is safe: the underlying blob is unchanged on abort, and all entries
 * from the aborted flush remain in `pending` for the next attempt.
 *
 * While an explicit flush is running, {@link upsert} warns but still buffers
 * the write. The flush loop will pick it up in a subsequent pass.
 *
 * The underlying `NdjsonMap` uses a no-op codec since values are already
 * stringified when they enter the pending buffer.
 */

export class LazyNdjsonMap<T, K extends string = string> {
  private readonly inner: NdjsonMap<string, K>;
  private readonly codec: Codec<T>;
  private readonly autoFlushThresholdBytes: number;

  /** Pre-stringified pending entries keyed by the original key */
  private pending = new Map<K, string>();
  /** Total UTF-8 encoded byte length for all `pending` values (excludes keys and overhead) */
  private pendingBytes = 0;

  /**
   * At most one flush runs at a time. Explicit flushes block upserts and drain
   * until pending is empty; auto-flushes are single-pass and abortable.
   */
  private active?: {
    promise: Promise<void>;
    explicit: boolean;
    controller?: AbortController;
  };

  constructor(codec: Codec<T>, options: {autoFlushThresholdBytes: number;}, compressed?: string | Buffer) {
    this.codec = codec;
    this.autoFlushThresholdBytes = options.autoFlushThresholdBytes;
    this.inner = new NdjsonMap<string, K>(identity, compressed);
  }

  /*//////////////////////////////////////////////////////////////
                                PUBLIC
  //////////////////////////////////////////////////////////////*/

  /** Flush pending entries and return the compressed base64 string. */
  async toBase64(): Promise<string> {
    await this.flush();
    return this.inner.toBase64();
  }

  /**
   * Buffer a single entry for a deferred upsert. The value is stringified
   * immediately so its byte cost is tracked; duplicate keys within the
   * pending buffer are collapsed (last write wins).
   *
   * If pending value bytes exceed `autoFlushThresholdBytes` and no flush is
   * already active, a background auto-flush is started. The threshold is
   * best-effort, not absolute. Auto-flush errors are silently dropped — they
   * surface only through {@link flush} or {@link toBase64}.
   */
  upsert(entry: Entry<T, K>): void {
    if (this.active?.explicit) {
      console.warn(`[LazyNdjsonMap] Upserting key '${entry.key}' while explicit flush is in progress. This is an anti-pattern, as it delays the flush.`);
    }

    const rawValue = this.codec.toJson(entry.value);

    const existing = this.pending.get(entry.key);
    if (existing !== undefined) this.pendingBytes -= measureUtf8Bytes(existing);
    this.pending.set(entry.key, rawValue);
    this.pendingBytes += measureUtf8Bytes(rawValue);

    if (this.pendingBytes >= this.autoFlushThresholdBytes && !this.active) {
      this.startAutoFlush();
    }
  }

  /**
   * Flush all pending entries into the underlying compressed blob. If an
   * auto-flush is in progress it is aborted first. Keeps draining until
   * `pending` is empty (writes that land during a pass are picked up by
   * the next pass). Concurrent callers share the same promise.
   */
  flush(): Promise<void> {
    if (this.active?.explicit) return this.active.promise;

    // Abort any in-progress auto-flush; capture it so we can await settlement.
    const dying = this.active;
    dying?.controller?.abort();

    const promise = (async () => {
      await dying?.promise;
      while (this.pending.size > 0) {
        await this.drainOnce();
      }
    })().finally(() => {
      if (this.active?.promise === promise) this.active = undefined;
    });

    this.active = { promise, explicit: true };
    return promise;
  }

  /** Stream-decompress and fold every entry (flushed + pending) through `fn`. */
  reduce<Acc>(fn: (acc: Acc, record: Entry<T, K>) => Acc, init: Acc): Promise<Acc> {
    const pendingSnapshot = new Map(this.pending);

    return this.inner.reduce<Acc>((acc, record) => {
      if (pendingSnapshot.has(record.key)) return acc;
      return fn(acc, { key: record.key, value: this.codec.fromJson(record.value) });
    }, init).then((acc) => {
      for (const [key, rawValue] of pendingSnapshot) {
        acc = fn(acc, { key, value: this.codec.fromJson(rawValue) });
      }
      return acc;
    });
  }

  /** Async generator that yields each entry (flushed + pending). */
  async *records(): AsyncGenerator<Entry<T, K>, void, void> {
    const pendingSnapshot = new Map(this.pending);

    for await (const record of this.inner.records()) {
      if (pendingSnapshot.has(record.key)) continue;
      yield { key: record.key, value: this.codec.fromJson(record.value) };
    }

    for (const [key, rawValue] of pendingSnapshot) {
      yield { key, value: this.codec.fromJson(rawValue) };
    }
  }

  /*//////////////////////////////////////////////////////////////
                              PRIVATE
  //////////////////////////////////////////////////////////////*/

  private startAutoFlush(): void {
    const controller = new AbortController();
    const promise = this.drainOnce(controller.signal)
      .catch(() => {}) // auto-flush is best-effort; errors surface via explicit flush
      .finally(() => {
        if (this.active?.promise === promise) this.active = undefined;
      });

    this.active = { promise, explicit: false, controller };
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
        this.pendingBytes -= measureUtf8Bytes(rawValue);
      }
    }
  }
}
