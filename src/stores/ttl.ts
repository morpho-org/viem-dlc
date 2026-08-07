import type { MaybePromise } from "viem";

import type { Store } from "../types.js";

// Header prepended to each stored value: a 4-byte magic (also a format version) followed by an 8-byte
// big-endian float64 write timestamp. The magic lets a read tell entries this store wrote apart from
// foreign or pre-existing values in the wrapped store, so those degrade to a miss instead of being
// misread as a stamped value with its first bytes stripped. Bump the magic ("TTL2"...) if the encoding
// ever changes -- entries under the old magic then read as a miss (a safe refetch), never as garbage.
const MAGIC = 0x54544c31; // "TTL1"
const STAMP_BYTES = 8;
const HEADER_BYTES = 4 + STAMP_BYTES;

function isThenable<T>(value: MaybePromise<T>): value is Promise<T> {
  return typeof (value as { then?: unknown } | null | undefined)?.then === "function";
}

export type TtlStoreOptions = {
  /**
   * Absolute time-to-live, in ms. An entry is served for at most `ttlMs` after it was written, then
   * reported as a miss so the next read falls through to whatever backs the wrapped store. The stamp
   * is set on `set` and is **never** refreshed on `get`.
   */
  ttlMs: number;
};

/**
 * A decorator that adds an **absolute TTL** to any wrapped {@link Store}.
 *
 * Each value is stamped with the wall-clock time it was written; a read past `ttlMs` reports a miss
 * (leaving the stale bytes for the wrapped store to overwrite or evict). The stamp rides *inside* the
 * stored value as a small header, so it lives and dies with the entry -- there is no side table to
 * leak when the wrapped store silently evicts under its own pressure (e.g. an `LruStore`'s byte cap),
 * and nothing to reconcile when it persists or shards the value. The header carries a magic marker, so
 * a value this store didn't write (a pre-existing entry in a persistent store, or one from another
 * consumer sharing the wrapped store) is recognized and reported as a miss rather than misread.
 *
 * That stamp is the "last written into this tier" clock -- orthogonal to any freshness metadata inside
 * the cached value itself. Because it starts at `set` and never advances on `get`, a value can be
 * served for at most `ttlMs`, bounding how long this tier may diverge from a fresher source behind it.
 * The intended shape is an in-memory front (e.g. `new TtlStore(new LruStore(maxBytes), { ttlMs })`) as
 * the top level of a {@link HierarchicalStore}: a plain `LruStore` would pin a warm copy for the whole
 * process lifetime, whereas this expires it and lets the next read pick up the shared source of truth.
 *
 * Passes the wrapped store's sync/async nature straight through (an `LruStore` stays synchronous, a
 * remote store stays a promise). Best-effort and non-throwing, per the `Store` contract -- a value with
 * no recognizable header is treated as a miss rather than throwing. It reads `Date.now()`, so a serverless
 * freeze/thaw that jumps the wall clock forward simply expires warm entries early: a safe refetch,
 * consistent with the contract's tolerance for wall-clock gaps.
 *
 * @dev The header is an implementation detail of *this* store's encoding. Read values back through
 * the `TtlStore`, not by inspecting the wrapped store directly. A read copies the value only when the
 * wrapped store reframes it (rechunks or collapses the `Buffer[]`); a framing-preserving store such as
 * `LruStore` returns the value buffers by reference, so even massive values are copy-free on read.
 */
export class TtlStore implements Store {
  private readonly ttlMs: number;

  constructor(
    private readonly store: Store,
    { ttlMs }: TtlStoreOptions,
  ) {
    // Reject non-finite values too: `NaN`/`Infinity` slip past a bare `< 1` check and then make every
    // `age > ttlMs` comparison false, silently disabling expiry -- the opposite of this store's job.
    if (!Number.isFinite(ttlMs) || ttlMs < 1) throw new Error("[TtlStore] ttlMs must be a finite number >= 1");
    this.ttlMs = ttlMs;
  }

  get(key: string): MaybePromise<Buffer[] | null> {
    const stored = this.store.get(key);
    return isThenable(stored) ? stored.then((value) => this.unwrap(value)) : this.unwrap(stored);
  }

  set(key: string, value: Buffer[]): MaybePromise<void> {
    const header = Buffer.allocUnsafe(HEADER_BYTES);
    header.writeUInt32BE(MAGIC, 0);
    header.writeDoubleBE(Date.now(), 4);
    return this.store.set(key, [header, ...value]);
  }

  delete(key: string): MaybePromise<void> {
    return this.store.delete(key);
  }

  flush(): MaybePromise<void> {
    return this.store.flush();
  }

  /** Locate and validate the header, check the stamp, and strip it -- without copying where possible. */
  private unwrap(stored: Buffer[] | null): Buffer[] | null {
    if (stored === null) return null;

    let insertedAt: number;
    let value: Buffer[];
    const head = stored[0];
    if (head !== undefined && head.length === HEADER_BYTES && head.readUInt32BE(0) === MAGIC) {
      // Fast path: the wrapped store preserved our `[header, ...value]` framing, so the header is a
      // standalone leading buffer. The value buffers pass straight back by reference -- no byte copy,
      // even for massive values. (A leading buffer of exactly `HEADER_BYTES` bytes starting with the
      // magic must be the header, since the stored byte stream always begins with it.)
      insertedAt = head.readDoubleBE(4);
      value = stored.slice(1);
    } else {
      // Fallback: the wrapped store rechunked or collapsed the framing (e.g. a remote/compressed
      // tier). Reassemble to one buffer to locate the header regardless of chunk boundaries -- this
      // copies, but such stores already reframe the value on their own.
      const whole = stored.length === 1 ? stored[0]! : Buffer.concat(stored);
      // No/short/foreign header (missing magic) -- e.g. a value this store didn't write. Treat as a
      // miss rather than misreading it or throwing.
      if (whole.length < HEADER_BYTES || whole.readUInt32BE(0) !== MAGIC) return null;
      insertedAt = whole.readDoubleBE(4);
      value = [whole.subarray(HEADER_BYTES)];
    }

    // Report a miss once past the TTL, but do NOT delete: with an async wrapped store, a delete here
    // could race a concurrent `set` and clobber a fresh write (the `Store` interface has no
    // compare-and-delete). The stale bytes stay bounded by the wrapped store's own cap and are
    // overwritten on the next `set` for this key -- so cleanup is free and race-free.
    return Date.now() - insertedAt > this.ttlMs ? null : value;
  }
}
