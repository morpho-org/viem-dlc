import { Buffer } from "buffer";
import { zstdCompressSync } from "zlib";

import type { LinesBlob, Slot } from "./lines-blob.js";
import { LinesBlobCompressed, zstdOptions } from "./lines-blob-compressed.js";
import { LinesBlobRaw } from "./lines-blob-raw.js";
import type { Codec, Entry, LazyEntry } from "./ndjson-map.js";
import { NdjsonMapLazy } from "./ndjson-map-lazy.js";

/** zstd frame magic number: identifies a compressed slot unambiguously. */
const ZSTD_MAGIC = Buffer.from([0x28, 0xb5, 0x2f, 0xfd]);

function sumBytes(chunks: readonly Buffer[]): number {
  let n = 0;
  for (const c of chunks) n += c.length;
  return n;
}

function looksCompressed(chunks: readonly Buffer[]): boolean {
  const first = chunks[0];
  return !!first && first.length >= 4 && first.subarray(0, 4).equals(ZSTD_MAGIC);
}

/**
 * One-shot, in-place upgrade of a slot from raw NDJSON bytes to a single
 * zstd-compressed buffer. Bounded by the caller's threshold, so the synchronous
 * compression is acceptable.
 */
function upgradeRawToCompressed(slot: Slot): void {
  const chunks = slot.get();
  if (chunks.length === 0) return;
  const raw = Buffer.concat(chunks);
  const compressed = zstdCompressSync(raw, zstdOptions);
  slot.set([compressed]);
}

/**
 * Polymorphic wrapper around {@link NdjsonMapLazy} that picks one of two
 * blob backends at construction time based on the slot's stored byte size:
 *
 * - {@link LinesBlobCompressed} (zstd) when the slot is large or already
 *   compressed.
 * - {@link LinesBlobRaw} (uncompressed NDJSON) when the slot is small.
 *
 * The mode is fixed for the lifetime of the instance — there is no mid-life
 * migration. A small raw slot stays raw; a large raw slot is upgraded to
 * compressed once during construction; a slot already in zstd format always
 * stays compressed regardless of size.
 *
 * Detection is by zstd magic bytes (`0x28 B5 2F FD`); the NDJSON envelope
 * guarantees the first byte is `{` (`0x7B`) so the two formats cannot be
 * confused. Empty slots start in raw mode.
 *
 * The threshold is intentionally loose. Stored slot bytes is the threshold
 * unit — sum of `Buffer.length` over `slot.get()`. Note that this measures
 * compressed bytes once compressed and raw bytes when raw, so the same
 * logical entry count produces ~10× fewer bytes after upgrade. That is fine
 * because once a slot is compressed it stays compressed.
 *
 * Public API mirrors {@link NdjsonMapLazy} exactly; drop-in replacement at
 * call sites that today construct `NdjsonMapLazy` over a `Slot`.
 *
 * @dev Each instance expects to own its `slot`, i.e., no other entity should
 * cause `slot` to mutate or return different data.
 */
export class NdjsonMapAdaptive<T, K extends string = string> {
  private readonly inner: NdjsonMapLazy<T, K>;
  readonly mode: "raw" | "compressed";

  constructor(codec: Codec<T>, slot: Slot, opts: { debounceMs: number; maxDelayMs: number; thresholdBytes: number }) {
    const chunks = slot.get();

    if (looksCompressed(chunks)) {
      this.mode = "compressed";
    } else if (sumBytes(chunks) >= opts.thresholdBytes) {
      upgradeRawToCompressed(slot);
      this.mode = "compressed";
    } else {
      this.mode = "raw";
    }

    const blob: LinesBlob = this.mode === "compressed" ? new LinesBlobCompressed(slot) : new LinesBlobRaw(slot);
    this.inner = new NdjsonMapLazy<T, K>(codec, blob, opts);
  }

  upsert(entries: Entry<T, K>[]): void {
    this.inner.upsert(entries);
  }

  flush(): Promise<void> {
    return this.inner.flush();
  }

  flushAndFold<Acc>(fn: (acc: Acc, record: LazyEntry<T, K>) => Acc, init: Acc): Promise<Acc> {
    return this.inner.flushAndFold(fn, init);
  }

  reduce<Acc>(fn: (acc: Acc, record: LazyEntry<T, K>) => Acc, init: Acc): Promise<Acc> {
    return this.inner.reduce(fn, init);
  }

  scan(fn: (record: LazyEntry<T, K>) => boolean | undefined): Promise<void> {
    return this.inner.scan(fn);
  }
}
