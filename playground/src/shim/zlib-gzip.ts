/**
 * Browser stand-in for the slice of `node:zlib` that `src/internal/compressed-lines-blob.ts` uses,
 * aliased in at build time.
 *
 * The library asks for streaming zstd. No published JS or WASM zstd exposes a push-driven
 * compressor — they all take a complete buffer — and using one would force the write path to
 * materialise the whole uncompressed blob, which is exactly the property the blob module's
 * backpressured pipeline exists to avoid. The browser's own `CompressionStream` *is* a genuine
 * streaming transform, so this keeps the memory behaviour and substitutes the codec: gzip, not zstd.
 *
 * Consequences, both bounded: `blob_bytes_written` on the wide event is a gzip figure, and a blob
 * written here is unreadable by Node. Nothing in `src/` inspects the format — {@link isZstdFailure}
 * upstream keys on error *codes*, which this reproduces — and the browser Store is per-session, so
 * it only ever reads what it wrote.
 *
 * The principled version of this is an injectable codec on `NdjsonMap`/`LazyNdjsonMap`, at which
 * point this file becomes a supported configuration rather than an alias.
 */
import { Transform, type TransformCallback } from "stream";

const FORMAT = "gzip";

/** What `compressed-lines-blob.ts` matches on to treat a bad blob as empty rather than throwing. */
const ZSTD_FAILED = "ERR_ZLIB_ZSTD_FAILED";

function asZstdFailure(cause: unknown): NodeJS.ErrnoException {
  const error: NodeJS.ErrnoException = new Error(`decompression failed: ${cause}`);
  error.code = ZSTD_FAILED;
  return error;
}

/**
 * Bridges a web `TransformStream` into a Node `Transform`, propagating backpressure in both
 * directions: writes await the web writer, and the read pump parks whenever `push` reports the
 * readable side is full, resuming from `_read`.
 */
class WebCodecTransform extends Transform {
  private readonly writer: WritableStreamDefaultWriter<Uint8Array>;
  private readonly pump: Promise<void>;
  private unpark: (() => void) | undefined;
  private failed: unknown;

  constructor(stream: TransformStream<BufferSource, Uint8Array>) {
    super();
    const writer = stream.writable.getWriter() as WritableStreamDefaultWriter<Uint8Array>;
    const reader = stream.readable.getReader();
    this.writer = writer;

    this.pump = (async () => {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) return;
        if (!this.push(Buffer.from(value))) {
          await new Promise<void>((resolve) => {
            this.unpark = resolve;
          });
        }
      }
    })().catch((error) => {
      this.failed = error;
    });
  }

  override _read(size: number): void {
    super._read(size);
    const unpark = this.unpark;
    this.unpark = undefined;
    unpark?.();
  }

  override _transform(chunk: Buffer, _encoding: BufferEncoding, callback: TransformCallback): void {
    this.writer.write(chunk).then(
      () => callback(this.failed ? asZstdFailure(this.failed) : null),
      (error) => callback(asZstdFailure(error)),
    );
  }

  override _flush(callback: TransformCallback): void {
    this.writer
      .close()
      .then(() => this.pump)
      .then(
        () => callback(this.failed ? asZstdFailure(this.failed) : null),
        (error) => callback(asZstdFailure(error)),
      );
  }
}

export function createZstdCompress(_options?: unknown): Transform {
  return new WebCodecTransform(new CompressionStream(FORMAT));
}

export function createZstdDecompress(_options?: unknown): Transform {
  return new WebCodecTransform(new DecompressionStream(FORMAT));
}

async function through(stream: TransformStream<BufferSource, Uint8Array>, data: Uint8Array) {
  const chunks: Uint8Array[] = [];
  const collecting = (async () => {
    const reader = stream.readable.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) return;
      chunks.push(value);
    }
  })();

  const writer = stream.writable.getWriter() as WritableStreamDefaultWriter<Uint8Array>;
  await writer.write(data);
  await writer.close();
  await collecting;

  return Buffer.concat(chunks.map((chunk) => Buffer.from(chunk)));
}

/** Callback-shaped to match `node:zlib`, which `src/stores/compressed.ts` wraps in `promisify`. */
export function zstdCompress(data: Uint8Array, _options: unknown, callback?: (e: Error | null, r?: Buffer) => void) {
  const done = typeof _options === "function" ? (_options as (e: Error | null, r?: Buffer) => void) : callback!;
  through(new CompressionStream(FORMAT), data).then((result) => done(null, result), done);
}

export function zstdDecompress(data: Uint8Array, _options: unknown, callback?: (e: Error | null, r?: Buffer) => void) {
  const done = typeof _options === "function" ? (_options as (e: Error | null, r?: Buffer) => void) : callback!;
  through(new DecompressionStream(FORMAT), data).then(
    (result) => done(null, result),
    (error) => done(asZstdFailure(error)),
  );
}

/** Only `ZSTD_c_compressionLevel` is read upstream; `CompressionStream` has no level control. */
export const constants = { ZSTD_c_compressionLevel: 100 };

export type ZstdOptions = { params?: Record<number, number> };
