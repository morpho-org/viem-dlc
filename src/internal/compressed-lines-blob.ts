/// <reference types="node" />
import { PassThrough, Readable, Transform, type TransformCallback, Writable } from "stream";
import { pipeline } from "stream/promises";
import { StringDecoder } from "string_decoder";
import { createZstdCompress, createZstdDecompress, type ZstdOptions, constants as zlib } from "zlib";

export type Slot = {
  get(): Buffer[];
  set(value: Buffer[]): void;
};

export type RewriteSession = {
  emit(line: string): void;
  forEachLine(fn: (line: string) => void): Promise<void>;
};

export function createSlot(compressed?: Buffer | Buffer[]): Slot {
  let chunks: Buffer[] = [];

  if (compressed) {
    if (Array.isArray(compressed)) {
      chunks = compressed;
    } else if (compressed.length > 0) {
      chunks = [compressed];
    }
  }

  return {
    get: () => chunks,
    set: (v) => {
      chunks = v;
    },
  };
}

// NOTE: Default sliding window for level 1 is 512KB
const zstdOptions: ZstdOptions = {
  params: {
    [zlib.ZSTD_c_compressionLevel]: 1,
  },
};

/**
 * Transform that splits a byte stream into individual lines (object-mode output).
 * Handles both `\n` and `\r\n` line endings.
 */
class SplitLines extends Transform {
  private readonly decoder = new StringDecoder("utf8");
  private remainder = "";

  constructor() {
    super({ readableObjectMode: true, decodeStrings: true });
  }

  override _transform(chunk: Buffer | string, _enc: BufferEncoding, callback: TransformCallback): void {
    this.remainder += typeof chunk === "string" ? chunk : this.decoder.write(chunk);

    let lineStart = 0;
    let i = this.remainder.indexOf("\n", lineStart);
    while (i !== -1) {
      let line = this.remainder.slice(lineStart, i);
      if (line.endsWith("\r")) line = line.slice(0, -1);
      this.push(line);
      lineStart = i + 1;
      i = this.remainder.indexOf("\n", lineStart);
    }

    this.remainder = this.remainder.slice(lineStart);
    callback();
  }

  override _flush(callback: TransformCallback): void {
    this.remainder += this.decoder.end();
    if (this.remainder.length > 0) {
      const line = this.remainder.endsWith("\r") ? this.remainder.slice(0, -1) : this.remainder;
      this.push(line);
    }
    callback();
  }
}

/**
 * zstd-compressed line buffer.
 *
 * Stores newline-delimited UTF-8 text in zstd-compressed form and exposes a small streaming API
 * for reading and rewriting logical lines.
 *
 * Peak live decompressed memory is proportional to the largest logical line. Rewrites also
 * buffer the full new **compressed blob** as chunks in memory before swapping it into place.
 *
 * @dev IMPORTANT: Each instance expects to own its `slot`, i.e., no other entity
 * should cause `slot` to mutate or return different data.
 */
export class CompressedLinesBlob {
  constructor(private readonly slot: Slot) {
    const chunks = slot.get();
    console.assert(chunks.length === 0 || chunks[0]!.length > 0, "Slot contains an empty buffer in array");
  }

  /** Stream-decompress and yield logical lines (without trailing newline characters). */
  async *lines(): AsyncGenerator<string, void, void> {
    if (this.slot.get().length === 0) return;

    const input = Readable.from(this.slot.get());
    const decompressor = createZstdDecompress();
    const splitter = new SplitLines();

    // pipeline() wires error propagation + cleanup across the chain.
    // Errors surface through `for await` on splitter; we don't await inline.
    const done = pipeline(input, decompressor, splitter);

    try {
      for await (const line of splitter) {
        yield line as string;
      }
    } finally {
      // Wait for pipeline cleanup (may reject on early generator return).
      await done.catch(() => {});
    }
  }

  /**
   * Rewrite the blob transactionally via a push-based session.
   *
   * `emit()` appends one logical line to the replacement blob.
   * `forEachLine()` streams existing logical lines through a synchronous callback.
   *
   * On success, swaps the slot. On abort (or error), the slot is unchanged.
   */
  async rewrite(
    run: (session: RewriteSession) => void | Promise<void>,
    signal?: AbortSignal,
  ): Promise<void> {
    const outputChunks: Buffer[] = [];
    let emittedLineCount = 0;
    const input = new PassThrough();
    let writableReady: Promise<void> | undefined;

    const output = new Writable({
      write(chunk, _enc, cb) {
        outputChunks.push(chunk as Buffer);
        cb();
      },
    });

    const done = pipeline(input, createZstdCompress(zstdOptions), output, { signal });
    const waitForWritable = () => writableReady ?? Promise.resolve();

    const emit = (line: string) => {
      emittedLineCount += 1;
      if (input.write(`${line}\n`)) return;

      writableReady ??= new Promise<void>((resolve, reject) => {
        const cleanup = () => {
          input.off("drain", onDrain);
          input.off("error", onError);
        };
        const onDrain = () => {
          cleanup();
          writableReady = undefined;
          resolve();
        };
        const onError = (error: Error) => {
          cleanup();
          writableReady = undefined;
          reject(error);
        };

        input.once("drain", onDrain);
        input.once("error", onError);
      });
    };

    try {
      await run({
        emit,
        forEachLine: async (fn) => {
          await this.forEachStoredLine(async (line) => {
            fn(line);
            await waitForWritable();
          }, signal);
        },
      });
      await waitForWritable();
      input.end();
      await done;
      this.slot.set(emittedLineCount === 0 ? [] : outputChunks);
    } catch (error) {
      input.destroy(error as Error);
      await done.catch(() => {});
      throw error;
    }
  }

  private async forEachStoredLine(
    fn: (line: string) => void | Promise<void>,
    signal?: AbortSignal,
  ): Promise<void> {
    if (this.slot.get().length === 0) return;

    const output = new Writable({
      objectMode: true,
      write(chunk, _enc, cb) {
        Promise.resolve(fn(chunk as string)).then(
          () => cb(),
          (error) => cb(error as Error),
        );
      },
    });

    await pipeline(Readable.from(this.slot.get()), createZstdDecompress(), new SplitLines(), output, { signal });
  }
}
