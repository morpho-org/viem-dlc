/// <reference types="node" />
import { Readable, Transform, type TransformCallback, Writable } from "stream";
import { pipeline } from "stream/promises";
import { StringDecoder } from "string_decoder";
import { createZstdCompress, createZstdDecompress, type ZstdOptions, constants as zlib } from "zlib";

export type Slot = {
  get(): Buffer[];
  set(value: Buffer[]): void;
};

export type EmitLine = (line: string) => void;

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
 * Stores newline-delimited UTF-8 text in zstd-compressed form and exposes a small API
 * for convenience iteration via {@link lines} and transactional rewrites via {@link rewrite}.
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

  /**
   * Stream-decompress and yield logical lines (without trailing newline characters).
   *
   * This is the ergonomic read API. Hot paths that also need to persist data
   * should prefer higher-level fused visitors such as `NdjsonMap.scan()`.
   */
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
    } catch {
      // Unreadable data (e.g. corrupt or wrong-format blob) — treat as empty.
      console.warn("[CompressedLinesBlob] Failed to decompress slot; treating as empty.");
    } finally {
      // Wait for pipeline cleanup (may reject on early generator return).
      await done.catch(() => {});
    }
  }

  /**
   * Rewrite the blob line-by-line via a single decompress -> transform -> compress pipeline.
   *
   * `onLine` is called for each existing line and may call `emit` zero or more times.
   * `onFlush` is called after all existing lines have been consumed and may emit trailing lines.
   * `emit` must be called synchronously before `onLine`/`onFlush` returns.
   *
   * Backpressure flows end-to-end through the pipeline automatically.
   * On success, swaps the slot. On abort (or error), the slot is unchanged.
   * On decompression failure, the slot is cleared and the write is dropped.
   */
  async rewrite(
    onLine: (line: string, emit: EmitLine) => void,
    onFlush?: (emit: EmitLine) => void,
    signal?: AbortSignal,
  ): Promise<void> {
    const outputChunks: Buffer[] = [];
    let emittedLineCount = 0;
    const rewriteStream = new Transform({
      readableObjectMode: false,
      writableObjectMode: true,
      transform(this: Transform, line: string, _enc, callback) {
        try {
          onLine(line, (nextLine) => {
            emittedLineCount += 1;
            this.push(`${nextLine}\n`);
          });
          callback();
        } catch (error) {
          callback(error as Error);
        }
      },
      flush(this: Transform, callback) {
        try {
          onFlush?.((line) => {
            emittedLineCount += 1;
            this.push(`${line}\n`);
          });
          callback();
        } catch (error) {
          callback(error as Error);
        }
      },
    });
    const output = new Writable({
      write(chunk, _enc, cb) {
        outputChunks.push(chunk as Buffer);
        cb();
      },
    });

    if (this.slot.get().length === 0) {
      await pipeline(Readable.from([] as string[]), rewriteStream, createZstdCompress(zstdOptions), output, {
        signal,
      });
    } else {
      try {
        await pipeline(
          Readable.from(this.slot.get()),
          createZstdDecompress(),
          new SplitLines(),
          rewriteStream,
          createZstdCompress(zstdOptions),
          output,
          { signal },
        );
      } catch (e) {
        if (e instanceof Error && e.name === "AbortError") throw e;
        // Unreadable data (e.g. corrupt or wrong-format blob) — clear and drop this write.
        console.warn("[CompressedLinesBlob] Failed to decompress slot during rewrite; clearing.", e);
        this.slot.set([]);
        return;
      }
    }

    this.slot.set(emittedLineCount === 0 ? [] : outputChunks);
  }
}
