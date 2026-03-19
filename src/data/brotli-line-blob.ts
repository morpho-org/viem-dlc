/// <reference types="node" />
import { Readable, Transform, type TransformCallback, Writable } from "stream";
import { pipeline } from "stream/promises";
import { StringDecoder } from "string_decoder";
import { type BrotliOptions, createBrotliCompress, createBrotliDecompress, constants as zlib } from "zlib";

const brotliOptions: BrotliOptions = {
  params: {
    [zlib.BROTLI_PARAM_QUALITY]: 4,
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

type EmitLine = (line: string) => void;

/**
 * Brotli-compressed line buffer.
 *
 * Stores newline-delimited UTF-8 text in brotli-compressed form and exposes a small streaming API
 * for reading, reducing, and rewriting logical lines.
 *
 * When streaming via {@link reduceLines} or {@link rewriteLines}, peak live decompressed memory is
 * proportional to the largest logical line. Rewrites also buffer the full new **compressed blob**
 * in memory before swapping it into place.
 */
export class BrotliLineBlob {
  private data: Buffer;

  constructor(compressed?: string | Buffer) {
    this.data = compressed
      ? typeof compressed === "string"
        ? Buffer.from(compressed, "base64")
        : compressed
      : Buffer.alloc(0);
  }

  toBase64(): string {
    return this.data.toString("base64");
  }

  /** Stream-decompress and yield logical lines (without trailing newline characters). */
  async *lines(): AsyncGenerator<string, void, void> {
    if (this.data.length === 0) return;

    const input = Readable.from(this.data);
    const decompressor = createBrotliDecompress();
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

  /** Fold each logical line through `fn` without materializing the full decompressed payload. */
  async reduceLines<Acc>(fn: (acc: Acc, line: string) => Acc, init: Acc): Promise<Acc> {
    let acc = init;
    for await (const line of this.lines()) {
      acc = fn(acc, line);
    }
    return acc;
  }

  /**
   * Rewrite the buffer line-by-line.
   *
   * `rewriteLine` may emit zero or more replacement lines for each input line.
   * `onFlush` may emit trailing lines after the source has been fully consumed.
   * `emit` must be called synchronously before `rewriteLine`/`onFlush` returns.
   *
   * If `signal` is provided and aborted, the pipeline is destroyed and `this.data`
   * remains unchanged (the assignment only happens on successful completion).
   * The resulting `AbortError` propagates to the caller.
   */
  async rewriteLines(
    rewriteLine: (line: string, emit: EmitLine) => void,
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
          rewriteLine(line, (nextLine) => {
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

    if (this.data.length === 0) {
      await pipeline(Readable.from([] as string[]), rewriteStream, createBrotliCompress(brotliOptions), output, { signal });
    } else {
      await pipeline(
        Readable.from(this.data),
        createBrotliDecompress(),
        new SplitLines(),
        rewriteStream,
        createBrotliCompress(brotliOptions),
        output,
        { signal },
      );
    }

    this.data = emittedLineCount === 0 ? Buffer.alloc(0) : Buffer.concat(outputChunks);
  }
}
