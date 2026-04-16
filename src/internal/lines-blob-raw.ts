/// <reference types="node" />
import { Readable, Transform, Writable } from "stream";
import { pipeline } from "stream/promises";

import type { EmitLine, LinesBlob, Slot } from "./lines-blob.js";
import { SplitLines } from "./lines-blob-compressed.js";

/**
 * Plain (uncompressed) line buffer over a {@link Slot}.
 *
 * Stores newline-delimited UTF-8 text directly in the slot. Implements the
 * same {@link LinesBlob} surface as `LinesBlobCompressed`, so `NdjsonMap` and
 * `NdjsonMapLazy` can be backed by either without behavior changes.
 *
 * Intended for slots small enough that zstd's per-rewrite overhead outweighs
 * any compression gain. Pair with {@link NdjsonMapAdaptive} for size-based
 * backend selection at construction time.
 *
 * @dev IMPORTANT: Each instance expects to own its `slot`, i.e., no other entity
 * should cause `slot` to mutate or return different data.
 */
export class LinesBlobRaw implements LinesBlob {
  constructor(private readonly slot: Slot) {
    const chunks = slot.get();
    console.assert(chunks.length === 0 || chunks[0]!.length > 0, "Slot contains an empty buffer in array");
  }

  async *lines(): AsyncGenerator<string, void, void> {
    if (this.slot.get().length === 0) return;

    const input = Readable.from(this.slot.get());
    const splitter = new SplitLines();

    const done = pipeline(input, splitter);

    try {
      for await (const line of splitter) {
        yield line as string;
      }
    } finally {
      await done.catch(() => {});
    }
  }

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
      await pipeline(Readable.from([] as string[]), rewriteStream, output, { signal });
    } else {
      await pipeline(Readable.from(this.slot.get()), new SplitLines(), rewriteStream, output, { signal });
    }

    this.slot.set(emittedLineCount === 0 ? [] : outputChunks);
  }
}
