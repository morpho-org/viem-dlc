/// <reference types="node" />
import { Readable, Transform, type TransformCallback, Writable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { StringDecoder } from "node:string_decoder";
import { type BrotliOptions, createBrotliCompress, createBrotliDecompress, constants as zlib } from "node:zlib";

export type Entry<T, K extends string = string> = { key: K; value: T };

/** Codec for the value portion of each NDJSON entry. The class handles key serialization. */
export type Codec<T> = {
  parse: (s: string) => T | undefined;
  stringify: (value: T) => string;
};

const brotliOptions: BrotliOptions = {
  params: {
    [zlib.BROTLI_PARAM_QUALITY]: 4,
  },
};

/**
 * Each line is serialized as `{"key":<JSON key>,"value":<codec value>}`.
 * The separator `","value":` (with a real unescaped `"`) can only appear at
 * the key/value boundary because `JSON.stringify` escapes quotes inside the key.
 */
const KEY_PREFIX = '{"key":';
const SEPARATOR = '","value":';
const VALUE_INFIX = SEPARATOR.slice(1); // ,"value":
const KEY_START = KEY_PREFIX.length; // 7

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
 * Streaming NDJSON container backed by a brotli-compressed buffer (base64-encoded).
 *
 * Each line is `{"key":<json-key>,"value":<codec-value>}`. The class owns the
 * envelope (key serialization via `JSON.stringify`); the codec handles only the
 * value portion of type `T`.
 *
 * Supports streaming reduce (without materializing the full dataset) and
 * streaming upsert (decompress → filter → append → recompress).
 */
export class NDJSON<T, K extends string = string> {
  private data: Buffer;

  constructor(
    private readonly codec: Codec<T>,
    compressed?: string | Buffer,
  ) {
    this.data = compressed
      ? typeof compressed === "string"
        ? Buffer.from(compressed, "base64")
        : compressed
      : Buffer.alloc(0);
  }

  toBase64(): string {
    return this.data.toString("base64");
  }

  /** Build an NDJSON line from a pre-stringified JSON key token and a value. */
  private serializeRawKey(rawKey: string, value: T): string {
    return `${KEY_PREFIX}${rawKey}${VALUE_INFIX}${this.codec.stringify(value)}}`;
  }

  /** Compress a stream of NDJSON lines into a single brotli buffer. */
  private async compressLines(lines: Iterable<string> | AsyncIterable<string>): Promise<Buffer> {
    const outputChunks: Buffer[] = [];

    await pipeline(
      Readable.from(lines),
      createBrotliCompress(brotliOptions),
      new Writable({
        write(chunk, _enc, cb) {
          outputChunks.push(chunk as Buffer);
          cb();
        },
      }),
    );

    return Buffer.concat(outputChunks);
  }

  /** Parse a line into an entry, returning `undefined` if the envelope is malformed. */
  private parseLine(line: string): Entry<T, K> | undefined {
    const parsed = this.parseEnvelope(line);
    if (parsed === undefined) return undefined;

    // Codec errors propagate — the caller controls throwing vs returning undefined
    const value = this.codec.parse(line.slice(parsed.valueStart, line.length - 1));
    if (value === undefined) return undefined;
    return { key: parsed.key, value };
  }

  /** Parse only the envelope metadata needed to validate a line and identify its key. */
  private parseEnvelope(line: string): { key: K; rawKey: string; valueStart: number } | undefined {
    if (!line.startsWith(KEY_PREFIX) || !line.endsWith("}")) return undefined;

    const i = line.indexOf(SEPARATOR, KEY_START);
    if (i === -1) return undefined;

    const rawKey = line.slice(KEY_START, i + 1);

    let key: K;
    try {
      key = JSON.parse(rawKey) as K;
    } catch {
      return undefined;
    }
    if (typeof key !== "string") return undefined;

    return { key, rawKey, valueStart: i + SEPARATOR.length };
  }

  /**
   * Async generator that yields each parsed record from the compressed NDJSON.
   * Error handling depends on the codec: codecs that return `undefined` on bad
   * input cause the line to be skipped; codecs that throw will propagate the error.
   */
  async *records(): AsyncGenerator<Entry<T, K>, void, void> {
    if (this.data.length === 0) return;

    const input = Readable.from(this.data);
    const decompressor = createBrotliDecompress();
    const splitter = new SplitLines();
    const stream = input.pipe(decompressor).pipe(splitter);

    try {
      for await (const line of stream) {
        if ((line as string).length === 0) continue;
        const entry = this.parseLine(line as string);
        if (entry !== undefined) yield entry;
      }
    } finally {
      input.destroy();
      decompressor.destroy();
      splitter.destroy();
    }
  }

  /**
   * Stream-decompress and fold every entry through `fn`.
   *
   * @param fn Reducer: `(accumulator, record, initialValue) => newAccumulator`
   * @param init Initial accumulator value (also forwarded to `fn` as its third argument)
   */
  async reduce<Acc>(fn: (acc: Acc, record: Entry<T, K>, init: Acc) => Acc, init: Acc): Promise<Acc> {
    let acc = init;
    for await (const record of this.records()) {
      acc = fn(acc, record, init);
    }
    return acc;
  }

  /**
   * Stream-decompress existing data, replace or append entries by key, and recompress.
   *
   * Entries whose keys match an upsert are replaced at their first occurrence;
   * later duplicates are dropped. Existing duplicate keys are also collapsed so
   * the rewritten blob contains at most one line per key. Unmatched upserts are
   * appended at the end. Always outputs brotli q=4.
   *
   * Mutates internal state — call {@link toBase64} afterwards to retrieve the result.
   */
  async upsert(entries: Entry<T, K>[]): Promise<void> {
    if (entries.length === 0) return;

    const serializeRawKey = this.serializeRawKey.bind(this);

    const pending = new Map<string, T>();
    for (const entry of entries) {
      pending.set(JSON.stringify(entry.key), entry.value);
    }

    // Fast path: no existing data, just compress the new entries
    if (this.data.length === 0) {
      this.data = await this.compressLines(
        (function* () {
          for (const [rawKey, value] of pending) {
            yield `${serializeRawKey(rawKey, value)}\n`;
          }
        })(),
      );
      return;
    }

    const outputChunks: Buffer[] = [];
    const seen = new Set<string>();
    const parseEnvelope = this.parseEnvelope.bind(this);

    await pipeline(
      Readable.from(this.data),
      createBrotliDecompress(),
      new SplitLines(),
      new Transform({
        readableObjectMode: false,
        writableObjectMode: true,
        transform(line: string, _enc, callback) {
          if (line.length === 0) {
            callback();
            return;
          }

          const envelope = parseEnvelope(line);
          if (envelope === undefined) {
            callback();
            return;
          }
          const { rawKey } = envelope;
          if (seen.has(rawKey)) {
            callback();
            return;
          }
          seen.add(rawKey);

          if (pending.has(rawKey)) {
            const value = pending.get(rawKey)!;
            pending.delete(rawKey);
            // rawKey is already JSON-stringified, so we can splice it directly
            callback(null, `${serializeRawKey(rawKey, value)}\n`);
          } else {
            callback(null, line + "\n");
          }
        },
        flush(callback) {
          // Append entries that didn't match any existing key
          for (const [rawKey, value] of pending) {
            this.push(`${serializeRawKey(rawKey, value)}\n`);
          }
          callback();
        },
      }),
      createBrotliCompress(brotliOptions),
      new Writable({
        write(chunk, _enc, cb) {
          outputChunks.push(chunk as Buffer);
          cb();
        },
      }),
    );

    this.data = Buffer.concat(outputChunks);
  }
}
