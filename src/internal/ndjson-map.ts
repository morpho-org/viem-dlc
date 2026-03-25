import { BrotliLineBlob, type Slot } from "./brotli-line-blob.js";

export type Entry<T, K extends string = string> = { key: K; value: T };

/** Codec for the value portion of each NDJSON entry. The class handles key serialization. */
export type Codec<T> = {
  fromJson: (s: string) => T;
  /** Serializes `value` to a valid JSON string, which MUST NOT contain literal newlines. */
  toJson: (value: T) => string;
};

/**
 * Each line is serialized as `{"key":<json key>,"value":<json value>}`.
 *
 * `codec.toJson()` must return a single JSON value token suitable for direct
 * embedding after `,"value":`. In practice that means strings must already be
 * quoted/escaped, and the output must not contain literal newlines.
 *
 * The separator `,"value":` can only appear at the key/value boundary because
 * `JSON.stringify` escapes all `"` inside the key as `\"`, so an unescaped `"`
 * (which `,"value":` contains) cannot match within the key token.
 */
const KEY_PREFIX = '{"key":';
const SEPARATOR = ',"value":';
const KEY_START = KEY_PREFIX.length; // 7

/**
 * Extract the raw JSON key token from a line via string slicing only (no JSON.parse).
 * Returns `undefined` if the line doesn't match the expected envelope structure.
 */
function extractRawKey(line: string): string | undefined {
  if (!line.startsWith(KEY_PREFIX) || !line.endsWith("}")) return undefined;
  const i = line.indexOf(SEPARATOR, KEY_START);
  if (i === -1) return undefined;
  return line.slice(KEY_START, i);
}

/** Parse the full envelope: validates structure, JSON-parses the key, locates the value. */
function parseEnvelope<K extends string>(line: string): { key: K; valueStart: number } | undefined {
  const rawKey = extractRawKey(line);
  if (rawKey === undefined) return undefined;

  let key: K;
  try {
    key = JSON.parse(rawKey) as K;
  } catch {
    return undefined;
  }
  if (typeof key !== "string") return undefined;

  return { key, valueStart: KEY_START + rawKey.length + SEPARATOR.length };
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
 * 
 * @dev IMPORTANT: Each instance expects to own its `slot`, i.e., no other entity
 * should cause `slot` to mutate or return different data.
 */
export class NdjsonMap<T, K extends string = string> {
  private readonly blob: BrotliLineBlob;

  constructor(
    private readonly codec: Codec<T>,
    slot: Slot,
  ) {
    this.blob = new BrotliLineBlob(slot);
  }

  /** Build a full NDJSON line from a pre-stringified JSON key token and a value. */
  private serializeLine(rawKey: string, value: T): string {
    return `${KEY_PREFIX}${rawKey}${SEPARATOR}${this.codec.toJson(value)}}`;
  }

  /** Parse a line into an entry, returning `undefined` if the envelope is malformed. */
  private parseLine(line: string): Entry<T, K> | undefined {
    const parsed = parseEnvelope<K>(line);
    if (parsed === undefined) return undefined;
    return { key: parsed.key, value: this.codec.fromJson(line.slice(parsed.valueStart, line.length - 1)) };
  }

  /** Async generator that yields each record from the compressed NDJSON. */
  async *records(): AsyncGenerator<Entry<T, K>, void, void> {
    for await (const line of this.blob.lines()) {
      if (line.length === 0) continue;
      const entry = this.parseLine(line);
      if (entry !== undefined) yield entry;
    }
  }

  /** Stream-decompress and fold every entry through `fn`. */
  reduce<Acc>(fn: (acc: Acc, record: Entry<T, K>) => Acc, init: Acc): Promise<Acc> {
    return this.blob.reduceLines((acc, line) => {
      if (line.length === 0) return acc;
      const entry = this.parseLine(line);
      return entry === undefined ? acc : fn(acc, entry);
    }, init);
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
   * Callers must not overlap `upsert()` calls on the same instance; concurrent
   * upserts are unsafe and may lose writes.
   */
  async upsert(entries: Entry<T, K>[], signal?: AbortSignal): Promise<void> {
    if (entries.length === 0) return;

    const serializeLine = this.serializeLine.bind(this);
    const pending = new Map<string, T>();
    for (const entry of entries) {
      pending.set(JSON.stringify(entry.key), entry.value);
    }
    const seen = new Set<string>();

    await this.blob.rewriteLines(
      (line, emit) => {
        if (line.length === 0) return;

        // Best-effort rewrite: we intentionally avoid fully parsing the value
        // here for speed. That means a line with a valid key envelope but malformed
        // value can still claim the key during dedupe and shadow a later valid line.
        // This tradeoff is acceptable since the data should never be malformed.
        const rawKey = extractRawKey(line);
        if (rawKey === undefined) return;

        if (seen.has(rawKey)) return;
        seen.add(rawKey);

        if (pending.has(rawKey)) {
          const value = pending.get(rawKey)!;
          pending.delete(rawKey);
          emit(serializeLine(rawKey, value));
        } else {
          emit(line);
        }
      },
      (emit) => {
        for (const [rawKey, value] of pending) {
          emit(serializeLine(rawKey, value));
        }
      },
      signal,
    );
  }
}
