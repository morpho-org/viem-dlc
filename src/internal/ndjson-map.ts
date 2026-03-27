import { CompressedLinesBlob, type Slot } from "./compressed-lines-blob.js";

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

/** Produce the raw JSON key token for a given key. Inverse of JSON.parse on the token. */
export function toRawKey(key: string): string {
  return JSON.stringify(key);
}

function compareRawKeys(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

export function sortEntriesByRawKey<K extends string, V>(
  entries: Iterable<readonly [key: K, value: V]>,
): [rawKey: string, key: K, value: V][] {
  return [...entries]
    .map(([key, value]) => [toRawKey(key), key, value] as [string, K, V])
    .sort(([a], [b]) => compareRawKeys(a, b));
}

/**
 * Streaming NDJSON container backed by a brotli-compressed buffer (base64-encoded).
 *
 * Each line is `{"key":<json-key>,"value":<codec-value>}`. The class owns the
 * envelope (key serialization via `JSON.stringify`); the codec handles only the
 * value portion of type `T`.
 *
 * Lines are maintained in lexicographic sorted order by raw JSON key;
 * see {@link upsert}. Supports streaming reduce (without materializing
 * the full dataset) and streaming upsert (decompress → merge-insert →
 * recompress).
 *
 * @dev IMPORTANT: Each instance expects to own its `slot`, i.e., no other entity
 * should cause `slot` to mutate or return different data.
 */
export class NdjsonMap<T, K extends string = string> {
  private readonly blob: CompressedLinesBlob;

  constructor(
    private readonly codec: Codec<T>,
    slot: Slot,
  ) {
    this.blob = new CompressedLinesBlob(slot);
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
  async reduce<Acc>(fn: (acc: Acc, record: Entry<T, K>) => Acc, init: Acc): Promise<Acc> {
    let acc = init;
    for await (const record of this.records()) {
      acc = fn(acc, record);
    }
    return acc;
  }

  /**
   * Stream-decompress existing data, merge-insert entries by key, and recompress.
   *
   * Maintains lexicographic sorted order by raw JSON key: pending entries are
   * sorted, then interleaved with existing (already-sorted) lines during
   * rewrite. Entries whose keys match an upsert are replaced in-place; new
   * keys are inserted at their sorted position. Always outputs brotli q=4.
   *
   * Mutates internal state — call {@link toBase64} afterwards to retrieve the result.
   * Callers must not overlap `upsert()` calls on the same instance; concurrent
   * upserts are unsafe and may lose writes.
   *
   * @dev Assumes the existing blob is already sorted by key with no duplicates.
   * If that invariant is violated, the offending line and the remaining suffix
   * are treated as garbage: a warning is logged and rewrite continues with only
   * the already-emitted prefix plus any remaining pending entries.
   */
  async upsert(entries: Entry<T, K>[], signal?: AbortSignal): Promise<void> {
    if (entries.length === 0) return;

    const serializeLine = this.serializeLine.bind(this);
    // Deduplicate (last write wins) then sort by raw JSON key for merge-insert
    const byKey = new Map<K, T>();
    for (const entry of entries) {
      byKey.set(entry.key, entry.value);
    }
    const sorted = sortEntriesByRawKey(byKey);
    let idx = 0;

    let prevRawKey: string | undefined;
    let corrupted = false;

    await this.blob.rewriteLines(
      (line, emit) => {
        if (corrupted || line.length === 0) return;

        const rawKey = extractRawKey(line);
        if (rawKey === undefined) return;

        if (prevRawKey !== undefined) {
          const ordering = compareRawKeys(rawKey, prevRawKey);
          if (ordering <= 0) {
            const reason = ordering === 0 ? "Duplicate" : "Unsorted";
            console.warn(
              `[NdjsonMap] ${reason} key in blob: ${rawKey}${ordering === 0 ? "" : ` after ${prevRawKey}`}. Discarding remaining blob lines.`,
            );
            corrupted = true;
            return;
          }
        }
        prevRawKey = rawKey;

        // Merge-insert: emit sorted pending entries that belong before this key
        while (idx < sorted.length && compareRawKeys(sorted[idx]![0], rawKey) < 0) {
          const [pKey, , pValue] = sorted[idx++]!;
          emit(serializeLine(pKey, pValue));
        }

        // Replace in-place if this key is being upserted, otherwise keep existing line
        if (idx < sorted.length && sorted[idx]![0] === rawKey) {
          emit(serializeLine(rawKey, sorted[idx++]![2]));
        } else {
          emit(line);
        }
      },
      (emit) => {
        while (idx < sorted.length) {
          const [pKey, , pValue] = sorted[idx++]!;
          emit(serializeLine(pKey, pValue));
        }
      },
      signal,
    );
  }
}
