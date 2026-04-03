import { CompressedLinesBlob, type Slot } from "./compressed-lines-blob.js";

export type Entry<T, K extends string = string> = { key: K; value: T };

/**
 * Entry with deferred value parsing. The raw JSON value string is available
 * immediately for cheap pre-filtering (e.g. `.includes()` / regex); the
 * parsed value is materialized lazily on first `.value` access.
 *
 * Backed by a class so getters live on the prototype rather than as per-instance closures.
 */
export class LazyEntry<T, K extends string = string> {
  readonly key: K;
  readonly rawValue: string;
  private parsed: T | undefined;
  private done = false;
  private readonly codec: Codec<T>;

  constructor(key: K, rawValue: string, codec: Codec<T>) {
    this.key = key;
    this.rawValue = rawValue;
    this.codec = codec;
  }

  get value(): T {
    if (!this.done) {
      this.parsed = this.codec.fromJson(this.rawValue);
      this.done = true;
    }
    return this.parsed as T;
  }
}

/**
 * Codec for the value portion of each NDJSON entry. The class handles key serialization.
 *
 * Must be lossless: `fromJson(toJson(x))` should deeply equal `x` for any valid value.
 * This invariant is required for read-your-writes consistency in {@link LazyNdjsonMap}
 * (pending entries may be surfaced before or after a codec round-trip).
 */
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

function sortEntriesByRawKey<K extends string, V>(
  entries: Iterable<readonly [key: K, value: V]>,
): [rawKey: string, key: K, value: V][] {
  return [...entries]
    .map(([key, value]) => [JSON.stringify(key), key, value] as [string, K, V])
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
}

/**
 * Streaming NDJSON container backed by a compressed line buffer.
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

  /** Parse a line into a lazy entry, returning `undefined` if the envelope is malformed. */
  private parseLine(line: string): LazyEntry<T, K> | undefined {
    const parsed = parseEnvelope<K>(line);
    if (parsed === undefined) return undefined;
    const rawValue = line.slice(parsed.valueStart, line.length - 1);
    return new LazyEntry(parsed.key, rawValue, this.codec);
  }

  /**
   * Async generator that yields each record from the compressed NDJSON.
   * If `extra` is provided, its entries are merge-sorted into the stream
   * by key, with extra entries taking precedence over stored entries on
   * key collision.
   */
  async *records(extra?: ReadonlyMap<K, T>): AsyncGenerator<LazyEntry<T, K>, void, void> {
    const sorted = extra?.size ? sortEntriesByRawKey(extra) : undefined;
    let idx = 0;

    for await (const line of this.blob.lines()) {
      if (line.length === 0) continue;

      if (sorted) {
        const rawKey = extractRawKey(line);
        if (rawKey === undefined) continue;

        // Yield extra entries that sort before this stored key
        while (idx < sorted.length && sorted[idx]![0] < rawKey) {
          const [, key, value] = sorted[idx++]!;
          yield new LazyEntry(key, this.codec.toJson(value), this.codec);
        }

        // Extra entry overrides this stored key — yield the override, skip stored
        if (idx < sorted.length && sorted[idx]![0] === rawKey) {
          const [, key, value] = sorted[idx++]!;
          yield new LazyEntry(key, this.codec.toJson(value), this.codec);
          continue;
        }
      }

      const entry = this.parseLine(line);
      if (entry !== undefined) yield entry;
    }

    if (sorted) {
      while (idx < sorted.length) {
        const [, key, value] = sorted[idx++]!;
        yield new LazyEntry(key, this.codec.toJson(value), this.codec);
      }
    }
  }

  /** Stream-decompress and fold every entry through `fn`. */
  async reduce<Acc>(
    fn: (acc: Acc, record: LazyEntry<T, K>) => Acc,
    init: Acc,
    extra?: ReadonlyMap<K, T>,
  ): Promise<Acc> {
    let acc = init;
    for await (const record of this.records(extra)) {
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
   * keys are inserted at their sorted position. Rewrites use the current
   * {@link CompressedLinesBlob} codec and settings.
   *
   * Mutates the underlying slot via {@link CompressedLinesBlob}.
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
    return this.mergeAndRewrite(entries, signal);
  }

  /**
   * Like {@link upsert}, but also folds through every entry (existing + new)
   * in sorted key order during the rewrite pass. When `entries` is empty,
   * degenerates to a pure {@link reduce} (no rewrite).
   */
  async upsertAndFold<Acc>(
    entries: Entry<T, K>[],
    fn: (acc: Acc, entry: LazyEntry<T, K>) => Acc,
    init: Acc,
    signal?: AbortSignal,
  ): Promise<Acc> {
    if (entries.length === 0) return this.reduce(fn, init);
    let acc = init;
    await this.mergeAndRewrite(entries, signal, (entry) => {
      acc = fn(acc, entry);
    });
    return acc;
  }

  /*//////////////////////////////////////////////////////////////
                              PRIVATE
  //////////////////////////////////////////////////////////////*/

  /**
   * Core merge-insert-rewrite logic shared by {@link upsert} and {@link upsertAndFold}.
   * If `onEntry` is provided, it is called synchronously for each emitted entry
   * (existing lines kept as-is, replaced lines, and newly inserted lines) in
   * sorted key order.
   */
  private async mergeAndRewrite(
    entries: Entry<T, K>[],
    signal?: AbortSignal,
    onEntry?: (entry: LazyEntry<T, K>) => void,
  ): Promise<void> {
    const toJson = this.codec.toJson;
    const parseLine = onEntry ? this.parseLine.bind(this) : undefined;

    const codec = this.codec;
    const emitNew = (emit: (line: string) => void, rawKey: string, key: K, value: T) => {
      const rawValue = toJson(value);
      emit(`${KEY_PREFIX}${rawKey}${SEPARATOR}${rawValue}}`);
      if (onEntry) onEntry(new LazyEntry(key, rawValue, codec));
    };

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

        if (prevRawKey !== undefined && rawKey <= prevRawKey) {
          const reason = rawKey === prevRawKey ? "Duplicate" : "Unsorted";
          console.warn(
            `[NdjsonMap] ${reason} key in blob: ${rawKey}${rawKey === prevRawKey ? "" : ` after ${prevRawKey}`}. Discarding remaining blob lines.`,
          );
          corrupted = true;
          return;
        }
        prevRawKey = rawKey;

        // Merge-insert: emit sorted pending entries that belong before this key
        while (idx < sorted.length && sorted[idx]![0] < rawKey) {
          const [pKey, pOrigKey, pValue] = sorted[idx++]!;
          emitNew(emit, pKey, pOrigKey, pValue);
        }

        // Replace in-place if this key is being upserted, otherwise keep existing line
        if (idx < sorted.length && sorted[idx]![0] === rawKey) {
          const [, pOrigKey, pValue] = sorted[idx++]!;
          emitNew(emit, rawKey, pOrigKey, pValue);
        } else {
          emit(line);
          if (parseLine) {
            const entry = parseLine(line);
            if (entry) onEntry!(entry);
          }
        }
      },
      (emit) => {
        while (idx < sorted.length) {
          const [pKey, pOrigKey, pValue] = sorted[idx++]!;
          emitNew(emit, pKey, pOrigKey, pValue);
        }
      },
      signal,
    );
  }
}
