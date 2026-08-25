import type { Logger } from "../observability.js";
import type { Store } from "../types.js";

function sizeOf(buffers: Buffer[]) {
  return buffers.reduce((acc, b) => acc + b.byteLength, 0);
}

export type LruStoreOptions = {
  maxBytes: number;
  /** Optional Logger for non-request-bound emissions (e.g. oversized-value drops). */
  logger?: Logger;
};

/** LRU cache with byte-based size limit (only values counted, keys assumed negligible). */
export class LruStore implements Store {
  private readonly maxBytes: number;
  private readonly logger?: Logger;
  private readonly map = new Map<string, Buffer[]>();
  private bytes = 0;

  constructor({ maxBytes, logger }: LruStoreOptions) {
    // Rejects a non-number outright so the superseded `new LruStore(bytes)` form fails
    // loudly; left to the `< 1` check alone it would yield `undefined`, making every
    // size comparison false and turning this into an unbounded map.
    if (typeof maxBytes !== "number" || !Number.isFinite(maxBytes) || maxBytes < 1) {
      const err = new Error(`[LruStore] maxBytes must be at least 1 (got ${String(maxBytes)})`);
      logger?.withMetadata({ class: LruStore.name, method: "constructor" }).withError(err).error();
      throw err;
    }
    this.maxBytes = maxBytes;
    this.logger = logger;
  }

  get(key: string) {
    const value = this.map.get(key);
    if (!value) return null;
    this.map.delete(key);
    this.map.set(key, value);
    return value;
  }

  set(key: string, value: Buffer[]) {
    this.delete(key);

    const size = sizeOf(value);
    if (size > this.maxBytes) {
      this.logger
        ?.withMetadata({ class: LruStore.name, method: "set", key, size, max_bytes: this.maxBytes })
        .warn("value exceeds maxBytes, skipping");
      return;
    }

    const evicted: string[] = [];
    while (this.bytes + size > this.maxBytes) {
      // Non-null assertion is safe because map has entries until `this.bytes === 0`,
      // and once it's zero, the loop condition breaks because `size <= this.maxBytes`.
      const [oldestKey, oldest] = this.map.entries().next().value!;
      this.bytes -= sizeOf(oldest);
      this.map.delete(oldestKey);

      evicted.push(oldestKey);
    }

    this.map.set(key, value);
    this.bytes += size;

    this.logger?.metadataOnly({ class: LruStore.name, method: "set", key, size, max_bytes: this.maxBytes, evicted });
  }

  delete(key: string) {
    const value = this.map.get(key);
    if (value) {
      this.bytes -= sizeOf(value);
      this.map.delete(key);
    }
  }

  flush() {}
}
