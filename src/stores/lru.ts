import type { Store } from "../types.js";

const encoder = new TextEncoder();

/** LRU cache with byte-based size limit (only values counted, keys assumed negligible). */
export class LruStore implements Store {
  private readonly maxBytes: number;
  private readonly map = new Map<string, { value: string; size: number }>();
  private bytes = 0;

  constructor(maxBytes: number) {
    if (maxBytes < 1) throw new Error("[LruStore] maxBytes must be at least 1");
    this.maxBytes = maxBytes;
  }

  get(key: string) {
    const entry = this.map.get(key);
    if (!entry) return null;
    this.map.delete(key);
    this.map.set(key, entry);
    return entry.value;
  }

  set(key: string, value: string) {
    this.delete(key);

    const size = encoder.encode(value).length;
    if (size > this.maxBytes) {
      console.warn(`[LruStore] Value exceeds maxBytes (${size} > ${this.maxBytes}), skipping`);
      return;
    }

    while (this.bytes + size > this.maxBytes) {
      // Non-null assertion is safe because map has entries until `this.bytes === 0`,
      // and once it's zero, the loop condition breaks because `size <= this.maxBytes`.
      const [oldestKey, oldest] = this.map.entries().next().value!;
      this.bytes -= oldest.size;
      this.map.delete(oldestKey);
    }

    this.map.set(key, { value, size });
    this.bytes += size;
  }

  delete(key: string) {
    const entry = this.map.get(key);
    if (entry) {
      this.bytes -= entry.size;
      this.map.delete(key);
    }
  }
}
