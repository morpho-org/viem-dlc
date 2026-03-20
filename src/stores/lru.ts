import type { Store } from "../types.js";

/** LRU cache with byte-based size limit (only values counted, keys assumed negligible). */
export class LruStore implements Store {
  private readonly maxBytes: number;
  private readonly map = new Map<string, Buffer[]>();
  private bytes = 0;

  constructor(maxBytes: number) {
    if (maxBytes < 1) throw new Error("[LruStore] maxBytes must be at least 1");
    this.maxBytes = maxBytes;
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

    const size = value.reduce((acc, b) => acc + b.byteLength, 0);
    if (size > this.maxBytes) {
      console.warn(`[LruStore] Value exceeds maxBytes (${size} > ${this.maxBytes}), skipping`);
      return;
    }

    while (this.bytes + size > this.maxBytes) {
      // Non-null assertion is safe because map has entries until `this.bytes === 0`,
      // and once it's zero, the loop condition breaks because `size <= this.maxBytes`.
      const [oldestKey, oldest] = this.map.entries().next().value!;
      this.bytes -= oldest.reduce((acc, b) => acc + b.byteLength, 0);
      this.map.delete(oldestKey);
    }

    this.map.set(key, value);
    this.bytes += size;
  }

  delete(key: string) {
    const value = this.map.get(key);
    if (value) {
      this.bytes -= value.reduce((acc, b) => acc + b.byteLength, 0);
      this.map.delete(key);
    }
  }

  flush() {}
}
