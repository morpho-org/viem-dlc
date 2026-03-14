import type { Store } from "../types.js";

/**
 * An in-memory store, intended for demonstration purposes only.
 *
 * @dev Use with caution since memory growth is unbounded.
 * @deprecated `LruStore` is the safer option and incurs a negligible performance hit.
 */
export class MemoryStore implements Store {
  private readonly map = new Map<string, string>();

  get(key: string) {
    return this.map.get(key) ?? null;
  }

  set(key: string, value: string) {
    this.map.set(key, value);
  }

  delete(key: string) {
    this.map.delete(key);
  }

  flush() {}
}
