import type { Store } from "../types.js";

/**
 * A multi-level (hierarchical) store.
 * Reads from sub-stores consecutively, returning the first hit.
 * Writes through to all sub-stores in parallel.
 */
export class HierarchicalStore implements Store {
  constructor(private readonly stores: readonly Store[]) {}

  async get(key: string) {
    for (const store of this.stores) {
      const value = await store.get(key);
      if (value !== null) return value;
    }
    return null;
  }

  async set(key: string, value: string) {
    await Promise.allSettled(this.stores.map((store) => store.set(key, value)));
  }

  async delete(key: string) {
    await Promise.allSettled(this.stores.map((store) => store.delete(key)));
  }

  async flush() {
    await Promise.allSettled(this.stores.map((store) => store.flush()));
  }
}
