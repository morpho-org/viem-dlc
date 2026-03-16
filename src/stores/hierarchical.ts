import type { Store } from "../types.js";

/**
 * A multi-level (hierarchical) store.
 * Reads from sub-stores consecutively, returning the first hit.
 * Writes through to all sub-stores in parallel.
 *
 * When `populateOnMiss` is true, a cache miss that is resolved by a lower store
 * will warm all higher stores with the value (cache-aside pattern).
 */
export class HierarchicalStore implements Store {
  constructor(
    private readonly stores: readonly Store[],
    private readonly options?: { populateOnMiss?: boolean },
  ) {}

  async get(key: string) {
    for (let i = 0; i < this.stores.length; i++) {
      const value = await this.stores[i]!.get(key);
      if (value !== null) {
        if (this.options?.populateOnMiss) {
          void Promise.allSettled(this.stores.slice(0, i).map((store) => store.set(key, value)));
        }
        return value;
      }
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
