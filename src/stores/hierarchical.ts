import type { Logger } from "../observability.js";
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
    private readonly options?: { populateOnMiss?: boolean; logger?: Logger },
  ) {}

  async get(key: string) {
    for (let i = 0; i < this.stores.length; i++) {
      const value = await this.stores[i]!.get(key);
      if (value !== null) {
        this.options?.logger
          ?.withMetadata({ class: HierarchicalStore.name, method: "get", level: i, key })
          .info("cache hit");
        if (this.options?.populateOnMiss) {
          void Promise.all(this.stores.slice(0, i).map((store) => store.set(key, value)));
        }
        return value;
      }
    }
    this.options?.logger?.withMetadata({ class: HierarchicalStore.name, method: "get", key }).info("cache miss");
    return null;
  }

  async set(key: string, value: Buffer[]) {
    await Promise.all(this.stores.map((store) => store.set(key, value)));
  }

  async delete(key: string) {
    await Promise.all(this.stores.map((store) => store.delete(key)));
  }

  async flush() {
    await Promise.all(this.stores.map((store) => store.flush()));
  }
}
