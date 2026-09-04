import type { Logger } from "../observability.js";
import type { Store } from "../types.js";

/**
 * A multi-level (hierarchical) store.
 * Reads from sub-stores consecutively, returning the first hit.
 * Writes through to all sub-stores in parallel.
 *
 * When `populateOnMiss` is true, a cache miss that is resolved by a lower store
 * will warm all higher stores with the value (cache-aside pattern) — unless a
 * write for the same key overlapped the read, which would otherwise leave the
 * higher tiers holding the pre-write value.
 */
export class HierarchicalStore implements Store {
  /** Reads in progress per key, and whether a write landed on that key while one was open. */
  private readonly reads = new Map<string, { depth: number; superseded: boolean }>();
  /** Writes in progress per key. A read opening mid-write is superseded from the start. */
  private readonly writes = new Map<string, number>();

  constructor(
    private readonly stores: readonly Store[],
    private readonly options?: { populateOnMiss?: boolean; logger?: Logger },
  ) {}

  async get(key: string) {
    const read = this.reads.get(key) ?? { depth: 0, superseded: this.writes.has(key) };
    read.depth++;
    this.reads.set(key, read);

    try {
      for (let i = 0; i < this.stores.length; i++) {
        const value = await this.stores[i]!.get(key);
        if (value !== null) {
          this.options?.logger
            ?.withMetadata({ class: HierarchicalStore.name, method: "get", level: i, key })
            .info("cache hit");
          if (this.options?.populateOnMiss && !read.superseded) {
            void Promise.all(this.stores.slice(0, i).map((store) => store.set(key, value)));
          }
          return value;
        }
      }
      this.options?.logger?.withMetadata({ class: HierarchicalStore.name, method: "get", key }).info("cache miss");
      return null;
    } finally {
      if (--read.depth === 0) this.reads.delete(key);
    }
  }

  async set(key: string, value: Buffer[]) {
    this.beginWrite(key);
    try {
      await Promise.all(this.stores.map((store) => store.set(key, value)));
    } finally {
      this.endWrite(key);
    }
  }

  async delete(key: string) {
    this.beginWrite(key);
    try {
      await Promise.all(this.stores.map((store) => store.delete(key)));
    } finally {
      this.endWrite(key);
    }
  }

  async flush() {
    await Promise.all(this.stores.map((store) => store.flush()));
  }

  private beginWrite(key: string): void {
    this.writes.set(key, (this.writes.get(key) ?? 0) + 1);
    const read = this.reads.get(key);
    if (read !== undefined) read.superseded = true;
  }

  private endWrite(key: string): void {
    const depth = (this.writes.get(key) ?? 1) - 1;
    if (depth === 0) this.writes.delete(key);
    else this.writes.set(key, depth);
  }
}
