import type { Logger } from "../observability.js";
import type { ProvenanceAwareStore, Store, StoreRead } from "../types.js";

function isProvenanceAware(store: Store): store is ProvenanceAwareStore {
  return typeof (store as Partial<ProvenanceAwareStore>).getWithProvenance === "function";
}

/**
 * A multi-level (hierarchical) store.
 * Reads from sub-stores consecutively, returning the first hit.
 * Writes through to all sub-stores in parallel.
 *
 * When `populateOnMiss` is true, a cache miss that is resolved by a lower store
 * will warm all higher stores with the value (cache-aside pattern) — unless the
 * value is provisional (see {@link ProvenanceAwareStore}) or a write for the same
 * key overlapped the read, either of which would leave a higher tier holding a
 * value that is already superseded.
 */
export class HierarchicalStore implements ProvenanceAwareStore {
  /** Reads in progress per key, and whether a write landed on that key while one was open. */
  private readonly reads = new Map<string, { depth: number; superseded: boolean }>();
  /** Writes in progress per key. A read opening mid-write is superseded from the start. */
  private readonly writes = new Map<string, number>();

  constructor(
    private readonly stores: readonly Store[],
    private readonly options?: { populateOnMiss?: boolean; logger?: Logger },
  ) {}

  async get(key: string) {
    return (await this.getWithProvenance(key)).value;
  }

  async getWithProvenance(key: string): Promise<StoreRead> {
    const read = this.reads.get(key) ?? { depth: 0, superseded: this.writes.has(key) };
    read.depth++;
    this.reads.set(key, read);

    try {
      for (let i = 0; i < this.stores.length; i++) {
        const store = this.stores[i]!;
        const result = isProvenanceAware(store)
          ? await store.getWithProvenance(key)
          : { value: await store.get(key), provisional: false };

        // A provisional answer is terminal, including a `null` tombstone for a pending delete: falling
        // through would let a lower tier resurrect the value that delete supersedes.
        if (result.value === null && !result.provisional) continue;

        this.options?.logger
          ?.withMetadata({
            class: HierarchicalStore.name,
            method: "get",
            level: i,
            key,
            provisional: result.provisional,
          })
          .info(result.value !== null ? "cache hit" : "cache miss");

        const value = result.value;
        if (this.options?.populateOnMiss && !result.provisional && !read.superseded && value !== null) {
          void Promise.all(this.stores.slice(0, i).map((higher) => higher.set(key, value)));
        }
        return result;
      }

      this.options?.logger?.withMetadata({ class: HierarchicalStore.name, method: "get", key }).info("cache miss");
      return { value: null, provisional: false };
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
