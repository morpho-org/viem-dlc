import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { HierarchicalStore, LruStore, TtlStore } from "../../src/stores/index.js";
import type { Store } from "../../src/types.js";

const bytes = (s: string): Buffer[] => [Buffer.from(s)];

/** Minimal async, framing-preserving store. */
const asyncStore = (): Store => {
  const m = new Map<string, Buffer[]>();
  return {
    get: async (key) => m.get(key) ?? null,
    set: async (key, value) => void m.set(key, value),
    delete: async (key) => void m.delete(key),
    flush: async () => {},
  };
};

/** Store that flattens on set and hands back one buffer *per byte* on get — stresses reunification. */
const rechunkingStore = (): Store => {
  const m = new Map<string, Buffer>();
  return {
    get: (key) => {
      const b = m.get(key);
      return b ? [...b].map((byte) => Buffer.from([byte])) : null;
    },
    set: (key, value) => void m.set(key, Buffer.concat(value)),
    delete: (key) => void m.delete(key),
    flush: () => {},
  };
};

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(0);
});
afterEach(() => {
  vi.useRealTimers();
});

describe("TtlStore", () => {
  it("throws if ttlMs is not a finite number >= 1", () => {
    const msg = "[TtlStore] ttlMs must be a finite number >= 1";
    expect(() => new TtlStore(new LruStore({ maxBytes: 1024 }), { ttlMs: 0 })).toThrow(msg);
    expect(() => new TtlStore(new LruStore({ maxBytes: 1024 }), { ttlMs: -1 })).toThrow(msg);
    expect(() => new TtlStore(new LruStore({ maxBytes: 1024 }), { ttlMs: Number.NaN })).toThrow(msg);
    expect(() => new TtlStore(new LruStore({ maxBytes: 1024 }), { ttlMs: Number.POSITIVE_INFINITY })).toThrow(msg);
  });

  it("returns null for missing keys", () => {
    const store = new TtlStore(new LruStore({ maxBytes: 1024 }), { ttlMs: 1000 });
    expect(store.get("missing")).toBeNull();
  });

  it("serves a value within ttlMs", () => {
    const store = new TtlStore(new LruStore({ maxBytes: 1024 }), { ttlMs: 1000 });
    store.set("k", bytes("v"));
    vi.advanceTimersByTime(999);
    expect(store.get("k")).toEqual(bytes("v"));
  });

  it("misses once past ttlMs", () => {
    const store = new TtlStore(new LruStore({ maxBytes: 1024 }), { ttlMs: 1000 });
    store.set("k", bytes("v"));
    vi.advanceTimersByTime(1001);
    expect(store.get("k")).toBeNull();
  });

  it("serves at exactly the ttlMs deadline and expires only strictly past it", () => {
    const store = new TtlStore(new LruStore({ maxBytes: 1024 }), { ttlMs: 1000 });
    store.set("k", bytes("v"));
    vi.advanceTimersByTime(1000); // age === ttlMs → still fresh
    expect(store.get("k")).toEqual(bytes("v"));
    vi.advanceTimersByTime(1); // age === ttlMs + 1 → expired
    expect(store.get("k")).toBeNull();
  });

  it("never refreshes the ttl on get (absolute expiry from set)", () => {
    const store = new TtlStore(new LruStore({ maxBytes: 1024 }), { ttlMs: 1000 });
    store.set("k", bytes("v"));
    // A read just before expiry must not extend the entry's life.
    vi.advanceTimersByTime(999);
    expect(store.get("k")).toEqual(bytes("v"));
    vi.advanceTimersByTime(2); // now age = 1001 > ttl
    expect(store.get("k")).toBeNull();
  });

  it("re-stamps the ttl on set", () => {
    const store = new TtlStore(new LruStore({ maxBytes: 1024 }), { ttlMs: 1000 });
    store.set("k", bytes("v1"));
    vi.advanceTimersByTime(600);
    store.set("k", bytes("v2")); // resets the clock
    vi.advanceTimersByTime(600); // age since original = 1200, since re-set = 600
    expect(store.get("k")).toEqual(bytes("v2"));
  });

  it("reports a miss past ttlMs without deleting from the wrapped store", () => {
    // Expiry must NOT issue a delete: with an async store it could race a concurrent write and
    // clobber a fresh value. Spy is attached after `set` (LruStore.set self-calls delete internally).
    const inner = new LruStore({ maxBytes: 1024 });
    const store = new TtlStore(inner, { ttlMs: 1000 });
    store.set("k", bytes("v"));
    const deleteSpy = vi.spyOn(inner, "delete");
    vi.advanceTimersByTime(1001);
    expect(store.get("k")).toBeNull();
    expect(deleteSpy).not.toHaveBeenCalled();
  });

  it("returns value buffers by reference over a framing-preserving store (no copy)", () => {
    const store = new TtlStore(new LruStore({ maxBytes: 1024 }), { ttlMs: 1000 });
    const a = Buffer.from("ab");
    const b = Buffer.from("cd");
    store.set("k", [a, b]);
    const result = store.get("k") as Buffer[];
    expect(result).toEqual([a, b]); // framing preserved, not collapsed
    expect(result[0]).toBe(a); // ...and the same underlying buffers — not a copy
    expect(result[1]).toBe(b);
  });

  it("reunifies values rechunked by the wrapped store", () => {
    const store = new TtlStore(rechunkingStore(), { ttlMs: 1000 });
    store.set("k", [Buffer.from("hello"), Buffer.from("world")]);
    expect(store.get("k")).toEqual([Buffer.from("helloworld")]);
    vi.advanceTimersByTime(1001);
    expect(store.get("k")).toBeNull();
  });

  it("treats an entry too short to carry the header as a miss", () => {
    const inner = new LruStore({ maxBytes: 1024 });
    inner.set("k", [Buffer.from("short")]); // 5 bytes < header — could not have been written by TtlStore
    const store = new TtlStore(inner, { ttlMs: 1000 });
    expect(store.get("k")).toBeNull();
  });

  it("treats a foreign value (no magic header) as a miss instead of misreading it", () => {
    // A value written to the wrapped store by something other than TtlStore (a pre-existing entry, or
    // another consumer sharing the store). Long enough to look header-sized, but lacks the magic — so
    // it must NOT be served as a stamped value with its leading bytes stripped.
    const inner = new LruStore({ maxBytes: 1024 });
    const foreign = Buffer.from("a plain value that was never written through the TtlStore wrapper");
    inner.set("k", [foreign]);
    const store = new TtlStore(inner, { ttlMs: 1000 });
    expect(store.get("k")).toBeNull();
  });

  it("delete removes the entry and flush does not throw", () => {
    const store = new TtlStore(new LruStore({ maxBytes: 1024 }), { ttlMs: 1000 });
    store.set("k", bytes("v"));
    store.delete("k");
    expect(store.get("k")).toBeNull();
    expect(() => store.flush()).not.toThrow();
  });

  it("delegates byte-cap eviction to a wrapped LruStore (the header counts toward the cap)", () => {
    // Each stored entry is a 12-byte header + 3-byte payload = 15 bytes; a cap of 18 holds one, not two.
    const store = new TtlStore(new LruStore({ maxBytes: 12 + 3 + 3 }), { ttlMs: 1_000_000 });
    store.set("a", bytes("xxx")); // 15 bytes
    store.set("b", bytes("yyy")); // 15 more → total 30 > 18, evicts 'a'
    expect(store.get("a")).toBeNull();
    expect(store.get("b")).toEqual(bytes("yyy"));
  });

  describe("over an async wrapped store", () => {
    it("stays asynchronous and honors the ttl", async () => {
      const store = new TtlStore(asyncStore(), { ttlMs: 1000 });

      const setResult = store.set("k", bytes("v"));
      expect(setResult).toBeInstanceOf(Promise);
      await setResult;

      const getResult = store.get("k");
      expect(getResult).toBeInstanceOf(Promise);
      expect(await getResult).toEqual(bytes("v"));

      vi.advanceTimersByTime(1001);
      expect(await store.get("k")).toBeNull();
    });

    it("never deletes on expiry, so it cannot race a concurrent write", async () => {
      const inner = asyncStore();
      const store = new TtlStore(inner, { ttlMs: 1000 });
      await store.set("k", bytes("v"));
      const deleteSpy = vi.spyOn(inner, "delete");

      vi.advanceTimersByTime(1001);
      expect(await store.get("k")).toBeNull(); // expired → miss
      expect(deleteSpy).not.toHaveBeenCalled(); // ...but no delete that could clobber a fresh write
    });
  });

  it("passes through the wrapped store's synchronous nature", () => {
    const store = new TtlStore(new LruStore({ maxBytes: 1024 }), { ttlMs: 1000 });
    expect(store.set("k", bytes("v"))).toBeUndefined();
    const getResult = store.get("k");
    expect(getResult).not.toBeInstanceOf(Promise);
    expect(getResult).toEqual(bytes("v"));
  });
});

// The coherence guarantee that motivates the store: fronting a shared remote with a TtlStore-wrapped
// LruStore (via HierarchicalStore + populateOnMiss) masks a cross-instance write for at most `ttlMs`,
// then falls through to the remote and picks up the fresher value — instead of pinning it indefinitely
// like a plain LruStore.
describe("TtlStore fronting a shared remote (HierarchicalStore)", () => {
  const makeRemote = (): Store => {
    const m = new Map<string, Buffer[]>();
    return {
      get: (key) => m.get(key) ?? null,
      set: (key, value) => void m.set(key, value),
      delete: (key) => void m.delete(key),
      flush: () => {},
    };
  };

  it("masks a fresher remote write within ttlMs, then serves it after the cap", async () => {
    const remote = makeRemote();
    const store = new HierarchicalStore([new TtlStore(new LruStore({ maxBytes: 1024 }), { ttlMs: 1000 }), remote], {
      populateOnMiss: true,
    });

    // Instance reads a value from the remote → warms the in-memory tier.
    remote.set("k", bytes("v1"));
    expect(await store.get("k")).toEqual(bytes("v1"));

    // Another instance revalidates: the remote now holds v2, but this process still has v1 warm.
    remote.set("k", bytes("v2"));
    vi.advanceTimersByTime(999);
    expect(await store.get("k")).toEqual(bytes("v1")); // masked within the cap (expected)

    // Past the cap: the in-memory copy expires, the read falls through to the authoritative remote.
    vi.advanceTimersByTime(2);
    expect(await store.get("k")).toEqual(bytes("v2"));

    // The remote only ever saw the clean value — the internal stamp header never leaked out to it.
    expect(remote.get("k")).toEqual(bytes("v2"));
  });
});
