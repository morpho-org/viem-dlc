import { describe, expect, it } from "vitest";

import { HierarchicalStore, MemoryStore } from "../../src/stores/index.js";
import type { Store } from "../../src/types.js";

describe("HierarchicalStore", () => {
  it("returns null when all stores miss", async () => {
    const store = new HierarchicalStore([new MemoryStore(), new MemoryStore()]);
    expect(await store.get("missing")).toBeNull();
  });

  it("returns value from first store that has it", async () => {
    const first = new MemoryStore();
    const second = new MemoryStore();
    await second.set("key", "from-second");

    const store = new HierarchicalStore([first, second]);
    expect(await store.get("key")).toBe("from-second");
  });

  it("prioritizes earlier stores", async () => {
    const first = new MemoryStore();
    const second = new MemoryStore();
    await first.set("key", "from-first");
    await second.set("key", "from-second");

    const store = new HierarchicalStore([first, second]);
    expect(await store.get("key")).toBe("from-first");
  });

  it("writes to all stores", async () => {
    const first = new MemoryStore();
    const second = new MemoryStore();
    const store = new HierarchicalStore([first, second]);

    await store.set("key", "value");

    expect(await first.get("key")).toBe("value");
    expect(await second.get("key")).toBe("value");
  });

  it("deletes from all stores", async () => {
    const first = new MemoryStore();
    const second = new MemoryStore();
    await first.set("key", "value");
    await second.set("key", "value");

    const store = new HierarchicalStore([first, second]);
    await store.delete("key");

    expect(await first.get("key")).toBeNull();
    expect(await second.get("key")).toBeNull();
  });

  it("surfaces child contract violations during writes", async () => {
    const failing: Store = {
      get: async () => null,
      set: async () => {
        throw new Error("write failed");
      },
      delete: async () => {
        throw new Error("delete failed");
      },
      flush: async () => {},
    };
    const working = new MemoryStore();

    const store = new HierarchicalStore([failing, working]);
    await expect(store.set("key", "value")).rejects.toThrow("write failed");
  });

  it("flushes all child stores", async () => {
    let resolveFlush: () => void = () => {};
    const flushGate = new Promise<void>((resolve) => {
      resolveFlush = resolve;
    });

    const first: Store = {
      get: async () => null,
      set: async () => {},
      delete: async () => {},
      flush: async () => {
        await flushGate;
      },
    };
    const second = new MemoryStore();

    const store = new HierarchicalStore([first, second]);
    const flushPromise = store.flush();

    let completed = false;
    void flushPromise.then(() => {
      completed = true;
    });

    await Promise.resolve();
    expect(completed).toBe(false);

    resolveFlush();
    await flushPromise;
    expect(completed).toBe(true);
  });

  it("handles empty store list", async () => {
    const store = new HierarchicalStore([]);
    expect(await store.get("key")).toBeNull();
    await store.set("key", "value"); // Should not throw
    await store.delete("key"); // Should not throw
    await store.flush(); // Should not throw
  });
});
