import { describe, expect, it } from "vitest";

import { HierarchicalStore, MemoryStore } from "../../src/stores/index.js";
import type { Store } from "../../src/types.js";
import { sleep } from "../../src/utils/sleep.js";

describe("HierarchicalStore", () => {
  it("returns null when all stores miss", async () => {
    const store = new HierarchicalStore([new MemoryStore(), new MemoryStore()]);
    expect(await store.get("missing")).toBeNull();
  });

  it("returns value from first store that has it", async () => {
    const first = new MemoryStore();
    const second = new MemoryStore();
    await second.set("key", [Buffer.from("from-second")]);

    const store = new HierarchicalStore([first, second]);
    expect(await store.get("key")).toEqual([Buffer.from("from-second")]);
  });

  it("prioritizes earlier stores", async () => {
    const first = new MemoryStore();
    const second = new MemoryStore();
    await first.set("key", [Buffer.from("from-first")]);
    await second.set("key", [Buffer.from("from-second")]);

    const store = new HierarchicalStore([first, second]);
    expect(await store.get("key")).toEqual([Buffer.from("from-first")]);
  });

  it("writes to all stores", async () => {
    const first = new MemoryStore();
    const second = new MemoryStore();
    const store = new HierarchicalStore([first, second]);

    await store.set("key", [Buffer.from("value")]);

    expect(await first.get("key")).toEqual([Buffer.from("value")]);
    expect(await second.get("key")).toEqual([Buffer.from("value")]);
  });

  it("deletes from all stores", async () => {
    const first = new MemoryStore();
    const second = new MemoryStore();
    await first.set("key", [Buffer.from("value")]);
    await second.set("key", [Buffer.from("value")]);

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
    await expect(store.set("key", [Buffer.from("value")])).rejects.toThrow("write failed");
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

  describe("populateOnMiss", () => {
    it("warms higher tiers from a hit in a lower store", async () => {
      const top = new MemoryStore();
      const lower = new MemoryStore();
      lower.set("key", [Buffer.from("v")]);

      const store = new HierarchicalStore([top, lower], { populateOnMiss: true });

      expect(await store.get("key")).toEqual([Buffer.from("v")]);
      expect(top.get("key")).toEqual([Buffer.from("v")]);
    });

    it("does not backfill a value read before an overlapping write", async () => {
      const top = new MemoryStore();
      let resolveRead!: (value: Buffer[] | null) => void;
      const lower: Store = {
        get: () =>
          new Promise<Buffer[] | null>((r) => {
            resolveRead = r;
          }),
        set: () => {},
        delete: () => {},
        flush: () => {},
      };

      const store = new HierarchicalStore([top, lower], { populateOnMiss: true });

      const pending = store.get("key");
      await sleep(1); // let the read reach the slow lower tier
      await store.set("key", [Buffer.from("v2")]);
      resolveRead([Buffer.from("v1")]); // pre-write value, resolving after the write landed

      expect(await pending).toEqual([Buffer.from("v1")]);
      expect(top.get("key")).toEqual([Buffer.from("v2")]); // the newer write survives
    });
  });

  it("handles empty store list", async () => {
    const store = new HierarchicalStore([]);
    expect(await store.get("key")).toBeNull();
    await store.set("key", [Buffer.from("value")]); // Should not throw
    await store.delete("key"); // Should not throw
    await store.flush(); // Should not throw
  });
});
