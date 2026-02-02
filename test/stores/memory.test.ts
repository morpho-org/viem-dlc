import { describe, expect, it } from "vitest";

import { MemoryStore } from "../../src/stores/index.js";

describe("MemoryStore", () => {
  it("returns null for missing keys", async () => {
    const store = new MemoryStore();
    expect(await store.get("missing")).toBeNull();
  });

  it("stores and retrieves values", async () => {
    const store = new MemoryStore();
    await store.set("key", "value");
    expect(await store.get("key")).toBe("value");
  });

  it("overwrites existing values", async () => {
    const store = new MemoryStore();
    await store.set("key", "first");
    await store.set("key", "second");
    expect(await store.get("key")).toBe("second");
  });

  it("deletes values", async () => {
    const store = new MemoryStore();
    await store.set("key", "value");
    await store.delete("key");
    expect(await store.get("key")).toBeNull();
  });

  it("handles empty string values", async () => {
    const store = new MemoryStore();
    await store.set("key", "");
    expect(await store.get("key")).toBe("");
  });

  it("isolates keys from each other", async () => {
    const store = new MemoryStore();
    await store.set("a", "1");
    await store.set("b", "2");
    expect(await store.get("a")).toBe("1");
    expect(await store.get("b")).toBe("2");
    await store.delete("a");
    expect(await store.get("a")).toBeNull();
    expect(await store.get("b")).toBe("2");
  });
});
