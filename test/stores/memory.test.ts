import { describe, expect, it } from "vitest";

import { MemoryStore } from "../../src/stores/index.js";

describe("MemoryStore", () => {
  it("returns null for missing keys", async () => {
    const store = new MemoryStore();
    expect(await store.get("missing")).toBeNull();
  });

  it("stores and retrieves values", async () => {
    const store = new MemoryStore();
    await store.set("key", [Buffer.from("value")]);
    expect(await store.get("key")).toEqual([Buffer.from("value")]);
  });

  it("overwrites existing values", async () => {
    const store = new MemoryStore();
    await store.set("key", [Buffer.from("first")]);
    await store.set("key", [Buffer.from("second")]);
    expect(await store.get("key")).toEqual([Buffer.from("second")]);
  });

  it("deletes values", async () => {
    const store = new MemoryStore();
    await store.set("key", [Buffer.from("value")]);
    await store.delete("key");
    expect(await store.get("key")).toBeNull();
  });

  it("handles empty string values", async () => {
    const store = new MemoryStore();
    await store.set("key", [Buffer.from("")]);
    expect(await store.get("key")).toEqual([Buffer.from("")]);
  });

  it("isolates keys from each other", async () => {
    const store = new MemoryStore();
    await store.set("a", [Buffer.from("1")]);
    await store.set("b", [Buffer.from("2")]);
    expect(await store.get("a")).toEqual([Buffer.from("1")]);
    expect(await store.get("b")).toEqual([Buffer.from("2")]);
    await store.delete("a");
    expect(await store.get("a")).toBeNull();
    expect(await store.get("b")).toEqual([Buffer.from("2")]);
  });
});
