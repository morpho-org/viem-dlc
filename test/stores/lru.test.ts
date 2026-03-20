import { describe, expect, it, vi } from "vitest";

import { LruStore } from "../../src/stores/index.js";

describe("LruStore", () => {
  it("throws if maxBytes is less than 1", () => {
    expect(() => new LruStore(0)).toThrow("[LruStore] maxBytes must be at least 1");
    expect(() => new LruStore(-1)).toThrow("[LruStore] maxBytes must be at least 1");
  });

  it("returns null for missing keys", async () => {
    const store = new LruStore(1000);
    expect(await store.get("missing")).toBeNull();
  });

  it("stores and retrieves values", async () => {
    const store = new LruStore(1000);
    await store.set("key", [Buffer.from("value")]);
    expect(await store.get("key")).toEqual([Buffer.from("value")]);
  });

  it("overwrites existing values", async () => {
    const store = new LruStore(1000);
    await store.set("key", [Buffer.from("first")]);
    await store.set("key", [Buffer.from("second")]);
    expect(await store.get("key")).toEqual([Buffer.from("second")]);
  });

  it("deletes values", async () => {
    const store = new LruStore(1000);
    await store.set("key", [Buffer.from("value")]);
    await store.delete("key");
    expect(await store.get("key")).toBeNull();
  });

  it("handles empty string values", async () => {
    const store = new LruStore(1000);
    await store.set("key", [Buffer.from("")]);
    expect(await store.get("key")).toEqual([Buffer.from("")]);
  });

  it("isolates keys from each other", async () => {
    const store = new LruStore(1000);
    await store.set("a", [Buffer.from("1")]);
    await store.set("b", [Buffer.from("2")]);
    expect(await store.get("a")).toEqual([Buffer.from("1")]);
    expect(await store.get("b")).toEqual([Buffer.from("2")]);
    await store.delete("a");
    expect(await store.get("a")).toBeNull();
    expect(await store.get("b")).toEqual([Buffer.from("2")]);
  });

  it("evicts oldest entries when byte limit is exceeded", async () => {
    // Only value bytes are counted (keys assumed negligible)
    const store = new LruStore(2);
    await store.set("a", [Buffer.from("1")]); // 1 byte
    await store.set("b", [Buffer.from("2")]); // 1 byte, total 2
    await store.set("c", [Buffer.from("3")]); // 1 byte, would be 3, evicts 'a', total 2

    expect(await store.get("a")).toBeNull();
    expect(await store.get("b")).toEqual([Buffer.from("2")]);
    expect(await store.get("c")).toEqual([Buffer.from("3")]);
  });

  it("updates access order on get", async () => {
    const store = new LruStore(2);
    await store.set("a", [Buffer.from("1")]); // 1 byte
    await store.set("b", [Buffer.from("2")]); // 1 byte, total 2

    // Access 'a' to make it most recently used
    await store.get("a");

    // Now 'b' is oldest, so it should be evicted
    await store.set("c", [Buffer.from("3")]); // 1 byte, evicts 'b'

    expect(await store.get("a")).toEqual([Buffer.from("1")]);
    expect(await store.get("b")).toBeNull();
    expect(await store.get("c")).toEqual([Buffer.from("3")]);
  });

  it("updates access order on set of existing key", async () => {
    const store = new LruStore(2);
    await store.set("a", [Buffer.from("1")]); // 1 byte
    await store.set("b", [Buffer.from("2")]); // 1 byte, total 2

    // Update 'a' to make it most recently used
    await store.set("a", [Buffer.from("x")]);

    // Now 'b' is oldest, so it should be evicted
    await store.set("c", [Buffer.from("3")]); // 1 byte, evicts 'b'

    expect(await store.get("a")).toEqual([Buffer.from("x")]);
    expect(await store.get("b")).toBeNull();
    expect(await store.get("c")).toEqual([Buffer.from("3")]);
  });

  it("evicts multiple entries if needed for a large value", async () => {
    const store = new LruStore(10);
    await store.set("a", [Buffer.from("11")]); // 2 bytes
    await store.set("b", [Buffer.from("22")]); // 2 bytes
    await store.set("c", [Buffer.from("33")]); // 2 bytes, total 6

    // New value is 8 bytes, total would be 14, must evict 'a' and 'b' to fit (need <= 2 bytes)
    await store.set("x", [Buffer.from("12345678")]); // 8 bytes

    expect(await store.get("a")).toBeNull();
    expect(await store.get("b")).toBeNull();
    expect(await store.get("c")).toEqual([Buffer.from("33")]);
    expect(await store.get("x")).toEqual([Buffer.from("12345678")]);
  });

  it("handles deleting non-existent keys", async () => {
    const store = new LruStore(1000);
    await store.delete("nonexistent");
    expect(await store.get("nonexistent")).toBeNull();
  });

  it("correctly tracks bytes when updating with different sized values", async () => {
    const store = new LruStore(10);
    await store.set("a", [Buffer.from("123456789")]); // 9 bytes
    await store.set("a", [Buffer.from("1")]); // now 1 byte

    // Should have room for more entries now
    await store.set("b", [Buffer.from("12345678")]); // 8 bytes, total 9
    expect(await store.get("a")).toEqual([Buffer.from("1")]);
    expect(await store.get("b")).toEqual([Buffer.from("12345678")]);
  });

  it("warns and skips values that exceed maxBytes", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const store = new LruStore(5);

    await store.set("a", [Buffer.from("1")]); // 1 byte, fits
    await store.set("big", [Buffer.from("123456")]); // 6 bytes, exceeds 5

    expect(warnSpy).toHaveBeenCalledWith("[LruStore] Value exceeds maxBytes (6 > 5), skipping");
    expect(await store.get("a")).toEqual([Buffer.from("1")]); // original entry still there
    expect(await store.get("big")).toBeNull(); // oversized value was not stored

    warnSpy.mockRestore();
  });

  it("does not evict existing entries when new value exceeds maxBytes", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const store = new LruStore(5);

    await store.set("a", [Buffer.from("12")]); // 2 bytes
    await store.set("b", [Buffer.from("34")]); // 2 bytes, total 4
    await store.set("huge", [Buffer.from("123456")]); // 6 bytes, exceeds 5

    // Existing entries should not be evicted
    expect(await store.get("a")).toEqual([Buffer.from("12")]);
    expect(await store.get("b")).toEqual([Buffer.from("34")]);
    expect(await store.get("huge")).toBeNull();

    warnSpy.mockRestore();
  });

  it("evicts other entries when updating existing key with larger value", async () => {
    const store = new LruStore(10);
    await store.set("a", [Buffer.from("12345")]); // 5 bytes
    await store.set("b", [Buffer.from("12345")]); // 5 bytes, total 10

    // Update 'a' with larger value - should evict 'b' to make room
    await store.set("a", [Buffer.from("1234567890")]); // 10 bytes

    expect(await store.get("a")).toEqual([Buffer.from("1234567890")]);
    expect(await store.get("b")).toBeNull(); // evicted to make room
  });
});
