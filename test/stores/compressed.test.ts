import { describe, expect, it, vi } from "vitest";

import { CompressedStore } from "../../src/stores/compressed.js";
import { MemoryStore } from "../../src/stores/index.js";

describe("CompressedStore", () => {
  it("compresses and decompresses values transparently", async () => {
    const underlying = new MemoryStore();
    const store = new CompressedStore(underlying);

    await store.set("key", "hello world");
    expect(await store.get("key")).toBe("hello world");
  });

  it("stores compressed data in underlying store", async () => {
    const underlying = new MemoryStore();
    const store = new CompressedStore(underlying);

    const original = "hello world";
    await store.set("key", original);

    const compressed = await underlying.get("key");
    expect(compressed).not.toBeNull();
    expect(compressed).not.toBe(original);
    // Compressed data is base64 encoded
    expect(compressed).toMatch(/^[A-Za-z0-9+/]+=*$/);
  });

  it("returns null for missing keys", async () => {
    const underlying = new MemoryStore();
    const store = new CompressedStore(underlying);

    expect(await store.get("missing")).toBeNull();
  });

  it("handles empty string values", async () => {
    const underlying = new MemoryStore();
    const store = new CompressedStore(underlying);

    await store.set("key", "");
    expect(await store.get("key")).toBe("");
  });

  it("handles large values", async () => {
    const underlying = new MemoryStore();
    const store = new CompressedStore(underlying);

    const large = "x".repeat(100_000);
    await store.set("key", large);
    expect(await store.get("key")).toBe(large);

    // Verify compression actually reduced size
    const compressed = await underlying.get("key");
    expect(compressed!.length).toBeLessThan(large.length);
  });

  it("handles JSON values", async () => {
    const underlying = new MemoryStore();
    const store = new CompressedStore(underlying);

    const data = JSON.stringify({ foo: "bar", nums: [1, 2, 3] });
    await store.set("key", data);
    expect(await store.get("key")).toBe(data);
  });

  it("deletes from underlying store", async () => {
    const underlying = new MemoryStore();
    const store = new CompressedStore(underlying);

    await store.set("key", "value");
    await store.delete("key");

    expect(await underlying.get("key")).toBeNull();
    expect(await store.get("key")).toBeNull();
  });

  it("returns null for corrupted data", async () => {
    const underlying = new MemoryStore();
    const store = new CompressedStore(underlying);

    // Write invalid base64/gzip data directly
    await underlying.set("key", "not-valid-gzip-data");

    const consoleSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const result = await store.get("key");
    consoleSpy.mockRestore();

    expect(result).toBeNull();
  });

  it("logs warning for corrupted data", async () => {
    const underlying = new MemoryStore();
    const store = new CompressedStore(underlying);

    await underlying.set("key", "not-valid-gzip-data");

    const consoleSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    await store.get("key");

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("[CompressedStore]"), expect.anything());
    consoleSpy.mockRestore();
  });

  it("preserves unicode characters", async () => {
    const underlying = new MemoryStore();
    const compressed = new CompressedStore(underlying);

    const unicode = "你好世界 🌍 émojis";
    await compressed.set("key", unicode);
    expect(await compressed.get("key")).toBe(unicode);
  });

  it("can be composed with other stores", async () => {
    const underlying = new MemoryStore();
    const compressed = new CompressedStore(underlying);

    await compressed.set("key", "value");

    const raw = await underlying.get("key");
    expect(raw).not.toBe("value");
  });

  it("flush waits for fire-and-forget writes accepted before the barrier", async () => {
    const underlying = new MemoryStore();
    const compressed = new CompressedStore(underlying);

    const setPromise = compressed.set("key", "value");
    await compressed.flush();

    expect(await compressed.get("key")).toBe("value");
    await setPromise;
  });

  it("flush delegates to the underlying store", async () => {
    const underlying = new MemoryStore();
    let resolveFlush: () => void = () => {};
    const flushGate = new Promise<void>((resolve) => {
      resolveFlush = resolve;
    });

    underlying.flush = async () => {
      await flushGate;
    };

    const compressed = new CompressedStore(underlying);
    const flushPromise = compressed.flush();

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
});
