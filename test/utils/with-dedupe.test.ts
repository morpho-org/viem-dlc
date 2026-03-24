import { describe, expect, it } from "vitest";

import { withDedupe } from "../../src/utils/with-dedupe.js";

describe("withDedupe", () => {
  it("executes function and returns result", async () => {
    const result = await withDedupe(() => Promise.resolve("hello"), { id: "a" });
    expect(result).toBe("hello");
  });

  it("propagates errors from the wrapped function", async () => {
    await expect(withDedupe(() => Promise.reject(new Error("boom")), { id: "a" })).rejects.toThrow("boom");
  });

  describe("deduplication", () => {
    it("returns the same promise for concurrent calls with the same id", async () => {
      let callCount = 0;
      const fn = () =>
        new Promise<number>((resolve) => {
          callCount++;
          setTimeout(() => resolve(42), 30);
        });

      const p1 = withDedupe(fn, { id: "same" });
      const p2 = withDedupe(fn, { id: "same" });

      const [r1, r2] = await Promise.all([p1, p2]);

      expect(r1).toBe(42);
      expect(r2).toBe(42);
      expect(callCount).toBe(1);
    });

    it("allows new calls after the first promise settles", async () => {
      let callCount = 0;
      const fn = () => {
        callCount++;
        return Promise.resolve(callCount);
      };

      const r1 = await withDedupe(fn, { id: "seq" });
      const r2 = await withDedupe(fn, { id: "seq" });

      expect(r1).toBe(1);
      expect(r2).toBe(2);
      expect(callCount).toBe(2);
    });

    it("cleans up cache after rejection", async () => {
      let callCount = 0;
      const fn = () => {
        callCount++;
        if (callCount === 1) return Promise.reject(new Error("first fails"));
        return Promise.resolve("ok");
      };

      await expect(withDedupe(fn, { id: "err" })).rejects.toThrow("first fails");

      const result = await withDedupe(fn, { id: "err" });
      expect(result).toBe("ok");
      expect(callCount).toBe(2);
    });

    it("uses separate slots for different ids", async () => {
      let callCount = 0;
      const fn = () =>
        new Promise<number>((resolve) => {
          callCount++;
          setTimeout(() => resolve(callCount), 30);
        });

      const p1 = withDedupe(fn, { id: "x" });
      const p2 = withDedupe(fn, { id: "y" });

      await Promise.all([p1, p2]);

      expect(callCount).toBe(2);
    });
  });

  describe("enabled flag", () => {
    it("skips deduplication when enabled is false", async () => {
      let callCount = 0;
      const fn = () =>
        new Promise<number>((resolve) => {
          callCount++;
          setTimeout(() => resolve(callCount), 30);
        });

      const p1 = withDedupe(fn, { enabled: false, id: "dis" });
      const p2 = withDedupe(fn, { enabled: false, id: "dis" });

      await Promise.all([p1, p2]);

      expect(callCount).toBe(2);
    });
  });

  describe("missing id", () => {
    it("always calls fn when id is omitted", async () => {
      let callCount = 0;
      const fn = () =>
        new Promise<number>((resolve) => {
          callCount++;
          setTimeout(() => resolve(callCount), 30);
        });

      const p1 = withDedupe(fn, {});
      const p2 = withDedupe(fn, {});

      await Promise.all([p1, p2]);

      expect(callCount).toBe(2);
    });
  });
});
