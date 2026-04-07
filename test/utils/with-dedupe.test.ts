import { describe, expect, it } from "vitest";

import { createDedupe } from "../../src/utils/with-dedupe.js";

describe("withDedupe", () => {
  it("executes function and returns result", async () => {
    const { withDedupe } = createDedupe();
    const result = await withDedupe(() => Promise.resolve("hello"), { key: "k" });
    expect(result).toBe("hello");
  });

  it("propagates errors from the wrapped function", async () => {
    const { withDedupe } = createDedupe();
    const error = new Error("boom");
    await expect(withDedupe(() => Promise.reject(error), { key: "k" })).rejects.toBe(error);
  });

  it("deduplicates concurrent calls with the same key", async () => {
    const { withDedupe } = createDedupe();
    let calls = 0;

    const fn = () => {
      calls++;
      return Promise.resolve(42);
    };

    const [a, b, c] = await Promise.all([
      withDedupe(fn, { key: "k" }),
      withDedupe(fn, { key: "k" }),
      withDedupe(fn, { key: "k" }),
    ]);

    expect(calls).toBe(1);
    expect(a).toBe(42);
    expect(b).toBe(42);
    expect(c).toBe(42);
  });

  it("runs independently for different keys", async () => {
    const { withDedupe } = createDedupe();
    let calls = 0;

    const fn = () => {
      calls++;
      return Promise.resolve("v");
    };

    await Promise.all([withDedupe(fn, { key: "a" }), withDedupe(fn, { key: "b" })]);

    expect(calls).toBe(2);
  });

  it("executes fresh after prior call resolves", async () => {
    const { withDedupe } = createDedupe();
    let calls = 0;

    const fn = () => {
      calls++;
      return Promise.resolve(calls);
    };

    const first = await withDedupe(fn, { key: "k" });
    const second = await withDedupe(fn, { key: "k" });

    expect(first).toBe(1);
    expect(second).toBe(2);
    expect(calls).toBe(2);
  });

  it("executes fresh after prior call rejects", async () => {
    const { withDedupe } = createDedupe();
    let attempt = 0;

    const fn = () => {
      attempt++;
      if (attempt === 1) return Promise.reject(new Error("fail"));
      return Promise.resolve("ok");
    };

    await expect(withDedupe(fn, { key: "k" })).rejects.toThrow("fail");
    const result = await withDedupe(fn, { key: "k" });
    expect(result).toBe("ok");
  });

  it("propagates rejection to all deduped callers", async () => {
    const { withDedupe } = createDedupe();
    const error = new Error("shared failure");

    let resolve!: () => void;
    const gate = new Promise<void>((r) => {
      resolve = r;
    });

    const fn = () => gate.then(() => Promise.reject(error));

    const p1 = withDedupe(fn, { key: "k" });
    const p2 = withDedupe(fn, { key: "k" });

    resolve();

    const [r1, r2] = await Promise.allSettled([p1, p2]);
    expect(r1).toMatchObject({ status: "rejected", reason: error });
    expect(r2).toMatchObject({ status: "rejected", reason: error });
  });

  it("separate createDedupe instances are independent", async () => {
    const d1 = createDedupe();
    const d2 = createDedupe();
    let calls = 0;

    const fn = () => {
      calls++;
      return Promise.resolve("v");
    };

    await Promise.all([d1.withDedupe(fn, { key: "k" }), d2.withDedupe(fn, { key: "k" })]);

    expect(calls).toBe(2);
  });
});
