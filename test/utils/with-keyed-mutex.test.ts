import { describe, expect, it } from "vitest";

import { sleep } from "../../src/utils/sleep.js";
import { createKeyedMutex, withKeyedMutex } from "../../src/utils/with-keyed-mutex.js";

describe("createKeyedMutex", () => {
  it("creates an empty mutex map", () => {
    const mutex = createKeyedMutex();
    expect(mutex.size).toBe(0);
  });
});

describe("withKeyedMutex", () => {
  it("executes function and returns result", async () => {
    const mutex = createKeyedMutex();
    const result = await withKeyedMutex("key", () => Promise.resolve("hello"), { mutex });
    expect(result).toBe("hello");
  });

  it("propagates errors from the wrapped function", async () => {
    const mutex = createKeyedMutex();
    const error = new Error("test error");

    await expect(withKeyedMutex("key", () => Promise.reject(error), { mutex })).rejects.toThrow("test error");
  });

  it("cleans up lock after completion", async () => {
    const mutex = createKeyedMutex();

    await withKeyedMutex("key", () => Promise.resolve("done"), { mutex });

    expect(mutex.size).toBe(0);
  });

  it("cleans up lock after error", async () => {
    const mutex = createKeyedMutex();

    await expect(withKeyedMutex("key", () => Promise.reject(new Error("fail")), { mutex })).rejects.toThrow();

    expect(mutex.size).toBe(0);
  });

  describe("key isolation", () => {
    it("allows concurrent operations on different keys", async () => {
      const mutex = createKeyedMutex();
      const order: string[] = [];

      const promiseA = withKeyedMutex(
        "keyA",
        async () => {
          order.push("A-start");
          await sleep(50);
          order.push("A-end");
          return "A";
        },
        { mutex },
      );

      const promiseB = withKeyedMutex(
        "keyB",
        async () => {
          order.push("B-start");
          await sleep(20);
          order.push("B-end");
          return "B";
        },
        { mutex },
      );

      const [resultA, resultB] = await Promise.all([promiseA, promiseB]);

      expect(resultA).toBe("A");
      expect(resultB).toBe("B");

      // Both should start immediately (concurrent)
      expect(order[0]).toBe("A-start");
      expect(order[1]).toBe("B-start");

      // B should finish before A (shorter delay)
      expect(order[2]).toBe("B-end");
      expect(order[3]).toBe("A-end");
    });
  });

  describe("serialization", () => {
    it("serializes concurrent operations on the same key", async () => {
      const mutex = createKeyedMutex();
      const order: number[] = [];

      const promise1 = withKeyedMutex(
        "same-key",
        async () => {
          order.push(1);
          await sleep(30);
          order.push(11);
          return 1;
        },
        { mutex },
      );

      const promise2 = withKeyedMutex(
        "same-key",
        async () => {
          order.push(2);
          await sleep(10);
          order.push(22);
          return 2;
        },
        { mutex },
      );

      const promise3 = withKeyedMutex(
        "same-key",
        async () => {
          order.push(3);
          order.push(33);
          return 3;
        },
        { mutex },
      );

      const results = await Promise.all([promise1, promise2, promise3]);

      expect(results).toEqual([1, 2, 3]);

      // Operations should be strictly serialized (FIFO)
      expect(order).toEqual([1, 11, 2, 22, 3, 33]);
    });

    it("maintains FIFO order for queued operations", async () => {
      const mutex = createKeyedMutex();
      const order: number[] = [];

      // Queue up 5 operations on the same key
      const promises = [1, 2, 3, 4, 5].map((n) =>
        withKeyedMutex(
          "key",
          async () => {
            order.push(n);
            return n;
          },
          { mutex },
        ),
      );

      const results = await Promise.all(promises);

      expect(results).toEqual([1, 2, 3, 4, 5]);
      expect(order).toEqual([1, 2, 3, 4, 5]);
    });
  });

  describe("error handling", () => {
    it("releases lock and allows subsequent operations after error", async () => {
      const mutex = createKeyedMutex();

      // First operation fails
      await expect(withKeyedMutex("key", () => Promise.reject(new Error("first fails")), { mutex })).rejects.toThrow(
        "first fails",
      );

      // Second operation should succeed
      const result = await withKeyedMutex("key", () => Promise.resolve("success"), { mutex });
      expect(result).toBe("success");
    });

    it("serializes operations even when some fail", async () => {
      const mutex = createKeyedMutex();
      const order: string[] = [];

      const promise1 = withKeyedMutex(
        "key",
        async () => {
          order.push("op1-start");
          order.push("op1-end");
          return "ok1";
        },
        { mutex },
      );

      const promise2 = withKeyedMutex(
        "key",
        async () => {
          order.push("op2-start");
          throw new Error("op2 fails");
        },
        { mutex },
      );

      const promise3 = withKeyedMutex(
        "key",
        async () => {
          order.push("op3-start");
          order.push("op3-end");
          return "ok3";
        },
        { mutex },
      );

      const results = await Promise.allSettled([promise1, promise2, promise3]);

      expect(results[0]).toEqual({ status: "fulfilled", value: "ok1" });
      expect(results[1]).toMatchObject({ status: "rejected" });
      expect(results[2]).toEqual({ status: "fulfilled", value: "ok3" });

      // All operations should be serialized
      expect(order).toEqual(["op1-start", "op1-end", "op2-start", "op3-start", "op3-end"]);
    });
  });

  describe("edge cases", () => {
    it("handles single operation", async () => {
      const mutex = createKeyedMutex();
      const result = await withKeyedMutex("key", () => Promise.resolve(42), { mutex });
      expect(result).toBe(42);
      expect(mutex.size).toBe(0);
    });

    it("handles rapid sequential operations", async () => {
      const mutex = createKeyedMutex();
      const results: number[] = [];

      for (let i = 1; i <= 5; i++) {
        const result = await withKeyedMutex("key", () => Promise.resolve(i), { mutex });
        results.push(result);
      }

      expect(results).toEqual([1, 2, 3, 4, 5]);
      expect(mutex.size).toBe(0);
    });

    it("handles mixed concurrent and sequential operations across keys", async () => {
      const mutex = createKeyedMutex();
      const log: string[] = [];

      // Start operations on keyA
      const a1 = withKeyedMutex(
        "keyA",
        async () => {
          log.push("A1");
          await sleep(20);
          return "A1";
        },
        { mutex },
      );
      const a2 = withKeyedMutex(
        "keyA",
        async () => {
          log.push("A2");
          return "A2";
        },
        { mutex },
      );

      // Start operations on keyB (should run concurrently with keyA)
      const b1 = withKeyedMutex(
        "keyB",
        async () => {
          log.push("B1");
          return "B1";
        },
        { mutex },
      );

      const results = await Promise.all([a1, a2, b1]);

      expect(results).toEqual(["A1", "A2", "B1"]);

      // A1 starts first, then B1 can start immediately (different key)
      // A2 must wait for A1 to complete
      expect(log.indexOf("A1")).toBeLessThan(log.indexOf("A2"));
      expect(mutex.size).toBe(0);
    });
  });
});
