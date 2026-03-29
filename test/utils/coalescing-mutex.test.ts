import { describe, expect, it } from "vitest";

import { createCoalescingMutex } from "../../src/utils/coalescing-mutex.js";
import { sleep } from "../../src/utils/sleep.js";

describe("createCoalescing", () => {
  it("returns an object with coalesce", () => {
    const { coalesce } = createCoalescingMutex();
    expect(typeof coalesce).toBe("function");
  });
});

describe("coalesce", () => {
  it("executes handler and returns the leader result", async () => {
    const { coalesce } = createCoalescingMutex();

    const result = await coalesce("key", "args", async (args, collectFollowers) => {
      expect(collectFollowers()).toEqual([]);
      return { leader: { action: "resolve", result: `result:${args}` } };
    });

    expect(result).toBe("result:args");
  });

  it("propagates leader errors", async () => {
    const { coalesce } = createCoalescingMutex();

    await expect(
      coalesce("key", "args", async () => {
        throw new Error("handler failed");
      }),
    ).rejects.toThrow("handler failed");
  });

  it("cleans up after completion", async () => {
    const { coalesce } = createCoalescingMutex();

    await coalesce("key", "a", async (_args, collectFollowers) => {
      collectFollowers();
      return { leader: { action: "resolve", result: "first" } };
    });

    const result = await coalesce("key", "b", async (_args, collectFollowers) => {
      collectFollowers();
      return { leader: { action: "resolve", result: "second" } };
    });

    expect(result).toBe("second");
  });

  it("cleans up after error", async () => {
    const { coalesce } = createCoalescingMutex();

    await expect(
      coalesce("key", "a", async () => {
        throw new Error("fail");
      }),
    ).rejects.toThrow("fail");

    const result = await coalesce("key", "b", async (_args, collectFollowers) => {
      collectFollowers();
      return { leader: { action: "resolve", result: "recovered" } };
    });

    expect(result).toBe("recovered");
  });

  describe("key isolation", () => {
    it("allows concurrent leaders on different keys", async () => {
      const { coalesce } = createCoalescingMutex();
      const order: string[] = [];

      const p1 = coalesce("keyA", "a", async (_args, collectFollowers) => {
        order.push("A-start");
        await sleep(50);
        collectFollowers();
        order.push("A-end");
        return { leader: { action: "resolve", result: "A" } };
      });

      const p2 = coalesce("keyB", "b", async (_args, collectFollowers) => {
        order.push("B-start");
        await sleep(20);
        collectFollowers();
        order.push("B-end");
        return { leader: { action: "resolve", result: "B" } };
      });

      const [r1, r2] = await Promise.all([p1, p2]);

      expect(r1).toBe("A");
      expect(r2).toBe("B");
      expect(order).toEqual(["A-start", "B-start", "B-end", "A-end"]);
    });
  });

  describe("follower outcomes", () => {
    it("resolves followers individually by slot", async () => {
      const { coalesce } = createCoalescingMutex();

      const p1 = coalesce("key", 0, async (_args, collectFollowers) => {
        await sleep(30);
        const followers = collectFollowers();
        return {
          leader: { action: "resolve", result: 1 },
          followers: followers.map((f) => ({
            slot: f.slot,
            action: "resolve" as const,
            result: f.args * 10,
          })),
        };
      });

      const p2 = coalesce("key", 2, async () => {
        throw new Error("should not run");
      });
      const p3 = coalesce("key", 3, async () => {
        throw new Error("should not run");
      });

      const [r1, r2, r3] = await Promise.all([p1, p2, p3]);

      expect(r1).toBe(1);
      expect(r2).toBe(20);
      expect(r3).toBe(30);
    });

    it("rejects followers individually by slot", async () => {
      const { coalesce } = createCoalescingMutex();

      const p1 = coalesce("key", "leader", async (_args, collectFollowers) => {
        await sleep(20);
        const followers = collectFollowers();
        return {
          leader: { action: "resolve", result: "leader" },
          followers: followers.map((f) => ({
            slot: f.slot,
            action: "reject" as const,
            error: new Error(`reject:${f.args}`),
          })),
        };
      });

      const p2 = coalesce("key", "a", async () => {
        throw new Error("should not run");
      });
      const p3 = coalesce("key", "b", async () => {
        throw new Error("should not run");
      });

      const results = await Promise.allSettled([p1, p2, p3]);

      expect(results[0]).toEqual({ status: "fulfilled", value: "leader" });
      expect(results[1]).toMatchObject({ status: "rejected" });
      expect((results[1] as PromiseRejectedResult).reason.message).toBe("reject:a");
      expect(results[2]).toMatchObject({ status: "rejected" });
      expect((results[2] as PromiseRejectedResult).reason.message).toBe("reject:b");
    });

    it("passes follower args and slots through to the leader", async () => {
      const { coalesce } = createCoalescingMutex();

      const p1 = coalesce("key", { id: "leader", value: 1 }, async (_args, collectFollowers) => {
        await sleep(20);
        const followers = collectFollowers();

        expect(followers).toEqual([{ slot: 0, args: { id: "f1", value: 42 } }]);

        return {
          leader: { action: "resolve", result: "leader" },
          followers: followers.map((f) => ({
            slot: f.slot,
            action: "resolve" as const,
            result: `follower:${f.args.id}:${f.args.value}`,
          })),
        };
      });

      const p2 = coalesce("key", { id: "f1", value: 42 }, async () => {
        throw new Error("should not run");
      });

      const [r1, r2] = await Promise.all([p1, p2]);

      expect(r1).toBe("leader");
      expect(r2).toBe("follower:f1:42");
    });

    it("returns an empty array when no followers arrived", async () => {
      const { coalesce } = createCoalescingMutex();

      const result = await coalesce("key", "solo", async (_args, collectFollowers) => {
        expect(collectFollowers()).toEqual([]);
        return { leader: { action: "resolve", result: "alone" } };
      });

      expect(result).toBe("alone");
    });
  });

  describe("implicit deferral", () => {
    it("omitted follower slots are deferred to the next leader", async () => {
      const { coalesce } = createCoalescingMutex();
      const order: string[] = [];

      const p1 = coalesce("key", "A", async (_args, collectFollowers) => {
        order.push("L1-start");
        await sleep(30);
        const followers = collectFollowers();
        order.push(`L1-collected:${followers.length}`);

        return {
          leader: { action: "resolve", result: "L1" },
          followers: [{ slot: followers[0]!.slot, action: "resolve", result: "from-L1" }],
        };
      });

      const p2 = coalesce("key", "B", async () => {
        throw new Error("should not run");
      });

      const p3 = coalesce("key", "C", async (_args, collectFollowers) => {
        order.push("L2-start");
        expect(collectFollowers()).toEqual([]);
        order.push("L2-end");
        return { leader: { action: "resolve", result: "L2-ran-as-leader" } };
      });

      const [r1, r2, r3] = await Promise.all([p1, p2, p3]);

      expect(r1).toBe("L1");
      expect(r2).toBe("from-L1");
      expect(r3).toBe("L2-ran-as-leader");
      expect(order).toEqual(["L1-start", "L1-collected:2", "L2-start", "L2-end"]);
    });

    it("deferred followers preserve FIFO order", async () => {
      const { coalesce } = createCoalescingMutex();
      const leaderOrder: number[] = [];

      const handler = async (args: number, collectFollowers: () => { slot: number; args: number }[]) => {
        leaderOrder.push(args);
        await sleep(20);
        collectFollowers();
        return { leader: { action: "resolve" as const, result: args } };
      };

      const p1 = coalesce("key", 1, handler);
      const p2 = coalesce("key", 2, handler);
      const p3 = coalesce("key", 3, handler);

      await Promise.all([p1, p2, p3]);

      expect(leaderOrder).toEqual([1, 2, 3]);
    });

    it("deferred followers stay ahead of late arrivals", async () => {
      const { coalesce } = createCoalescingMutex();
      const order: string[] = [];

      const p1 = coalesce("key", "A", async (_args, collectFollowers) => {
        order.push("L1-start");
        await sleep(20);
        collectFollowers();
        order.push("L1-collected");
        await sleep(30);
        order.push("L1-end");
        return { leader: { action: "resolve", result: "L1" } };
      });

      await sleep(5);
      const p2 = coalesce("key", "B", async (_args, collectFollowers) => {
        order.push("L2-start");
        collectFollowers();
        order.push("L2-end");
        return { leader: { action: "resolve", result: "L2" } };
      });

      await sleep(25);
      const p3 = coalesce("key", "C", async (_args, collectFollowers) => {
        order.push("L3-start");
        collectFollowers();
        order.push("L3-end");
        return { leader: { action: "resolve", result: "L3" } };
      });

      const [r1, r2, r3] = await Promise.all([p1, p2, p3]);

      expect(r1).toBe("L1");
      expect(r2).toBe("L2");
      expect(r3).toBe("L3");
      expect(order).toEqual(["L1-start", "L1-collected", "L1-end", "L2-start", "L2-end", "L3-start", "L3-end"]);
    });
  });

  describe("collectFollowers", () => {
    it("may be called at most once", async () => {
      const { coalesce } = createCoalescingMutex();

      const p1 = coalesce("key", "A", async (_args, collectFollowers) => {
        await sleep(20);
        const followers = collectFollowers();
        expect(followers).toHaveLength(1);
        collectFollowers();
        return { leader: { action: "resolve", result: "unreachable" } };
      });

      const p2 = coalesce("key", "B", async (_args, collectFollowers) => {
        expect(collectFollowers()).toEqual([]);
        return { leader: { action: "resolve", result: "B-ran-as-leader" } };
      });

      const results = await Promise.allSettled([p1, p2]);

      expect(results[0]).toMatchObject({ status: "rejected" });
      expect((results[0] as PromiseRejectedResult).reason.message).toBe(
        "[coalescing] collectFollowers() called more than once",
      );
      expect(results[1]).toEqual({ status: "fulfilled", value: "B-ran-as-leader" });
    });
  });

  describe("error handling", () => {
    it("leader throw defers all collected followers", async () => {
      const { coalesce } = createCoalescingMutex();
      const order: string[] = [];

      const p1 = coalesce("key", "A", async (_args, collectFollowers) => {
        order.push("L1-start");
        await sleep(20);
        collectFollowers();
        throw new Error("L1 failed");
      });

      const p2 = coalesce("key", "B", async (_args, collectFollowers) => {
        order.push("L2-start");
        expect(collectFollowers()).toEqual([]);
        return { leader: { action: "resolve", result: "L2-ok" } };
      });

      const results = await Promise.allSettled([p1, p2]);

      expect(results[0]).toMatchObject({ status: "rejected" });
      expect((results[0] as PromiseRejectedResult).reason.message).toBe("L1 failed");
      expect(results[1]).toEqual({ status: "fulfilled", value: "L2-ok" });
      expect(order).toEqual(["L1-start", "L2-start"]);
    });

    it("followers deferred by leader throw can later be resolved", async () => {
      const { coalesce } = createCoalescingMutex();
      let attempt = 0;

      const handler = async (args: number, collectFollowers: () => { slot: number; args: number }[]) => {
        attempt++;
        await sleep(20);
        const followers = collectFollowers();

        if (attempt === 1) {
          throw new Error("first attempt fails");
        }

        return {
          leader: { action: "resolve" as const, result: args },
          followers: followers.map((f) => ({
            slot: f.slot,
            action: "resolve" as const,
            result: f.args * 10,
          })),
        };
      };

      const results = await Promise.allSettled([coalesce("key", 1, handler), coalesce("key", 2, handler)]);

      expect(results[0]).toMatchObject({ status: "rejected" });
      expect(results[1]).toEqual({ status: "fulfilled", value: 2 });
    });

    it("leader graceful reject still honors follower outcomes", async () => {
      const { coalesce } = createCoalescingMutex();

      const p1 = coalesce("key", "A", async (_args, collectFollowers) => {
        await sleep(20);
        const followers = collectFollowers();
        return {
          leader: { action: "reject", error: new Error("leader cannot serve itself") },
          followers: followers.map((f) => ({
            slot: f.slot,
            action: "resolve" as const,
            result: `served:${f.args}`,
          })),
        };
      });

      const p2 = coalesce("key", "B", async () => {
        throw new Error("should not run");
      });
      const p3 = coalesce("key", "C", async () => {
        throw new Error("should not run");
      });

      const results = await Promise.allSettled([p1, p2, p3]);

      expect(results[0]).toMatchObject({ status: "rejected" });
      expect((results[0] as PromiseRejectedResult).reason.message).toBe("leader cannot serve itself");
      expect(results[1]).toEqual({ status: "fulfilled", value: "served:B" });
      expect(results[2]).toEqual({ status: "fulfilled", value: "served:C" });
    });

    it("leader graceful reject with mixed follower outcomes and deferrals", async () => {
      const { coalesce } = createCoalescingMutex();

      const p1 = coalesce("key", "A", async (_args, collectFollowers) => {
        await sleep(20);
        const followers = collectFollowers();
        // Resolve first follower, reject second, defer third
        return {
          leader: { action: "reject", error: new Error("leader failed gracefully") },
          followers: [
            { slot: followers[0]!.slot, action: "resolve", result: "resolved-B" },
            { slot: followers[1]!.slot, action: "reject", error: new Error("rejected-C") },
          ],
        };
      });

      const p2 = coalesce("key", "B", async () => {
        throw new Error("should not run");
      });
      const p3 = coalesce("key", "C", async () => {
        throw new Error("should not run");
      });
      const p4 = coalesce("key", "D", async (_args, collectFollowers) => {
        expect(collectFollowers()).toEqual([]);
        return { leader: { action: "resolve", result: "D-ran-as-leader" } };
      });

      const results = await Promise.allSettled([p1, p2, p3, p4]);

      expect(results[0]).toMatchObject({ status: "rejected" });
      expect((results[0] as PromiseRejectedResult).reason.message).toBe("leader failed gracefully");
      expect(results[1]).toEqual({ status: "fulfilled", value: "resolved-B" });
      expect(results[2]).toMatchObject({ status: "rejected" });
      expect((results[2] as PromiseRejectedResult).reason.message).toBe("rejected-C");
      // D was deferred (omitted from follower outcomes) and ran as leader
      expect(results[3]).toEqual({ status: "fulfilled", value: "D-ran-as-leader" });
    });
  });

  describe("validation", () => {
    it("rejects the leader for invalid follower slots and defers followers", async () => {
      const { coalesce } = createCoalescingMutex();

      const p1 = coalesce("key", "A", async (_args, collectFollowers) => {
        collectFollowers();
        return {
          leader: { action: "resolve", result: "unreachable" },
          followers: [{ slot: 1, action: "resolve", result: "bad-slot" }],
        };
      });

      const p2 = coalesce("key", "B", async (_args, collectFollowers) => {
        expect(collectFollowers()).toEqual([]);
        return { leader: { action: "resolve", result: "B-ran-as-leader" } };
      });

      const results = await Promise.allSettled([p1, p2]);

      expect(results[0]).toMatchObject({ status: "rejected" });
      expect((results[0] as PromiseRejectedResult).reason.message).toBe("[coalescing] invalid follower slot 1");
      expect(results[1]).toEqual({ status: "fulfilled", value: "B-ran-as-leader" });
    });

    it("rejects the leader for duplicate follower slots", async () => {
      const { coalesce } = createCoalescingMutex();

      const p1 = coalesce("key", "A", async (_args, collectFollowers) => {
        await sleep(20);
        const followers = collectFollowers();
        return {
          leader: { action: "resolve", result: "unreachable" },
          followers: [
            { slot: followers[0]!.slot, action: "resolve", result: "first" },
            { slot: followers[0]!.slot, action: "resolve", result: "second" },
          ],
        };
      });

      const p2 = coalesce("key", "B", async (_args, collectFollowers) => {
        expect(collectFollowers()).toEqual([]);
        return { leader: { action: "resolve", result: "B-ran-as-leader" } };
      });

      const results = await Promise.allSettled([p1, p2]);

      expect(results[0]).toMatchObject({ status: "rejected" });
      expect((results[0] as PromiseRejectedResult).reason.message).toBe("[coalescing] duplicate follower slot 0");
      expect(results[1]).toEqual({ status: "fulfilled", value: "B-ran-as-leader" });
    });
  });

  describe("mutual exclusion", () => {
    it("serializes leaders on the same key", async () => {
      const { coalesce } = createCoalescingMutex();
      const order: string[] = [];

      const handler = async (args: string, collectFollowers: () => { slot: number; args: string }[]) => {
        order.push(`${args}-start`);
        await sleep(20);
        collectFollowers();
        order.push(`${args}-end`);
        return { leader: { action: "resolve" as const, result: args } };
      };

      const p1 = coalesce("key", "A", handler);
      const p2 = coalesce("key", "B", handler);
      const p3 = coalesce("key", "C", handler);

      await Promise.all([p1, p2, p3]);

      expect(order).toEqual(["A-start", "A-end", "B-start", "B-end", "C-start", "C-end"]);
    });

    it("allows concurrent leaders on different keys", async () => {
      const { coalesce } = createCoalescingMutex();
      const order: string[] = [];

      const p1 = coalesce("x", "A", async (args, collectFollowers) => {
        order.push(`${args}-start`);
        await sleep(40);
        collectFollowers();
        order.push(`${args}-end`);
        return { leader: { action: "resolve", result: args } };
      });

      const p2 = coalesce("y", "B", async (args, collectFollowers) => {
        order.push(`${args}-start`);
        await sleep(10);
        collectFollowers();
        order.push(`${args}-end`);
        return { leader: { action: "resolve", result: args } };
      });

      await Promise.all([p1, p2]);

      expect(order).toEqual(["A-start", "B-start", "B-end", "A-end"]);
    });
  });

  describe("edge cases", () => {
    it("handles a single operation", async () => {
      const { coalesce } = createCoalescingMutex();

      const result = await coalesce("key", "x", async (_args, collectFollowers) => {
        expect(collectFollowers()).toEqual([]);
        return { leader: { action: "resolve", result: 42 } };
      });

      expect(result).toBe(42);
    });

    it("handles rapid sequential operations", async () => {
      const { coalesce } = createCoalescingMutex();
      const results: number[] = [];

      for (let i = 1; i <= 5; i++) {
        const result = await coalesce("key", i, async (args, collectFollowers) => {
          collectFollowers();
          return { leader: { action: "resolve", result: args * 2 } };
        });
        results.push(result);
      }

      expect(results).toEqual([2, 4, 6, 8, 10]);
    });

    it("handler that never calls collectFollowers leaves followers queued", async () => {
      const { coalesce } = createCoalescingMutex();
      const order: string[] = [];

      const p1 = coalesce("key", "A", async () => {
        order.push("L1");
        await sleep(20);
        return { leader: { action: "resolve", result: "L1" } };
      });

      const p2 = coalesce("key", "B", async (_args, collectFollowers) => {
        order.push("L2");
        expect(collectFollowers()).toEqual([]);
        return { leader: { action: "resolve", result: "L2" } };
      });

      const [r1, r2] = await Promise.all([p1, p2]);

      expect(r1).toBe("L1");
      expect(r2).toBe("L2");
      expect(order).toEqual(["L1", "L2"]);
    });

    it("resolves followers with undefined", async () => {
      const { coalesce } = createCoalescingMutex();

      const p1 = coalesce("key", "leader", async (_args, collectFollowers) => {
        await sleep(10);
        const followers = collectFollowers();
        return {
          leader: { action: "resolve", result: undefined },
          followers: followers.map((f) => ({ slot: f.slot, action: "resolve" as const, result: undefined })),
        };
      });

      const p2 = coalesce("key", "follower", async () => {
        throw new Error("should not run");
      });

      const [r1, r2] = await Promise.all([p1, p2]);

      expect(r1).toBeUndefined();
      expect(r2).toBeUndefined();
    });
  });
});
