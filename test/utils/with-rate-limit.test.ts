import { describe, expect, it } from "vitest";

import { sleep } from "../../src/utils/sleep.js";
import { createRateLimit, createTokenBucket } from "../../src/utils/with-rate-limit.js";

describe("createTokenBucket", () => {
  it("creates bucket with specified maxTokens and refillRate", () => {
    const bucket = createTokenBucket(5, 10);

    expect(bucket.maxTokens).toBe(5);
    expect(bucket.refillRate).toBe(10);
    expect(bucket.tokens).toBe(5); // starts full
  });

  it("starts with full tokens for burst capacity", () => {
    const bucket = createTokenBucket(10, 1);
    expect(bucket.tokens).toBe(10);
  });
});

describe("withRateLimit", () => {
  it("executes function and returns result", async () => {
    const { withRateLimit } = createRateLimit(5, 10, Infinity);
    const result = await withRateLimit(() => Promise.resolve("hello"), {});
    expect(result).toBe("hello");
  });

  it("propagates errors from the wrapped function", async () => {
    const { withRateLimit } = createRateLimit(5, 10, Infinity);
    const error = new Error("test error");

    await expect(withRateLimit(() => Promise.reject(error), {})).rejects.toThrow("test error");
  });

  describe("burst capacity", () => {
    it("allows burst of requests up to maxTokens without delay", async () => {
      const { withRateLimit } = createRateLimit(5, 10, Infinity);
      const start = Date.now();

      // Fire 5 requests concurrently - should all complete immediately
      const results = await Promise.all([
        withRateLimit(() => Promise.resolve(1), {}),
        withRateLimit(() => Promise.resolve(2), {}),
        withRateLimit(() => Promise.resolve(3), {}),
        withRateLimit(() => Promise.resolve(4), {}),
        withRateLimit(() => Promise.resolve(5), {}),
      ]);

      const elapsed = Date.now() - start;

      expect(results).toEqual([1, 2, 3, 4, 5]);
      expect(elapsed).toBeLessThan(50); // Should be nearly instant
    });
  });

  describe("FIFO ordering", () => {
    it("processes requests in the order they were received", async () => {
      const { withRateLimit } = createRateLimit(2, 100, Infinity); // 2 burst, fast refill
      const order: number[] = [];

      // Launch 6 requests concurrently
      // First 2 should resolve immediately (burst)
      // Rest should queue and resolve in FIFO order
      const promises = [1, 2, 3, 4, 5, 6].map((n) =>
        withRateLimit(async () => {
          order.push(n);
          return n;
        }, {}),
      );

      const results = await Promise.all(promises);

      // Results should match input order
      expect(results).toEqual([1, 2, 3, 4, 5, 6]);

      // Execution order should be FIFO
      expect(order).toEqual([1, 2, 3, 4, 5, 6]);
    });
  });

  describe("rate limiting", () => {
    it("delays requests when tokens exhausted", async () => {
      const { withRateLimit } = createRateLimit(1, 10, Infinity); // 1 burst, 10/sec = 100ms per token
      const start = Date.now();

      // First request uses the burst token
      await withRateLimit(() => Promise.resolve(1), {});

      // Second request must wait for refill
      await withRateLimit(() => Promise.resolve(2), {});

      const elapsed = Date.now() - start;

      // Should have waited ~100ms for token refill
      expect(elapsed).toBeGreaterThanOrEqual(80); // allow some timing slack
      expect(elapsed).toBeLessThan(200);
    });

    it("refills tokens over time", async () => {
      const { withRateLimit } = createRateLimit(2, 10, Infinity); // 2 max, 10/sec

      // Exhaust tokens
      await withRateLimit(() => Promise.resolve(1), {});
      await withRateLimit(() => Promise.resolve(2), {});

      // Wait 150ms - should refill ~1.5 tokens
      await sleep(150);

      // Trigger a refill by starting another request
      const start = Date.now();
      await withRateLimit(() => Promise.resolve(3), {});
      const elapsed = Date.now() - start;

      // Should have had a token available (or nearly so)
      expect(elapsed).toBeLessThan(50);
    });
  });

  describe("queue behavior", () => {
    it("drains queue as tokens become available", async () => {
      const { withRateLimit } = createRateLimit(1, 20, Infinity); // 1 burst, 20/sec = 50ms per token
      const timestamps: number[] = [];
      const start = Date.now();

      // Queue up 4 requests
      const promises = [1, 2, 3, 4].map((n) =>
        withRateLimit(async () => {
          timestamps.push(Date.now() - start);
          return n;
        }, {}),
      );

      await Promise.all(promises);

      // First should be immediate (~0ms)
      expect(timestamps[0]).toBeLessThan(30);

      // Each subsequent should be ~50ms apart
      for (let i = 1; i < timestamps.length; i++) {
        const gap = timestamps[i]! - timestamps[i - 1]!;
        expect(gap).toBeGreaterThanOrEqual(30); // Allow timing slack
        expect(gap).toBeLessThan(100);
      }
    });

    it("handles empty queue gracefully", async () => {
      const { withRateLimit } = createRateLimit(5, 10, Infinity);

      // Single request, no queue buildup
      const result = await withRateLimit(() => Promise.resolve("solo"), {});
      expect(result).toBe("solo");
    });
  });

  describe("edge cases", () => {
    it("handles zero-delay scenario (tokens available)", async () => {
      const { withRateLimit } = createRateLimit(10, 10, Infinity);

      const start = Date.now();
      await withRateLimit(() => Promise.resolve("fast"), {});
      const elapsed = Date.now() - start;

      expect(elapsed).toBeLessThan(20);
    });

    it("handles rapid sequential requests", async () => {
      const { withRateLimit } = createRateLimit(3, 100, Infinity);
      const results: number[] = [];

      for (let i = 1; i <= 5; i++) {
        const result = await withRateLimit(() => Promise.resolve(i), {});
        results.push(result);
      }

      expect(results).toEqual([1, 2, 3, 4, 5]);
    });

    it("handles concurrent requests exceeding burst capacity", async () => {
      const { withRateLimit } = createRateLimit(2, 50, Infinity); // 2 burst, fast refill

      // Launch 10 concurrent requests
      const promises = Array.from({ length: 10 }, (_, i) => withRateLimit(() => Promise.resolve(i + 1), {}));

      const results = await Promise.all(promises);

      // All should complete and return correct values
      expect(results).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    });
  });
});

describe("priority scheduling", () => {
  it("executes lower priority values before higher ones", async () => {
    // 10 burst tokens, fast refill, 1 concurrent — forces serialization
    const { withRateLimit } = createRateLimit(10, 1000, 1);
    const order: string[] = [];

    let resolveGate!: () => void;
    const gate = new Promise<void>((r) => {
      resolveGate = r;
    });

    // First job takes the only concurrency slot
    const p0 = withRateLimit(
      async () => {
        await gate;
        order.push("gate");
      },
      { priority: 0 },
    );
    // These queue while p0 is in-flight
    const p1 = withRateLimit(
      async () => {
        order.push("low");
      },
      { priority: 10 },
    );
    const p2 = withRateLimit(
      async () => {
        order.push("high");
      },
      { priority: 0 },
    );
    const p3 = withRateLimit(
      async () => {
        order.push("medium");
      },
      { priority: 5 },
    );

    resolveGate();
    await Promise.all([p0, p1, p2, p3]);

    expect(order).toEqual(["gate", "high", "medium", "low"]);
  });

  it("uses FIFO order among jobs with the same priority", async () => {
    const { withRateLimit } = createRateLimit(10, 1000, 1);
    const order: number[] = [];

    let resolveGate!: () => void;
    const gate = new Promise<void>((r) => {
      resolveGate = r;
    });

    const p0 = withRateLimit(
      async () => {
        await gate;
      },
      { priority: 0 },
    );
    // All same priority — should be FIFO
    const p1 = withRateLimit(
      async () => {
        order.push(1);
      },
      { priority: 5 },
    );
    const p2 = withRateLimit(
      async () => {
        order.push(2);
      },
      { priority: 5 },
    );
    const p3 = withRateLimit(
      async () => {
        order.push(3);
      },
      { priority: 5 },
    );
    const p4 = withRateLimit(
      async () => {
        order.push(4);
      },
      { priority: 5 },
    );

    resolveGate();
    await Promise.all([p0, p1, p2, p3, p4]);

    expect(order).toEqual([1, 2, 3, 4]);
  });

  it("handles mixed priorities with ties correctly", async () => {
    const { withRateLimit } = createRateLimit(10, 1000, 1);
    const order: string[] = [];

    let resolveGate!: () => void;
    const gate = new Promise<void>((r) => {
      resolveGate = r;
    });

    const p0 = withRateLimit(
      async () => {
        await gate;
      },
      { priority: 0 },
    );
    // Two priority-1 jobs interleaved with a priority-0 job
    const p1 = withRateLimit(
      async () => {
        order.push("p1-first");
      },
      { priority: 1 },
    );
    const p2 = withRateLimit(
      async () => {
        order.push("p0");
      },
      { priority: 0 },
    );
    const p3 = withRateLimit(
      async () => {
        order.push("p1-second");
      },
      { priority: 1 },
    );

    resolveGate();
    await Promise.all([p0, p1, p2, p3]);

    expect(order).toEqual(["p0", "p1-first", "p1-second"]);
  });

  it("default priority runs after explicit priorities", async () => {
    const { withRateLimit } = createRateLimit(10, 1000, 1);
    const order: string[] = [];

    let resolveGate!: () => void;
    const gate = new Promise<void>((r) => {
      resolveGate = r;
    });

    const p0 = withRateLimit(
      async () => {
        await gate;
      },
      { priority: 0 },
    );
    const p1 = withRateLimit(async () => {
      order.push("default");
    }, {});
    const p2 = withRateLimit(
      async () => {
        order.push("explicit");
      },
      { priority: 0 },
    );

    resolveGate();
    await Promise.all([p0, p1, p2]);

    expect(order).toEqual(["explicit", "default"]);
  });
});
