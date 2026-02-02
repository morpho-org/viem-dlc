import { describe, expect, it } from "vitest";

import { sleep } from "../../src/utils/sleep.js";
import { createTokenBucket, withRateLimit } from "../../src/utils/with-rate-limit.js";

describe("createTokenBucket", () => {
  it("creates bucket with specified maxTokens and refillRate", () => {
    const bucket = createTokenBucket(5, 10);

    expect(bucket.maxTokens).toBe(5);
    expect(bucket.refillRate).toBe(10);
    expect(bucket.tokens).toBe(5); // starts full
    expect(bucket.queue).toEqual([]);
    expect(bucket.drainScheduled).toBe(false);
  });

  it("starts with full tokens for burst capacity", () => {
    const bucket = createTokenBucket(10, 1);
    expect(bucket.tokens).toBe(10);
  });
});

describe("withRateLimit", () => {
  it("executes function and returns result", async () => {
    const bucket = createTokenBucket(5, 10);
    const result = await withRateLimit(() => Promise.resolve("hello"), { bucket });
    expect(result).toBe("hello");
  });

  it("propagates errors from the wrapped function", async () => {
    const bucket = createTokenBucket(5, 10);
    const error = new Error("test error");

    await expect(withRateLimit(() => Promise.reject(error), { bucket })).rejects.toThrow("test error");
  });

  it("consumes a token per request", async () => {
    const bucket = createTokenBucket(5, 10);

    await withRateLimit(() => Promise.resolve(1), { bucket });
    expect(bucket.tokens).toBeCloseTo(4, 1);

    await withRateLimit(() => Promise.resolve(2), { bucket });
    expect(bucket.tokens).toBeCloseTo(3, 1);
  });

  describe("burst capacity", () => {
    it("allows burst of requests up to maxTokens without delay", async () => {
      const bucket = createTokenBucket(5, 10);
      const start = Date.now();

      // Fire 5 requests concurrently - should all complete immediately
      const results = await Promise.all([
        withRateLimit(() => Promise.resolve(1), { bucket }),
        withRateLimit(() => Promise.resolve(2), { bucket }),
        withRateLimit(() => Promise.resolve(3), { bucket }),
        withRateLimit(() => Promise.resolve(4), { bucket }),
        withRateLimit(() => Promise.resolve(5), { bucket }),
      ]);

      const elapsed = Date.now() - start;

      expect(results).toEqual([1, 2, 3, 4, 5]);
      expect(elapsed).toBeLessThan(50); // Should be nearly instant
    });
  });

  describe("FIFO ordering", () => {
    it("processes requests in the order they were received", async () => {
      const bucket = createTokenBucket(2, 100); // 2 burst, fast refill
      const order: number[] = [];

      // Launch 6 requests concurrently
      // First 2 should resolve immediately (burst)
      // Rest should queue and resolve in FIFO order
      const promises = [1, 2, 3, 4, 5, 6].map((n) =>
        withRateLimit(
          async () => {
            order.push(n);
            return n;
          },
          { bucket },
        ),
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
      const bucket = createTokenBucket(1, 10); // 1 burst, 10/sec = 100ms per token
      const start = Date.now();

      // First request uses the burst token
      await withRateLimit(() => Promise.resolve(1), { bucket });

      // Second request must wait for refill
      await withRateLimit(() => Promise.resolve(2), { bucket });

      const elapsed = Date.now() - start;

      // Should have waited ~100ms for token refill
      expect(elapsed).toBeGreaterThanOrEqual(80); // allow some timing slack
      expect(elapsed).toBeLessThan(200);
    });

    it("refills tokens over time", async () => {
      const bucket = createTokenBucket(2, 10); // 2 max, 10/sec

      // Exhaust tokens
      await withRateLimit(() => Promise.resolve(1), { bucket });
      await withRateLimit(() => Promise.resolve(2), { bucket });

      expect(bucket.tokens).toBeCloseTo(0, 1);

      // Wait 150ms - should refill ~1.5 tokens
      await sleep(150);

      // Trigger a refill by starting another request
      const start = Date.now();
      await withRateLimit(() => Promise.resolve(3), { bucket });
      const elapsed = Date.now() - start;

      // Should have had a token available (or nearly so)
      expect(elapsed).toBeLessThan(50);
    });
  });

  describe("queue behavior", () => {
    it("drains queue as tokens become available", async () => {
      const bucket = createTokenBucket(1, 20); // 1 burst, 20/sec = 50ms per token
      const timestamps: number[] = [];
      const start = Date.now();

      // Queue up 4 requests
      const promises = [1, 2, 3, 4].map((n) =>
        withRateLimit(
          async () => {
            timestamps.push(Date.now() - start);
            return n;
          },
          { bucket },
        ),
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
      const bucket = createTokenBucket(5, 10);

      // Single request, no queue buildup
      const result = await withRateLimit(() => Promise.resolve("solo"), { bucket });
      expect(result).toBe("solo");
      expect(bucket.queue).toEqual([]);
    });

    it("clears queue after all requests complete", async () => {
      const bucket = createTokenBucket(2, 100);

      await Promise.all([
        withRateLimit(() => Promise.resolve(1), { bucket }),
        withRateLimit(() => Promise.resolve(2), { bucket }),
        withRateLimit(() => Promise.resolve(3), { bucket }),
      ]);

      expect(bucket.queue).toEqual([]);
      expect(bucket.drainScheduled).toBe(false);
    });
  });

  describe("edge cases", () => {
    it("handles zero-delay scenario (tokens available)", async () => {
      const bucket = createTokenBucket(10, 10);

      const start = Date.now();
      await withRateLimit(() => Promise.resolve("fast"), { bucket });
      const elapsed = Date.now() - start;

      expect(elapsed).toBeLessThan(20);
    });

    it("handles rapid sequential requests", async () => {
      const bucket = createTokenBucket(3, 100);
      const results: number[] = [];

      for (let i = 1; i <= 5; i++) {
        const result = await withRateLimit(() => Promise.resolve(i), { bucket });
        results.push(result);
      }

      expect(results).toEqual([1, 2, 3, 4, 5]);
    });

    it("handles concurrent requests exceeding burst capacity", async () => {
      const bucket = createTokenBucket(2, 50); // 2 burst, fast refill

      // Launch 10 concurrent requests
      const promises = Array.from({ length: 10 }, (_, i) => withRateLimit(() => Promise.resolve(i + 1), { bucket }));

      const results = await Promise.all(promises);

      // All should complete and return correct values
      expect(results).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    });
  });
});
