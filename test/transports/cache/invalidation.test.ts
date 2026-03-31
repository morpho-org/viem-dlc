import { describe, expect, it } from "vitest";

import type { InvalidationStrategy } from "../../../src/transports/cache/types.js";

describe("invalidation strategies", () => {
  it("returns 0 when cache age is below minimum", () => {
    const strategy: InvalidationStrategy = ({ cacheAgeMs }) => (cacheAgeMs < 5000 ? 0 : 0.5);

    expect(strategy({ confirmations: 0, cacheAgeMs: 0, totalChunks: 1 })).toBe(0);
    expect(strategy({ confirmations: 0, cacheAgeMs: 4999, totalChunks: 1 })).toBe(0);
  });

  it("returns 1 for hot blocks (few confirmations)", () => {
    const strategy: InvalidationStrategy = ({ confirmations, cacheAgeMs }) => {
      if (cacheAgeMs < 5000) return 0;
      return confirmations < 128 ? 1 : 0.001;
    };

    expect(strategy({ confirmations: 10, cacheAgeMs: 10000, totalChunks: 1 })).toBe(1);
    expect(strategy({ confirmations: 127, cacheAgeMs: 10000, totalChunks: 1 })).toBe(1);
  });

  it("returns low probability for deeply confirmed entries", () => {
    const strategy: InvalidationStrategy = ({ confirmations, cacheAgeMs }) => {
      if (cacheAgeMs < 5000) return 0;
      return confirmations < 128 ? 1 : 0.001;
    };

    expect(strategy({ confirmations: 1000, cacheAgeMs: 10000, totalChunks: 1 })).toBe(0.001);
  });
});
