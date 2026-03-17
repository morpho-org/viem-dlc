import { describe, expect, it } from "vitest";

import { createInFlightBarrier } from "../../src/utils/in-flight.js";

describe("createInFlightBarrier", () => {
  it("flush waits for tracked promises to settle", async () => {
    const barrier = createInFlightBarrier();

    let resolveTracked: () => void = () => {};
    const tracked = new Promise<void>((resolve) => {
      resolveTracked = resolve;
    });

    barrier.track(tracked);

    let flushed = false;
    const flushPromise = barrier.flush().then(() => {
      flushed = true;
    });

    await Promise.resolve();
    expect(flushed).toBe(false);

    resolveTracked();
    await flushPromise;
    expect(flushed).toBe(true);
  });

  it("flush does not reject when tracked promises reject", async () => {
    const barrier = createInFlightBarrier();

    const tracked = Promise.reject(new Error("boom"));
    tracked.catch(() => {});

    barrier.track(tracked);

    await expect(barrier.flush()).resolves.toBeUndefined();
  });

  it("flush only waits for promises accepted before the barrier", async () => {
    const barrier = createInFlightBarrier();

    let resolveFirst: () => void = () => {};
    const first = new Promise<void>((resolve) => {
      resolveFirst = resolve;
    });

    barrier.track(first);

    let flushed = false;
    const flushPromise = barrier.flush().then(() => {
      flushed = true;
    });

    let resolveSecond: () => void = () => {};
    barrier.track(
      new Promise<void>((resolve) => {
        resolveSecond = resolve;
      }),
    );

    await Promise.resolve();
    expect(flushed).toBe(false);

    resolveFirst();
    await flushPromise;
    expect(flushed).toBe(true);

    resolveSecond();
    await barrier.flush();
  });
});
