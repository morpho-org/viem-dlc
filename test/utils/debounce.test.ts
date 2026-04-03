import { afterEach, describe, expect, it, vi } from "vitest";

import { debounce } from "../../src/utils/debounce.js";

type TimerEntry = {
  callback: () => void;
  delay: number;
};

const createDeferred = () => {
  let resolve!: () => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<void>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};

const installManualTimers = () => {
  let now = 0;
  let nextTimerId = 1;
  const timers = new Map<number, TimerEntry>();

  vi.spyOn(Date, "now").mockImplementation(() => now);
  vi.spyOn(globalThis, "setTimeout").mockImplementation(((callback: () => void, delay = 0) => {
    const id = nextTimerId++;
    timers.set(id, { callback, delay });
    return id as ReturnType<typeof setTimeout>;
  }) as typeof setTimeout);
  vi.spyOn(globalThis, "clearTimeout").mockImplementation(((timer?: ReturnType<typeof setTimeout>) => {
    if (typeof timer === "number") timers.delete(timer);
  }) as typeof clearTimeout);

  return {
    getNow: () => now,
    setNow: (value: number) => {
      now = value;
    },
    timers,
  };
};

describe("debounce", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("debounces repeated calls and only dispatches the latest args", () => {
    const fn = vi.fn(async () => {});
    const clock = installManualTimers();

    const debounced = debounce(fn, {
      debounceMs: 100,
      maxDelayMs: 500,
      maxStalenessMs: Infinity,
    });

    debounced("first");
    expect([...clock.timers.values()].map((timer) => timer.delay).sort((a, b) => a - b)).toEqual([100, 500]);

    clock.setNow(40);
    debounced("second");

    expect(clock.timers.size).toBe(2);
    expect([...clock.timers.values()].map((timer) => timer.delay).sort((a, b) => a - b)).toEqual([100, 500]);

    const debounceTimer = [...clock.timers.values()].find((timer) => timer.delay === 100);
    debounceTimer?.callback();

    expect(fn).toHaveBeenCalledTimes(1);
    expect(fn.mock.calls[0]?.[1]).toBe("second");
    expect(clock.timers.size).toBe(0);
  });

  it("caps the debounce wait at the remaining maxDelay in a window", () => {
    const fn = vi.fn(async () => {});
    const clock = installManualTimers();

    const debounced = debounce(fn, {
      debounceMs: 100,
      maxDelayMs: 150,
      maxStalenessMs: Infinity,
    });

    debounced("first");

    clock.setNow(80);
    debounced("second");

    expect([...clock.timers.values()].map((timer) => timer.delay).sort((a, b) => a - b)).toEqual([70, 150]);
  });

  it("drops stale pending work before dispatch when a timer fires late", () => {
    const fn = vi.fn(async () => {});
    const clock = installManualTimers();

    const debounced = debounce(fn, {
      debounceMs: 100,
      maxDelayMs: 1_000,
      maxStalenessMs: 50,
    });

    debounced("stale");

    clock.setNow(1_000);
    const staleFire = clock.timers.values().next().value;
    staleFire?.callback();

    expect(fn).not.toHaveBeenCalled();
    expect(clock.timers.size).toBe(0);

    debounced("fresh");
    clock.setNow(1_040);
    const freshFire = clock.timers.values().next().value;
    freshFire?.callback();

    expect(fn).toHaveBeenCalledTimes(1);
    expect(fn.mock.calls[0]?.[1]).toBe("fresh");
  });

  it("drops stale queued work after an in-flight invocation settles", async () => {
    const firstRun = createDeferred();
    const fn = vi.fn(async () => firstRun.promise);
    const clock = installManualTimers();

    const debounced = debounce(fn, {
      debounceMs: 100,
      maxDelayMs: 500,
      maxStalenessMs: 50,
    });

    debounced.immediate("first");
    expect(fn).toHaveBeenCalledTimes(1);

    clock.setNow(10);
    debounced("stale");

    clock.setNow(100);
    firstRun.resolve();
    await firstRun.promise;
    await Promise.resolve();

    expect(fn).toHaveBeenCalledTimes(1);
    expect(clock.timers.size).toBe(0);
  });

  it("tracks pending work while running and fires immediately on settle once maxDelay is exceeded", async () => {
    const firstRun = createDeferred();
    const fn = vi.fn(async () => firstRun.promise);
    const clock = installManualTimers();

    const debounced = debounce(fn, {
      debounceMs: 100,
      maxDelayMs: 500,
      maxStalenessMs: Infinity,
    });

    debounced.immediate("first");
    expect(fn).toHaveBeenCalledTimes(1);

    clock.setNow(100);
    debounced("second");
    expect(clock.timers.size).toBe(0);

    clock.setNow(700);
    firstRun.resolve();
    await firstRun.promise;
    await vi.waitFor(() => expect(fn).toHaveBeenCalledTimes(2));

    expect(fn.mock.calls[1]?.[1]).toBe("second");
    expect(clock.timers.size).toBe(0);
  });

  it("reschedules queued work from the original window and keeps only the latest args", async () => {
    const firstRun = createDeferred();
    const fn = vi.fn(async (...args) => {
      void args;
      return fn.mock.calls.length === 1 ? firstRun.promise : Promise.resolve();
    });
    const clock = installManualTimers();

    const debounced = debounce(fn, {
      debounceMs: 100,
      maxDelayMs: 500,
      maxStalenessMs: Infinity,
    });

    debounced.immediate("first");
    expect(fn).toHaveBeenCalledTimes(1);

    clock.setNow(100);
    debounced("second");
    clock.setNow(250);
    debounced("third");

    clock.setNow(300);
    firstRun.resolve();
    await firstRun.promise;
    await vi.waitFor(() => expect(clock.timers.size).toBe(2));

    expect([...clock.timers.values()].map((timer) => timer.delay).sort((a, b) => a - b)).toEqual([50, 300]);

    const debounceTimer = [...clock.timers.values()].find((timer) => timer.delay === 50);
    debounceTimer?.callback();

    expect(fn).toHaveBeenCalledTimes(2);
    expect(fn.mock.calls[1]?.[1]).toBe("third");
    expect(clock.timers.size).toBe(0);
  });

  it("fires immediate() queued work as soon as the running invocation settles", async () => {
    const firstRun = createDeferred();
    const fn = vi.fn(async (...args) => {
      void args;
      return fn.mock.calls.length === 1 ? firstRun.promise : Promise.resolve();
    });
    const clock = installManualTimers();

    const debounced = debounce(fn, {
      debounceMs: 100,
      maxDelayMs: 1_000,
      maxStalenessMs: Infinity,
    });

    debounced.immediate("first");
    expect(fn).toHaveBeenCalledTimes(1);

    clock.setNow(25);
    debounced.immediate("second");
    expect(clock.timers.size).toBe(0);

    clock.setNow(30);
    firstRun.resolve();
    await firstRun.promise;
    await vi.waitFor(() => expect(fn).toHaveBeenCalledTimes(2));

    expect(fn.mock.calls[1]?.[1]).toBe("second");
    expect(clock.timers.size).toBe(0);
  });

  it("cancel() clears pending work, aborts the active invocation, and returns its promise", async () => {
    const abortError = Object.assign(new Error("aborted"), { name: "AbortError" });
    const firstRun = createDeferred();
    const fn = vi.fn((signal: AbortSignal, _value: string) => {
      signal.addEventListener("abort", () => firstRun.reject(abortError), { once: true });
      return firstRun.promise;
    });
    const clock = installManualTimers();

    const debounced = debounce(fn, {
      debounceMs: 100,
      maxDelayMs: 500,
      maxStalenessMs: Infinity,
    });

    // Cancel with only pending work (no running invocation)
    debounced("before-cancel");
    expect(clock.timers.size).toBe(2);
    expect(debounced.cancel()).toBeUndefined();
    expect(clock.timers.size).toBe(0);

    // Cancel with a running invocation
    debounced.immediate("first");
    expect(fn).toHaveBeenCalledTimes(1);

    debounced("queued");
    const dying = debounced.cancel();
    expect(dying).toBeInstanceOf(Promise);
    expect(clock.timers.size).toBe(0);

    await dying;

    // Aborted invocation does not re-invoke despite prior pending work
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it("calls onError for non-abort errors and suppresses abort errors", async () => {
    const onError = vi.fn();
    const firstRun = createDeferred();
    const abortError = Object.assign(new Error("aborted"), { name: "AbortError" });

    const fn = vi.fn((signal: AbortSignal, value: string) => {
      if (value === "first") {
        signal.addEventListener("abort", () => {
          firstRun.reject(abortError);
        }, { once: true });
        return firstRun.promise;
      }

      return Promise.reject(new Error("real failure"));
    });

    const debounced = debounce(fn, {
      debounceMs: 0,
      maxDelayMs: 0,
      maxStalenessMs: Infinity,
      onError,
    });

    debounced.immediate("first");
    debounced.immediate("second");

    await vi.waitFor(() => expect(fn).toHaveBeenCalledTimes(2));
    await vi.waitFor(() => expect(onError).toHaveBeenCalledTimes(1));

    expect(onError.mock.calls[0]?.[0]).toBeInstanceOf(Error);
    expect((onError.mock.calls[0]?.[0] as Error).message).toBe("real failure");
    expect(onError.mock.calls[0]?.[1]).toEqual(["second"]);
  });

  it("unrefs internal timers when the runtime supports it", () => {
    const fn = vi.fn(async () => {});
    const unref = vi.fn();

    vi.spyOn(globalThis, "setTimeout").mockImplementation(((callback: () => void) => {
      void callback;
      return { unref } as ReturnType<typeof setTimeout>;
    }) as typeof setTimeout);

    const debounced = debounce(fn, {
      debounceMs: 100,
      maxDelayMs: 1_000,
      maxStalenessMs: Infinity,
    });

    debounced("value");

    expect(unref).toHaveBeenCalledTimes(2);
  });
});
