import { custom } from "viem";
import { describe, expect, it, vi } from "vitest";

import { createFacetId, getObservability, isObserved, observe, withLogging } from "../src/observability.js";
import { failover } from "../src/transports/failover/index.js";
import { handleEthGetLogs } from "../src/transports/logs-divider/handlers.js";
import { logsDividerTransportKey } from "../src/transports/logs-divider/schema.js";
import { logsSieve } from "../src/transports/logs-sieve/index.js";
import { rateLimiter } from "../src/transports/rate-limiter/index.js";

import { createStubLogger, findDotted } from "./helpers/logger.js";

describe("observability", () => {
  /** Stands in for a transport's own facet id at the outermost boundary. */
  const ROOT = createFacetId("test-root");
  const T = createFacetId("t");

  describe("withLogging + observe", () => {
    it("derives a per-call child carrying seeded context, library, call_id, req, and emits one wide event", async () => {
      const { logger, events } = createStubLogger();

      const observed = observe(async (req: { method: string }) => {
        const obs = getObservability();
        expect(obs?.logger).toBeDefined();
        expect(obs?.call_id).toMatch(/^[0-9a-f-]{36}$/);
        expect(typeof obs?.facet).toBe("function");
        return `ok:${req.method}`;
      }, ROOT);

      const result = await withLogging(() => observed({ method: "eth_blockNumber" }), {
        logger,
        request_id: "rq-1",
        tag: "user-op",
      });
      expect(result).toBe("ok:eth_blockNumber");

      expect(events).toHaveLength(1);
      expect(events[0]!.name).toBe("concluded");
      expect(events[0]!.context).toMatchObject({
        library: "viem-dlc",
        request_id: "rq-1",
        tag: "user-op",
        req: { method: "eth_blockNumber" },
      });
      expect(events[0]!.context.call_id).toMatch(/^[0-9a-f-]{36}$/);
      expect(events[0]!.context.status).toBe("ok");
      expect(typeof events[0]!.context.duration_ms).toBe("number");
    });

    it("emits exactly one error-status wide event when the wrapped fn rejects", async () => {
      const { logger, events } = createStubLogger();
      const thrown = new Error("boom");
      const keysBefore = Object.keys(thrown);

      const observed = observe(async () => {
        throw thrown;
      }, ROOT);

      await expect(withLogging(() => observed({}), { logger })).rejects.toBe(thrown);

      expect(events).toHaveLength(1);
      expect(events[0]!.name).toBe("concluded");
      expect(events[0]!.context.status).toBe("error");
      expect(events[0]!.error).toBeInstanceOf(Error);
      expect(typeof events[0]!.context.duration_ms).toBe("number");
      expect(isObserved(thrown)).toBe(true);
      expect(Object.keys(thrown)).toEqual(keysBefore);
      expect(thrown.message).toBe("boom");
    });

    it("isObserved walks the cause chain", async () => {
      const { logger } = createStubLogger();
      const inner = new Error("inner");
      const observed = observe(async () => {
        throw inner;
      }, ROOT);

      await expect(withLogging(() => observed({}), { logger })).rejects.toBe(inner);

      const outer = new Error("outer", { cause: inner });
      expect(isObserved(outer)).toBe(true);
      expect(isObserved(new Error("fresh"))).toBe(false);
      expect(isObserved("string")).toBe(false);
      expect(isObserved(null)).toBe(false);
      expect(isObserved(undefined)).toBe(false);
    });

    it("errors thrown outside a withLogging scope are not marked", async () => {
      const thrown = new Error("outside");
      const observed = observe(async () => {
        throw thrown;
      }, ROOT);

      await expect(observed({})).rejects.toBe(thrown);
      expect(isObserved(thrown)).toBe(false);
    });

    it("non-object rejections rethrow unchanged", async () => {
      const { logger, events } = createStubLogger();
      const observed = observe(async () => {
        throw "plain";
      }, ROOT);

      await expect(withLogging(() => observed({}), { logger })).rejects.toBe("plain");
      expect(events).toHaveLength(1);
    });

    it("frozen errors still rethrow", async () => {
      const { logger } = createStubLogger();
      const thrown = Object.freeze(new Error("frozen"));
      const observed = observe(async () => {
        throw thrown;
      }, ROOT);

      await expect(withLogging(() => observed({}), { logger })).rejects.toBe(thrown);
      expect(isObserved(thrown)).toBe(false);
    });

    it("nested observe boundaries reuse the per-call logger and call_id", async () => {
      const { logger, events } = createStubLogger();

      let outer: ReturnType<typeof getObservability>;
      let inner: ReturnType<typeof getObservability>;

      const innerObserved = observe(async () => {
        inner = getObservability();
      }, createFacetId("inner"));
      const outerObserved = observe(async () => {
        outer = getObservability();
        await innerObserved({});
        await innerObserved({});
      }, ROOT);

      await withLogging(() => outerObserved({ method: "eth_getLogs" }), { logger });

      expect(inner?.call_id).toBe(outer?.call_id);
      // Same LogLayer instance, so every layer's facet writes accumulate onto one wide event.
      expect(inner?.logger).toBe(outer?.logger);

      // Only the outermost boundary emits.
      expect(events).toHaveLength(1);
      expect(events[0]!.name).toBe("concluded");
    });

    it("parallel calls inside one withLogging scope get distinct call_ids and isolated contexts", async () => {
      const { logger, events } = createStubLogger();

      const observed = observe(async (req: { which: string }) => {
        getObservability()?.facet(ROOT).set({ which: req.which });
      }, ROOT);

      await withLogging(() => Promise.all([observed({ which: "a" }), observed({ which: "b" })]), { logger });

      expect(events).toHaveLength(2);
      expect(events.map((e) => e.name)).toEqual(["concluded", "concluded"]);
      expect(events[0]!.context.call_id).not.toBe(events[1]!.context.call_id);
      expect(new Set(events.map((e) => e.context["test-root.which"]))).toEqual(new Set(["a", "b"]));
    });

    it("parallel withLogging scopes do not bleed context", async () => {
      const { logger: a, events: aEvents } = createStubLogger();
      const { logger: b, events: bEvents } = createStubLogger();

      const observed = observe(async () => {}, ROOT);

      await Promise.all([
        withLogging(() => observed({ method: "eth_blockNumber" }), { logger: a, request_id: "A" }),
        withLogging(() => observed({ method: "eth_chainId" }), { logger: b, request_id: "B" }),
      ]);

      expect(aEvents).toHaveLength(1);
      expect(bEvents).toHaveLength(1);
      expect(aEvents[0]!.context.request_id).toBe("A");
      expect(bEvents[0]!.context.request_id).toBe("B");
      expect(aEvents[0]!.context.req).toEqual({ method: "eth_blockNumber" });
      expect(bEvents[0]!.context.req).toEqual({ method: "eth_chainId" });
    });

    it("trims long strings in the per-call `req` context", async () => {
      const { logger, events } = createStubLogger();
      const long = "0x".concat("ab".repeat(200));
      const short = "0x1234";

      const observed = observe(async (_req: unknown) => {}, ROOT);
      await withLogging(() => observed({ method: "eth_call", params: [{ data: long, to: short }] }), { logger });

      const req = events[0]!.context.req as { params: [{ data: string; to: string }] };
      expect(req.params[0].data).toHaveLength(100);
      expect(req.params[0].data).toBe(long.slice(0, 97).concat("..."));
      expect(req.params[0].to).toBe(short);
    });

    it("shares one AsyncLocalStorage across scopes that race the cold-start import", async () => {
      // Fresh module registry, so both calls below hit `loadAls` before it resolves.
      vi.resetModules();
      const fresh = await import("../src/observability.js");
      const id = fresh.createFacetId("racer");
      const observed = fresh.observe(async () => {}, id);

      const a = createStubLogger();
      const b = createStubLogger();
      await Promise.all([
        fresh.withLogging(() => observed({ method: "eth_blockNumber" }), { logger: a.logger }),
        fresh.withLogging(() => observed({ method: "eth_chainId" }), { logger: b.logger }),
      ]);

      // If each call built its own storage, only the last-assigned one would be
      // visible to `observe` and the other scope would emit nothing.
      expect(a.events).toHaveLength(1);
      expect(b.events).toHaveLength(1);
    });

    it("emits nothing and reports no observability when no scope is active", async () => {
      const { logger, events } = createStubLogger();

      expect(getObservability()).toBeUndefined();

      const observed = observe(async (req: { method: string }) => {
        expect(getObservability()).toBeUndefined();
        return `ok:${req.method}`;
      }, ROOT);
      expect(await observed({ method: "eth_blockNumber" })).toBe("ok:eth_blockNumber");

      // ...and a real transport still works outside a scope.
      const transport = failover([custom({ request: vi.fn().mockResolvedValue("0x42") }, { retryCount: 0 })])(
        {} as never,
      );
      expect(await transport.request({ method: "eth_blockNumber" })).toBe("0x42");

      expect(events).toHaveLength(0);
      // Reference `logger` so it isn't reported unused.
      void logger;
    });
  });

  describe("facets", () => {
    /** Runs `fn` inside a fresh withLogging + observe scope and returns the wide event's context. */
    async function concluded(fn: () => Promise<void> | void) {
      const { logger, events } = createStubLogger();
      const observed = observe(async () => fn(), ROOT);
      await withLogging(() => observed({ method: "test" }), { logger });
      expect(events).toHaveLength(1);
      return events[0]!.context;
    }

    it("labels ids sharing a key in first-touch order, independent across keys", async () => {
      const a = createFacetId("alpha");
      const b = createFacetId("alpha");
      const b2 = createFacetId("beta");
      const context = await concluded(() => {
        const obs = getObservability()!;
        obs.facet(a).set({ x: 1 });
        obs.facet(b).set({ x: 2 });
        // Re-allocating with the same id returns the same slot.
        obs.facet(a).set({ y: 3 });
        obs.facet(b2).set({ y: 4 });
      });

      expect(context["alpha.x"]).toBe(1);
      expect(context["alpha.1.x"]).toBe(2);
      expect(context["alpha.y"]).toBe(3);
      // Each key labels independently: beta's first id also gets a bare label.
      expect(context["beta.y"]).toBe(4);
    });

    it("set merges per field, and writes remain valid after awaits", async () => {
      const context = await concluded(async () => {
        const facet = getObservability()!.facet(T);
        facet.set({ a: 1, b: "old" });
        await Promise.resolve();
        facet.set({ b: "new" });
      });

      expect(context["t.a"]).toBe(1);
      expect(context["t.b"]).toBe("new");
    });

    it("add accumulates and stat summarizes", async () => {
      const context = await concluded(() => {
        const facet = getObservability()!.facet(T);
        facet.add("hits");
        facet.add("hits", 4);
        facet.stat("ms", 10);
        facet.stat("ms", 30);
        facet.stat("ms", 20);
      });

      expect(context["t.hits"]).toBe(5);
      expect(context["t.ms.count"]).toBe(3);
      expect(context["t.ms.min"]).toBe(10);
      expect(context["t.ms.max"]).toBe(30);
      expect(context["t.ms.avg"]).toBe(20);
    });

    it("push bounds the array and counts the overflow", async () => {
      const context = await concluded(() => {
        const facet = getObservability()!.facet(T);
        facet.push("ids", "a", 2);
        facet.push("ids", "b", 2);
        facet.push("ids", "c", 2);
        facet.push("ids", "d", 2);
      });

      expect(context["t.ids"]).toEqual(["a", "b"]);
      expect(context["t.ids_truncated"]).toBe(2);
    });

    it("drops oversized fields at conclusion and records them in truncated_fields", async () => {
      const context = await concluded(() => {
        const facet = getObservability()!.facet(T);
        facet.set({ huge: "x".repeat(64 * 1024), small: 1 });
      });

      expect(context["t.huge"]).toBeUndefined();
      expect(context["t.small"]).toBe(1);
      expect(context.truncated_fields).toEqual(["t.huge"]);
    });

    it("a facet that never writes contributes no fields", async () => {
      const context = await concluded(() => {
        getObservability()!.facet(createFacetId("silent"));
      });

      expect(Object.keys(context).some((k) => k.startsWith("silent."))).toBe(false);
    });

    it("sub scopes field names within the same slot", async () => {
      const context = await concluded(() => {
        const facet = getObservability()!.facet(T);
        facet.sub("eth_call").set({ x: 1 });
        facet.set({ y: 2 });
      });

      expect(context["t.eth_call.x"]).toBe(1);
      expect(context["t.y"]).toBe(2);
    });

    it("repeated facet lookups for one id aggregate on its slot", async () => {
      const context = await concluded(() => {
        // Simulates a per-chunk layer allocating its facet on every crossing.
        getObservability()!.facet(T).add("n", 2);
        getObservability()!.facet(T).add("n", 3);
      });

      expect(context["t.n"]).toBe(5);
      // No second label was claimed.
      expect(Object.keys(context).some((k) => /^t\.\d+\./.test(k))).toBe(false);
    });

    it("observe stamps a crossings count per id", async () => {
      const { logger, events } = createStubLogger();

      const innerA = createFacetId("inner");
      const innerB = createFacetId("inner");
      const innerObservedA = observe(async () => {}, innerA);
      const innerObservedB = observe(async () => {}, innerB);
      const outerObserved = observe(async () => {
        await innerObservedA({});
        await innerObservedA({});
        await innerObservedB({});
      }, createFacetId("outer"));

      await withLogging(() => outerObserved({ method: "eth_getLogs" }), { logger });

      const { context } = events[0]!;
      expect(context["outer.crossings"]).toBe(1);
      expect(context["inner.crossings"]).toBe(2);
      // A second id sharing that key gets its own first-touch label.
      expect(context["inner.1.crossings"]).toBe(1);
    });
  });

  describe("canonical enrichment through real transports", () => {
    it("failover stamps branch stats onto the wide event", async () => {
      const { logger, events } = createStubLogger();
      const a = vi.fn().mockRejectedValue(new Error("a-failed"));
      const b = vi.fn().mockResolvedValue("0x42");

      const transport = failover([
        custom({ request: a }, { retryCount: 0 }),
        custom({ request: b }, { retryCount: 0 }),
      ])({} as never);

      const result = await withLogging(() => transport.request({ method: "eth_blockNumber" }), { logger });
      expect(result).toBe("0x42");

      expect(events).toHaveLength(1);
      const { context } = events[0]!;
      expect(events[0]!.name).toBe("concluded");

      const key = "viem-dlc-failover";
      expect(findDotted(context, key, "succeeded_index")).toBe(1);
      expect(findDotted(context, key, "branches_attempted")).toBe(2);
      expect(findDotted(context, key, "terminated_by_should_throw")).toBe(false);

      const errs = findDotted(context, key, "branch_errors") as { message: string }[];
      expect(errs).toHaveLength(1);
      expect(errs[0]!.message).toContain("a-failed");

      const branchDurations = findDotted(context, key, "branch_durations_ms") as number[];
      expect(branchDurations).toHaveLength(2);
      expect(branchDurations.every((d) => typeof d === "number" && d >= 0)).toBe(true);
    });

    it("logs-divider stamps split stats when a range error triggers a halving", async () => {
      const { logger, events } = createStubLogger();
      let firstFailure = true;
      const requestFn = vi.fn().mockImplementation(async () => {
        if (firstFailure) {
          firstFailure = false;
          throw Object.assign(new Error("query returned more than 10000 results"), { code: -32005 });
        }
        return [];
      });

      // Call the handler directly to bypass viem's `buildRequest` retry layers, but wrap it
      // in `observe` so it sees an ambient per-call scope (which it reads itself).
      const dividerId = createFacetId(logsDividerTransportKey);
      const observed = observe(
        () =>
          handleEthGetLogs(
            requestFn,
            [{ fromBlock: "0x0", toBlock: "0x10" }, undefined, { latestBlock: "0x20" }],
            { maxBlockRange: 100, alignTo: 1 },
            dividerId,
          ),
        dividerId,
      );

      const logs = await withLogging(() => observed({ method: "eth_getLogs" }), { logger });
      expect(logs).toEqual([]);
      // 1 failed full-range fetch + 2 successful halves.
      expect(requestFn).toHaveBeenCalledTimes(3);

      expect(events).toHaveLength(1);
      const { context } = events[0]!;
      expect(events[0]!.name).toBe("concluded");

      const key = "viem-dlc-logs-divider";
      expect(findDotted(context, key, "from_block")).toBe(0);
      expect(findDotted(context, key, "to_block")).toBe(0x10);
      expect(findDotted(context, key, "latest_block")).toBe(0x20);
      expect(findDotted(context, key, "nominal_ranges")).toBe(1);
      expect(findDotted(context, key, "logs_fetched")).toBe(0);
      expect(findDotted(context, key, "splits_count")).toBe(1);
      expect(findDotted(context, key, "splits_range")).toBe(1);
      expect(findDotted(context, key, "splits_timeout")).toBe(0);
      expect(findDotted(context, key, "splits_max_depth")).toBe(1);

      // Histogram of leaf fetch durations, keyed by 100ms-bin lower bound.
      const durations = findDotted(context, key, "fetch_durations_ms") as Record<number, number>;
      expect(Object.values(durations).reduce((a, b) => a + b, 0)).toBe(3);
    });

    it("logs-divider records unhalvable failures in failed_ranges and the event carries error status", async () => {
      const { logger, events } = createStubLogger();
      const failure = new Error("connection refused");
      const requestFn = vi.fn().mockRejectedValue(failure);

      const dividerId = createFacetId(logsDividerTransportKey);
      const observed = observe(
        () =>
          handleEthGetLogs(
            requestFn,
            [{ fromBlock: "0x0", toBlock: "0x10" }, undefined, { latestBlock: "0x20" }],
            { maxBlockRange: 100, alignTo: 1 },
            dividerId,
          ),
        dividerId,
      );

      await expect(withLogging(() => observed({ method: "eth_getLogs" }), { logger })).rejects.toThrow(
        "connection refused",
      );

      expect(events).toHaveLength(1);
      const { context } = events[0]!;
      expect(context.status).toBe("error");
      expect(events[0]!.error).toBe(failure);

      const failedRanges = findDotted(context, "viem-dlc-logs-divider", "failed_ranges") as {
        from_block: number;
        to_block: number;
        error: { message: string };
      }[];
      expect(failedRanges).toHaveLength(1);
      expect(failedRanges[0]).toMatchObject({ from_block: 0, to_block: 0x10 });
      expect(failedRanges[0]!.error.message).toContain("connection refused");
    });

    it("logs-sieve summarizes the sizes of the logs it drops", async () => {
      const { logger, events } = createStubLogger();
      const small = { address: "0x1", data: "0x" };
      const big = { address: "0x2", data: `0x${"ab".repeat(400)}` };
      const requestFn = vi.fn().mockResolvedValue([small, big, big]);

      const transport = logsSieve(custom({ request: requestFn }, { retryCount: 0 }), [{ maxBytes: 128 }])({} as never);
      const kept = await withLogging(() => transport.request({ method: "eth_getLogs", params: [{}] }), { logger });
      expect(kept).toHaveLength(1);

      const { context } = events[0]!;
      const key = "viem-dlc-logs-sieve";
      expect(findDotted(context, key, "logs_dropped")).toBe(2);
      expect(findDotted(context, key, "dropped_log_bytes.count")).toBe(2);
      expect(findDotted(context, key, "dropped_log_bytes.min")).toBeGreaterThan(128);
      expect(findDotted(context, key, "dropped_log_bytes.avg")).toBe(findDotted(context, key, "dropped_log_bytes.max"));
    });

    it("rate-limiter summarizes queue wait, attributing each sample to the call that waited", async () => {
      const { logger, events } = createStubLogger();
      const requestFn = vi.fn().mockResolvedValue("0x1");

      // One token, no refill: the first call is admitted immediately and the second
      // only after the first releases — so the two calls must record different waits.
      const transport = rateLimiter(custom({ request: requestFn }, { retryCount: 0 }), [
        { maxRequestsPerSecond: 1000, maxBurstRequests: 1, maxConcurrentRequests: 1 },
      ])({} as never);

      await Promise.all([
        withLogging(() => transport.request({ method: "eth_blockNumber" }), { logger, tag: "first" }),
        withLogging(() => transport.request({ method: "eth_chainId" }), { logger, tag: "second" }),
      ]);

      expect(events).toHaveLength(2);
      const key = "viem-dlc-rate-limiter";
      for (const event of events) {
        // Each event carries exactly its own call's single wait sample, not both.
        expect(findDotted(event.context, key, "queue_wait_ms.count")).toBe(1);
        expect(findDotted(event.context, key, "queue_wait_ms.max")).toBeGreaterThanOrEqual(0);
      }
    });
  });
});
