import { custom } from "viem";
import { describe, expect, it, vi } from "vitest";

import { enterRequest, withLogging } from "../src/observability.js";
import { failover } from "../src/transports/failover/index.js";
import { handleEthGetLogs } from "../src/transports/logs-divider/handlers.js";

/**
 * Hand-rolled LogLayer stub. Implements just enough of the API for our scaffold
 * to call into without importing the real package. Each method returns `this`
 * so chaining works; emissions are captured into `events`.
 */
function createStubLogger() {
  const events: { name: string; context: Record<string, unknown>; metadata: Record<string, unknown> }[] = [];

  // biome-ignore lint/suspicious/noExplicitAny: stub LogLayer surface
  function makeLayer(parentContext: Record<string, unknown>): any {
    let context = { ...parentContext };
    let pendingMetadata: Record<string, unknown> = {};

    const layer = {
      child() {
        return makeLayer(context);
      },
      withContext(extra: Record<string, unknown>) {
        context = { ...context, ...extra };
        return layer;
      },
      withMetadata(extra: Record<string, unknown>) {
        pendingMetadata = { ...pendingMetadata, ...extra };
        return layer;
      },
      info(name: string) {
        events.push({ name, context: { ...context }, metadata: pendingMetadata });
        pendingMetadata = {};
        return layer;
      },
      error(name: string) {
        events.push({ name, context: { ...context }, metadata: pendingMetadata });
        pendingMetadata = {};
        return layer;
      },
      withError(_err: unknown) {
        return layer;
      },
    };
    return layer;
  }

  return { logger: makeLayer({}), events };
}

describe("observability", () => {
  describe("withLogging + enterRequest", () => {
    it("derives a per-call child carrying request_id and call_id", async () => {
      const { logger, events } = createStubLogger();

      await withLogging(
        async () => {
          await enterRequest("eth_blockNumber", async (log, call_id) => {
            expect(log).toBeDefined();
            expect(call_id).toMatch(/^[0-9a-f-]{36}$/);
            log?.withMetadata({ ok: true }).info("test.emit");
          });
        },
        { logger, request_id: "rq-1", tag: "user-op" },
      );

      expect(events).toHaveLength(1);
      expect(events[0]!.name).toBe("test.emit");
      expect(events[0]!.context).toMatchObject({ request_id: "rq-1", tag: "user-op", method: "eth_blockNumber" });
      expect(events[0]!.context.call_id).toMatch(/^[0-9a-f-]{36}$/);
      expect(events[0]!.metadata).toMatchObject({ ok: true });
    });

    it("inner enterRequest reuses outer per-call child for progressive enrichment", async () => {
      const { logger, events } = createStubLogger();

      await withLogging(
        async () => {
          await enterRequest("eth_getLogs", async (outerLog, outerCallId) => {
            await enterRequest("eth_getLogs", async (innerLog, innerCallId) => {
              expect(innerCallId).toBe(outerCallId);
              // Same LogLayer instance — so any layer's `.withContext()` enrichment
              // accumulates onto a single wide event per call.
              expect(innerLog).toBe(outerLog);
              innerLog?.info("inner.emit");
            });
            outerLog?.info("outer.emit");
          });
        },
        { logger },
      );

      expect(events).toHaveLength(2);
      expect(events[0]!.context.call_id).toBe(events[1]!.context.call_id);
    });

    it("parallel enterRequest calls get isolated call_ids", async () => {
      const { logger, events } = createStubLogger();

      await withLogging(
        () =>
          Promise.all([
            enterRequest("eth_call", async (log) => {
              log?.info("a.emit");
            }),
            enterRequest("eth_call", async (log) => {
              log?.info("b.emit");
            }),
          ]),
        { logger },
      );

      expect(events).toHaveLength(2);
      expect(events[0]!.context.call_id).not.toBe(events[1]!.context.call_id);
    });

    it("parallel withLogging scopes do not bleed request_id", async () => {
      const { logger: a, events: aEvents } = createStubLogger();
      const { logger: b, events: bEvents } = createStubLogger();

      await Promise.all([
        withLogging(
          async () => {
            await enterRequest("eth_blockNumber", async (log) => {
              log?.info("scope.a.emit");
            });
          },
          { logger: a, request_id: "A" },
        ),
        withLogging(
          async () => {
            await enterRequest("eth_blockNumber", async (log) => {
              log?.info("scope.b.emit");
            });
          },
          { logger: b, request_id: "B" },
        ),
      ]);

      expect(aEvents).toHaveLength(1);
      expect(bEvents).toHaveLength(1);
      expect(aEvents[0]!.context.request_id).toBe("A");
      expect(bEvents[0]!.context.request_id).toBe("B");
    });

    it("emits nothing when no scope is active", async () => {
      const { logger, events } = createStubLogger();
      // call enterRequest outside a withLogging scope
      await enterRequest("eth_blockNumber", async (log, call_id) => {
        expect(log).toBeUndefined();
        expect(call_id).toBeUndefined();
      });
      expect(events).toHaveLength(0);
      // use logger so it isn't reported unused
      void logger;
    });
  });

  describe("canonical emissions", () => {
    it("divider.completed accumulates split stats when a range error triggers a halving", async () => {
      const { logger, events } = createStubLogger();
      let firstFailure = true;
      const requestFn = vi.fn().mockImplementation(async () => {
        if (firstFailure) {
          firstFailure = false;
          throw Object.assign(new Error("query returned more than 10000 results"), { code: -32005 });
        }
        return [];
      });

      // Call handler directly to bypass viem's `buildRequest` retry layers.
      // Build a per-call child the same way `enterRequest` would.
      await withLogging(
        () =>
          enterRequest("eth_getLogs", (log, call_id) =>
            handleEthGetLogs(
              requestFn,
              [{ fromBlock: "0x0", toBlock: "0x10" }, undefined, { latestBlock: "0x20" }],
              { maxBlockRange: 100, alignTo: 1 },
              { log, call_id },
            ),
          ),
        { logger },
      );

      expect(events.find((e) => e.name === "divider.halving"), "should no longer emit per-halving events").toBeUndefined();

      const completed = events.find((e) => e.name === "divider.completed");
      expect(completed, "expected divider.completed terminal event").toBeDefined();
      expect(completed!.context).toMatchObject({
        "divider.from_block": 0,
        "divider.to_block": 0x10,
        "divider.range_blocks": 0x11,
        "divider.latest_block": 0x20,
        "divider.chunk_count": 1,
        "divider.total_logs": 0,
        "divider.split_count": 1,
        "divider.split_causes_range": 1,
        "divider.split_causes_timeout": 0,
        "divider.max_depth": 1,
      });
      expect(typeof completed!.context["divider.duration_ms"]).toBe("number");
    });

    it("failover emits failover.attempted with branch_errors and succeeded_index", async () => {
      const { logger, events } = createStubLogger();
      const a = vi.fn().mockRejectedValue(new Error("a-failed"));
      const b = vi.fn().mockResolvedValue("0x42");

      const transport = failover([
        custom({ request: a }, { retryCount: 0 }),
        custom({ request: b }, { retryCount: 0 }),
      ])({} as never);

      const result = await withLogging(() => transport.request({ method: "eth_blockNumber" }), { logger });
      expect(result).toBe("0x42");

      const attempted = events.find((e) => e.name === "failover.attempted");
      expect(attempted, "expected failover.attempted emission").toBeDefined();
      expect(attempted!.context).toMatchObject({
        "failover.succeeded_index": 1,
        "failover.branches_attempted": 2,
      });
      const errs = attempted!.context["failover.branch_errors"] as { message: string }[];
      expect(errs).toHaveLength(1);
      expect(errs[0]!.message).toContain("a-failed");

      const branchDurations = attempted!.context["failover.branch_durations_ms"] as number[];
      expect(branchDurations).toHaveLength(2);
      expect(branchDurations.every((d) => typeof d === "number" && d >= 0)).toBe(true);
      expect(typeof attempted!.context["failover.duration_ms"]).toBe("number");
    });
  });
});
