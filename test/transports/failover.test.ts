import type { Transport } from "viem";
import { describe, expect, it, vi } from "vitest";

import { defaultShouldThrow, failover } from "../../src/transports/failover/index.js";

const buildParams = { retryCount: 0 } as never;

/**
 * Raw mock transport that bypasses viem's `buildRequest` error-wrapping (which would
 * otherwise turn generic Errors into `UnknownRpcError` and break identity assertions).
 */
function rawTransport(request: ReturnType<typeof vi.fn>): Transport {
  const factory = (() => ({
    config: { key: "mock", name: "mock", type: "mock", retryCount: 0, request },
    request,
    value: undefined,
  })) as unknown as Transport;
  return factory;
}

function trackedTransport(request: ReturnType<typeof vi.fn>) {
  const buildSpy = vi.fn(rawTransport(request));
  return { transport: buildSpy as unknown as Transport, buildSpy };
}

describe("failover", () => {
  it("returns branch A's response when it succeeds (branch B is built but its request is not invoked)", async () => {
    const requestA = vi.fn().mockResolvedValue("0xA");
    const requestB = vi.fn().mockResolvedValue("0xB");
    const a = trackedTransport(requestA);
    const b = trackedTransport(requestB);

    const transport = failover([a.transport, b.transport])(buildParams);
    const result = await transport.request({ method: "eth_blockNumber" });

    expect(result).toBe("0xA");
    expect(requestA).toHaveBeenCalledTimes(1);
    expect(requestB).not.toHaveBeenCalled();
    // Both branches are built once at composition.
    expect(a.buildSpy).toHaveBeenCalledTimes(1);
    expect(b.buildSpy).toHaveBeenCalledTimes(1);
  });

  it("falls over to branch B when branch A throws a non-fatal error", async () => {
    const requestA = vi.fn().mockRejectedValue(new Error("connection refused"));
    const requestB = vi.fn().mockResolvedValue("0xB");
    const a = trackedTransport(requestA);
    const b = trackedTransport(requestB);

    const transport = failover([a.transport, b.transport])(buildParams);
    const result = await transport.request({ method: "eth_blockNumber" });

    expect(result).toBe("0xB");
    expect(requestA).toHaveBeenCalledTimes(1);
    expect(requestB).toHaveBeenCalledTimes(1);
  });

  it("propagates contract reverts immediately without trying the next branch", async () => {
    const revertErr = Object.assign(new Error("execution reverted: ds-math-sub-underflow"), { code: 3 });
    const requestA = vi.fn().mockRejectedValue(revertErr);
    const requestB = vi.fn().mockResolvedValue("0xB");
    const a = trackedTransport(requestA);
    const b = trackedTransport(requestB);

    const transport = failover([a.transport, b.transport])(buildParams);

    await expect(transport.request({ method: "eth_call", params: [] as never })).rejects.toThrow(/execution reverted/);
    expect(requestA).toHaveBeenCalledTimes(1);
    expect(requestB).not.toHaveBeenCalled();
  });

  it("propagates user-rejection errors immediately without trying the next branch", async () => {
    const userRejErr = Object.assign(new Error("User rejected the request."), { code: 4001 });
    const requestA = vi.fn().mockRejectedValue(userRejErr);
    const requestB = vi.fn().mockResolvedValue("0xB");
    const a = trackedTransport(requestA);
    const b = trackedTransport(requestB);

    const transport = failover([a.transport, b.transport])(buildParams);

    await expect(transport.request({ method: "eth_blockNumber" })).rejects.toMatchObject({ code: 4001 });
    expect(requestB).not.toHaveBeenCalled();
  });

  it("rethrows the last error when every branch fails", async () => {
    const errA = new Error("network timeout A");
    const errB = new Error("network timeout B");
    const requestA = vi.fn().mockRejectedValue(errA);
    const requestB = vi.fn().mockRejectedValue(errB);
    const a = trackedTransport(requestA);
    const b = trackedTransport(requestB);

    const transport = failover([a.transport, b.transport])(buildParams);

    await expect(transport.request({ method: "eth_blockNumber" })).rejects.toThrow(/network timeout B/);
    expect(requestA).toHaveBeenCalledTimes(1);
    expect(requestB).toHaveBeenCalledTimes(1);
  });

  it("calls each branch's factory exactly once across many requests (no per-request rebuild)", async () => {
    const requestA = vi.fn().mockResolvedValue("0xA");
    const requestB = vi.fn().mockResolvedValue("0xB");
    const a = trackedTransport(requestA);
    const b = trackedTransport(requestB);

    const transport = failover([a.transport, b.transport])(buildParams);

    for (let i = 0; i < 50; i++) {
      await transport.request({ method: "eth_blockNumber" });
    }

    expect(a.buildSpy).toHaveBeenCalledTimes(1);
    expect(b.buildSpy).toHaveBeenCalledTimes(1);
    expect(requestA).toHaveBeenCalledTimes(50);
    expect(requestB).not.toHaveBeenCalled();
  });

  it("preserves stateful inner-transport behavior across requests", async () => {
    // Simulates a stateful transport (e.g. a counter that would be reset if the
    // factory ran per-request, the way viem's stock fallback rebuilds).
    let callCount = 0;
    const requestFn = vi.fn(async () => {
      callCount += 1;
      return `0x${callCount.toString(16)}`;
    });
    const buildSpy = vi.fn(rawTransport(requestFn));

    const transport = failover([buildSpy as unknown as Transport, rawTransport(vi.fn())])(buildParams);

    const r1 = await transport.request({ method: "eth_blockNumber" });
    const r2 = await transport.request({ method: "eth_blockNumber" });

    expect(r1).toBe("0x1");
    expect(r2).toBe("0x2"); // counter incremented across requests, proving state persists
    expect(buildSpy).toHaveBeenCalledTimes(1);
  });

  it("supports more than two branches", async () => {
    const requestA = vi.fn().mockRejectedValue(new Error("A down"));
    const requestB = vi.fn().mockRejectedValue(new Error("B down"));
    const requestC = vi.fn().mockResolvedValue("0xC");

    const transport = failover([rawTransport(requestA), rawTransport(requestB), rawTransport(requestC)])(buildParams);

    const result = await transport.request({ method: "eth_blockNumber" });
    expect(result).toBe("0xC");
    expect(requestA).toHaveBeenCalledTimes(1);
    expect(requestB).toHaveBeenCalledTimes(1);
    expect(requestC).toHaveBeenCalledTimes(1);
  });

  it("respects a custom shouldThrow predicate", async () => {
    const fatalErr = Object.assign(new Error("auth"), { code: 42 });
    const requestA = vi.fn().mockRejectedValue(fatalErr);
    const requestB = vi.fn().mockResolvedValue("0xB");

    const transport = failover([rawTransport(requestA), rawTransport(requestB)], {
      shouldThrow: (err) => (err as { code?: number }).code === 42,
    })(buildParams);

    await expect(transport.request({ method: "eth_blockNumber" })).rejects.toMatchObject({ message: /auth/ });
    expect(requestB).not.toHaveBeenCalled();
  });

  it("rejects empty transport arrays at composition time", () => {
    expect(() => failover([])).toThrow(/at least one transport/i);
  });
});

describe("defaultShouldThrow", () => {
  it("throws on contract revert (code 3)", () => {
    expect(defaultShouldThrow(Object.assign(new Error("execution reverted"), { code: 3 }))).toBe(true);
  });

  it("throws on TransactionRejectedRpcError (code -32003)", () => {
    expect(defaultShouldThrow(Object.assign(new Error("rejected"), { code: -32003 }))).toBe(true);
  });

  it("throws on UserRejectedRequestError (code 4001)", () => {
    expect(defaultShouldThrow(Object.assign(new Error("user rejected"), { code: 4001 }))).toBe(true);
  });

  it("throws on CAIP UserRejectedRequestError (code 5000)", () => {
    expect(defaultShouldThrow(Object.assign(new Error("user rejected"), { code: 5000 }))).toBe(true);
  });

  it("throws on revert message when code isn't in the special-cased list", () => {
    expect(defaultShouldThrow(Object.assign(new Error("execution reverted with reason: nope"), { code: 0 }))).toBe(
      true,
    );
    expect(defaultShouldThrow(Object.assign(new Error("gas required exceeds allowance"), { code: 0 }))).toBe(true);
  });

  it("does not throw on generic network errors", () => {
    expect(defaultShouldThrow(new Error("connection refused"))).toBe(false);
    expect(defaultShouldThrow(Object.assign(new Error("timeout"), { code: -32000 }))).toBe(false);
  });

  it("handles non-error values gracefully", () => {
    expect(defaultShouldThrow(null)).toBe(false);
    expect(defaultShouldThrow(undefined)).toBe(false);
    expect(defaultShouldThrow("string")).toBe(false);
  });
});
