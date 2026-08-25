import type { Address, Hex } from "viem";
import {
  type AbiFunction,
  BaseError,
  concat,
  custom,
  decodeAbiParameters,
  deploylessCallViaFactoryBytecode,
  encodeAbiParameters,
  encodeDeployData,
  pad,
  parseAbiItem,
  toFunctionSelector,
  toHex,
} from "viem";
import { describe, expect, it, vi } from "vitest";

import { withLogging } from "../../src/observability.js";
import { deployless } from "../../src/transports/deployless/index.js";
import { ETH_CALL_POLICY_ADDRESS } from "../../src/transports/state-overrides.js";
import type { EIP1193Parameters } from "../../src/types.js";
import {
  FACTORY_BYTECODE_REVERT,
  OK_SENTINEL,
  OOG_SENTINEL,
  unwrapDeploylessFactoryCall,
  wrapDeploylessFactoryCall,
} from "../../src/utils/deployless/codec.envelope.js";
import { flzDecompress } from "../../src/utils/deployless/flz.js";
import { createStubLogger, findDotted } from "../helpers/logger.js";

type EthCallRequest = EIP1193Parameters<import("viem").PublicRpcSchema, "eth_call">;

const TARGET_TO = "0x1111111111111111111111111111111111111111" as const;
const FACTORY = "0x2222222222222222222222222222222222222222" as const;
const FACTORY_DATA = "0xcafebabe" as const;

const balancesOfAbi = parseAbiItem("function balancesOf(address[] accounts) view returns (uint256[])") as AbiFunction;

function buildTargetCalldata(abi: AbiFunction, addrs: readonly Address[]): Hex {
  return concat([toFunctionSelector(abi), encodeAbiParameters([{ type: "address[]" }], [addrs])]);
}

/** Inbound shape: viem's stock RETURN-mode wrapper. The transport re-wraps for upstream. */
function buildDeploylessCall(targetData: Hex): Hex {
  return encodeDeployData({
    abi: [
      {
        type: "constructor",
        inputs: [
          { name: "to", type: "address" },
          { name: "data", type: "bytes" },
          { name: "factory", type: "address" },
          { name: "factoryData", type: "bytes" },
        ],
        stateMutability: "nonpayable",
      },
    ],
    bytecode: deploylessCallViaFactoryBytecode,
    args: [TARGET_TO, targetData, FACTORY, FACTORY_DATA],
  });
}

type PolicyOpts = {
  batch?: {
    batchSize?: number;
    compress?: boolean;
    gas?: { constant: number; linear: number; quadratic: number };
  };
  withCache?: boolean;
};

function policySentinel(abi: AbiFunction, opts: PolicyOpts = {}) {
  const policy: Record<string, unknown> = { abi };
  if (opts.batch) policy.batch = opts.batch;
  if (opts.withCache) policy.cache = { blobKey: "test-blob", ttl: 60_000 };
  return {
    [ETH_CALL_POLICY_ADDRESS]: { code: toHex(JSON.stringify(policy)) },
  };
}

type RequestOpts = PolicyOpts & { abi?: AbiFunction };

function createRequest(addrs: readonly Address[], opts: RequestOpts = {}): EthCallRequest {
  const abi = opts.abi ?? balancesOfAbi;
  return {
    method: "eth_call",
    params: [
      { data: buildDeploylessCall(buildTargetCalldata(abi, addrs)) },
      "latest",
      policySentinel(abi, { batch: opts.batch, withCache: opts.withCache }),
    ],
  };
}

/** Recovers `addrs` from the upstream-wrapped data (works for either RETURN or REVERT prefix). */
function decodeSentAddresses(data: Hex): readonly Address[] {
  const { targetData } = unwrapDeploylessFactoryCall(data);
  const [addrs] = decodeAbiParameters([{ type: "address[]" }], `0x${targetData.slice(10)}` as Hex);
  return addrs as readonly Address[];
}

/**
 * Mode-aware mock. Inspects the wrapper prefix on the outgoing `data`:
 *   - RETURN wrapper → resolves with the encoded uint256[].
 *   - REVERT wrapper        → throws an error with `.data` = OK_SENTINEL || encoded uint256[].
 *
 * Behavioral tests can stay mode-agnostic; both code paths exercise the same logic.
 */
function mockBalancesOfFn() {
  return vi.fn().mockImplementation(async (args: { method: string; params: readonly unknown[] }) => {
    const data = (args.params[0] as { data: Hex }).data;
    const outputs = decodeSentAddresses(data).map((a) => BigInt(a));
    const encoded = encodeAbiParameters([{ type: "uint256[]" }], [outputs]);
    throw revertWithSentinel(encoded);
  });
}

/**
 * Full-fidelity mock.
 * - Decompresses incoming targetData when compress=true (input-only compression).
 * - Returns raw (uncompressed) output regardless of compress flag.
 */
function mockCompressibleFn(compress: boolean) {
  return vi.fn().mockImplementation(async (args: { method: string; params: readonly unknown[] }) => {
    const data = (args.params[0] as { data: Hex }).data;
    const { targetData: raw } = unwrapDeploylessFactoryCall(data);
    const targetData = compress ? flzDecompress(raw) : raw;
    const [addrs] = decodeAbiParameters([{ type: "address[]" }], `0x${targetData.slice(10)}` as Hex);
    const outputs = (addrs as readonly Address[]).map((a) => BigInt(a));
    const encoded = encodeAbiParameters([{ type: "uint256[]" }], [outputs]);
    throw revertWithSentinel(encoded);
  });
}

/** Builds a viem-shaped error whose `.data` field carries OK_SENTINEL || payload. */
function revertWithSentinel(payload: Hex): Error & { data: Hex } {
  const err = new Error("execution reverted") as Error & { data: Hex };
  err.data = `${OK_SENTINEL}${payload.slice(2)}` as Hex;
  return err;
}

/** Builds a viem-shaped error whose `.data` is a raw revert (no sentinel — like a real lens revert). */
function revertRaw(data: Hex): Error & { data: Hex } {
  const err = new Error("execution reverted") as Error & { data: Hex };
  err.data = data;
  return err;
}

function createTransport(requestFn: ReturnType<typeof vi.fn>, gasLimit = 30_000_000) {
  return deployless(custom({ request: requestFn as never }), { gasLimit })({ retryCount: 0 } as never);
}

const addr = (n: number) => pad(toHex(n), { size: 20 });

describe("deployless", () => {
  it("passes unmarked eth_call requests through unchanged", async () => {
    const requestFn = vi.fn().mockResolvedValue("0x1234");
    const transport = createTransport(requestFn);
    const req: EthCallRequest = {
      method: "eth_call",
      params: [{ data: "0xabcdef" as Hex }, "latest"],
    };

    const result = await transport.request(req);

    expect(result).toBe("0x1234");
    // viem's `buildRequest` appends an options argument, so assert on the payload only.
    expect(requestFn).toHaveBeenCalledOnce();
    expect(requestFn.mock.lastCall?.[0]).toEqual(req);
  });

  it("passes non-eth_call requests through unchanged", async () => {
    const requestFn = vi.fn().mockResolvedValue("0x64");
    const transport = createTransport(requestFn);

    const result = await transport.request({ method: "eth_blockNumber" });

    expect(result).toBe("0x64");
    expect(requestFn).toHaveBeenCalledOnce();
    expect(requestFn.mock.lastCall?.[0]).toEqual({ method: "eth_blockNumber" });
  });

  describe("batching", () => {
    it("batchSize splits marked deployless calls without exceeding the byte budget", async () => {
      // Both wrappers are ~130 bytes, so one budget splits 5×32-byte elements in either mode.
      const batchSize = 520;
      const overshootCap = 600;
      const requestFn = mockBalancesOfFn();
      const transport = createTransport(requestFn);
      const req = createRequest([addr(1), addr(2), addr(3), addr(4), addr(5)], { batch: { batchSize } });

      const result = await transport.request(req);

      expect(requestFn.mock.calls.length).toBeGreaterThan(1);
      for (const [arg] of requestFn.mock.calls) {
        const data = (arg.params[0] as { data: Hex }).data;
        // packByCalldataBytes guarantees ≥1 element per batch, so a single oversize
        // element may push past `batchSize`. Cap at a sensible single-element ceiling.
        expect((data.length - 2) / 2).toBeLessThanOrEqual(overshootCap);
      }

      const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
      expect(decoded).toEqual([addr(1), addr(2), addr(3), addr(4), addr(5)].map((a) => BigInt(a)));
    });

    it("stamps input_elements, nominal_batches, and splits onto the wide event", async () => {
      const batchSize = 520;
      const requestFn = mockBalancesOfFn();
      const transport = createTransport(requestFn);
      const req = createRequest([addr(1), addr(2), addr(3), addr(4), addr(5)], { batch: { batchSize } });

      const { logger, events } = createStubLogger();
      await withLogging(() => transport.request(req), { logger });

      expect(events).toHaveLength(1);
      const { context } = events[0]!;
      const field = (name: string) => findDotted(context, "viem-dlc-deployless", `eth_call.${name}`);
      expect(field("input_elements")).toBe(5);
      expect(field("elements_requested")).toBe(5);
      expect(field("elements_fetched")).toBe(5);
      expect(field("nominal_batches")).toBeGreaterThan(1);
      expect(field("splits_count")).toBe(0);
      expect(field("splits_size")).toBe(0);
      expect(field("splits_timeout")).toBe(0);
      expect(field("splits_max_depth")).toBe(0);
      // Non-paged lens: the paged-only fields are absent, which is how a consumer tells
      // a paged run from an ordinary one.
      expect(field("pages_waves")).toBeUndefined();
      expect(field("pages_continued")).toBeUndefined();
      expect(field("elements_missing")).toBeUndefined();

      // One packed-size sample per batch, none exceeding the budget they were packed under.
      expect(field("batch_bytes.count")).toBe(field("nominal_batches"));
      expect(field("batch_bytes.max")).toBeLessThanOrEqual(batchSize);
    });

    it("forwards block, cleaned stateOverride, and blockOverride upstream", async () => {
      const requestFn = mockBalancesOfFn();
      const transport = createTransport(requestFn);
      const extraOverride = { "0x4444444444444444444444444444444444444444": { balance: "0x1" } } as const;
      const blockOverride = { time: "0x1234" } as NonNullable<EthCallRequest["params"][3]>;
      const req: EthCallRequest = {
        method: "eth_call",
        params: [
          { data: buildDeploylessCall(buildTargetCalldata(balancesOfAbi, [addr(1)])) },
          "0x100" as Hex,
          { ...policySentinel(balancesOfAbi, { batch: { batchSize: 8192 } }), ...extraOverride },
          blockOverride,
        ],
      };

      await transport.request(req);

      const call = requestFn.mock.calls[0]![0] as { params: readonly unknown[] };
      expect(call.params[1]).toBe("0x100");
      expect(call.params[2]).toEqual(extraOverride);
      expect(call.params[3]).toEqual(blockOverride);
    });
  });

  it("returns immediately for empty input arrays", async () => {
    const requestFn = vi.fn();
    const transport = createTransport(requestFn);

    const result = await transport.request(createRequest([]));

    expect(requestFn).not.toHaveBeenCalled();
    const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
    expect(decoded).toEqual([]);
  });

  it("throws when policy is present but data is missing", async () => {
    const transport = createTransport(vi.fn());
    const req: EthCallRequest = {
      method: "eth_call",
      params: [{}, "latest", policySentinel(balancesOfAbi)],
    };

    await expect(transport.request(req)).rejects.toThrow(/requires `data`/);
  });

  it("throws when marked calls include unsupported tx fields", async () => {
    const transport = createTransport(vi.fn());
    const req: EthCallRequest = {
      method: "eth_call",
      params: [
        {
          data: buildDeploylessCall(buildTargetCalldata(balancesOfAbi, [addr(1)])),
          to: "0x3333333333333333333333333333333333333333",
          from: "0x5555555555555555555555555555555555555555",
          gas: "0xffff",
          value: "0x0",
        },
        "latest",
        policySentinel(balancesOfAbi),
      ],
    };

    await expect(transport.request(req)).rejects.toThrow(/found extras:.*to.*from.*gas.*value/);
  });

  it("throws when data is not a deployless factory wrapper", async () => {
    const transport = createTransport(vi.fn());
    const req: EthCallRequest = {
      method: "eth_call",
      params: [{ data: "0xabcdef" as Hex }, "latest", policySentinel(balancesOfAbi)],
    };

    await expect(transport.request(req)).rejects.toThrow(/deployless factory wrapper/);
  });

  it("throws when policy abi is not a single-array-in/single-array-out function", async () => {
    const badAbi = parseAbiItem("function foo(address) view returns (uint256[])") as AbiFunction;
    const transport = createTransport(vi.fn());

    await expect(transport.request(createRequest([addr(1)], { abi: badAbi }))).rejects.toThrow(/dynamic-array input/);
  });

  it("throws when target calldata selector mismatches the policy abi", async () => {
    const otherAbi = parseAbiItem("function otherFn(address[] xs) view returns (uint256[])") as AbiFunction;
    const transport = createTransport(vi.fn());
    const req: EthCallRequest = {
      method: "eth_call",
      params: [
        { data: buildDeploylessCall(buildTargetCalldata(balancesOfAbi, [addr(1)])) },
        "latest",
        policySentinel(otherAbi),
      ],
    };

    await expect(transport.request(req)).rejects.toThrow(/selector/);
  });

  it("throws when upstream returns the wrong number of outputs", async () => {
    const requestFn = vi
      .fn()
      .mockRejectedValue(revertWithSentinel(encodeAbiParameters([{ type: "uint256[]" }], [[1n, 2n]])));
    const transport = createTransport(requestFn);
    const req = createRequest([addr(1), addr(2), addr(3)], { batch: { batchSize: 8192 } });

    await expect(transport.request(req)).rejects.toThrow(/returned 2.*expected 3/);
  });

  it("ignores policy.cache and behaves like split-only mode", async () => {
    const addrs = [addr(1), addr(2), addr(3), addr(4), addr(5)];
    const requestFnWithoutCache = mockBalancesOfFn();
    const requestFnWithCache = mockBalancesOfFn();
    const transportWithoutCache = createTransport(requestFnWithoutCache);
    const transportWithCache = createTransport(requestFnWithCache);

    const withoutCache = await transportWithoutCache.request(createRequest(addrs, { batch: { batchSize: 1088 } }));
    const withCache = await transportWithCache.request(
      createRequest(addrs, { batch: { batchSize: 1088 }, withCache: true }),
    );

    expect(withCache).toEqual(withoutCache);
    expect(requestFnWithCache.mock.calls.length).toBe(requestFnWithoutCache.mock.calls.length);
    expect(requestFnWithCache.mock.calls.map(([arg]) => (arg.params[0] as { data: Hex }).data.toLowerCase())).toEqual(
      requestFnWithoutCache.mock.calls.map(([arg]) => (arg.params[0] as { data: Hex }).data.toLowerCase()),
    );
  });

  describe("revert-mode wrapper", () => {
    it("propagates a real lens revert verbatim (no sentinel)", async () => {
      const lensRevertData = "0xabcd1234" as Hex;
      const requestFn = vi.fn().mockImplementation(async () => {
        throw revertRaw(lensRevertData);
      });
      const transport = createTransport(requestFn);

      try {
        await transport.request(createRequest([addr(1)]));
        throw new Error("expected throw");
      } catch (e) {
        // The transport rethrows real lens reverts verbatim. Walk the BaseError chain
        // (viem may wrap) and check the raw revert hex is preserved.
        const walked = e instanceof BaseError ? e.walk() : e;
        const data = (walked as { data?: unknown }).data;
        const raw = typeof data === "string" ? data : (data as { data?: string } | null)?.data;
        expect(raw).toBe(lensRevertData);
      }
    });

    it("decodes empty success (sentinel-only revert) as empty output array", async () => {
      const requestFn = vi.fn().mockImplementation(async () => {
        // Encode an empty uint256[] (just the offset+length=0 words).
        const empty = encodeAbiParameters([{ type: "uint256[]" }], [[]]);
        throw revertWithSentinel(empty);
      });
      const transport = createTransport(requestFn);
      const req = createRequest([], { batch: { batchSize: 1088 } });

      const result = await transport.request(req);
      const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
      expect(decoded).toEqual([]);
    });

    it("decodes sentinel data from an intermediate BaseError", async () => {
      const encoded = encodeAbiParameters([{ type: "uint256[]" }], [[1n]]);
      const dataError = Object.assign(new BaseError("rpc", { cause: new Error("inner") }), {
        data: `${OK_SENTINEL}${encoded.slice(2)}` as Hex,
      });
      const requestFn = vi.fn().mockRejectedValue(new BaseError("outer", { cause: dataError }));
      const transport = createTransport(requestFn);

      const result = await transport.request(createRequest([addr(1)], { batch: { batchSize: 8192 } }));

      const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
      expect(decoded).toEqual([1n]);
    });

    it("uses REVERT-mode wrapper bytecode upstream (no batch opts)", async () => {
      const requestFn = mockBalancesOfFn();
      const transport = createTransport(requestFn);
      await transport.request(createRequest([addr(1)]));

      const sentData = (requestFn.mock.calls[0]![0] as { params: [{ data: Hex }] }).params[0].data;
      expect(sentData.toLowerCase().startsWith(FACTORY_BYTECODE_REVERT)).toBe(true);
    });
  });

  describe("compress=true", () => {
    it("round-trips addresses correctly", async () => {
      const addrs = [addr(1), addr(2), addr(3)];
      const requestFn = mockCompressibleFn(true);
      const transport = createTransport(requestFn);

      const result = await transport.request(createRequest(addrs, { batch: { batchSize: 8192, compress: true } }));

      const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
      expect(decoded).toEqual(addrs.map((a) => BigInt(a)));
    });

    it("does not use viem's stock RETURN bytecode as prefix", async () => {
      const requestFn = mockCompressibleFn("return", true);
      const transport = createTransport(requestFn);

      await transport.request(createRequest([addr(1)], { batch: { batchSize: 8192, compress: true } }));

      const sentData = (requestFn.mock.calls[0]![0] as { params: [{ data: Hex }] }).params[0].data;
      expect(sentData.toLowerCase().startsWith(deploylessCallViaFactoryBytecode.toLowerCase())).toBe(false);
    });

    it("keeps compressed chunks within the actual wrapped byte budget", async () => {
      const addrs = Array.from({ length: 200 }, () => addr(1));
      const singleWrapped = wrapDeploylessFactoryCall(
        {
          target: { address: TARGET_TO, factory: FACTORY, factoryData: FACTORY_DATA },
          targetData: buildTargetCalldata(balancesOfAbi, [addr(1)]),
        },
        { compress: true },
      );
      const batchSize = (singleWrapped.length - 2) / 2 + 8;
      const requestFn = mockCompressibleFn("return", true);
      const transport = createTransport(requestFn);

      const result = await transport.request(createRequest(addrs, { batch: { batchSize, compress: true } }));

      expect(requestFn.mock.calls.length).toBeGreaterThan(1);
      for (const [arg] of requestFn.mock.calls) {
        const data = (arg.params[0] as { data: Hex }).data;
        expect((data.length - 2) / 2).toBeLessThanOrEqual(batchSize);
      }
      const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
      expect(decoded).toHaveLength(addrs.length);
    });
  });

  describe("halve-on-error retries", () => {
    it("halves and retries when upstream rejects with a batch-size error", async () => {
      const addrs = [addr(1), addr(2), addr(3), addr(4)];
      let firstCall = true;
      const requestFn = vi.fn().mockImplementation(async (args: { method: string; params: readonly unknown[] }) => {
        // First call (full batch) → batch-size error; subsequent calls (halves) → succeed.
        if (firstCall) {
          firstCall = false;
          const err = new Error("request body too large") as Error & { data: Hex };
          err.data = "0x" as Hex;
          throw err;
        }
        const data = (args.params[0] as { data: Hex }).data;
        const outputs = decodeSentAddresses(data).map((a) => BigInt(a));
        throw revertWithSentinel(encodeAbiParameters([{ type: "uint256[]" }], [outputs]));
      });
      const transport = createTransport(requestFn);
      const req = createRequest(addrs, { batch: { batchSize: 8192 } });

      const result = await transport.request(req);

      expect(requestFn).toHaveBeenCalledTimes(3); // 1 failed + 2 halves
      const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
      expect(decoded).toEqual(addrs.map((a) => BigInt(a)));
    });

    it("bisects to singletons on the wrapper's out-of-gas marker", async () => {
      // Stands in for a chunk whose lens frame ran out of gas: every multi-element chunk reports
      // OOG, so the batcher must keep halving until each element is alone.
      const addrs = [addr(1), addr(2), addr(3), addr(4)];
      const requestFn = vi.fn().mockImplementation(async (args: { method: string; params: readonly unknown[] }) => {
        const data = (args.params[0] as { data: Hex }).data;
        const sent = decodeSentAddresses(data);
        if (sent.length > 1) throw Object.assign(new Error("execution reverted"), { data: OOG_SENTINEL });
        throw revertWithSentinel(encodeAbiParameters([{ type: "uint256[]" }], [sent.map((a) => BigInt(a))]));
      });
      const transport = createTransport(requestFn);

      const result = await transport.request(createRequest(addrs, { batch: { batchSize: 8192 } }));

      // 1 full batch + 2 halves + 4 singletons.
      expect(requestFn).toHaveBeenCalledTimes(7);
      const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
      expect(decoded).toEqual(addrs.map((a) => BigInt(a)));
    });

    it("does not treat lens revert data that merely starts with the marker as out-of-gas", async () => {
      const lensError = Object.assign(new Error("execution reverted"), {
        data: `${OOG_SENTINEL}${"00".repeat(32)}` as Hex,
      });
      const requestFn = vi.fn().mockRejectedValue(lensError);
      const transport = createTransport(requestFn);

      await expect(
        transport.request(createRequest([addr(1), addr(2)], { batch: { batchSize: 8192 } })),
      ).rejects.toThrow("execution reverted");
      expect(requestFn).toHaveBeenCalledTimes(1);
    });

    it("rethrows when a single-element batch fails with a batch-size error", async () => {
      const batchSizeError = Object.assign(new Error("request body too large"), { data: "0x" as Hex });
      const requestFn = vi.fn().mockRejectedValue(batchSizeError);
      const transport = createTransport(requestFn);

      await expect(transport.request(createRequest([addr(1)], { batch: { batchSize: 8192 } }))).rejects.toThrow(
        "request body too large",
      );
      expect(requestFn).toHaveBeenCalledTimes(1);
    });

    it("does not retry on unrecognized errors", async () => {
      const unrelated = new Error("nonce too low");
      const requestFn = vi.fn().mockRejectedValue(unrelated);
      const transport = createTransport(requestFn);

      await expect(
        transport.request(createRequest([addr(1), addr(2)], { batch: { batchSize: 8192 } })),
      ).rejects.toThrow("nonce too low");
      expect(requestFn).toHaveBeenCalledTimes(1);
    });

    it.each([
      ["TimeoutError name", () => Object.assign(new Error("aborted"), { name: "TimeoutError" })],
      ["timeout message", () => new Error("request timed out after 10000ms")],
      ["HTTP 504", () => Object.assign(new Error("gateway timeout"), { status: 504 })],
    ])("halves once on timeout (%s) and rethrows if the halves also time out", async (_label, makeError) => {
      const addrs = [addr(1), addr(2), addr(3), addr(4)];
      const requestFn = vi.fn().mockImplementation(async () => {
        throw makeError();
      });
      const transport = createTransport(requestFn);

      await expect(transport.request(createRequest(addrs, { batch: { batchSize: 8192 } }))).rejects.toThrow();
      // 1 full-batch attempt + 2 half-batch attempts; halves exhaust the timeout budget so no further bisect.
      expect(requestFn).toHaveBeenCalledTimes(3);
    });

    it("halves on timeout and succeeds when smaller chunks come back", async () => {
      const addrs = [addr(1), addr(2), addr(3), addr(4)];
      let firstCall = true;
      const requestFn = vi.fn().mockImplementation(async (args: { method: string; params: readonly unknown[] }) => {
        if (firstCall) {
          firstCall = false;
          throw Object.assign(new Error("request timed out"), { name: "TimeoutError" });
        }
        const data = (args.params[0] as { data: Hex }).data;
        const outputs = decodeSentAddresses(data).map((a) => BigInt(a));
        throw revertWithSentinel(encodeAbiParameters([{ type: "uint256[]" }], [outputs]));
      });
      const transport = createTransport(requestFn);

      const result = await transport.request(createRequest(addrs, { batch: { batchSize: 8192 } }));

      expect(requestFn).toHaveBeenCalledTimes(3); // 1 timed-out + 2 halves
      const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
      expect(decoded).toEqual(addrs.map((a) => BigInt(a)));
    });
  });

  describe("gas budget", () => {
    it("splits chunks by largest N with G(N) ≤ gasLimit when gas is the binding constraint", async () => {
      // 5 elements, gasLimit 30M, G(N) = 12M·N → max 2 items per chunk → 3 chunks (2+2+1).
      const addrs = [addr(1), addr(2), addr(3), addr(4), addr(5)];
      const requestFn = mockBalancesOfFn();
      const transport = createTransport(requestFn, 30_000_000);
      const req = createRequest(addrs, {
        batch: { gas: { constant: 0, linear: 12_000_000, quadratic: 0 } },
      });

      const result = await transport.request(req);

      expect(requestFn).toHaveBeenCalledTimes(3);
      for (const [arg] of requestFn.mock.calls) {
        const data = (arg.params[0] as { data: Hex }).data;
        expect(decodeSentAddresses(data).length).toBeLessThanOrEqual(2);
      }
      const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
      expect(decoded).toEqual(addrs.map((a) => BigInt(a)));
    });

    it("does not constrain by gas when `gas` is unset", async () => {
      const addrs = [addr(1), addr(2), addr(3), addr(4), addr(5)];
      const requestFn = mockBalancesOfFn();
      const transport = createTransport(requestFn, 1_000_000); // tight gasLimit, but no model
      const req = createRequest(addrs, { batch: {} });

      await transport.request(req);

      expect(requestFn).toHaveBeenCalledTimes(1);
    });

    it("subtracts `constant` overhead from the gas budget before solving", async () => {
      // gasLimit 1100, G(N) = 1000 + 100·N → at N=1, G=1100 (fits exactly). N=2 would overflow.
      const addrs = [addr(1), addr(2)];
      const requestFn = mockBalancesOfFn();
      const transport = createTransport(requestFn, 1100);
      const req = createRequest(addrs, {
        batch: { gas: { constant: 1000, linear: 100, quadratic: 0 } },
      });

      await transport.request(req);

      expect(requestFn).toHaveBeenCalledTimes(2);
      for (const [arg] of requestFn.mock.calls) {
        const data = (arg.params[0] as { data: Hex }).data;
        expect(decodeSentAddresses(data).length).toBe(1);
      }
    });

    it("solves the quadratic when `quadratic` is set", async () => {
      // gasLimit 100, G(N) = N² → max N = floor(sqrt(100)) = 10. 12 elements → 2 chunks (10+2).
      const addrs = Array.from({ length: 12 }, (_, i) => addr(i + 1));
      const requestFn = mockBalancesOfFn();
      const transport = createTransport(requestFn, 100);
      const req = createRequest(addrs, {
        batch: { gas: { constant: 0, linear: 0, quadratic: 1 } },
      });

      await transport.request(req);

      expect(requestFn).toHaveBeenCalledTimes(2);
      const sizes = requestFn.mock.calls
        .map(([arg]) => decodeSentAddresses((arg.params[0] as { data: Hex }).data).length)
        .sort((a, b) => a - b);
      expect(sizes).toEqual([2, 10]);
    });

    it("throws synchronously when one item exceeds the gas budget (no upstream calls)", async () => {
      const requestFn = vi.fn();
      const transport = createTransport(requestFn, 10_000_000);
      const req = createRequest([addr(1), addr(2)], {
        batch: { gas: { constant: 0, linear: 11_000_000, quadratic: 0 } },
      });

      await expect(transport.request(req)).rejects.toThrow(/cannot fit a single item/);
      expect(requestFn).not.toHaveBeenCalled();
    });

    it("intersects byte and gas budgets — whichever is tighter wins per chunk", async () => {
      // 6 elements. Gas allows 3 per chunk; bytes allow ≥4. Gas wins → 2 chunks of 3.
      const addrs = [addr(1), addr(2), addr(3), addr(4), addr(5), addr(6)];
      const requestFn = mockBalancesOfFn();
      const transport = createTransport(requestFn, 30_000_000);
      const req = createRequest(addrs, {
        batch: { batchSize: 8192, gas: { constant: 0, linear: 10_000_000, quadratic: 0 } },
      });

      await transport.request(req);

      expect(requestFn).toHaveBeenCalledTimes(2);
      for (const [arg] of requestFn.mock.calls) {
        const data = (arg.params[0] as { data: Hex }).data;
        expect(decodeSentAddresses(data).length).toBeLessThanOrEqual(3);
      }
    });

    it("supports gas-only chunking when batchSize is unset", async () => {
      // 4 elements, gasLimit 30M, G(N) = 15M·N → max 2 per chunk → 2 chunks.
      const addrs = [addr(1), addr(2), addr(3), addr(4)];
      const requestFn = mockBalancesOfFn();
      const transport = createTransport(requestFn, 30_000_000);
      const req = createRequest(addrs, {
        batch: { gas: { constant: 0, linear: 15_000_000, quadratic: 0 } },
      });

      await transport.request(req);

      expect(requestFn).toHaveBeenCalledTimes(2);
      for (const [arg] of requestFn.mock.calls) {
        const data = (arg.params[0] as { data: Hex }).data;
        expect(decodeSentAddresses(data).length).toBe(2);
      }
    });
  });
});
