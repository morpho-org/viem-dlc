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
  getAddress,
  pad,
  parseAbiItem,
  parseAbiParameters,
  toFunctionSelector,
  toHex,
} from "viem";
import { describe, expect, it, vi } from "vitest";

import { MAX_INITCODE_SIZE } from "../../src/actions/call.js";
import { withLogging } from "../../src/observability.js";
import { type DeploylessConfig, deployless } from "../../src/transports/deployless/index.js";
import { ETH_CALL_POLICY_ADDRESS } from "../../src/transports/state-overrides.js";
import type { EIP1193Parameters } from "../../src/types.js";
import type { LensGas } from "../../src/utils/deployless/call.js";
import {
  COUNTERFACTUAL_DEPLOY_FAILED_SELECTOR,
  envelopeConfig,
  FACTORY_BYTECODE_REVERT,
  OK_SENTINEL,
  OOG_SENTINEL,
  unwrapDeploylessFactoryCall,
  wrapDeploylessFactoryCall,
} from "../../src/utils/deployless/codec.envelope.js";
import { arrayToWire, pageToWire, resolveArrayFunction, wireToArray } from "../../src/utils/deployless/codec.inner.js";
import { createStubLogger, findDotted } from "../helpers/logger.js";
import { flatGas } from "../helpers/page.js";

type EthCallRequest = EIP1193Parameters<import("viem").PublicRpcSchema, "eth_call">;

const TARGET_TO = "0x1111111111111111111111111111111111111111" as const;
const FACTORY = "0x2222222222222222222222222222222222222222" as const;
const FACTORY_DATA = "0xcafebabe" as const;

const pageAbi = parseAbiItem(
  "function balancesOf(address[] accounts) view returns (uint256[] results, uint256[] skipped)",
) as AbiFunction;

const addr = (n: number) => pad(toHex(n), { size: 20 });
const addrs = (n: number) => Array.from({ length: n }, (_, i) => addr(i + 1));
const word = (n: number | bigint | Hex) => BigInt(n).toString(16).padStart(64, "0");
const WORD = { mode: "static", size: 32 } as const;

/** The envelope's config word for {@link pageAbi} — invariant across chunks. */
const CONFIG = envelopeConfig(resolveArrayFunction(pageAbi), false);

/** The per-item function {@link pageAbi} paginates, as the envelope must call it. */
const ITEM_SELECTOR = toFunctionSelector("function balancesOf(address) view returns (uint256)");

function buildTargetCalldata(abi: AbiFunction, accounts: readonly Address[]): Hex {
  return concat([toFunctionSelector(abi), encodeAbiParameters([{ type: "address[]" }], [accounts])]);
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

/** Bytes the transport puts on the wire for a chunk of `count` elements. */
function wireBytesFor(count: number, compress = false): number {
  const wrapped = wrapDeploylessFactoryCall(
    {
      target: { address: TARGET_TO, factory: FACTORY, factoryData: FACTORY_DATA },
      targetData: arrayToWire(
        WORD,
        addrs(count).map((a) => pad(a, { size: 32 })),
      ),
    },
    { compress, config: envelopeConfig(resolveArrayFunction(pageAbi), compress) },
  );
  return (wrapped.length - 2) / 2;
}

const byteLength = (hex: Hex) => (hex.length - 2) / 2;

type PolicyOpts = {
  batch?: { batchSize?: number; compress?: boolean; gas?: LensGas };
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

function createRequest(accounts: readonly Address[], opts: RequestOpts = {}): EthCallRequest {
  const abi = opts.abi ?? pageAbi;
  return {
    method: "eth_call",
    params: [
      { data: buildDeploylessCall(buildTargetCalldata(abi, accounts)) },
      "latest",
      policySentinel(abi, { batch: opts.batch, withCache: opts.withCache }),
    ],
  };
}

/** Recovers the accounts from the upstream-wrapped data, compressed or not. */
function decodeSentAddresses(data: Hex): readonly Address[] {
  const { targetData } = unwrapDeploylessFactoryCall(data);
  return wireToArray(WORD, targetData).map((w) => getAddress(`0x${w.slice(26)}`));
}

/** The `data` field of each upstream `eth_call`, in call order. */
function sentData(requestFn: ReturnType<typeof vi.fn>): Hex[] {
  return requestFn.mock.calls.map((call) => (call[0] as EthCallRequest).params[0].data as Hex);
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

function pageRevert(results: readonly bigint[], skipped: readonly number[] = []) {
  return revertWithSentinel(
    pageToWire({
      results: results.map((r) => `0x${word(r)}` as Hex),
      skipped,
      gas: flatGas(results.length + skipped.length),
    }),
  );
}

/** The wrapper always exfiltrates via REVERT, so a served page arrives as a sentinel-framed throw. */
function mockPagedFn() {
  return vi.fn().mockImplementation(async (args: { method: string; params: readonly unknown[] }) => {
    const data = (args.params[0] as { data: Hex }).data;
    throw pageRevert(decodeSentAddresses(data).map((a) => BigInt(a)));
  });
}

function createTransport(requestFn: ReturnType<typeof vi.fn>, config?: DeploylessConfig) {
  return deployless(custom({ request: requestFn as never }), config)({ retryCount: 0 } as never);
}

function decodeResults(result: unknown): readonly bigint[] {
  const [results] = decodeAbiParameters([{ type: "uint256[]" }, { type: "uint256[]" }], result as Hex);
  return results as readonly bigint[];
}

function decodePage(result: unknown): { results: bigint[]; skipped: number[] } {
  const [results, skipped] = decodeAbiParameters([{ type: "uint256[]" }, { type: "uint256[]" }], result as Hex);
  return { results: [...(results as readonly bigint[])], skipped: (skipped as readonly bigint[]).map(Number) };
}

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
      // Room for three 32-byte elements beside the envelope, so 5 elements need two chunks.
      const batchSize = wireBytesFor(3);
      const requestFn = mockPagedFn();
      const transport = createTransport(requestFn);

      const result = await transport.request(createRequest(addrs(5), { batch: { batchSize } }));

      expect(requestFn.mock.calls.length).toBe(2);
      for (const data of sentData(requestFn)) expect(byteLength(data)).toBeLessThanOrEqual(batchSize);
      expect(decodeResults(result)).toEqual(addrs(5).map((a) => BigInt(a)));
    });

    it("wraps each chunk in the 5-arg REVERT constructor carrying the envelope config word", async () => {
      const requestFn = mockPagedFn();
      const transport = createTransport(requestFn);

      await transport.request(createRequest(addrs(5), { batch: { batchSize: wireBytesFor(3) } }));

      const configs = sentData(requestFn).map((data) => {
        expect(data.toLowerCase().startsWith(FACTORY_BYTECODE_REVERT)).toBe(true);
        const [address, targetData, factory, factoryData, config] = decodeAbiParameters(
          parseAbiParameters("address, bytes, address, bytes, uint256"),
          `0x${data.slice(FACTORY_BYTECODE_REVERT.length)}` as Hex,
        );
        expect({ address, factory, factoryData }).toEqual({
          address: TARGET_TO,
          factory: FACTORY,
          factoryData: FACTORY_DATA,
        });
        // The structural decode agrees with the envelope's own inverse.
        expect(unwrapDeploylessFactoryCall(data).targetData).toBe(targetData);
        return config;
      });

      // The config describes the lens, not the chunk, so both chunks carry the same word.
      expect(configs).toEqual([CONFIG, CONFIG]);
      // Per-item selector in the top 32 bits, no flag bits set, both strides 32 bytes.
      expect(CONFIG).toBe((BigInt(ITEM_SELECTOR) << 224n) | (32n << 64n) | 32n);
    });

    it("stamps input_elements, nominal_batches, and splits onto the wide event", async () => {
      const batchSize = wireBytesFor(3);
      const requestFn = mockPagedFn();
      const transport = createTransport(requestFn);
      const req = createRequest(addrs(5), { batch: { batchSize } });

      const { logger, events } = createStubLogger();
      await withLogging(() => transport.request(req), { logger });

      expect(events).toHaveLength(1);
      const { context } = events[0]!;
      const field = (name: string) => findDotted(context, "viem-dlc-deployless", `eth_call.${name}`);
      expect(field("input_elements")).toBe(5);
      expect(field("elements_requested")).toBe(5);
      expect(field("elements_fetched")).toBe(5);
      expect(field("nominal_batches")).toBe(2);
      expect(field("splits_count")).toBe(0);
      expect(field("splits_size")).toBe(0);
      expect(field("splits_timeout")).toBe(0);
      expect(field("splits_max_depth")).toBe(0);
      expect(field("pages_waves")).toBe(1);
      expect(field("pages_continued")).toBe(0);
      expect(field("pages_escalated")).toBe(0);
      expect(field("attempts_unresolved")).toBe(0);
      expect(field("elements_declined_oversize")).toBe(0);
      expect(field("elements_missing")).toBe(0);
      expect(field("elements_unresolved")).toBe(0);

      // One sample per batch, none exceeding the budget it was packed under.
      expect(field("batch_bytes.count")).toBe(field("nominal_batches"));
      expect(field("batch_bytes.max")).toBeLessThanOrEqual(batchSize);
    });

    it("lets the smaller of the gas prediction and batchSize bind the opening wave", async () => {
      const batchSize = wireBytesFor(3);
      const gas = { fixed: 0, item: { avg: 1_000_000 } };
      const predicted = mockPagedFn();
      const capped = mockPagedFn();

      await createTransport(predicted, { gasLimit: 2_500_000 }).request(
        createRequest(addrs(5), { batch: { batchSize, gas } }),
      );
      await createTransport(capped, { gasLimit: 4_500_000 }).request(
        createRequest(addrs(5), { batch: { batchSize, gas } }),
      );

      expect(predicted.mock.calls.length).toBe(3);
      expect(capped.mock.calls.length).toBe(2);
    });

    it("sends everything in one chunk when no batchSize is set", async () => {
      const requestFn = mockPagedFn();
      const transport = createTransport(requestFn);

      const result = await transport.request(createRequest(addrs(40_000)));

      expect(requestFn).toHaveBeenCalledOnce();
      expect(decodeResults(result)).toHaveLength(40_000);
    });

    it("packs a wide output stride no differently: nothing is reserved for results", async () => {
      const wideAbi = parseAbiItem(
        "function wide(address[] accounts) view returns ((uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256)[] results, uint256[] skipped)",
      );
      const requestFn = vi.fn().mockImplementation(async (args: { params: readonly unknown[] }) => {
        const sent = decodeSentAddresses((args.params[0] as { data: Hex }).data);
        throw revertWithSentinel(
          pageToWire({
            results: sent.map((a) => `0x${word(a).repeat(8)}` as Hex),
            skipped: [],
            gas: flatGas(sent.length),
          }),
        );
      });
      const transport = createTransport(requestFn);
      const narrowFn = mockPagedFn();
      const narrow = createTransport(narrowFn);

      const wide = await transport.request(createRequest(addrs(40_000), { abi: wideAbi }));
      await narrow.request(createRequest(addrs(40_000)));

      expect(requestFn.mock.calls.length).toBe(narrowFn.mock.calls.length);
      const [results] = decodeAbiParameters([{ type: "uint256[8][]" }, { type: "uint256[]" }], wide as Hex);
      expect(results).toHaveLength(40_000);
    }, 20_000);

    it("packs a compression bomb as one chunk under the wire cap alone", async () => {
      // One address repeated compresses to a few hundred bytes; the decompressed size is the
      // envelope's concern, element by element, not the packer's.
      const requestFn = mockPagedFn();
      const transport = createTransport(requestFn);
      const req = createRequest(Array<Address>(40_000).fill(addrs(1)[0]!), {
        batch: { batchSize: MAX_INITCODE_SIZE, compress: true },
      });

      const { logger, events } = createStubLogger();
      const result = await withLogging(() => transport.request(req), { logger });
      const field = (name: string) => findDotted(events[0]!.context, "viem-dlc-deployless", `eth_call.${name}`);

      expect(requestFn).toHaveBeenCalledOnce();
      expect(field("batch_bytes.max")).toBeLessThan(MAX_INITCODE_SIZE);
      expect(decodeResults(result)).toHaveLength(40_000);
    });

    it("forwards block, cleaned stateOverride, and blockOverride upstream", async () => {
      const requestFn = mockPagedFn();
      const transport = createTransport(requestFn);
      const extraOverride = { "0x4444444444444444444444444444444444444444": { balance: "0x1" } } as const;
      const blockOverride = { time: "0x1234" } as NonNullable<EthCallRequest["params"][3]>;
      const req: EthCallRequest = {
        method: "eth_call",
        params: [
          { data: buildDeploylessCall(buildTargetCalldata(pageAbi, [addr(1)])) },
          "0x100" as Hex,
          { ...policySentinel(pageAbi, { batch: { batchSize: 8192 } }), ...extraOverride },
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
    expect(decodePage(result)).toEqual({ results: [], skipped: [] });
  });

  it("throws when policy is present but data is missing", async () => {
    const transport = createTransport(vi.fn());
    const req: EthCallRequest = {
      method: "eth_call",
      params: [{}, "latest", policySentinel(pageAbi)],
    };

    await expect(transport.request(req)).rejects.toThrow(/requires `data`/);
  });

  it("throws when marked calls include unsupported tx fields", async () => {
    const transport = createTransport(vi.fn());
    const req: EthCallRequest = {
      method: "eth_call",
      params: [
        {
          data: buildDeploylessCall(buildTargetCalldata(pageAbi, [addr(1)])),
          to: "0x3333333333333333333333333333333333333333",
          from: "0x5555555555555555555555555555555555555555",
          gas: "0xffff",
          value: "0x0",
        },
        "latest",
        policySentinel(pageAbi),
      ],
    };

    await expect(transport.request(req)).rejects.toThrow(/found extras:.*to.*from.*gas.*value/);
  });

  it("throws when data is not a deployless factory wrapper", async () => {
    const transport = createTransport(vi.fn());
    const req: EthCallRequest = {
      method: "eth_call",
      params: [{ data: "0xabcdef" as Hex }, "latest", policySentinel(pageAbi)],
    };

    await expect(transport.request(req)).rejects.toThrow(/deployless factory wrapper/);
  });

  it("throws when the policy abi does not take a dynamic array", async () => {
    const badAbi = parseAbiItem("function foo(address a) view returns (uint256[] r, uint256[] s)") as AbiFunction;
    const transport = createTransport(vi.fn());

    await expect(transport.request(createRequest([addr(1)], { abi: badAbi }))).rejects.toThrow(/dynamic-array input/);
  });

  it("throws when the policy abi is not a paginated lens", async () => {
    const unarrayifiedAbi = parseAbiItem("function balancesOf(address[] a) view returns (uint256[])") as AbiFunction;
    const transport = createTransport(vi.fn());

    await expect(transport.request(createRequest([addr(1)], { abi: unarrayifiedAbi }))).rejects.toThrow(
      /must return \(U\[\] results, uint256\[\] skipped\)/,
    );
  });

  it("throws when target calldata selector mismatches the policy abi", async () => {
    const otherAbi = parseAbiItem(
      "function otherFn(address[] xs) view returns (uint256[] r, uint256[] s)",
    ) as AbiFunction;
    const transport = createTransport(vi.fn());
    const req: EthCallRequest = {
      method: "eth_call",
      params: [
        { data: buildDeploylessCall(buildTargetCalldata(pageAbi, [addr(1)])) },
        "latest",
        policySentinel(otherAbi),
      ],
    };

    await expect(transport.request(req)).rejects.toThrow(/selector/);
  });

  it("ignores policy.cache and behaves like split-only mode", async () => {
    const accounts = addrs(5);
    const requestFnWithoutCache = mockPagedFn();
    const requestFnWithCache = mockPagedFn();
    const batch = { batchSize: wireBytesFor(2) };

    const withoutCache = await createTransport(requestFnWithoutCache).request(createRequest(accounts, { batch }));
    const withCache = await createTransport(requestFnWithCache).request(
      createRequest(accounts, { batch, withCache: true }),
    );

    expect(withCache).toEqual(withoutCache);
    expect(sentData(requestFnWithCache).map((d) => d.toLowerCase())).toEqual(
      sentData(requestFnWithoutCache).map((d) => d.toLowerCase()),
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

    it("decodes a sentinel-framed page that served nothing", async () => {
      const requestFn = vi.fn().mockImplementation(async () => {
        throw pageRevert([], [0]);
      });
      const transport = createTransport(requestFn);

      const result = await transport.request(createRequest([addr(1)]));

      expect(decodePage(result)).toEqual({ results: [], skipped: [0] });
    });

    it("decodes sentinel data from an intermediate BaseError", async () => {
      const encoded = pageToWire({ results: [`0x${word(1)}`], skipped: [], gas: flatGas(1) });
      const dataError = Object.assign(new BaseError("rpc", { cause: new Error("inner") }), {
        data: `${OK_SENTINEL}${encoded.slice(2)}` as Hex,
      });
      const requestFn = vi.fn().mockRejectedValue(new BaseError("outer", { cause: dataError }));
      const transport = createTransport(requestFn);

      const result = await transport.request(createRequest([addr(1)], { batch: { batchSize: 8192 } }));

      expect(decodeResults(result)).toEqual([1n]);
    });

    it("uses REVERT-mode wrapper bytecode upstream (no batch opts)", async () => {
      const requestFn = mockPagedFn();
      const transport = createTransport(requestFn);

      await transport.request(createRequest([addr(1)]));

      expect(sentData(requestFn)[0]!.toLowerCase().startsWith(FACTORY_BYTECODE_REVERT)).toBe(true);
    });
  });

  describe("compress=true", () => {
    it("round-trips addresses correctly", async () => {
      const accounts = addrs(3);
      const requestFn = mockPagedFn();
      const transport = createTransport(requestFn);

      const result = await transport.request(createRequest(accounts, { batch: { batchSize: 8192, compress: true } }));

      expect(decodeResults(result)).toEqual(accounts.map((a) => BigInt(a)));
    });

    it("sets the compression bit and compresses only the body", async () => {
      const requestFn = mockPagedFn();
      const transport = createTransport(requestFn);
      const accounts = addrs(3);

      await transport.request(createRequest(accounts, { batch: { batchSize: 8192, compress: true } }));

      const data = sentData(requestFn)[0]!;
      expect(data.toLowerCase().startsWith(FACTORY_BYTECODE_REVERT)).toBe(true);
      const [, wire, , , config] = decodeAbiParameters(
        parseAbiParameters("address, bytes, address, bytes, uint256"),
        `0x${data.slice(FACTORY_BYTECODE_REVERT.length)}` as Hex,
      );
      expect(config).toBe(CONFIG | (1n << 221n));
      // `n` and `bodyLen` stay clear; the body behind them is the compressed one.
      expect(wire.slice(2, 2 + 128)).toBe(`${word(3)}${word(96)}`);
      expect(unwrapDeploylessFactoryCall(data).targetData).toBe(
        arrayToWire(
          WORD,
          accounts.map((a) => pad(a, { size: 32 })),
        ),
      );
    });

    it("keeps compressed chunks within the actual wrapped byte budget", async () => {
      const accounts = Array.from({ length: 60 }, () => addr(1));
      const batchSize = wireBytesFor(1, true) + 8;
      const requestFn = mockPagedFn();
      const transport = createTransport(requestFn);

      const result = await transport.request(createRequest(accounts, { batch: { batchSize, compress: true } }));

      expect(requestFn.mock.calls.length).toBeGreaterThan(1);
      for (const data of sentData(requestFn)) expect(byteLength(data)).toBeLessThanOrEqual(batchSize);
      expect(decodeResults(result)).toHaveLength(accounts.length);
    });
  });

  describe("halve-on-error retries", () => {
    it("halves and retries when upstream rejects with a batch-size error", async () => {
      const accounts = addrs(4);
      let firstCall = true;
      const requestFn = vi.fn().mockImplementation(async (args: { method: string; params: readonly unknown[] }) => {
        // First call (full batch) → batch-size error; subsequent calls (halves) → succeed.
        if (firstCall) {
          firstCall = false;
          throw Object.assign(new Error("request body too large"), { data: "0x" as Hex });
        }
        const data = (args.params[0] as { data: Hex }).data;
        throw pageRevert(decodeSentAddresses(data).map((a) => BigInt(a)));
      });
      const transport = createTransport(requestFn);

      const { logger, events } = createStubLogger();
      const result = await withLogging(
        () => transport.request(createRequest(accounts, { batch: { batchSize: 8192 } })),
        { logger },
      );
      const field = (name: string) => findDotted(events[0]!.context, "viem-dlc-deployless", `eth_call.${name}`);

      expect(requestFn).toHaveBeenCalledTimes(3); // 1 failed + 2 halves
      expect(decodeResults(result)).toEqual(accounts.map((a) => BigInt(a)));
      expect(field("splits_size")).toBe(1);
    });

    it("throws on the wrapper's out-of-gas marker instead of halving", async () => {
      // The deploy is invariant across chunks, so a constructor the cap cannot fund is terminal.
      const requestFn = vi.fn().mockRejectedValue(revertRaw(OOG_SENTINEL));
      const transport = createTransport(requestFn);

      await expect(transport.request(createRequest(addrs(4), { batch: { batchSize: 8192 } }))).rejects.toThrow(
        /ran out of gas under this node's cap/,
      );
      expect(requestFn).toHaveBeenCalledOnce();
    });

    it("does not treat lens revert data that merely starts with the marker as out-of-gas", async () => {
      const requestFn = vi.fn().mockRejectedValue(revertRaw(`${OOG_SENTINEL}${"00".repeat(32)}` as Hex));
      const transport = createTransport(requestFn);

      await expect(
        transport.request(createRequest([addr(1), addr(2)], { batch: { batchSize: 8192 } })),
      ).rejects.toThrow("execution reverted");
      expect(requestFn).toHaveBeenCalledTimes(1);
    });

    it("throws a counterfactual-deploy failure instead of halving it", async () => {
      // `CounterfactualDeployFailed(bytes)` with an empty payload: selector, offset word, length 0.
      const deployFailed =
        `${COUNTERFACTUAL_DEPLOY_FAILED_SELECTOR}${(32).toString(16).padStart(64, "0")}${"00".repeat(32)}` as Hex;
      const requestFn = vi.fn().mockRejectedValue(revertRaw(deployFailed));
      const transport = createTransport(requestFn);

      await expect(transport.request(createRequest(addrs(4), { batch: { batchSize: 8192 } }))).rejects.toThrow(
        /counterfactual deploy failed/,
      );
      // The deploy is invariant across chunks, so a smaller chunk cannot help.
      expect(requestFn).toHaveBeenCalledOnce();
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

      const { logger, events } = createStubLogger();
      const error = await withLogging(
        () => transport.request(createRequest([addr(1), addr(2)], { batch: { batchSize: 8192 } })),
        { logger },
      ).catch((e) => e);
      const field = (name: string) => findDotted(events[0]!.context, "viem-dlc-deployless", `eth_call.${name}`);

      expect(error.message).toMatch("nonce too low");
      expect(requestFn).toHaveBeenCalledTimes(1);
      expect(field("splits_count")).toBe(0);
    });

    it.each([
      ["TimeoutError name", () => Object.assign(new Error("aborted"), { name: "TimeoutError" })],
      ["timeout message", () => new Error("request timed out after 10000ms")],
      ["HTTP 504", () => Object.assign(new Error("gateway timeout"), { status: 504 })],
    ])("halves once on timeout (%s) and rethrows if the halves also time out", async (_label, makeError) => {
      const requestFn = vi.fn().mockImplementation(async () => {
        throw makeError();
      });
      const transport = createTransport(requestFn);

      await expect(transport.request(createRequest(addrs(4), { batch: { batchSize: 8192 } }))).rejects.toThrow();
      // 1 full-batch attempt + 2 half-batch attempts; halves exhaust the timeout budget so no further bisect.
      expect(requestFn).toHaveBeenCalledTimes(3);
    });

    it("halves on timeout and succeeds when smaller chunks come back", async () => {
      const accounts = addrs(4);
      let firstCall = true;
      const requestFn = vi.fn().mockImplementation(async (args: { method: string; params: readonly unknown[] }) => {
        if (firstCall) {
          firstCall = false;
          throw Object.assign(new Error("request timed out"), { name: "TimeoutError" });
        }
        const data = (args.params[0] as { data: Hex }).data;
        throw pageRevert(decodeSentAddresses(data).map((a) => BigInt(a)));
      });
      const transport = createTransport(requestFn);

      const result = await transport.request(createRequest(accounts, { batch: { batchSize: 8192 } }));

      expect(requestFn).toHaveBeenCalledTimes(3); // 1 timed-out + 2 halves
      expect(decodeResults(result)).toEqual(accounts.map((a) => BigInt(a)));
    });
  });
});
