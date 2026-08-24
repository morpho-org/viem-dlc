import {
  type AbiFunction,
  type Address,
  concat,
  decodeAbiParameters,
  deploylessCallViaFactoryBytecode,
  encodeAbiParameters,
  encodeDeployData,
  type Hex,
  pad,
  parseAbiItem,
  toFunctionSelector,
  toHex,
} from "viem";
import { describe, expect, it, vi } from "vitest";

import { LazyNdjsonMap } from "../../../../src/internal/index.js";
import { MemoryStore } from "../../../../src/stores/memory.js";
import { handleEthCall } from "../../../../src/transports/cache/eth-call/handler.js";
import type { CachedEthCallEntry } from "../../../../src/transports/cache/eth-call/types.js";
import { keychain } from "../../../../src/transports/cache/keychain.js";
import type { CacheSchema } from "../../../../src/transports/cache/schema.js";
import type { HandlerContext } from "../../../../src/transports/cache/types.js";
import { ETH_CALL_POLICY_ADDRESS } from "../../../../src/transports/state-overrides.js";
import type { EIP1193Parameters } from "../../../../src/types.js";
import { createCoalescingMutex } from "../../../../src/utils/coalescing-mutex.js";
import { OK_SENTINEL, unwrapDeploylessFactoryCall } from "../../../../src/utils/deployless/codec.envelope.js";
import { flzDecompress } from "../../../../src/utils/deployless/flz.js";
import { parse, stringify } from "../../../../src/utils/json.js";

type EthCallRequest = EIP1193Parameters<CacheSchema, "eth_call">;

const codec = { toJson: stringify, fromJson: parse } as const;
const chainId = 1;
const ttl = 60_000;

const TARGET_TO = "0x1111111111111111111111111111111111111111" as const;
const FACTORY = "0x2222222222222222222222222222222222222222" as const;
const FACTORY_DATA = "0xcafebabe" as const;

const balancesOfAbi = parseAbiItem("function balancesOf(address[] accounts) view returns (uint256[])") as AbiFunction;

function buildTargetCalldata(abi: AbiFunction, addrs: readonly Address[]): Hex {
  return concat([toFunctionSelector(abi), encodeAbiParameters([{ type: "address[]" }], [addrs])]);
}

/** Inbound shape: viem's stock RETURN-mode wrapper. The handler re-wraps for upstream. */
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
};

function cachePolicySentinel(abi: AbiFunction, opts: PolicyOpts = {}) {
  return {
    [ETH_CALL_POLICY_ADDRESS]: {
      code: toHex(
        JSON.stringify({
          abi,
          ...(opts.batch ? { batch: opts.batch } : {}),
          cache: { blobKey: "test-blob", ttl },
        }),
      ),
    },
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
      cachePolicySentinel(abi, { batch: opts.batch }),
    ],
  };
}

function ctx(requestFn: HandlerContext["requestFn"], store = new MemoryStore()): HandlerContext {
  return {
    store,
    coalesce: createCoalescingMutex().coalesce,
    requestFn,
    chainId,
    binSize: 10_000,
    invalidationStrategy: () => 0,
    gasLimit: 30_000_000,
  };
}

/** Recovers `addrs` from upstream-wrapped data (works for either RETURN or REVERT prefix). */
function decodeSentAddresses(data: Hex): readonly Address[] {
  const { targetData } = unwrapDeploylessFactoryCall(data);
  const [addrs] = decodeAbiParameters([{ type: "address[]" }], `0x${targetData.slice(10)}` as Hex);
  return addrs as readonly Address[];
}

/** Builds a viem-shaped error whose `.data` field carries OK_SENTINEL || payload. */
function revertWithSentinel(payload: Hex): Error & { data: Hex } {
  const err = new Error("execution reverted") as Error & { data: Hex };
  err.data = `${OK_SENTINEL}${payload.slice(2)}` as Hex;
  return err;
}

/** The wrapper always exfiltrates via REVERT, so success arrives as a sentinel-framed throw. */
function mockBalancesOfFn() {
  return vi.fn().mockImplementation(async (args: { method: string; params: readonly unknown[] }) => {
    const data = (args.params[0] as { data: Hex }).data;
    const outputs = decodeSentAddresses(data).map((a) => BigInt(a));
    throw revertWithSentinel(encodeAbiParameters([{ type: "uint256[]" }], [outputs]));
  });
}

function mockCompressibleFn(compress: boolean) {
  return vi.fn().mockImplementation(async (args: { method: string; params: readonly unknown[] }) => {
    const data = (args.params[0] as { data: Hex }).data;
    const { targetData: raw } = unwrapDeploylessFactoryCall(data);
    const targetData = compress ? flzDecompress(raw) : raw;
    const [addrs] = decodeAbiParameters([{ type: "address[]" }], `0x${targetData.slice(10)}` as Hex);
    const outputs = (addrs as readonly Address[]).map((a) => BigInt(a));
    throw revertWithSentinel(encodeAbiParameters([{ type: "uint256[]" }], [outputs]));
  });
}

function entryKeyFor(element: Hex) {
  return keychain.entryKey(chainId, "eth_call", {
    target: { address: TARGET_TO, factory: FACTORY, factoryData: FACTORY_DATA },
    selector: toFunctionSelector(balancesOfAbi),
    element,
    restOfEthCallParams: ["latest"],
  }).data;
}

async function populateStore(
  store: MemoryStore,
  blobKey: string,
  entries: { key: string; value: CachedEthCallEntry }[],
) {
  let buffers = store.get(blobKey) ?? [];
  const ndjson = new LazyNdjsonMap<CachedEthCallEntry>(
    codec,
    {
      get: () => buffers,
      set: (next) => {
        buffers = next;
        store.set(blobKey, next);
      },
    },
    { debounceMs: 86_400_000, maxDelayMs: 86_400_000 },
  );
  ndjson.upsert(entries);
  await ndjson.flush();
}

const addr = (n: number) => pad(toHex(n), { size: 20 });

describe("handleEthCall", () => {
  it("passes through when policy sentinel is absent", async () => {
    const requestFn = vi.fn().mockResolvedValue("0x1234");
    const req: EthCallRequest = {
      method: "eth_call",
      params: [{ data: "0xabcdef" as Hex }, "latest"],
    };

    const result = await handleEthCall(ctx(requestFn), req);

    expect(result).toBe("0x1234");
    expect(requestFn).toHaveBeenCalledWith({ method: "eth_call", params: [{ data: "0xabcdef" }, "latest"] });
  });

  it("throws when policy is present but `to` is set", async () => {
    const req: EthCallRequest = {
      method: "eth_call",
      params: [
        {
          to: "0x3333333333333333333333333333333333333333",
          data: buildDeploylessCall(buildTargetCalldata(balancesOfAbi, [addr(1)])),
        },
        "latest",
        cachePolicySentinel(balancesOfAbi),
      ],
    };
    await expect(handleEthCall(ctx(vi.fn()), req)).rejects.toThrow(/found extras: to/);
  });

  it("throws when data is not a deployless factory wrapper", async () => {
    const req: EthCallRequest = {
      method: "eth_call",
      params: [{ data: "0xabcdef" as Hex }, "latest", cachePolicySentinel(balancesOfAbi)],
    };
    await expect(handleEthCall(ctx(vi.fn()), req)).rejects.toThrow(/deployless factory wrapper/);
  });

  it("throws when policy abi is not a single-array-in/single-array-out function", async () => {
    const badAbi = parseAbiItem("function foo(address) view returns (uint256[])") as AbiFunction;
    const req: EthCallRequest = {
      method: "eth_call",
      params: [
        { data: buildDeploylessCall(buildTargetCalldata(balancesOfAbi, [addr(1)])) },
        "latest",
        cachePolicySentinel(badAbi),
      ],
    };
    await expect(handleEthCall(ctx(vi.fn()), req)).rejects.toThrow(/dynamic-array input/);
  });

  it("throws when target calldata selector mismatches the policy abi", async () => {
    const otherAbi = parseAbiItem("function otherFn(address[] xs) view returns (uint256[])") as AbiFunction;
    const req: EthCallRequest = {
      method: "eth_call",
      params: [
        { data: buildDeploylessCall(buildTargetCalldata(balancesOfAbi, [addr(1)])) },
        "latest",
        cachePolicySentinel(otherAbi),
      ],
    };
    await expect(handleEthCall(ctx(vi.fn()), req)).rejects.toThrow(/selector/);
  });

  it("fetches on cold cache and caches per-element outputs", async () => {
    const store = new MemoryStore();
    const requestFn = mockBalancesOfFn();
    const addrs = [addr(1), addr(2), addr(3)];
    const req = createRequest(addrs);

    const result = await handleEthCall(ctx(requestFn, store), req);

    expect(requestFn).toHaveBeenCalledTimes(1);
    const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
    expect(decoded).toEqual(addrs.map((a) => BigInt(a)));

    const blobKey = keychain.blobKey(chainId, req)!;
    expect(store.get(blobKey)).not.toBeNull();
  });

  it("warm cache: returns cached values without calling RPC", async () => {
    const store = new MemoryStore();
    const addrs = [addr(1), addr(2), addr(3)];
    const req = createRequest(addrs);
    const blobKey = keychain.blobKey(chainId, req)!;

    const addressElements = addrs.map((a) => pad(a, { size: 32 }));
    const outputElements = addrs.map((a) => pad(toHex(BigInt(a)), { size: 32 }));
    await populateStore(
      store,
      blobKey,
      addressElements.map((el, i) => ({
        key: entryKeyFor(el),
        value: { output: outputElements[i]!, fetchedAt: Date.now() },
      })),
    );

    const requestFn = vi.fn() as unknown as HandlerContext["requestFn"];
    const result = await handleEthCall(ctx(requestFn, store), req);

    expect(requestFn).not.toHaveBeenCalled();
    const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
    expect(decoded).toEqual(addrs.map((a) => BigInt(a)));
  });

  it("mixed: fetches only misses, leaves hits alone", async () => {
    const store = new MemoryStore();
    const addrs = [addr(1), addr(2), addr(3), addr(4)];
    const req = createRequest(addrs);
    const blobKey = keychain.blobKey(chainId, req)!;

    const addressElements = addrs.map((a) => pad(a, { size: 32 }));
    const outputElements = addrs.map((a) => pad(toHex(BigInt(a)), { size: 32 }));
    await populateStore(store, blobKey, [
      { key: entryKeyFor(addressElements[0]!), value: { output: outputElements[0]!, fetchedAt: Date.now() } },
      { key: entryKeyFor(addressElements[2]!), value: { output: outputElements[2]!, fetchedAt: Date.now() } },
    ]);

    const requestFn = mockBalancesOfFn();
    const result = await handleEthCall(ctx(requestFn, store), req);

    expect(requestFn).toHaveBeenCalledTimes(1);
    const sentData = (requestFn.mock.calls[0]![0] as { params: [{ data: Hex }] }).params[0].data;
    const sentAddrs = decodeSentAddresses(sentData);
    expect(sentAddrs.map((a) => a.toLowerCase()).sort()).toEqual(
      [addrs[1]!, addrs[3]!].map((a) => a.toLowerCase()).sort(),
    );

    const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
    expect(decoded).toEqual(addrs.map((a) => BigInt(a)));
  });

  it("dedupes repeated input elements into a single blob entry", async () => {
    const store = new MemoryStore();
    const requestFn = mockBalancesOfFn();
    const addrs = [addr(1), addr(2), addr(1), addr(2)];
    const req = createRequest(addrs);

    const result = await handleEthCall(ctx(requestFn, store), req);

    expect(requestFn).toHaveBeenCalledTimes(1);
    const sentData = (requestFn.mock.calls[0]![0] as { params: [{ data: Hex }] }).params[0].data;
    expect(decodeSentAddresses(sentData).length).toBe(2);

    const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
    expect(decoded).toEqual(addrs.map((a) => BigInt(a)));
  });

  it("empty input array: fast path, no RPC call", async () => {
    const requestFn = vi.fn() as unknown as HandlerContext["requestFn"];
    const req = createRequest([]);

    const result = await handleEthCall(ctx(requestFn), req);

    expect(requestFn).not.toHaveBeenCalled();
    const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
    expect(decoded).toEqual([]);
  });

  describe("batching", () => {
    it("batchSize splits misses, never exceeding the byte budget", async () => {
      const batchSize = 520;
      const overshootCap = 600;
      const requestFn = mockBalancesOfFn();
      const addrs = [addr(1), addr(2), addr(3), addr(4), addr(5)];
      const req = createRequest(addrs, { batch: { batchSize } });

      const result = await handleEthCall(ctx(requestFn), req);

      expect(requestFn.mock.calls.length).toBeGreaterThan(1);
      for (const [arg] of requestFn.mock.calls) {
        const data = (arg.params[0] as { data: Hex }).data;
        expect((data.length - 2) / 2).toBeLessThanOrEqual(overshootCap);
      }

      const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
      expect(decoded).toEqual(addrs.map((a) => BigInt(a)));
    });
  });

  describe("compress=true", () => {
    it("round-trips addresses correctly", async () => {
      const addrs = [addr(1), addr(2), addr(3)];
      const requestFn = mockCompressibleFn(true);
      const req = createRequest(addrs, { batch: { batchSize: 8192, compress: true } });

      const result = await handleEthCall(ctx(requestFn), req);

      const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
      expect(decoded).toEqual(addrs.map((a) => BigInt(a)));
    });
  });

  it("TTL expiry: stale entries are refetched", async () => {
    const store = new MemoryStore();
    const addrs = [addr(1), addr(2)];
    const req = createRequest(addrs);
    const blobKey = keychain.blobKey(chainId, req)!;

    const addressElements = addrs.map((a) => pad(a, { size: 32 }));
    const staleOutput = pad(toHex(0xdeadbeefn), { size: 32 });
    await populateStore(store, blobKey, [
      { key: entryKeyFor(addressElements[0]!), value: { output: staleOutput, fetchedAt: Date.now() - 2 * ttl } },
      { key: entryKeyFor(addressElements[1]!), value: { output: staleOutput, fetchedAt: Date.now() - 2 * ttl } },
    ]);

    const requestFn = mockBalancesOfFn();
    const result = await handleEthCall(ctx(requestFn, store), req);

    expect(requestFn).toHaveBeenCalledTimes(1);
    const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
    expect(decoded).toEqual(addrs.map((a) => BigInt(a)));
  });

  it("handles dynamic output element types (string[])", async () => {
    const getNamesAbi = parseAbiItem("function getNames(address[] accounts) view returns (string[])") as AbiFunction;
    const addrs = [addr(1), addr(2), addr(3)];
    const expectedNames = ["one", "two", "three"];

    const requestFn = vi
      .fn()
      .mockRejectedValue(revertWithSentinel(encodeAbiParameters([{ type: "string[]" }], [expectedNames])));
    const req = createRequest(addrs, { abi: getNamesAbi, batch: { batchSize: 8192 } });

    const result = await handleEthCall(ctx(requestFn), req);

    expect(requestFn).toHaveBeenCalledTimes(1);
    const [decoded] = decodeAbiParameters([{ type: "string[]" }], result);
    expect(decoded).toEqual(expectedNames);
  });

  it("throws when upstream returns wrong number of outputs", async () => {
    const requestFn = vi
      .fn()
      .mockRejectedValue(revertWithSentinel(encodeAbiParameters([{ type: "uint256[]" }], [[1n, 2n]])));
    const req = createRequest([addr(1), addr(2), addr(3)], { batch: { batchSize: 8192 } });
    await expect(handleEthCall(ctx(requestFn), req)).rejects.toThrow(/returned 2.*expected 3/);
  });

  it("forwards block / cleanStateOverride / blockOverride to upstream", async () => {
    const requestFn = mockBalancesOfFn();
    const addrs = [addr(1)];
    const extraOverride = { "0x4444444444444444444444444444444444444444": { balance: "0x1" } } as const;
    const req: EthCallRequest = {
      method: "eth_call",
      params: [
        { data: buildDeploylessCall(buildTargetCalldata(balancesOfAbi, addrs)) },
        "0x100" as Hex,
        { ...cachePolicySentinel(balancesOfAbi), ...extraOverride },
      ],
    };

    await handleEthCall(ctx(requestFn), req);

    const call = requestFn.mock.calls[0]![0] as { params: readonly unknown[] };
    expect(call.params[1]).toBe("0x100");
    expect(call.params[2]).toEqual(extraOverride);
  });

  describe("halve-on-error retries", () => {
    it("halves and retries when upstream rejects with a batch-size error", async () => {
      const addrs = [addr(1), addr(2), addr(3), addr(4)];
      let firstCall = true;
      const requestFn = vi.fn().mockImplementation(async (args: { method: string; params: readonly unknown[] }) => {
        if (firstCall) {
          firstCall = false;
          throw Object.assign(new Error("request body too large"), { data: "0x" as Hex });
        }
        const data = (args.params[0] as { data: Hex }).data;
        const outputs = decodeSentAddresses(data).map((a) => BigInt(a));
        throw revertWithSentinel(encodeAbiParameters([{ type: "uint256[]" }], [outputs]));
      });
      const req = createRequest(addrs, { batch: { batchSize: 8192 } });

      const result = await handleEthCall(ctx(requestFn), req);

      expect(requestFn).toHaveBeenCalledTimes(3);
      const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
      expect(decoded).toEqual(addrs.map((a) => BigInt(a)));
    });

    it("rethrows when a single-element batch fails with a batch-size error", async () => {
      const requestFn = vi
        .fn()
        .mockRejectedValue(Object.assign(new Error("request body too large"), { data: "0x" as Hex }));
      const req = createRequest([addr(1)], { batch: { batchSize: 8192 } });

      await expect(handleEthCall(ctx(requestFn), req)).rejects.toThrow("request body too large");
      expect(requestFn).toHaveBeenCalledTimes(1);
    });

    it("does not retry on unrecognized errors", async () => {
      const requestFn = vi.fn().mockRejectedValue(new Error("nonce too low"));
      const req = createRequest([addr(1), addr(2)], { batch: { batchSize: 8192 } });

      await expect(handleEthCall(ctx(requestFn), req)).rejects.toThrow("nonce too low");
      expect(requestFn).toHaveBeenCalledTimes(1);
    });
  });

  it("throws when caller supplies non-cache-keyed tx fields (from/gas/value)", async () => {
    const addrs = [addr(1)];
    const req: EthCallRequest = {
      method: "eth_call",
      params: [
        {
          data: buildDeploylessCall(buildTargetCalldata(balancesOfAbi, addrs)),
          from: "0x5555555555555555555555555555555555555555",
          gas: "0xffff",
          value: "0x0",
        },
        "latest",
        cachePolicySentinel(balancesOfAbi),
      ],
    };

    await expect(handleEthCall(ctx(vi.fn()), req)).rejects.toThrow(/found extras:.*from.*gas.*value/);
  });

  it("honors gas budget on the cache miss-fetch path", async () => {
    // 5 elements, gasLimit 30M (default), G(N) = 12M·N → max 2 per chunk → 3 chunks (2+2+1).
    const addrs = [addr(1), addr(2), addr(3), addr(4), addr(5)];
    const requestFn = mockBalancesOfFn();
    const req = createRequest(addrs, {
      batch: { gas: { constant: 0, linear: 12_000_000, quadratic: 0 } },
    });

    await handleEthCall(ctx(requestFn), req);

    expect(requestFn).toHaveBeenCalledTimes(3);
    for (const [arg] of requestFn.mock.calls) {
      const data = (arg.params[0] as { data: Hex }).data;
      expect(decodeSentAddresses(data).length).toBeLessThanOrEqual(2);
    }
  });
});
