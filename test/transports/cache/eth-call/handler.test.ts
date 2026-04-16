import type { Address, Hex } from "viem";
import {
  type AbiFunction,
  concat,
  decodeAbiParameters,
  deploylessCallViaFactoryBytecode,
  encodeAbiParameters,
  encodeDeployData,
  pad,
  parseAbiItem,
  parseAbiParameters,
  toFunctionSelector,
  toHex,
} from "viem";
import { describe, expect, it, vi } from "vitest";

import { LazyNdjsonMap } from "../../../../src/internal/index.js";
import { MemoryStore } from "../../../../src/stores/memory.js";
import { handleEthCall } from "../../../../src/transports/cache/eth-call/handler.js";
import { ETH_CALL_POLICY_ADDRESS } from "../../../../src/transports/state-overrides.js";
import type { CachedEthCallEntry } from "../../../../src/transports/cache/eth-call/types.js";
import { keychain } from "../../../../src/transports/cache/keychain.js";
import type { CacheSchema } from "../../../../src/transports/cache/schema.js";
import type { HandlerContext } from "../../../../src/transports/cache/types.js";
import type { EIP1193Parameters } from "../../../../src/types.js";
import { createCoalescingMutex } from "../../../../src/utils/coalescing-mutex.js";
import { parse, stringify } from "../../../../src/utils/json.js";

type EthCallRequest = EIP1193Parameters<CacheSchema, "eth_call">;

const codec = { toJson: stringify, fromJson: parse } as const;
const chainId = 1;
const ttl = 60_000;

const TARGET_TO = "0x1111111111111111111111111111111111111111" as const;
const FACTORY = "0x2222222222222222222222222222222222222222" as const;
const FACTORY_DATA = "0xcafebabe" as const;

const DEPLOYLESS_CTOR_PARAMS = parseAbiParameters("address, bytes, address, bytes");

const balancesOfAbi = parseAbiItem("function balancesOf(address[] accounts) view returns (uint256[])") as AbiFunction;

function buildTargetCalldata(abi: AbiFunction, addrs: readonly Address[]): Hex {
  return concat([toFunctionSelector(abi), encodeAbiParameters([{ type: "address[]" }], [addrs])]);
}

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

function cachePolicySentinel(abi: AbiFunction, batchSize?: number) {
  return {
    [ETH_CALL_POLICY_ADDRESS]: {
      code: toHex(
        JSON.stringify({
          abi,
          batchSize,
          cache: { blobKey: "test-blob", ttl },
        }),
      ),
    },
  };
}

function createRequest(addrs: readonly Address[], opts?: { batchSize?: number; abi?: AbiFunction }): EthCallRequest {
  const abi = opts?.abi ?? balancesOfAbi;
  return {
    method: "eth_call",
    params: [
      { data: buildDeploylessCall(buildTargetCalldata(abi, addrs)) },
      "latest",
      cachePolicySentinel(abi, opts?.batchSize),
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
  };
}

/** Extracts the target calldata's input addresses from a deployless-wrapped `eth_call`. */
function decodeSentAddresses(data: Hex): readonly Address[] {
  const argsHex = `0x${data.slice(deploylessCallViaFactoryBytecode.length)}` as Hex;
  const [, targetData] = decodeAbiParameters(DEPLOYLESS_CTOR_PARAMS, argsHex);
  const [addrs] = decodeAbiParameters([{ type: "address[]" }], `0x${targetData.slice(10)}` as Hex);
  return addrs as readonly Address[];
}

/** Mock RPC: answer a `balancesOf(address[])` call with `balance[i] = BigInt(address[i])`. */
function mockBalancesOfFn() {
  return vi.fn().mockImplementation(async (args: { method: string; params: readonly unknown[] }) => {
    const data = (args.params[0] as { data: Hex }).data;
    const outputs = decodeSentAddresses(data).map((a) => BigInt(a));
    return encodeAbiParameters([{ type: "uint256[]" }], [outputs]);
  });
}

function entryKeyFor(element: Hex) {
  return keychain.entryKey(chainId, "eth_call", {
    targetTo: TARGET_TO,
    factory: FACTORY,
    factoryData: FACTORY_DATA,
    selector: toFunctionSelector(balancesOfAbi),
    inputElement: element,
    block: "latest",
    stateOverride: undefined,
    blockOverride: undefined,
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

    // Pre-populate entries for addr(1) and addr(3)
    const addressElements = addrs.map((a) => pad(a, { size: 32 }));
    const outputElements = addrs.map((a) => pad(toHex(BigInt(a)), { size: 32 }));
    await populateStore(store, blobKey, [
      { key: entryKeyFor(addressElements[0]!), value: { output: outputElements[0]!, fetchedAt: Date.now() } },
      { key: entryKeyFor(addressElements[2]!), value: { output: outputElements[2]!, fetchedAt: Date.now() } },
    ]);

    const requestFn = mockBalancesOfFn();
    const result = await handleEthCall(ctx(requestFn, store), req);

    expect(requestFn).toHaveBeenCalledTimes(1);
    // The upstream RPC should only see addr(2) and addr(4) as inputs.
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
    // Upstream should receive exactly 2 unique addresses
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

  it("batchSize splits misses exactly, never exceeding the byte budget", async () => {
    const requestFn = mockBalancesOfFn();
    // For balancesOf(address[]): overhead = 1024 bytes (factory bytecode 704 + constructor
    // head 128 + targetData bytes-wrapper 128 + factoryData bytes-wrapper 64); per-element
    // = 32 bytes (static address slot). batchSize = 1088 fits exactly 2 elements per batch,
    // so 5 misses split into 3 batches of sizes 2/2/1.
    const addrs = [addr(1), addr(2), addr(3), addr(4), addr(5)];
    const req = createRequest(addrs, { batchSize: 1088 });

    const result = await handleEthCall(ctx(requestFn), req);

    expect(requestFn.mock.calls.length).toBe(3);
    for (const [arg] of requestFn.mock.calls) {
      const data = (arg.params[0] as { data: Hex }).data;
      expect((data.length - 2) / 2).toBeLessThanOrEqual(1088);
    }

    const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
    expect(decoded).toEqual(addrs.map((a) => BigInt(a)));
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

    const requestFn = vi.fn().mockResolvedValue(encodeAbiParameters([{ type: "string[]" }], [expectedNames]));
    const req = createRequest(addrs, { abi: getNamesAbi });

    const result = await handleEthCall(ctx(requestFn), req);

    expect(requestFn).toHaveBeenCalledTimes(1);
    const [decoded] = decodeAbiParameters([{ type: "string[]" }], result);
    expect(decoded).toEqual(expectedNames);
  });

  it("throws when upstream returns wrong number of outputs", async () => {
    const requestFn = vi.fn().mockResolvedValue(encodeAbiParameters([{ type: "uint256[]" }], [[1n, 2n]]));
    const req = createRequest([addr(1), addr(2), addr(3)]);
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
});
