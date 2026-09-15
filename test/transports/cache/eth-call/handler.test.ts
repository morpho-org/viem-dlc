import {
  type AbiFunction,
  type Address,
  concat,
  decodeAbiParameters,
  deploylessCallViaFactoryBytecode,
  encodeAbiParameters,
  encodeDeployData,
  getAddress,
  type Hex,
  pad,
  parseAbiItem,
  toFunctionSelector,
  toHex,
} from "viem";
import { describe, expect, it, vi } from "vitest";

import { LazyNdjsonMap } from "../../../../src/internal/index.js";
import { createFacetId, observe, withLogging } from "../../../../src/observability.js";
import { MemoryStore } from "../../../../src/stores/memory.js";
import { handleEthCall } from "../../../../src/transports/cache/eth-call/handler.js";
import type { CachedEthCallEntry } from "../../../../src/transports/cache/eth-call/types.js";
import { keychain } from "../../../../src/transports/cache/keychain.js";
import type { CacheSchema } from "../../../../src/transports/cache/schema.js";
import { cacheTransportKey } from "../../../../src/transports/cache/schema.js";
import type { HandlerContext } from "../../../../src/transports/cache/types.js";
import { ETH_CALL_POLICY_ADDRESS } from "../../../../src/transports/state-overrides.js";
import type { EIP1193Parameters } from "../../../../src/types.js";
import { createCoalescingMutex } from "../../../../src/utils/coalescing-mutex.js";
import type { LensGas } from "../../../../src/utils/deployless/call.js";
import {
  envelopeConfig,
  OK_SENTINEL,
  unwrapDeploylessFactoryCall,
  wrapDeploylessFactoryCall,
} from "../../../../src/utils/deployless/codec.envelope.js";
import {
  arrayToWire,
  hexToArray,
  pageToWire,
  resolveArrayFunction,
  wireToArray,
} from "../../../../src/utils/deployless/codec.inner.js";
import { parse, stringify } from "../../../../src/utils/json.js";
import { createStubLogger, findDotted } from "../../../helpers/logger.js";
import { flatGas } from "../../../helpers/page.js";

type EthCallRequest = EIP1193Parameters<CacheSchema, "eth_call">;

const codec = { toJson: stringify, fromJson: parse } as const;
const chainId = 1;
const ttl = 60_000;

const TARGET_TO = "0x1111111111111111111111111111111111111111" as const;
const FACTORY = "0x2222222222222222222222222222222222222222" as const;
const FACTORY_DATA = "0xcafebabe" as const;

const pageAbi = parseAbiItem(
  "function balancesOf(address[] accounts) view returns (uint256[] results, uint256[] skipped)",
) as AbiFunction;

const addr = (n: number) => pad(toHex(n), { size: 20 });
const addrs = (n: number) => Array.from({ length: n }, (_, i) => addr(i + 1));

/** The envelope's config word for {@link pageAbi} — invariant across chunks. */
const CONFIG = envelopeConfig(resolveArrayFunction(pageAbi), false);

function buildTargetCalldata(abi: AbiFunction, accounts: readonly Address[]): Hex {
  return concat([toFunctionSelector(abi), encodeAbiParameters([{ type: "address[]" }], [accounts])]);
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

/** Bytes the handler puts on the wire for a chunk of `count` address elements. */
function wireBytesFor(count: number): number {
  const wrapped = wrapDeploylessFactoryCall(
    {
      target: { address: TARGET_TO, factory: FACTORY, factoryData: FACTORY_DATA },
      targetData: arrayToWire(
        WORD,
        addrs(count).map((a) => pad(a, { size: 32 })),
      ),
    },
    { compress: false, config: CONFIG },
  );
  return (wrapped.length - 2) / 2;
}

type PolicyOpts = {
  batch?: { batchSize?: number; compress?: boolean; gas?: LensGas };
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

function createRequest(accounts: readonly Address[], opts: RequestOpts = {}): EthCallRequest {
  const abi = opts.abi ?? pageAbi;
  return {
    method: "eth_call",
    params: [
      { data: buildDeploylessCall(buildTargetCalldata(abi, accounts)) },
      "latest",
      cachePolicySentinel(abi, opts),
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
    facetId: createFacetId(cacheTransportKey),
  };
}

/** Runs the handler under a stub logger, returning the response and a wide-event field reader. */
async function withFacet(context: HandlerContext, req: EthCallRequest) {
  const { logger, events } = createStubLogger();
  // Same id on the boundary as the handler uses, so both write the bare key.
  const observed = observe(() => handleEthCall(context, req), context.facetId);
  const result = await withLogging(() => observed({ method: "eth_call" }), { logger }).catch((e) => e as Error);
  return {
    result,
    field: (name: string) => findDotted(events[0]!.context, cacheTransportKey, `eth_call.${name}`),
  };
}

const WORD = { mode: "static", size: 32 } as const;
const DYNAMIC = { mode: "dynamic" } as const;

/** Recovers the accounts from upstream-wrapped data, compressed or not. */
function decodeSentAddresses(data: Hex): readonly Address[] {
  const { targetData } = unwrapDeploylessFactoryCall(data);
  return wireToArray(WORD, targetData).map((w) => getAddress(`0x${w.slice(26)}`));
}

/** The raw element bytes viem's encoding of `values` yields for the array type `types`. */
function elementsOf(types: string, values: readonly unknown[]): readonly Hex[] {
  const layout = types === "uint256[]" ? WORD : DYNAMIC;
  return hexToArray(layout, encodeAbiParameters([{ type: types }], [values] as never));
}

/** Builds a viem-shaped error whose `.data` field carries OK_SENTINEL || payload. */
function revertWithSentinel(payload: Hex): Error & { data: Hex } {
  const err = new Error("execution reverted") as Error & { data: Hex };
  err.data = `${OK_SENTINEL}${payload.slice(2)}` as Hex;
  return err;
}

function pageRevert(types: string, results: readonly unknown[], skipped: readonly number[], died?: number) {
  const gas = flatGas(results.length + skipped.length);
  const page = { results: elementsOf(types, results), skipped, gas, ...(died === undefined ? {} : { died }) };
  return revertWithSentinel(pageToWire(page));
}

type LensBehavior = {
  /** Address values the lens declines outright. */
  decline?: readonly number[];
  /** Address values the lens reports a gas death on, ending the page at that index. */
  starve?: readonly number[];
};

/** A conforming paginated lens over `address[]`, echoing each account's numeric value. */
function mockPagedFn({ decline = [], starve = [] }: LensBehavior = {}) {
  return vi.fn().mockImplementation(async (args: { method: string; params: readonly unknown[] }) => {
    const accounts = decodeSentAddresses((args.params[0] as { data: Hex }).data);
    const results: bigint[] = [];
    const skipped: number[] = [];
    for (let i = 0; i < accounts.length; i++) {
      const value = Number(BigInt(accounts[i]!));
      if (starve.includes(value)) throw pageRevert("uint256[]", results, skipped, i);
      if (decline.includes(value)) skipped.push(i);
      else results.push(BigInt(value));
    }
    throw pageRevert("uint256[]", results, skipped);
  });
}

function entryKeyFor(element: Hex, abi: AbiFunction = pageAbi) {
  return keychain.entryKey(chainId, "eth_call", {
    target: { address: TARGET_TO, factory: FACTORY, factoryData: FACTORY_DATA },
    selector: toFunctionSelector(abi),
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

/** Cached entry keys, in the sorted order `scan` visits them. */
async function cachedKeys(store: MemoryStore, blobKey: string): Promise<string[]> {
  const cached = new LazyNdjsonMap<CachedEthCallEntry>(codec, { get: () => store.get(blobKey) ?? [] });
  const keys: string[] = [];
  await cached.scan((record) => void keys.push(record.key));
  return keys;
}

/** Entry keys for the given address values, sorted the way {@link cachedKeys} returns them. */
function expectedKeys(values: readonly number[], abi: AbiFunction = pageAbi): string[] {
  return values.map((n) => entryKeyFor(pad(toHex(n), { size: 32 }), abi)).sort();
}

/** Decodes the `(U[] results, uint256[] skipped)` tuple the handler responds with. */
function decodePage(result: unknown, types = "uint256[]") {
  const [results, skipped] = decodeAbiParameters([{ type: types }, { type: "uint256[]" }], result as Hex);
  return { results: [...(results as readonly unknown[])], skipped: (skipped as readonly bigint[]).map(Number) };
}

function decodeResults(result: unknown, types = "uint256[]"): unknown[] {
  return decodePage(result, types).results;
}

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
          data: buildDeploylessCall(buildTargetCalldata(pageAbi, [addr(1)])),
        },
        "latest",
        cachePolicySentinel(pageAbi),
      ],
    };
    await expect(handleEthCall(ctx(vi.fn()), req)).rejects.toThrow(/found extras: to/);
  });

  it("throws when data is not a deployless factory wrapper", async () => {
    const req: EthCallRequest = {
      method: "eth_call",
      params: [{ data: "0xabcdef" as Hex }, "latest", cachePolicySentinel(pageAbi)],
    };
    await expect(handleEthCall(ctx(vi.fn()), req)).rejects.toThrow(/deployless factory wrapper/);
  });

  it("throws when the policy abi does not take a dynamic array", async () => {
    const badAbi = parseAbiItem("function foo(address a) view returns (uint256[] r, uint256[] s)") as AbiFunction;
    const req: EthCallRequest = {
      method: "eth_call",
      params: [
        { data: buildDeploylessCall(buildTargetCalldata(pageAbi, [addr(1)])) },
        "latest",
        cachePolicySentinel(badAbi),
      ],
    };
    await expect(handleEthCall(ctx(vi.fn()), req)).rejects.toThrow(/dynamic-array input/);
  });

  it("throws when the policy abi is not a paginated lens", async () => {
    const unarrayifiedAbi = parseAbiItem("function balancesOf(address[] a) view returns (uint256[])") as AbiFunction;
    const req: EthCallRequest = {
      method: "eth_call",
      params: [
        { data: buildDeploylessCall(buildTargetCalldata(pageAbi, [addr(1)])) },
        "latest",
        cachePolicySentinel(unarrayifiedAbi),
      ],
    };
    await expect(handleEthCall(ctx(vi.fn()), req)).rejects.toThrow(
      /must return \(U\[\] results, uint256\[\] skipped\)/,
    );
  });

  it("throws when target calldata selector mismatches the policy abi", async () => {
    const otherAbi = parseAbiItem(
      "function otherFn(address[] xs) view returns (uint256[] r, uint256[] s)",
    ) as AbiFunction;
    const req: EthCallRequest = {
      method: "eth_call",
      params: [
        { data: buildDeploylessCall(buildTargetCalldata(pageAbi, [addr(1)])) },
        "latest",
        cachePolicySentinel(otherAbi),
      ],
    };
    await expect(handleEthCall(ctx(vi.fn()), req)).rejects.toThrow(/selector/);
  });

  it("fetches on cold cache and caches per-element outputs", async () => {
    const store = new MemoryStore();
    const requestFn = mockPagedFn();
    const accounts = addrs(3);
    const req = createRequest(accounts);

    const result = await handleEthCall(ctx(requestFn, store), req);

    expect(requestFn).toHaveBeenCalledTimes(1);
    expect(decodeResults(result)).toEqual(accounts.map((a) => BigInt(a)));

    const blobKey = keychain.blobKey(chainId, req)!;
    expect(store.get(blobKey)).not.toBeNull();
  });

  it("warm cache: returns cached values without calling RPC", async () => {
    const store = new MemoryStore();
    const accounts = addrs(3);
    const req = createRequest(accounts);
    const blobKey = keychain.blobKey(chainId, req)!;

    const addressElements = accounts.map((a) => pad(a, { size: 32 }));
    const outputElements = accounts.map((a) => pad(toHex(BigInt(a)), { size: 32 }));
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
    expect(decodeResults(result)).toEqual(accounts.map((a) => BigInt(a)));
  });

  it("mixed: fetches only misses, leaves hits alone", async () => {
    const store = new MemoryStore();
    const accounts = addrs(4);
    const req = createRequest(accounts);
    const blobKey = keychain.blobKey(chainId, req)!;

    const addressElements = accounts.map((a) => pad(a, { size: 32 }));
    const outputElements = accounts.map((a) => pad(toHex(BigInt(a)), { size: 32 }));
    await populateStore(store, blobKey, [
      { key: entryKeyFor(addressElements[0]!), value: { output: outputElements[0]!, fetchedAt: Date.now() } },
      { key: entryKeyFor(addressElements[2]!), value: { output: outputElements[2]!, fetchedAt: Date.now() } },
    ]);

    const requestFn = mockPagedFn();
    const result = await handleEthCall(ctx(requestFn, store), req);

    expect(requestFn).toHaveBeenCalledTimes(1);
    const sentData = (requestFn.mock.calls[0]![0] as { params: [{ data: Hex }] }).params[0].data;
    expect(decodeSentAddresses(sentData)).toEqual([accounts[1]!, accounts[3]!]);

    expect(decodeResults(result)).toEqual(accounts.map((a) => BigInt(a)));
  });

  it("dedupes repeated input elements into a single blob entry", async () => {
    const store = new MemoryStore();
    const requestFn = mockPagedFn();
    const accounts = [addr(1), addr(2), addr(1), addr(2)];
    const req = createRequest(accounts);

    const result = await handleEthCall(ctx(requestFn, store), req);

    expect(requestFn).toHaveBeenCalledTimes(1);
    const sentData = (requestFn.mock.calls[0]![0] as { params: [{ data: Hex }] }).params[0].data;
    expect(decodeSentAddresses(sentData).length).toBe(2);

    expect(decodeResults(result)).toEqual(accounts.map((a) => BigInt(a)));
  });

  it("empty input array: fast path, no RPC call", async () => {
    const requestFn = vi.fn() as unknown as HandlerContext["requestFn"];
    const req = createRequest([]);

    const result = await handleEthCall(ctx(requestFn), req);

    expect(requestFn).not.toHaveBeenCalled();
    expect(decodePage(result)).toEqual({ results: [], skipped: [] });
  });

  describe("batching", () => {
    it("batchSize splits misses, never exceeding the byte budget", async () => {
      const batchSize = wireBytesFor(3);
      const requestFn = mockPagedFn();
      const accounts = addrs(5);
      const req = createRequest(accounts, { batch: { batchSize } });

      const result = await handleEthCall(ctx(requestFn), req);

      expect(requestFn.mock.calls.length).toBe(2);
      for (const [arg] of requestFn.mock.calls) {
        const data = (arg.params[0] as { data: Hex }).data;
        expect((data.length - 2) / 2).toBeLessThanOrEqual(batchSize);
      }

      expect(decodeResults(result)).toEqual(accounts.map((a) => BigInt(a)));
    });

    it("the gas prediction caps the opening wave's chunks", async () => {
      const requestFn = mockPagedFn();
      const accounts = addrs(5);
      const gas = { fixed: 0, item: { avg: 1_000_000 } };

      const result = await handleEthCall(
        { ...ctx(requestFn), gasLimit: 2_500_000 },
        createRequest(accounts, { batch: { gas } }),
      );

      expect(requestFn.mock.calls.length).toBe(3);
      expect(decodeResults(result)).toEqual(accounts.map((a) => BigInt(a)));
    });
  });

  describe("compress=true", () => {
    it("round-trips addresses correctly", async () => {
      const accounts = addrs(3);
      const requestFn = mockPagedFn();
      const req = createRequest(accounts, { batch: { batchSize: 8192, compress: true } });

      const result = await handleEthCall(ctx(requestFn), req);

      expect(decodeResults(result)).toEqual(accounts.map((a) => BigInt(a)));
    });
  });

  it("TTL expiry: stale entries are refetched", async () => {
    const store = new MemoryStore();
    const accounts = addrs(2);
    const req = createRequest(accounts);
    const blobKey = keychain.blobKey(chainId, req)!;

    const addressElements = accounts.map((a) => pad(a, { size: 32 }));
    const staleOutput = pad(toHex(0xdeadbeefn), { size: 32 });
    await populateStore(store, blobKey, [
      { key: entryKeyFor(addressElements[0]!), value: { output: staleOutput, fetchedAt: Date.now() - 2 * ttl } },
      { key: entryKeyFor(addressElements[1]!), value: { output: staleOutput, fetchedAt: Date.now() - 2 * ttl } },
    ]);

    const requestFn = mockPagedFn();
    const result = await handleEthCall(ctx(requestFn, store), req);

    expect(requestFn).toHaveBeenCalledTimes(1);
    expect(decodeResults(result)).toEqual(accounts.map((a) => BigInt(a)));
  });

  it("forwards block / cleanStateOverride / blockOverride to upstream", async () => {
    const requestFn = mockPagedFn();
    const extraOverride = { "0x4444444444444444444444444444444444444444": { balance: "0x1" } } as const;
    const req: EthCallRequest = {
      method: "eth_call",
      params: [
        { data: buildDeploylessCall(buildTargetCalldata(pageAbi, [addr(1)])) },
        "0x100" as Hex,
        { ...cachePolicySentinel(pageAbi), ...extraOverride },
      ],
    };

    await handleEthCall(ctx(requestFn), req);

    const call = requestFn.mock.calls[0]![0] as { params: readonly unknown[] };
    expect(call.params[1]).toBe("0x100");
    expect(call.params[2]).toEqual(extraOverride);
  });

  describe("halve-on-error retries", () => {
    it("halves and retries when upstream rejects with a batch-size error", async () => {
      const accounts = addrs(4);
      let firstCall = true;
      const requestFn = vi.fn().mockImplementation(async (args: { method: string; params: readonly unknown[] }) => {
        if (firstCall) {
          firstCall = false;
          throw Object.assign(new Error("request body too large"), { data: "0x" as Hex });
        }
        const data = (args.params[0] as { data: Hex }).data;
        throw pageRevert(
          "uint256[]",
          decodeSentAddresses(data).map((a) => BigInt(a)),
          [],
        );
      });
      const req = createRequest(accounts, { batch: { batchSize: 8192 } });

      const result = await handleEthCall(ctx(requestFn), req);

      expect(requestFn).toHaveBeenCalledTimes(3);
      expect(decodeResults(result)).toEqual(accounts.map((a) => BigInt(a)));
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
    const req: EthCallRequest = {
      method: "eth_call",
      params: [
        {
          data: buildDeploylessCall(buildTargetCalldata(pageAbi, [addr(1)])),
          from: "0x5555555555555555555555555555555555555555",
          gas: "0xffff",
          value: "0x0",
        },
        "latest",
        cachePolicySentinel(pageAbi),
      ],
    };

    await expect(handleEthCall(ctx(vi.fn()), req)).rejects.toThrow(/found extras:.*from.*gas.*value/);
  });

  describe("partial pages", () => {
    it("caches the elements it fetched even when the response skips an element", async () => {
      const store = new MemoryStore();
      const req = createRequest(addrs(3));

      const result = await handleEthCall(ctx(mockPagedFn({ decline: [2] }), store), req);

      expect(decodePage(result)).toEqual({ results: [1n, 3n], skipped: [1] });
      const blobKey = keychain.blobKey(chainId, req)!;
      expect(await cachedKeys(store, blobKey)).toEqual(expectedKeys([1, 3]));
    });

    it("reports elements_missing against caller inputs, not deduped entries", async () => {
      // addr(2) appears twice, so one declined cache entry stands for two caller indices.
      const req = createRequest([addr(1), addr(2), addr(3), addr(2)]);

      const { result, field } = await withFacet(ctx(mockPagedFn({ decline: [2] })), req);

      const { skipped } = decodePage(result);
      expect(skipped).toEqual([1, 3]);
      // The field has to match the `skipped` array the caller actually receives.
      expect(field("elements_missing")).toBe(skipped.length);
    });

    it("expands an element the frame could not resolve across every caller index", async () => {
      // addr(2) dedupes to one miss that dies alone; both of its caller indices go unresolved.
      const req = createRequest([addr(2), addr(1), addr(2)]);

      const { result, field } = await withFacet(ctx(mockPagedFn({ starve: [2] })), req);

      expect(decodePage(result)).toEqual({ results: [1n], skipped: [0, 2] });
      expect(field("elements_missing")).toBe(2);
      expect(field("elements_unresolved")).toBe(2);
    });

    it("waits for a slow sibling chunk to commit before a failing chunk's error escapes", async () => {
      const store = new MemoryStore();
      const accounts = addrs(4);
      // Two chunks of two. The first rejects at once; the second resolves a few ticks later.
      const req = createRequest(accounts, { batch: { batchSize: wireBytesFor(2) } });
      const requestFn = vi.fn().mockImplementation(async (args: { params: readonly unknown[] }) => {
        const sent = decodeSentAddresses((args.params[0] as { data: Hex }).data);
        if (sent[0] === addr(1)) throw new Error("upstream exploded");
        await new Promise((resolve) => setTimeout(resolve, 5));
        throw pageRevert(
          "uint256[]",
          sent.map((a) => BigInt(a)),
          [],
        );
      });

      const error = await handleEthCall(ctx(requestFn, store), req).catch((e) => e);

      // The transport error wins over any partial state, but the slow chunk's work is kept.
      expect(error.message).toMatch(/upstream exploded/);
      const blobKey = keychain.blobKey(chainId, req)!;
      expect(await cachedKeys(store, blobKey)).toEqual(expectedKeys([3, 4]));
    });

    it("orders the response correctly when warm hits, fresh misses, and skips interleave", async () => {
      const store = new MemoryStore();
      const accounts = addrs(5);
      const req = createRequest(accounts);
      const blobKey = keychain.blobKey(chainId, req)!;
      // Pre-seed 2 and 4 so the fetch sees only [1, 3, 5]; the lens then declines 3.
      await populateStore(
        store,
        blobKey,
        [2, 4].map((n) => ({
          key: entryKeyFor(pad(toHex(n), { size: 32 })),
          value: { output: pad(toHex(n * 10), { size: 32 }), fetchedAt: Date.now() },
        })),
      );
      const requestFn = mockPagedFn({ decline: [3] });

      const result = await handleEthCall(ctx(requestFn, store), req);

      expect(decodeSentAddresses((requestFn.mock.calls[0]![0] as EthCallRequest).params[0].data as Hex)).toEqual([
        addr(1),
        addr(3),
        addr(5),
      ]);
      // Warm values (20, 40) must sit at their original positions among the fetched ones.
      expect(decodePage(result)).toEqual({ results: [1n, 20n, 40n, 5n], skipped: [2] });
    });

    it("reports missing indices against the caller's input, not the deduped miss list", async () => {
      const store = new MemoryStore();
      // addr(2) appears three times but dedupes to one miss; all three indices must be reported.
      const req = createRequest([addr(1), addr(2), addr(2), addr(3), addr(2)]);

      const result = await handleEthCall(ctx(mockPagedFn({ decline: [2] }), store), req);

      // `results` is the complement in original order; `skipped` names every original index.
      expect(decodePage(result)).toEqual({ results: [1n, 3n], skipped: [1, 2, 4] });
    });
  });

  describe("dynamic element types", () => {
    const namesAbi = parseAbiItem(
      "function getNames(address[] accounts) view returns (string[] results, uint256[] skipped)",
    ) as AbiFunction;
    const lengthsAbi = parseAbiItem(
      "function lengths(string[] input) view returns (uint256[] results, uint256[] skipped)",
    ) as AbiFunction;

    function stringRequest(values: readonly string[], opts: PolicyOpts): EthCallRequest {
      const targetData = concat([
        toFunctionSelector(lengthsAbi),
        encodeAbiParameters([{ type: "string[]" }], [values]),
      ]);
      return {
        method: "eth_call",
        params: [{ data: buildDeploylessCall(targetData) }, "latest", cachePolicySentinel(lengthsAbi, opts)],
      };
    }

    /** A lens over `string[]` answering each element's length, read off its length-prefixed tail. */
    function lengthsLens() {
      return vi.fn().mockImplementation(async (args: { params: readonly unknown[] }) => {
        const { targetData } = unwrapDeploylessFactoryCall((args.params[0] as { data: Hex }).data);
        const tails = wireToArray(DYNAMIC, targetData);
        throw pageRevert(
          "uint256[]",
          tails.map((t) => BigInt(`0x${t.slice(2, 66)}`)),
          [],
        );
      });
    }

    /** Bytes the transport puts on the wire for a chunk carrying exactly these strings. */
    function wireBytesForStrings(values: readonly string[]): number {
      const wrapped = wrapDeploylessFactoryCall(
        {
          target: { address: TARGET_TO, factory: FACTORY, factoryData: FACTORY_DATA },
          targetData: arrayToWire(DYNAMIC, elementsOf("string[]", values)),
        },
        { compress: false, config: envelopeConfig(resolveArrayFunction(lengthsAbi), false) },
      );
      return (wrapped.length - 2) / 2;
    }

    it("handles dynamic result element types (string[])", async () => {
      const expectedNames = ["one", "two", "three"];
      const requestFn = vi.fn().mockRejectedValue(pageRevert("string[]", expectedNames, []));
      const req = createRequest(addrs(3), { abi: namesAbi, batch: { batchSize: 8192 } });

      const result = await handleEthCall(ctx(requestFn), req);

      expect(requestFn).toHaveBeenCalledTimes(1);
      expect(decodeResults(result, "string[]")).toEqual(expectedNames);
    });

    it("sends dynamic input elements of any size as length-prefixed tails, with no bound to declare", async () => {
      const values = ["alpha", "x".repeat(40), "bee", "y".repeat(500)];
      const requestFn = lengthsLens();

      const { result, field } = await withFacet(
        ctx(requestFn as unknown as HandlerContext["requestFn"]),
        stringRequest(values, {}),
      );

      expect(requestFn).toHaveBeenCalledTimes(1);
      expect(decodePage(result)).toEqual({ results: [5n, 40n, 3n, 500n], skipped: [] });
      expect(field("elements_declined_oversize")).toBe(0);
    });

    it("declines an element that cannot fit a chunk alone under the wire cap, without asking upstream", async () => {
      const big = "x".repeat(200);
      const requestFn = lengthsLens();

      const { result, field } = await withFacet(
        ctx(requestFn as unknown as HandlerContext["requestFn"]),
        stringRequest(["alpha", big, "bee", big], { batch: { batchSize: wireBytesForStrings(["alpha", "bee"]) } }),
      );

      // The oversize element splits its neighbours into two chunks; it is never sent itself.
      const sent = requestFn.mock.calls.flatMap((call) =>
        wireToArray(DYNAMIC, unwrapDeploylessFactoryCall((call[0] as EthCallRequest).params[0].data as Hex).targetData),
      );
      expect(sent).toEqual(elementsOf("string[]", ["alpha", "bee"]));

      // The repeated element dedupes to one miss but is reported at every caller index.
      expect(decodePage(result)).toEqual({ results: [5n, 3n], skipped: [1, 3] });
      expect(field("elements_declined_oversize")).toBe(2);
      expect(field("elements_missing")).toBe(2);
    });
  });
});
