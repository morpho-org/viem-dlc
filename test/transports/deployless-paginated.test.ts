import type { Address, Hex } from "viem";
import {
  type AbiFunction,
  concat,
  createPublicClient,
  custom,
  decodeAbiParameters,
  decodeFunctionResult,
  deploylessCallViaFactoryBytecode,
  encodeAbiParameters,
  encodeDeployData,
  pad,
  parseAbiItem,
  toFunctionSelector,
  toHex,
} from "viem";
import { readContract } from "viem/actions";
import { describe, expect, it, vi } from "vitest";

import { policy } from "../../src/actions/call.js";
import { withLogging } from "../../src/observability.js";
import { deployless } from "../../src/transports/deployless/index.js";
import { ETH_CALL_POLICY_ADDRESS } from "../../src/transports/state-overrides.js";
import type { EIP1193Parameters } from "../../src/types.js";
import {
  MALFORMED_INPUT_SELECTOR,
  MALFORMED_RESULT_SELECTOR,
  OK_SENTINEL,
  OOG_SENTINEL,
  unwrapDeploylessFactoryCall,
} from "../../src/utils/deployless/codec.envelope.js";
import { pageToWire, wireToArray } from "../../src/utils/deployless/codec.inner.js";
import { createStubLogger, findDotted } from "../helpers/logger.js";

type EthCallRequest = EIP1193Parameters<import("viem").PublicRpcSchema, "eth_call">;

const TARGET_TO = "0x1111111111111111111111111111111111111111" as const;
const FACTORY = "0x2222222222222222222222222222222222222222" as const;
const FACTORY_DATA = "0xcafebabe" as const;

const pageAbi = parseAbiItem(
  "function page(address[] input) view returns (uint256[] results, uint256[] skipped)",
) as AbiFunction;

const addr = (n: number) => pad(toHex(n), { size: 20 });
const addrValue = (a: Hex) => Number(BigInt(a));

/** The wire form of a gas death at `index`: the 256-bit complement, `~index`. */
const tag = (index: number) => ((1n << 256n) - 1n) ^ BigInt(index);

const word = (n: number | bigint) => BigInt(n).toString(16).padStart(64, "0");
/** A success record carrying one word. */
const success = (value: bigint) => word((1n << 255n) | 32n) + word(value);

function createRequest(addrs: readonly Address[], batch?: Record<string, unknown>): EthCallRequest {
  const targetData = concat([toFunctionSelector(pageAbi), encodeAbiParameters([{ type: "address[]" }], [addrs])]);
  const policy: Record<string, unknown> = { abi: pageAbi };
  if (batch) policy.batch = batch;
  return {
    method: "eth_call",
    params: [
      {
        data: encodeDeployData({
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
        }),
      },
      "latest",
      { [ETH_CALL_POLICY_ADDRESS]: { code: toHex(JSON.stringify(policy)) } },
    ],
  };
}

/** The addresses a chunk carries, as 32-byte words off the wire. */
function sentAddresses(data: Hex): readonly Hex[] {
  return wireToArray({ mode: "static", size: 32 }, unwrapDeploylessFactoryCall(data).targetData);
}

function revertWith(data: Hex): Error & { data: Hex } {
  const err = new Error("execution reverted") as Error & { data: Hex };
  err.data = data;
  return err;
}

/** A lens response whose records are given verbatim. */
function revertWithRecords(...records: string[]) {
  return revertWith(`${OK_SENTINEL}${word(records.length)}${records.join("")}` as Hex);
}

function revertWithPage(results: readonly bigint[], skipped: readonly number[], died?: number) {
  const page = { results: results.map((r) => `0x${word(r)}` as Hex), skipped, ...(died === undefined ? {} : { died }) };
  return revertWith(`${OK_SENTINEL}${pageToWire(page).slice(2)}` as Hex);
}

type LensBehavior = {
  /** Most elements the lens will attempt in one call before stopping for gas. */
  pageSize?: number;
  /** Address values the lens deterministically declines. */
  decline?: readonly number[];
  /** Address values that exhaust the frame when attempted, taking the whole call with them. */
  fatal?: readonly number[];
  /** Address values the lens reports a gas death on, ending the page at that index. */
  starve?: readonly number[];
  /** Whether a `starve` element resolves once it is the only element in the chunk. */
  recoversAlone?: boolean;
};

/**
 * A conforming paginated lens: walks its input in index order, stops after `pageSize` attempts,
 * declines `decline` elements, reports a gas death on `starve` elements, and lets `fatal`
 * elements kill the frame without reporting (no per-element cap).
 */
function mockPagedLens({
  pageSize = Infinity,
  decline = [],
  fatal = [],
  starve = [],
  recoversAlone = false,
}: LensBehavior = {}) {
  return vi.fn().mockImplementation(async (args: { params: readonly unknown[] }) => {
    const addrs = sentAddresses((args.params[0] as { data: Hex }).data);
    const results: bigint[] = [];
    const skipped: number[] = [];
    for (let i = 0; i < addrs.length && i < pageSize; i++) {
      const value = addrValue(addrs[i]!);
      if (fatal.includes(value)) throw revertWith(OOG_SENTINEL);
      if (starve.includes(value) && !(recoversAlone && addrs.length === 1)) {
        throw revertWithPage(results, skipped, i);
      }
      if (decline.includes(value)) skipped.push(i);
      else results.push(BigInt(value));
    }
    throw revertWithPage(results, skipped);
  });
}

function createTransport(requestFn: ReturnType<typeof vi.fn>) {
  return deployless(custom({ request: requestFn as never }))({ retryCount: 0 } as never);
}

function decodeResults(result: unknown): bigint[] {
  const [values] = decodeAbiParameters([{ type: "uint256[]" }], result as Hex);
  return [...(values as readonly bigint[])];
}

/** Decodes the `(U[] results, uint256[] skipped)` tuple a paginated policy responds with. */
function decodePage(result: unknown): { results: bigint[]; skipped: number[] } {
  const [results, skipped] = decodeAbiParameters([{ type: "uint256[]" }, { type: "uint256[]" }], result as Hex);
  return { results: [...(results as readonly bigint[])], skipped: (skipped as readonly bigint[]).map(Number) };
}

/** Every element index each upstream call was asked about, one entry per call. */
function requestedIndices(requestFn: ReturnType<typeof vi.fn>): number[][] {
  return requestFn.mock.calls.map((call) =>
    sentAddresses((call[0] as EthCallRequest).params[0].data as Hex).map(addrValue),
  );
}

/** Runs `request` under a stub logger and returns the response plus a field reader. */
async function withFacet(request: () => Promise<unknown>) {
  const { logger, events } = createStubLogger();
  const result = await withLogging(request, { logger });
  const { context } = events[0]!;
  return {
    result,
    context,
    field: (name: string) => findDotted(context, "viem-dlc-deployless", `eth_call.${name}`),
  };
}

describe("deployless (paginated)", () => {
  it("returns a dense array when the lens serves everything in one page", async () => {
    const requestFn = mockPagedLens();
    const transport = createTransport(requestFn);

    const result = await transport.request(createRequest([addr(1), addr(2), addr(3)]));

    expect(decodeResults(result)).toEqual([1n, 2n, 3n]);
    expect(requestFn).toHaveBeenCalledOnce();
  });

  it("responds in the shape the lens abi declares, so viem can decode it", async () => {
    const requestFn = mockPagedLens({});
    const transport = createTransport(requestFn);

    const result = await transport.request(createRequest([1, 2].map(addr)));

    // The chunked calls aggregate into one page over the caller's whole input.
    expect(decodeFunctionResult({ abi: [pageAbi], functionName: "page", data: result as Hex })).toEqual([[1n, 2n], []]);
  });

  it("returns an empty page for empty input without an upstream call", async () => {
    const requestFn = mockPagedLens({});
    const transport = createTransport(requestFn);

    const result = await transport.request(createRequest([]));

    expect(decodePage(result)).toEqual({ results: [], skipped: [] });
    expect(requestFn).not.toHaveBeenCalled();
  });

  it("re-requests the untouched tail until every element is covered", async () => {
    const requestFn = mockPagedLens({ pageSize: 2 });
    const transport = createTransport(requestFn);

    const result = await transport.request(createRequest([1, 2, 3, 4, 5].map(addr)));

    expect(decodeResults(result)).toEqual([1n, 2n, 3n, 4n, 5n]);
    // The served prefix is never re-sent, and the remainder is re-packed at the size the lens
    // just demonstrated it could attempt — two chunks fired together, not one big retry.
    expect(requestedIndices(requestFn)).toEqual([[1, 2, 3, 4, 5], [3, 4], [5]]);
  });

  it("does not retry a declined element, but does retry the tail after it", async () => {
    const requestFn = mockPagedLens({ pageSize: 3, decline: [2] });
    const transport = createTransport(requestFn);

    const page = decodePage(await transport.request(createRequest([1, 2, 3, 4].map(addr))));

    expect(page).toEqual({ results: [1n, 3n, 4n], skipped: [1] });
    const asked = requestedIndices(requestFn).flat();
    expect(asked.filter((v) => v === 2)).toHaveLength(1);
    expect(asked).toContain(4);
  });

  it("keeps sibling results when a single element exhausts the frame", async () => {
    const requestFn = mockPagedLens({ fatal: [3] });
    const transport = createTransport(requestFn);

    const page = decodePage(await transport.request(createRequest([1, 2, 3, 4].map(addr))));

    expect(page).toEqual({ results: [1n, 2n, 4n], skipped: [2] });
  });

  it("reports every unservable element, in ascending order", async () => {
    const requestFn = mockPagedLens({ decline: [2], fatal: [4] });
    const transport = createTransport(requestFn);

    const page = decodePage(await transport.request(createRequest([1, 2, 3, 4].map(addr))));

    expect(page).toEqual({ results: [1n, 3n], skipped: [1, 3] });
  });

  it("stamps paginated continuations and unservable elements onto the wide event", async () => {
    // Serves 2 per call and declines the element valued 3, so the run both continues and
    // ends up short: one continuation, two waves, one element the lens refused.
    const requestFn = mockPagedLens({ pageSize: 2, decline: [3] });
    const transport = createTransport(requestFn);

    const { result, context, field } = await withFacet(() =>
      transport.request(createRequest([1, 2, 3, 4, 5].map(addr))),
    );

    // An unservable element is reported in `skipped`, not raised — the request still succeeds.
    expect(context.status).toBe("ok");
    expect(decodePage(result).skipped).toEqual([2]);
    expect(field("elements_missing")).toBe(1);
    expect(field("elements_unresolved")).toBe(0);
    expect(field("pages_continued")).toBe(1);
    expect(field("pages_waves")).toBe(2);
    expect(field("pages_escalated")).toBe(0);
    expect(field("attempts_unresolved")).toBe(0);
    // A lens stopping early is a continuation, not a bisect.
    expect(field("splits_count")).toBe(0);
    expect(field("splits_corpse")).toBe(0);
    // Every page here served at least one element.
    expect(field("pages_all_skipped")).toBe(0);
  });

  it("counts a page that adjudicated only declines on the wide event", async () => {
    const requestFn = mockPagedLens({ decline: [1, 2] });
    const transport = createTransport(requestFn);

    const { result, field } = await withFacet(() => transport.request(createRequest([1, 2].map(addr))));

    expect(decodePage(result)).toEqual({ results: [], skipped: [0, 1] });
    expect(field("pages_all_skipped")).toBe(1);
    expect(field("elements_missing")).toBe(2);
    expect(field("elements_unresolved")).toBe(0);
  });

  it("does not count a page that stopped for gas as all-skipped", async () => {
    const requestFn = mockPagedLens({ decline: [1], starve: [2] });
    const transport = createTransport(requestFn);

    const { result, field } = await withFacet(() => transport.request(createRequest([1, 2].map(addr))));

    expect(decodePage(result)).toEqual({ results: [], skipped: [0, 1] });
    expect(field("pages_all_skipped")).toBe(0);
    expect(field("elements_missing")).toBe(2);
    expect(field("elements_unresolved")).toBe(1);
  });

  it("propagates an ordinary lens revert instead of treating it as unservable", async () => {
    const requestFn = vi.fn().mockRejectedValue(revertWith("0xdeadbeef"));
    const transport = createTransport(requestFn);

    await expect(transport.request(createRequest([addr(1)]))).rejects.toThrow(/execution reverted/);
  });

  describe("gas deaths", () => {
    it("escalates a mid-chunk death to a singleton and still fetches the tail behind it", async () => {
      const requestFn = mockPagedLens({ starve: [2], recoversAlone: true });
      const transport = createTransport(requestFn);

      const result = await transport.request(createRequest([1, 2, 3, 4].map(addr)));

      expect(decodePage(result)).toEqual({ results: [1n, 2n, 3n, 4n], skipped: [] });
      // The death is retried exactly once, alone; the tail behind it is re-packed at the rate the
      // page realized (one element served), so it comes back as singletons of its own.
      expect(requestedIndices(requestFn)).toEqual([[1, 2, 3, 4], [2], [3], [4]]);
    });

    it("stamps the escalation on the wide event", async () => {
      const requestFn = mockPagedLens({ starve: [2], recoversAlone: true });
      const transport = createTransport(requestFn);

      const { field } = await withFacet(() => transport.request(createRequest([1, 2, 3, 4].map(addr))));

      expect(field("attempts_unresolved")).toBe(1);
      expect(field("pages_escalated")).toBe(1);
      expect(field("elements_unresolved")).toBe(0);
      expect(field("elements_fetched")).toBe(4);
    });

    it("is terminal when the element dies alone, without throwing", async () => {
      const requestFn = mockPagedLens({ starve: [1] });
      const transport = createTransport(requestFn);

      const { result, context, field } = await withFacet(() => transport.request(createRequest([addr(1)])));

      // A page that adjudicates nothing but its own death is still one element attempted.
      expect(context.status).toBe("ok");
      expect(decodePage(result)).toEqual({ results: [], skipped: [0] });
      expect(field("elements_unresolved")).toBe(1);
      expect(field("elements_missing")).toBe(1);
      expect(field("pages_escalated")).toBe(0);
    });

    it("gives up on an element that dies again as a singleton", async () => {
      const requestFn = mockPagedLens({ starve: [2] });
      const transport = createTransport(requestFn);

      const { result, field } = await withFacet(() => transport.request(createRequest([1, 2].map(addr))));

      expect(decodePage(result)).toEqual({ results: [1n], skipped: [1] });
      // Adjudicated once in the original chunk, once alone — never a third time.
      expect(requestedIndices(requestFn)).toEqual([[1, 2], [2]]);
      expect(field("attempts_unresolved")).toBe(2);
      expect(field("pages_escalated")).toBe(1);
      expect(field("elements_unresolved")).toBe(1);
    });
  });

  describe("frames that die without reporting", () => {
    it("halves a corpse and gives up only once the element is alone", async () => {
      const requestFn = mockPagedLens({ fatal: [3] });
      const transport = createTransport(requestFn);

      const { result, field } = await withFacet(() => transport.request(createRequest([1, 2, 3, 4].map(addr))));

      expect(decodePage(result)).toEqual({ results: [1n, 2n, 4n], skipped: [2] });
      expect(field("splits_corpse")).toBeGreaterThanOrEqual(1);
      expect(field("splits_count")).toBe(field("splits_corpse"));
      expect(field("splits_size")).toBe(0);
      expect(field("elements_unresolved")).toBe(1);
      expect(field("elements_missing")).toBe(1);
    });

    it("throws a malformed-result revert instead of halving it", async () => {
      const requestFn = vi.fn().mockRejectedValue(revertWith(`${MALFORMED_RESULT_SELECTOR}${"00".repeat(64)}` as Hex));
      const transport = createTransport(requestFn);

      await expect(transport.request(createRequest([1, 2, 3, 4].map(addr)))).rejects.toThrow(
        /does not fit its declared layout/,
      );
      expect(requestFn).toHaveBeenCalledOnce();
    });
  });

  describe("protocol violations", () => {
    it.each([
      ["makes no progress", [], /adjudicated no elements/],
      [
        "attempts more than it was given",
        [success(1n), success(2n), success(3n)],
        /attempted 3 of 2 elements, expected 1\.\.2/,
      ],
      ["skips an index it never attempted", [success(1n), word(5)], /record 1 declines element 5/],
      ["repeats a skipped index", [word(0), word(0)], /record 1 declines element 0/],
      ["returns skipped indices out of order", [word(1), word(0)], /record 0 declines element 1/],
      [
        "tags a death that is not the last element adjudicated",
        [word(tag(0)), word(1)],
        /record 0 of 2 reports a gas death at 0/,
      ],
      ["tags two deaths", [word(tag(0)), word(tag(1))], /record 0 of 2 reports a gas death at 0/],
      ["skips past the death it reported", [word(1), word(tag(1))], /record 0 declines element 1/],
      [
        "reports a death above the elements it adjudicated",
        [success(1n), word(tag(2))],
        /record 1 of 2 reports a gas death at 2/,
      ],
    ])("throws when the lens %s", async (_name, records, expected) => {
      const requestFn = vi.fn().mockRejectedValue(revertWithRecords(...(records as string[])));
      const transport = createTransport(requestFn);

      const error = await transport.request(createRequest([addr(1), addr(2)])).catch((e) => e);

      expect(error.message).toMatch(expected);
    });

    it("throws a malformed-input revert instead of halving it", async () => {
      const requestFn = vi.fn().mockRejectedValue(revertWith(`${MALFORMED_INPUT_SELECTOR}${"00".repeat(32)}` as Hex));
      const transport = createTransport(requestFn);

      await expect(transport.request(createRequest([1, 2, 3, 4].map(addr)))).rejects.toThrow(/rejected the input wire/);
      expect(requestFn).toHaveBeenCalledOnce();
    });
  });
});

describe("viem interop", () => {
  it("is readable through readContract, which decodes against the lens abi", async () => {
    const requestFn = mockPagedLens({ decline: [2] });
    const client = createPublicClient({ transport: deployless(custom({ request: requestFn as never })) });

    const [results, skipped] = await readContract(client, {
      abi: [pageAbi],
      functionName: "page",
      args: [[1, 2, 3].map(addr)],
      factory: FACTORY,
      factoryData: FACTORY_DATA,
      address: TARGET_TO,
      stateOverride: [policy({ abi: pageAbi })],
    } as never);

    expect(results).toEqual([1n, 3n]);
    expect(skipped).toEqual([1n]);
  });
});
