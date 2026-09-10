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
import { type DeploylessConfig, deployless } from "../../src/transports/deployless/index.js";
import { ETH_CALL_POLICY_ADDRESS } from "../../src/transports/state-overrides.js";
import type { EIP1193Parameters } from "../../src/types.js";
import {
  ENVELOPE_ADDRESS,
  FACTORY_BYTECODE_REVERT,
  MALFORMED_INPUT_SELECTOR,
  MALFORMED_RESULT_SELECTOR,
  OK_SENTINEL,
  OOG_SENTINEL,
  unwrapDeploylessFactoryCall,
} from "../../src/utils/deployless/codec.envelope.js";
import { type PageGas, pageToStream, wireToArray } from "../../src/utils/deployless/codec.inner.js";
import { copyGas, floorGas, wireSize } from "../../src/utils/deployless/pricing.js";
import { createStubLogger, findDotted } from "../helpers/logger.js";
import { flatGas, gasOf } from "../helpers/page.js";

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

function createRequest(
  addrs: readonly Address[],
  batch?: Record<string, unknown>,
  stateOverride?: Record<string, unknown>,
): EthCallRequest {
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
      { ...stateOverride, [ETH_CALL_POLICY_ADDRESS]: { code: toHex(JSON.stringify(policy)) } },
    ],
  };
}

/** True when a chunk went out by override: a call to the envelope's address carrying its code. */
function isOverrideShaped(params: readonly unknown[]): boolean {
  const { to } = params[0] as { to?: string };
  if (to?.toLowerCase() !== ENVELOPE_ADDRESS.toLowerCase()) return false;
  const entry = (params[2] as Record<string, { code?: string }> | undefined)?.[ENVELOPE_ADDRESS];
  return entry?.code === FACTORY_BYTECODE_REVERT;
}

/** A chunk's `data` as initcode, whichever delivery sent it: by override the tuple travels bare. */
function initcodeShaped(params: readonly unknown[]): Hex {
  const { data } = params[0] as { data: Hex };
  return isOverrideShaped(params) ? (`${FACTORY_BYTECODE_REVERT}${data.slice(2)}` as Hex) : data;
}

/** The addresses a chunk carries, as 32-byte words off the wire, in either delivery. */
function sentAddresses(params: readonly unknown[]): readonly Hex[] {
  return wireToArray({ mode: "static", size: 32 }, unwrapDeploylessFactoryCall(initcodeShaped(params)).targetData);
}

/** The `params` of the `n`-th upstream call. */
const paramsOf = (requestFn: ReturnType<typeof vi.fn>, n = 0) => (requestFn.mock.calls[n]![0] as EthCallRequest).params;

function revertWith(data: Hex): Error & { data: Hex } {
  const err = new Error("execution reverted") as Error & { data: Hex };
  err.data = data;
  return err;
}

/** The five telemetry words as they sit on the wire. */
const header = ({ budget, fixed, sum, sumSquares, max }: PageGas) =>
  word(budget) + word(fixed) + word(sum) + word(sumSquares) + word(max);

/** A lens response whose records are given verbatim, costed flat for every record but a death. */
function revertWithRecords(...records: string[]) {
  const served = records.filter((r) => !r.startsWith("ffff")).length;
  return revertWith(`${OK_SENTINEL}${word(records.length)}${header(flatGas(served))}${records.join("")}` as Hex);
}

function revertWithPage(results: readonly bigint[], skipped: readonly number[], gas: PageGas, died?: number) {
  const page = {
    results: results.map((r) => `0x${word(r)}` as Hex),
    skipped,
    gas,
    ...(died === undefined ? {} : { died }),
  };
  return revertWith(`${OK_SENTINEL}${pageToStream(page).slice(2)}` as Hex);
}

type LensBehavior = {
  /** Most elements the lens will attempt in one call before stopping for gas, per chunk by its first address value. */
  pageSize?: number | ((firstValue: number) => number);
  /** Address values the lens deterministically declines. */
  decline?: readonly number[];
  /** Address values the lens reports a gas death on, ending the page at that index. */
  starve?: readonly number[];
  /** Whether a `starve` element resolves once it is the only element in the chunk. */
  recoversAlone?: boolean;
  /** Gas an attempt on the element with this address value costs; flat when omitted. */
  itemGas?: (value: number) => number;
  /** What the lens's frame can spend on attempts, per chunk by its first address value; by default exactly `pageSize` flat attempts. */
  budget?: number | ((firstValue: number) => number);
  /** What the frame spent before its first attempt, by the chunk's first address value and its tuple's bytes. */
  fixed?: (firstValue: number, tupleBytes: number) => number;
  /** Milliseconds a chunk's response waits, by its first address value; to reorder completions. */
  delay?: (firstValue: number) => number;
  /** A promise a chunk's response also waits for, by its first address value; to release completions by hand. */
  hold?: (firstValue: number) => Promise<unknown> | undefined;
};

/**
 * A conforming paginated lens: walks its input in index order, stops after `pageSize` attempts,
 * declines `decline` elements and reports a gas death on `starve` elements (no per-element cap).
 */
function mockPagedLens({
  pageSize = Infinity,
  decline = [],
  starve = [],
  recoversAlone = false,
  itemGas = () => 1_000,
  budget = typeof pageSize === "number" && pageSize !== Infinity ? pageSize * 1_000 : 10_000_000,
  fixed = () => 100_000,
  delay = () => 0,
  hold = () => undefined,
}: LensBehavior = {}) {
  return vi.fn().mockImplementation(async (args: { params: readonly unknown[] }) => {
    const addrs = sentAddresses(args.params);
    const first = addrValue(addrs[0]!);
    const frame = typeof budget === "number" ? budget : budget(first);
    const prologue = fixed(first, (initcodeShaped(args.params).length - FACTORY_BYTECODE_REVERT.length) / 2);
    const stopAt = typeof pageSize === "number" ? pageSize : pageSize(first);
    await new Promise((resolve) => setTimeout(resolve, delay(first)));
    await hold(first);
    const results: bigint[] = [];
    const skipped: number[] = [];
    const costs: number[] = [];
    for (let i = 0; i < addrs.length && i < stopAt; i++) {
      const value = addrValue(addrs[i]!);
      if (starve.includes(value) && !(recoversAlone && addrs.length === 1)) {
        throw revertWithPage(results, skipped, gasOf(costs, frame, prologue), i);
      }
      costs.push(itemGas(value));
      if (decline.includes(value)) skipped.push(i);
      else results.push(BigInt(value));
    }
    throw revertWithPage(results, skipped, gasOf(costs, frame, prologue));
  });
}

function createTransport(requestFn: ReturnType<typeof vi.fn>, config?: DeploylessConfig) {
  return deployless(custom({ request: requestFn as never }), config)({ retryCount: 0 } as never);
}

/**
 * A cap and a stated cost that open at exactly `k` elements per chunk: a million per item, a cap
 * with half a million to spare, which the calldata's intrinsic gas (about a hundred thousand) fits.
 */
const openAt = (k: number) => ({
  gasLimit: k * 1_000_000 + 500_000,
  batch: { gas: { fixed: 0, item: { avg: 1_000_000 } } },
});

/** What a node deducts for an `eth_call`'s data before the envelope runs, as `pricing.ts` prices it. */
function intrinsicGasOf(data: Hex, delivery: "initcode" | "override" = "initcode"): number {
  const bytes = (data.length - 2) / 2;
  let zeros = 0;
  for (let i = 2; i < data.length; i += 2) if (data.slice(i, i + 2) === "00") zeros++;
  const calldata = 21_000 + 4 * zeros + 16 * (bytes - zeros) + 2;
  return delivery === "initcode" ? calldata + 32_000 + 2 * Math.ceil(bytes / 32) : calldata;
}

/** EIP-7623's floor for a chunk's sent bytes — the line that binds a lone element. */
const floorGasOf = (data: Hex) => floorGas(wireSize(data));

/** The prologue's copy of a creation `eth_call`'s argument tuple, the part of a page's `fixed` that is the chunk's bytes. */
function copyGasOf(data: Hex): number {
  return copyGas((data.length - FACTORY_BYTECODE_REVERT.length) / 2, false);
}

/** `fixed_gas` as the pool reports it: the largest `fixed` less the copy of its own chunk's bytes, over every call. */
function expectedFixed0(requestFn: ReturnType<typeof vi.fn>, fixed: (first: number) => number = () => 100_000): number {
  return Math.max(
    ...requestFn.mock.calls.map((call) => {
      const { params } = call[0] as EthCallRequest;
      return fixed(addrValue(sentAddresses(params)[0]!)) - copyGasOf(initcodeShaped(params));
    }),
  );
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
  return requestFn.mock.calls.map((call) => sentAddresses((call[0] as EthCallRequest).params).map(addrValue));
}

/** The delivery each upstream call went out in, one entry per call. */
function deliveries(requestFn: ReturnType<typeof vi.fn>): ("override" | "initcode")[] {
  return requestFn.mock.calls.map((call) =>
    isOverrideShaped((call[0] as EthCallRequest).params) ? "override" : "initcode",
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

  it("reports every unservable element, in ascending order", async () => {
    const requestFn = mockPagedLens({ decline: [2], starve: [4] });
    const transport = createTransport(requestFn);

    const page = decodePage(await transport.request(createRequest([1, 2, 3, 4].map(addr))));

    expect(page).toEqual({ results: [1n, 3n], skipped: [1, 3] });
  });

  it("stamps paginated continuations and unservable elements onto the wide event", async () => {
    // Serves 2 per call and declines the element valued 3, so the run both continues and
    // ends up short: one continuation, two flushes, one element the lens refused.
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
    expect(field("flushes")).toBe(2);
    expect(field("continuation_depth_max")).toBe(1);
    expect(field("pages_escalated")).toBe(0);
    expect(field("attempts_unresolved")).toBe(0);
    // A lens stopping early is a continuation, not a bisect.
    expect(field("splits_count")).toBe(0);
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
      // The death is retried exactly once, alone. The tail behind it is packed from what the page
      // reported about the element it did serve, not punished for the death, so it stays whole.
      expect(requestedIndices(requestFn)).toEqual([[1, 2, 3, 4], [2], [3, 4]]);
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
    it("throws on the deploy out-of-gas marker instead of halving it", async () => {
      const requestFn = vi.fn().mockRejectedValue(revertWith(OOG_SENTINEL));
      const transport = createTransport(requestFn);

      await expect(transport.request(createRequest([1, 2, 3, 4].map(addr)))).rejects.toThrow(
        /ran out of gas under this node's cap/,
      );
      expect(requestFn).toHaveBeenCalledOnce();
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

    it("propagates a page in the previous format as an ordinary revert", async () => {
      const previous = `0x1824683e${word(1)}${word(0).repeat(4)}${success(1n)}` as Hex;
      const requestFn = vi.fn().mockRejectedValue(revertWith(previous));

      await expect(createTransport(requestFn).request(createRequest([addr(1)]))).rejects.toThrow(/execution reverted/);
    });

    it("throws a malformed-input revert instead of halving it", async () => {
      const requestFn = vi.fn().mockRejectedValue(revertWith(`${MALFORMED_INPUT_SELECTOR}${"00".repeat(32)}` as Hex));
      const transport = createTransport(requestFn);

      await expect(transport.request(createRequest([1, 2, 3, 4].map(addr)))).rejects.toThrow(/rejected the input wire/);
      expect(requestFn).toHaveBeenCalledOnce();
    });
  });
});

describe("opening wave", () => {
  it("sizes the opening chunks from the provider's cap and the lens's stated cost", async () => {
    const requestFn = mockPagedLens();
    const { gasLimit, batch } = openAt(4);

    const { result, field } = await withFacet(() =>
      createTransport(requestFn, { gasLimit }).request(createRequest([1, 2, 3, 4, 5, 6, 7, 8].map(addr), batch)),
    );

    expect(decodeResults(result)).toEqual([1n, 2n, 3n, 4n, 5n, 6n, 7n, 8n]);
    expect(requestedIndices(requestFn)).toEqual([
      [1, 2, 3, 4],
      [5, 6, 7, 8],
    ]);
    expect(field("flushes")).toBe(0);
    expect(field("gas_limit")).toBe(gasLimit);
  });

  it("charges the fixed cost against the cap", async () => {
    const requestFn = mockPagedLens();
    const { gasLimit } = openAt(4);
    const batch = { gas: { fixed: 1_000_000, item: { avg: 1_000_000 } } };

    await createTransport(requestFn, { gasLimit }).request(createRequest([1, 2, 3, 4, 5, 6].map(addr), batch));

    expect(requestedIndices(requestFn)).toEqual([
      [1, 2, 3],
      [4, 5, 6],
    ]);
  });

  it("keeps headroom for the stated spread, as continuations do", async () => {
    const requestFn = mockPagedLens();
    const { gasLimit } = openAt(4);
    const batch = { gas: { fixed: 0, item: { avg: 1_000_000, stddev: 300_000 } } };

    await createTransport(requestFn, { gasLimit }).request(createRequest([1, 2, 3, 4, 5, 6].map(addr), batch));

    expect(requestedIndices(requestFn)).toEqual([
      [1, 2, 3],
      [4, 5, 6],
    ]);
  });

  it("charges the calldata's intrinsic gas against the cap", async () => {
    const requestFn = mockPagedLens();
    const { batch } = openAt(4);

    await createTransport(requestFn, { gasLimit: 4_000_000 }).request(
      createRequest([1, 2, 3, 4, 5, 6].map(addr), batch),
    );

    expect(requestedIndices(requestFn)).toEqual([
      [1, 2, 3],
      [4, 5, 6],
    ]);
  });

  it("prices a chunk's calldata to the gas of the request it actually sends", async () => {
    // A cap of exactly three one-gas elements plus their chunk's intrinsic gas, its copy and the
    // lens's prologue fits three, not four. The prologue is stated high so EIP-7623's floor, which
    // outweighs the intrinsic gas of a small nonzero-heavy request, is not what binds.
    const probe = mockPagedLens();
    await createTransport(probe).request(createRequest([1, 2, 3].map(addr)));
    const sent = (probe.mock.calls[0]![0] as EthCallRequest).params[0].data as Hex;
    const batch = { gas: { fixed: 1_000_000, item: { avg: 1 } } };
    const exact = mockPagedLens();
    const overByOne = mockPagedLens();

    const bytes = intrinsicGasOf(sent) + copyGasOf(sent) + 1_000_000;
    await createTransport(exact, { gasLimit: bytes + 3 }).request(createRequest([1, 2, 3, 4, 5, 6].map(addr), batch));
    await createTransport(overByOne, { gasLimit: bytes + 2 }).request(
      createRequest([1, 2, 3, 4, 5, 6].map(addr), batch),
    );

    expect(requestedIndices(exact)).toEqual([
      [1, 2, 3],
      [4, 5, 6],
    ]);
    expect(requestedIndices(overByOne)).toEqual([
      [1, 2],
      [3, 4],
      [5, 6],
    ]);
  });

  it("sends an element alone rather than withholding it when the stated cost exceeds the cap", async () => {
    const requestFn = mockPagedLens();
    const batch = { gas: { fixed: 0, item: { avg: 10_000_000 } } };

    const { result } = await withFacet(() =>
      createTransport(requestFn, { gasLimit: 1_000_000 }).request(createRequest([1, 2, 3].map(addr), batch)),
    );

    expect(requestedIndices(requestFn)).toEqual([[1], [2], [3]]);
    expect(decodePage(result)).toEqual({ results: [1n, 2n, 3n], skipped: [] });
  });

  it("recovers the wave a bytes-only opening always pays on a multi-page input", async () => {
    const nine = [1, 2, 3, 4, 5, 6, 7, 8, 9].map(addr);
    const blind = mockPagedLens({ pageSize: 3 });
    const sized = mockPagedLens({ pageSize: 3 });
    const { gasLimit, batch } = openAt(3);

    const a = await withFacet(() => createTransport(blind).request(createRequest(nine)));
    const b = await withFacet(() => createTransport(sized, { gasLimit }).request(createRequest(nine, batch)));

    expect(decodeResults(a.result)).toEqual(decodeResults(b.result));
    expect(requestedIndices(blind)).toEqual([
      [1, 2, 3, 4, 5, 6, 7, 8, 9],
      [4, 5, 6],
      [7, 8, 9],
    ]);
    expect(requestedIndices(sized)).toEqual([
      [1, 2, 3],
      [4, 5, 6],
      [7, 8, 9],
    ]);
    expect([a.field("flushes"), a.field("pages_continued")]).toEqual([2, 1]);
    expect([b.field("flushes"), b.field("pages_continued")]).toEqual([0, 0]);
  });

  it("degrades to the bytes-only opening when the stated cost is too low", async () => {
    const nine = [1, 2, 3, 4, 5, 6, 7, 8, 9].map(addr);
    const requestFn = mockPagedLens({ pageSize: 3 });
    const { gasLimit, batch } = openAt(10);

    const { field } = await withFacet(() =>
      createTransport(requestFn, { gasLimit }).request(createRequest(nine, batch)),
    );

    expect(requestedIndices(requestFn)).toEqual([
      [1, 2, 3, 4, 5, 6, 7, 8, 9],
      [4, 5, 6],
      [7, 8, 9],
    ]);
    expect(field("flushes")).toBe(2);
  });

  it("opens by bytes alone when either the cap or the cost is missing, stamping the cap when stated", async () => {
    const capOnly = mockPagedLens();
    const costOnly = mockPagedLens();
    const three = [1, 2, 3].map(addr);
    const { gasLimit, batch } = openAt(1);

    const a = await withFacet(() => createTransport(capOnly, { gasLimit }).request(createRequest(three)));
    const b = await withFacet(() => createTransport(costOnly).request(createRequest(three, batch)));

    expect(capOnly).toHaveBeenCalledOnce();
    expect(costOnly).toHaveBeenCalledOnce();
    expect([a.field("gas_limit"), b.field("gas_limit")]).toEqual([gasLimit, undefined]);
  });

  it.each([
    ["a non-positive cap", 0, { fixed: 0, item: { avg: 1_000_000 } }],
    ["a negative fixed cost", 1_500_000, { fixed: -1, item: { avg: 1_000_000 } }],
    ["a zero item cost", 1_500_000, { fixed: 0, item: { avg: 0 } }],
    ["a non-numeric item cost", 1_500_000, { fixed: 0, item: { avg: "1000000" } }],
    ["a negative spread", 1_500_000, { fixed: 0, item: { avg: 1_000_000, stddev: -1 } }],
    ["a null cost", 1_500_000, null],
    ["a cost that is not an object", 1_500_000, "cheap"],
    ["a cost without an item", 1_500_000, { fixed: 0 }],
    ["a null item", 1_500_000, { fixed: 0, item: null }],
  ])("ignores %s and packs by bytes alone", async (_name, gasLimit, gas) => {
    const requestFn = mockPagedLens();

    const { field } = await withFacet(() =>
      createTransport(requestFn, { gasLimit }).request(createRequest([1, 2, 3].map(addr), { gas })),
    );

    expect(requestFn).toHaveBeenCalledOnce();
    expect(field("gas_limit")).toBe(gasLimit > 0 ? gasLimit : undefined);
  });
});

describe("packing from telemetry", () => {
  it("packs continuations at the rate the pages reported, not at the parent's count", async () => {
    // The lens stops after two attempts for a reason of its own but reports a frame worth four.
    const requestFn = mockPagedLens({ pageSize: 2, budget: 4_000 });

    await createTransport(requestFn).request(createRequest([1, 2, 3, 4, 5, 6, 7, 8, 9].map(addr)));

    expect(requestedIndices(requestFn).slice(0, 3)).toEqual([
      [1, 2, 3, 4, 5, 6, 7, 8, 9],
      [3, 4, 5, 6],
      [7, 8, 9],
    ]);
  });

  it("packs a tail from the pool of every page that has landed", async () => {
    // Two opening chunks: one of cheap items, one of expensive ones, each stopping after two. Pooled,
    // the spread is wide enough that one item is all the budget admits with headroom; alone, the
    // cheap chunk would have asked for six.
    const requestFn = mockPagedLens({ pageSize: 2, budget: 6_000, itemGas: (v) => (v <= 4 ? 1_000 : 3_000) });

    const { gasLimit, batch } = openAt(4);

    await createTransport(requestFn, { gasLimit }).request(createRequest([1, 2, 3, 4, 5, 6, 7, 8].map(addr), batch));

    expect(requestedIndices(requestFn)).toEqual([[1, 2, 3, 4], [5, 6, 7, 8], [3], [4], [7], [8]]);
  });

  it("packs the same tails whichever chunk settles first", async () => {
    const cheapFirst = mockPagedLens({ pageSize: 2, budget: 6_000, itemGas: (v) => (v <= 4 ? 1_000 : 3_000) });
    const dearFirst = mockPagedLens({
      pageSize: 2,
      budget: 6_000,
      itemGas: (v) => (v <= 4 ? 1_000 : 3_000),
      delay: (first) => (first <= 4 ? 20 : 0),
    });
    const eight = [1, 2, 3, 4, 5, 6, 7, 8].map(addr);
    const { gasLimit, batch } = openAt(4);

    await createTransport(cheapFirst, { gasLimit }).request(createRequest(eight, batch));
    await createTransport(dearFirst, { gasLimit }).request(createRequest(eight, batch));

    expect(requestedIndices(dearFirst).slice(2).sort()).toEqual(requestedIndices(cheapFirst).slice(2).sort());
  });

  it("never packs below one element, however small the smallest frame", async () => {
    const requestFn = mockPagedLens({ pageSize: 2, budget: (first) => (first <= 4 ? 5_000 : 500) });
    const { gasLimit, batch } = openAt(4);

    await createTransport(requestFn, { gasLimit }).request(createRequest([1, 2, 3, 4, 5, 6, 7, 8].map(addr), batch));

    expect(requestedIndices(requestFn)).toEqual([[1, 2, 3, 4], [5, 6, 7, 8], [3], [4], [7], [8]]);
  });

  it("packs a tail whole while no attempt has been costed", async () => {
    const requestFn = mockPagedLens({ starve: [1], recoversAlone: true });

    await createTransport(requestFn).request(createRequest([1, 2, 3, 4].map(addr)));

    expect(requestedIndices(requestFn)).toEqual([[1, 2, 3, 4], [1], [2, 3, 4]]);
  });

  it("takes the smallest frame seen as the request's budget", async () => {
    const requestFn = mockPagedLens({ pageSize: 2, budget: (first) => (first <= 2 ? 5_000 : 3_000) });
    const { gasLimit, batch } = openAt(2);

    const { field } = await withFacet(() =>
      createTransport(requestFn, { gasLimit }).request(createRequest([1, 2, 3, 4].map(addr), batch)),
    );

    expect(field("frame_gas")).toBe(3_000);
  });

  it("packs a wide spread more conservatively than a flat one with the same mean", async () => {
    const flat = mockPagedLens({ pageSize: 2, budget: 6_000, itemGas: () => 2_000 });
    const spread = mockPagedLens({ pageSize: 2, budget: 6_000, itemGas: (v) => (v % 2 ? 1_000 : 3_000) });
    const six = [1, 2, 3, 4, 5, 6].map(addr);

    await createTransport(flat).request(createRequest(six));
    await createTransport(spread).request(createRequest(six));

    expect(requestedIndices(flat)[1]).toEqual([3, 4, 5]);
    expect(requestedIndices(spread)[1]).toEqual([3]);
  });

  it("stamps the pooled telemetry", async () => {
    const requestFn = mockPagedLens({ pageSize: 5, budget: 5_000 });

    const { field } = await withFacet(() =>
      createTransport(requestFn).request(createRequest([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].map(addr))),
    );

    expect(field("frame_gas")).toBe(5_000);
    expect(field("fixed_gas")).toBe(expectedFixed0(requestFn));
    expect(field("item_gas_avg")).toBe(1_000);
    expect(field("item_gas_stddev")).toBe(0);
    expect(field("item_gas_max")).toBe(1_000);
  });

  it("reads the provider's cap back off the frame, the prologue and the calldata", async () => {
    const requestFn = mockPagedLens();

    const { field } = await withFacet(() => createTransport(requestFn).request(createRequest([1, 2, 3].map(addr))));

    const sent = (requestFn.mock.calls[0]![0] as EthCallRequest).params[0].data as Hex;
    expect(field("gas_limit_observed")).toBe(intrinsicGasOf(sent) + 100_000 + 10_000_000);
  });

  it("bounds a continuation by the smallest cap and the largest prologue any page reported", async () => {
    // Two opening chunks report frames worth six and four attempts; the second also spent three
    // thousand more before its first attempt. The cap is read off the first, the prologue off the
    // second, and together they leave room for three attempts, which is what every tail is packed to.
    const requestFn = mockPagedLens({
      pageSize: 2,
      budget: (first) => (first <= 6 ? 6_000 : 4_000),
      fixed: (first) => (first <= 6 ? 100_000 : 103_000),
    });
    const { gasLimit, batch } = openAt(6);

    const { field } = await withFacet(() =>
      createTransport(requestFn, { gasLimit }).request(
        createRequest(
          [...Array(12).keys()].map((i) => addr(i + 1)),
          batch,
        ),
      ),
    );

    expect(requestedIndices(requestFn).slice(2, 4)).toEqual([
      [3, 4, 5],
      [6, 9, 10],
    ]);
    expect(field("fixed_gas")).toBe(expectedFixed0(requestFn, (first) => (first <= 6 ? 100_000 : 103_000)));
  });

  it("packs a tail from the stated item cost while nothing has been served", async () => {
    // Both opening pages die at their head, so the pool has a cap and a prologue but no attempt
    // cost. The stated cost fills in, against the observed cap: one item per chunk here.
    const requestFn = mockPagedLens({ starve: [1, 4], recoversAlone: true, budget: 1_500_000 });
    const { gasLimit, batch } = openAt(3);

    await createTransport(requestFn, { gasLimit }).request(createRequest([1, 2, 3, 4, 5, 6].map(addr), batch));

    expect(requestedIndices(requestFn).slice(2).sort()).toEqual([[1], [2], [3], [4], [5], [6]]);
  });

  it("stamps only the frame's words when nothing was served", async () => {
    const requestFn = mockPagedLens({ starve: [1] });

    const { field } = await withFacet(() => createTransport(requestFn).request(createRequest([addr(1)])));

    expect(field("frame_gas")).toBe(10_000_000);
    expect(field("fixed_gas")).toBe(expectedFixed0(requestFn));
    expect(field("item_gas_avg")).toBeUndefined();
  });
});

describe("continuations", () => {
  const eight = [1, 2, 3, 4, 5, 6, 7, 8].map(addr);

  function gate() {
    let release!: () => void;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    return { held, release };
  }

  /** Resolves once `requestFn` has gone two ticks of the timer queue without a new call; nothing here does I/O. */
  async function quiescence(...requestFns: ReturnType<typeof vi.fn>[]) {
    const calls = () => requestFns.reduce((n, fn) => n + fn.mock.calls.length, 0);
    for (let quiet = 0, seen = calls(); quiet < 2; ) {
      await new Promise((resolve) => setTimeout(resolve, 10));
      const now = calls();
      quiet = now === seen ? quiet + 1 : 0;
      seen = now;
    }
  }

  it("coalesces the small tails of several pages into one chunk", async () => {
    // Four opening pages each leave one element behind; the four are sent together once nothing
    // else is in flight, instead of as four requests.
    const requestFn = mockPagedLens({ pageSize: 1, budget: 10_000 });
    const { gasLimit, batch } = openAt(2);

    const { result, field } = await withFacet(() =>
      createTransport(requestFn, { gasLimit }).request(createRequest(eight, batch)),
    );

    expect(decodeResults(result)).toEqual([1n, 2n, 3n, 4n, 5n, 6n, 7n, 8n]);
    expect(requestedIndices(requestFn).slice(0, 5)).toEqual([
      [1, 2],
      [3, 4],
      [5, 6],
      [7, 8],
      [2, 4, 6, 8],
    ]);
    expect(field("continuations")).toBe("fill");
    expect(field("flushes_full")).toBe(0);
    expect(field("pages_continued")).toBe(7);
  });

  it("sends a full page while siblings are still in flight and holds a partial one for the drain", async () => {
    // Frames worth three attempts; the third opening chunk answers late. Two early tails make
    // more than a page: the page goes at once, its remainder waits for the third chunk.
    const late = gate();
    const requestFn = mockPagedLens({
      pageSize: 2,
      budget: 3_000,
      hold: (first) => (first === 9 ? late.held : undefined),
    });
    const twelve = [...Array(12).keys()].map((i) => addr(i + 1));
    const { gasLimit, batch } = openAt(4);

    const request = withFacet(() => createTransport(requestFn, { gasLimit }).request(createRequest(twelve, batch)));
    await quiescence(requestFn);
    const early = requestedIndices(requestFn);
    late.release();
    const { result, field } = await request;

    expect(early).toEqual([
      [1, 2, 3, 4],
      [5, 6, 7, 8],
      [9, 10, 11, 12],
      [3, 4, 7],
    ]);
    // Once the third chunk lands nothing earlier is open, so the remainder goes with the full page.
    expect(requestedIndices(requestFn)).toEqual([...early, [7, 8, 11], [12], [11]]);
    expect(decodeResults(result)).toHaveLength(12);
    expect([field("flushes_full"), field("flushes_drain"), field("flushes_eager")]).toEqual([2, 2, 0]);
    expect(field("flushes")).toBe(4);
  });

  it("sends the remainder with the full pages once nothing earlier could still add to it", async () => {
    // Two opening chunks leave four tails that pack one per chunk. All four go out together: the
    // full pages just dispatched belong to the next round and cannot feed this remainder.
    const late = gate();
    const requestFn = mockPagedLens({
      pageSize: 2,
      budget: 6_000,
      itemGas: (v) => (v <= 4 ? 1_000 : 3_000),
      hold: (first) => ([3, 4, 7].includes(first) ? late.held : undefined),
    });
    const { gasLimit, batch } = openAt(4);

    const request = createTransport(requestFn, { gasLimit }).request(createRequest(eight, batch));
    await quiescence(requestFn);
    const early = requestedIndices(requestFn);
    late.release();
    await request;

    expect(early).toEqual([[1, 2, 3, 4], [5, 6, 7, 8], [3], [4], [7], [8]]);
  });

  it("re-checks whether the pending tails fill a page when telemetry arrives without a tail", async () => {
    // The first page leaves two cheap-looking elements pending. The second serves everything it
    // was given, dearly; pooled, two attempts no longer fit with headroom, so one goes at once.
    const requestFn = mockPagedLens({
      pageSize: 2,
      budget: 10_000,
      itemGas: (v) => (v >= 5 ? 5_000 : 1_000),
      delay: (first) => (first === 5 ? 20 : 0),
    });
    const { gasLimit, batch } = openAt(4);

    await createTransport(requestFn, { gasLimit }).request(createRequest([1, 2, 3, 4, 5, 6].map(addr), batch));

    expect(requestedIndices(requestFn)).toEqual([[1, 2, 3, 4], [5, 6], [3], [4]]);
  });

  it("under `eager`, sends each tail as its page lands", async () => {
    const lateA = gate();
    const lateB = gate();
    const eager = mockPagedLens({
      pageSize: 1,
      budget: 10_000,
      hold: (first) => (first === 3 ? lateA.held : undefined),
    });
    const fill = mockPagedLens({
      pageSize: 1,
      budget: 10_000,
      hold: (first) => (first === 3 ? lateB.held : undefined),
    });
    const { gasLimit, batch } = openAt(2);
    const four = [1, 2, 3, 4].map(addr);

    const a = withFacet(() =>
      createTransport(eager, { gasLimit }).request(createRequest(four, { ...batch, continuations: "eager" })),
    );
    const b = withFacet(() => createTransport(fill, { gasLimit }).request(createRequest(four, batch)));
    await quiescence(eager, fill);
    const eagerEarly = requestedIndices(eager);
    const fillEarly = requestedIndices(fill);
    lateA.release();
    lateB.release();
    const [{ field }] = await Promise.all([a, b]);

    expect(eagerEarly).toEqual([[1, 2], [3, 4], [2]]);
    expect(fillEarly).toEqual([
      [1, 2],
      [3, 4],
    ]);
    expect(requestedIndices(eager)).toEqual([[1, 2], [3, 4], [2], [4]]);
    expect(requestedIndices(fill)).toEqual([[1, 2], [3, 4], [2, 4], [4]]);
    expect(field("continuations")).toBe("eager");
    expect([field("flushes_eager"), field("flushes_drain")]).toEqual([1, 1]);
  });

  it("reads an unknown mode as `fill`", async () => {
    const requestFn = mockPagedLens({ pageSize: 1, budget: 10_000 });
    const { gasLimit, batch } = openAt(2);

    const { field } = await withFacet(() =>
      createTransport(requestFn, { gasLimit }).request(
        createRequest([1, 2, 3, 4].map(addr), { ...batch, continuations: "sideways" }),
      ),
    );

    expect(requestedIndices(requestFn).slice(0, 3)).toEqual([
      [1, 2],
      [3, 4],
      [2, 4],
    ]);
    expect(field("continuations")).toBe("fill");
  });

  it("maps a coalesced chunk's declines, death and tail back to the caller's indices", async () => {
    // Opening pages stop after one; the coalesced chunk [2, 4, 6, 8] runs on: 4 is declined, 6
    // dies and is retried alone, 8 is left behind and fetched after.
    const requestFn = mockPagedLens({
      pageSize: (first) => (first % 2 ? 1 : Infinity),
      budget: 10_000,
      decline: [4],
      starve: [6],
      recoversAlone: true,
    });
    const { gasLimit, batch } = openAt(2);

    const { result, field } = await withFacet(() =>
      createTransport(requestFn, { gasLimit }).request(createRequest(eight, batch)),
    );

    expect(decodePage(result)).toEqual({ results: [1n, 2n, 3n, 5n, 6n, 7n, 8n], skipped: [3] });
    expect(requestedIndices(requestFn).slice(4)).toEqual([[2, 4, 6, 8], [6], [8]]);
    expect(field("pages_escalated")).toBe(1);
    expect(field("elements_unresolved")).toBe(0);
  });

  it("counts how deep the chain of continuations went", async () => {
    const requestFn = mockPagedLens({ pageSize: 1, budget: 10_000 });

    const { field } = await withFacet(() => createTransport(requestFn).request(createRequest([1, 2, 3].map(addr))));

    expect(requestedIndices(requestFn)).toEqual([[1, 2, 3], [2, 3], [3]]);
    expect(field("continuation_depth_max")).toBe(2);
    expect(field("flushes")).toBe(2);
  });

  it("settles without an upstream call when every element is oversize", async () => {
    const requestFn = mockPagedLens();

    const { result, field } = await withFacet(() =>
      createTransport(requestFn).request(createRequest([1, 2].map(addr), { batchSize: 100 })),
    );

    expect(requestFn).not.toHaveBeenCalled();
    expect(decodePage(result)).toEqual({ results: [], skipped: [0, 1] });
    expect(field("elements_declined_oversize")).toBe(2);
  });

  describe("failure", () => {
    /** Serves one element per page, except that the chunk opening at `first` fails once `held` resolves. */
    function failingAt(first: number, held: Promise<unknown>, lens = mockPagedLens({ pageSize: 1 })) {
      return vi.fn().mockImplementation(async (args: { params: readonly unknown[] }) => {
        if (addrValue(sentAddresses(args.params)[0]!) !== first) return lens(args);
        await held;
        throw revertWith("0xdeadbeef");
      });
    }

    it("dispatches nothing further once a chunk has failed", async () => {
      // The first page's tail is pending when the second chunk fails; it is never sent.
      const late = gate();
      const requestFn = failingAt(3, late.held);
      const { gasLimit, batch } = openAt(2);
      const { logger, events } = createStubLogger();

      const request = withLogging(
        () => createTransport(requestFn, { gasLimit }).request(createRequest([1, 2, 3, 4].map(addr), batch)),
        { logger },
      );
      request.catch(() => {});
      await quiescence(requestFn);
      late.release();

      await expect(request).rejects.toThrow(/execution reverted/);
      expect(requestedIndices(requestFn)).toEqual([
        [1, 2],
        [3, 4],
      ]);
      expect(findDotted(events[0]!.context, "viem-dlc-deployless", "eth_call.elements_fetched")).toBe(1);
    });

    it("lets chunks in flight settle, and commit, before the failure surfaces", async () => {
      const late = gate();
      const requestFn = failingAt(1, Promise.resolve(), mockPagedLens({ pageSize: 1, hold: () => late.held }));
      const { gasLimit, batch } = openAt(2);
      const { logger, events } = createStubLogger();
      let settled = false;

      const request = withLogging(
        () => createTransport(requestFn, { gasLimit }).request(createRequest([1, 2, 3, 4].map(addr), batch)),
        { logger },
      );
      request.then(
        () => {
          settled = true;
        },
        () => {
          settled = true;
        },
      );
      await quiescence(requestFn);
      const settledEarly = settled;
      late.release();

      await expect(request).rejects.toThrow(/execution reverted/);
      expect(settledEarly).toBe(false);
      // The surviving chunk's page was committed, and its tail was never sent.
      expect(findDotted(events[0]!.context, "viem-dlc-deployless", "eth_call.elements_fetched")).toBe(1);
      expect(requestFn).toHaveBeenCalledTimes(2);
    });

    it("does not escalate a death that lands after a failure", async () => {
      const late = gate();
      const lens = mockPagedLens({ starve: [3], recoversAlone: true, hold: () => late.held });
      const requestFn = failingAt(1, Promise.resolve(), lens);
      const { gasLimit, batch } = openAt(2);

      const request = createTransport(requestFn, { gasLimit }).request(createRequest([1, 2, 3, 4].map(addr), batch));
      request.catch(() => {});
      await quiescence(requestFn);
      late.release();

      await expect(request).rejects.toThrow(/execution reverted/);
      expect(requestedIndices(requestFn)).toEqual([
        [1, 2],
        [3, 4],
      ]);
    });
  });

  it("under `fill`, holds a tail for the other half of a refused chunk", async () => {
    // The halves are the round the tail waits on, so the held half gates the remainder; once it
    // lands, both tails go out together.
    const late = gate();
    const lens = mockPagedLens({ pageSize: 1, budget: 10_000, hold: (first) => (first === 3 ? late.held : undefined) });
    const requestFn = vi.fn().mockImplementation(async (args: { params: readonly unknown[] }) => {
      if (sentAddresses(args.params).length === 4) throw new Error("request too large");
      return lens(args);
    });

    const request = createTransport(requestFn).request(createRequest([1, 2, 3, 4].map(addr)));
    await quiescence(requestFn);
    const early = requestedIndices(requestFn);
    late.release();
    await request;

    expect(early).toEqual([
      [1, 2, 3, 4],
      [1, 2],
      [3, 4],
    ]);
    expect(requestedIndices(requestFn).slice(0, 4)).toEqual([...early, [2, 4]]);
  });

  it("lets each half of a refused chunk settle on its own", async () => {
    // The provider refuses the four-element chunk; the halves go out together, the second is held,
    // and under `eager` the first half's tail does not wait for it.
    const late = gate();
    const lens = mockPagedLens({ pageSize: 1, budget: 10_000, hold: (first) => (first === 3 ? late.held : undefined) });
    const requestFn = vi.fn().mockImplementation(async (args: { params: readonly unknown[] }) => {
      if (sentAddresses(args.params).length === 4) throw new Error("request too large");
      return lens(args);
    });

    const request = withFacet(() =>
      createTransport(requestFn).request(createRequest([1, 2, 3, 4].map(addr), { continuations: "eager" })),
    );
    await quiescence(requestFn);
    const early = requestedIndices(requestFn);
    late.release();
    const { result, field } = await request;

    expect(early).toEqual([[1, 2, 3, 4], [1, 2], [3, 4], [2]]);
    expect(decodeResults(result)).toEqual([1n, 2n, 3n, 4n]);
    expect([field("splits_size"), field("splits_max_depth")]).toEqual([1, 1]);
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

describe("stated cap on the wire", () => {
  const onChain = (id: number, requestFn: ReturnType<typeof vi.fn>, config?: DeploylessConfig) =>
    deployless(custom({ request: requestFn as never }), config)({ retryCount: 0, chain: { id } } as never);

  it("sends gasLimit as every chunk's gas on a chain whose nodes give an unspecified gas a fixed default", async () => {
    const requestFn = mockPagedLens();
    const { gasLimit, batch } = openAt(2);

    await onChain(143, requestFn, { gasLimit }).request(createRequest([1, 2, 3, 4].map(addr), batch));

    expect(requestFn).toHaveBeenCalledTimes(2);
    for (const call of requestFn.mock.calls) {
      expect((call[0] as { params: readonly unknown[] }).params[0]).toMatchObject({ gas: toHex(gasLimit) });
    }
  });

  it("sends no gas where an unspecified gas is the cap, on an unknown chain, or without a gasLimit", async () => {
    const { gasLimit, batch } = openAt(2);
    const txnOf = (requestFn: ReturnType<typeof vi.fn>) =>
      (requestFn.mock.calls[0]![0] as { params: readonly unknown[] }).params[0] as Record<string, unknown>;

    const mainnet = mockPagedLens();
    await onChain(1, mainnet, { gasLimit }).request(createRequest([1, 2].map(addr), batch));
    const unknown = mockPagedLens();
    await onChain(999_999, unknown, { gasLimit }).request(createRequest([1, 2].map(addr), batch));
    const unstated = mockPagedLens();
    await onChain(143, unstated).request(createRequest([1, 2].map(addr)));

    for (const requestFn of [mainnet, unknown, unstated])
      expect(txnOf(requestFn)).toEqual({ data: txnOf(requestFn).data });
  });
});

describe("override delivery", () => {
  const OVERRIDE = { envelope: "override" } as const;
  const four = [1, 2, 3, 4].map(addr);
  const CALLER = "0x4444444444444444444444444444444444444444" as const;
  const byteLength = (hex: Hex) => (hex.length - 2) / 2;

  /**
   * A provider that answers override-delivered chunks with `answer`; every other chunk, and any
   * override chunk `answer` hands back to `serve`, is served by the lens.
   */
  function withOverride(
    answer: (serve: () => Promise<unknown>, params: readonly unknown[]) => unknown,
    lens = mockPagedLens(),
  ) {
    return vi.fn().mockImplementation(async (args: { params: readonly unknown[] }) => {
      if (!isOverrideShaped(args.params)) return lens(args);
      return answer(() => lens(args), args.params);
    });
  }

  /** A provider that drops the third parameter: the call reaches an empty account and returns. */
  const ignoresOverrides = (lens?: ReturnType<typeof mockPagedLens>) => withOverride(() => "0x", lens);

  /** A provider that refuses the third parameter outright. */
  const rejectsOverrides = (lens?: ReturnType<typeof mockPagedLens>) =>
    withOverride(() => {
      throw Object.assign(new Error("invalid argument 2"), { code: -32602 });
    }, lens);

  it("calls the envelope's address with its code, merging the caller's own overrides", async () => {
    const requestFn = mockPagedLens({ pageSize: 2 });
    const plain = mockPagedLens({ pageSize: 2 });
    const callerOverride = { [CALLER]: { balance: "0x1" } };

    const { result, field } = await withFacet(() =>
      createTransport(requestFn).request(createRequest(four, OVERRIDE, callerOverride)),
    );
    const expected = await createTransport(plain).request(createRequest(four));

    // The delivery decides how the elements reach the node, and nothing about what comes back.
    expect(result).toEqual(expected);
    expect(requestedIndices(requestFn)).toEqual([
      [1, 2, 3, 4],
      [3, 4],
    ]);
    expect(requestedIndices(requestFn)).toEqual(requestedIndices(plain));
    for (const call of requestFn.mock.calls) {
      const { params } = call[0] as EthCallRequest;
      expect(params[0]).toEqual({ to: ENVELOPE_ADDRESS, data: params[0].data });
      expect(params[2]).toEqual({ ...callerOverride, [ENVELOPE_ADDRESS]: { code: FACTORY_BYTECODE_REVERT } });
    }
    expect(field("chunks_override")).toBe(2);
    expect(field("chunks_initcode")).toBe(0);
  });

  it.each([
    ["drops the third parameter", ignoresOverrides],
    ["refuses it outright", rejectsOverrides],
  ])("re-fetches the opening wave as initcode when the provider %s, every request", async (_name, provider) => {
    const requestFn = provider();
    const transport = createTransport(requestFn);

    const first = await withFacet(() => transport.request(createRequest(four, OVERRIDE)));
    const second = await withFacet(() => transport.request(createRequest(four, OVERRIDE)));

    // The range is re-packed under the initcode cap, not halved: one chunk covers it as it stands.
    // Nothing is remembered between requests: the option, not the provider's history, picks the delivery.
    expect(decodeResults(first.result)).toEqual([1n, 2n, 3n, 4n]);
    expect(requestedIndices(requestFn)).toEqual([
      [1, 2, 3, 4],
      [1, 2, 3, 4],
      [1, 2, 3, 4],
      [1, 2, 3, 4],
    ]);
    expect(deliveries(requestFn)).toEqual(["override", "initcode", "override", "initcode"]);
    expect(first.field("override_fallbacks_unsupported")).toBe(1);
    expect(second.field("override_fallbacks_unsupported")).toBe(1);
    expect(second.field("chunks_override")).toBe(1);
    expect(second.field("chunks_initcode")).toBe(1);
  });

  it("retries an unproven failure as initcode", async () => {
    let pending = true;
    const requestFn = withOverride((serve) => {
      if (!pending) return serve();
      pending = false;
      throw Object.assign(new Error("Internal Server Error"), { status: 500 });
    });
    const transport = createTransport(requestFn);

    const first = await withFacet(() => transport.request(createRequest(four, OVERRIDE)));
    const second = await withFacet(() => transport.request(createRequest(four, OVERRIDE)));

    // A paired failure and success says the chunk was serviceable, not that the provider is incapable.
    expect(decodeResults(first.result)).toEqual([1n, 2n, 3n, 4n]);
    expect(deliveries(requestFn)).toEqual(["override", "initcode", "override"]);
    expect(first.field("override_fallbacks_unproven")).toBe(1);
    expect(first.field("override_fallbacks_unsupported")).toBe(0);
    expect(second.field("chunks_override")).toBe(1);
  });

  it("halves an over-large override chunk to singletons and serves each as initcode", async () => {
    const requestFn = withOverride(() => {
      throw Object.assign(new Error("request entity too large"), { status: 413 });
    });

    const { result, field } = await withFacet(() => createTransport(requestFn).request(createRequest(four, OVERRIDE)));

    expect(decodeResults(result)).toEqual([1n, 2n, 3n, 4n]);
    expect(requestedIndices(requestFn).filter((_, i) => deliveries(requestFn)[i] === "initcode")).toEqual([
      [1],
      [2],
      [3],
      [4],
    ]);
    expect(field("chunks_override")).toBe(7);
    expect(field("splits_size")).toBe(3);
    expect(field("override_fallbacks_exhausted")).toBe(4);
    expect(field("override_fallbacks_unsupported")).toBe(0);
  });

  it("throws the deploy out-of-gas marker by override, as it does as initcode", async () => {
    const requestFn = vi.fn().mockRejectedValue(revertWith(OOG_SENTINEL));
    const { logger, events } = createStubLogger();

    await expect(
      withLogging(() => createTransport(requestFn).request(createRequest(four, OVERRIDE)), { logger }),
    ).rejects.toThrow(/ran out of gas under this node's cap/);

    // Terminal in either delivery: the predicate has already paid for the bytes.
    expect(deliveries(requestFn)).toEqual(["override"]);
    const field = (name: string) => findDotted(events[0]!.context, "viem-dlc-deployless", `eth_call.${name}`);
    expect(field("elements_unresolved")).toBe(0);
    expect(field("override_fallbacks")).toBe(0);
  });

  it("refuses a caller state override at the envelope's address, whatever its casing", async () => {
    const requestFn = mockPagedLens();
    const clash = { [ENVELOPE_ADDRESS.toLowerCase()]: { balance: "0x1" } };

    await expect(createTransport(requestFn).request(createRequest(four, OVERRIDE, clash))).rejects.toThrow(
      /conflicts with the envelope's own/,
    );
    expect(requestFn).not.toHaveBeenCalled();
  });

  it("forwards a caller override at the envelope's address untouched without the option", async () => {
    const requestFn = mockPagedLens();
    const entry = { [ENVELOPE_ADDRESS.toLowerCase()]: { balance: "0x1" } };

    await createTransport(requestFn).request(createRequest(four, undefined, entry));

    expect(paramsOf(requestFn)[2]).toEqual(entry);
  });

  it("sends a request without the option as initcode", async () => {
    const requestFn = mockPagedLens();

    const { field } = await withFacet(() => createTransport(requestFn).request(createRequest(four)));

    expect(deliveries(requestFn)).toEqual(["initcode"]);
    expect(field("chunks_override")).toBe(0);
    expect(field("chunks_initcode")).toBe(1);
    expect(field("override_fallbacks")).toBe(0);
  });

  it("sends the tails a fallback's pages leave behind by override, and falls back again", async () => {
    const requestFn = ignoresOverrides(mockPagedLens({ pageSize: 2 }));

    const { result, field } = await withFacet(() => createTransport(requestFn).request(createRequest(four, OVERRIDE)));

    // Nothing is remembered within the request either: the option picks every chunk's opening delivery.
    expect(decodeResults(result)).toEqual([1n, 2n, 3n, 4n]);
    expect(requestedIndices(requestFn)).toEqual([
      [1, 2, 3, 4],
      [1, 2, 3, 4],
      [3, 4],
      [3, 4],
    ]);
    expect(deliveries(requestFn)).toEqual(["override", "initcode", "override", "initcode"]);
    expect(field("override_fallbacks_unsupported")).toBe(2);
  });

  it.each([
    ["intrinsic gas", "intrinsic gas too low: have 21000, want 53000"],
    ["floor data gas", "insufficient gas for floor data gas cost"],
  ])("halves a chunk the node refused for %s, in either delivery", async (_name, message) => {
    const refusing = () => {
      const lens = mockPagedLens();
      return vi.fn().mockImplementation(async (args: { params: readonly unknown[] }) => {
        if (sentAddresses(args.params).length === 4) throw new Error(message);
        return lens(args);
      });
    };
    const initcode = refusing();
    const override = refusing();

    await createTransport(initcode).request(createRequest(four));
    const { field } = await withFacet(() => createTransport(override).request(createRequest(four, OVERRIDE)));

    // Bytes caused the refusal in either delivery, and fewer bytes cure it.
    expect(requestedIndices(initcode)).toEqual([
      [1, 2, 3, 4],
      [1, 2],
      [3, 4],
    ]);
    expect(requestedIndices(override)).toEqual(requestedIndices(initcode));
    expect(deliveries(override)).toEqual(["override", "override", "override"]);
    expect(field("override_fallbacks")).toBe(0);
  });

  it("packs a fallback under the caller's batchSize", async () => {
    const probe = mockPagedLens();
    await createTransport(probe).request(createRequest([1, 2, 3].map(addr)));
    const batchSize = byteLength(paramsOf(probe)[0].data as Hex);
    const requestFn = ignoresOverrides();
    const eight = [1, 2, 3, 4, 5, 6, 7, 8].map(addr);

    const { result } = await withFacet(() =>
      createTransport(requestFn).request(createRequest(eight, { ...OVERRIDE, batchSize })),
    );

    // The override chunk carries all eight; the pieces it falls back to carry the envelope too.
    expect(decodeResults(result)).toHaveLength(8);
    expect(requestedIndices(requestFn)).toEqual([
      [1, 2, 3, 4, 5, 6, 7, 8],
      [1, 2, 3],
      [4, 5, 6],
      [7, 8],
    ]);
    for (const call of requestFn.mock.calls.slice(1)) {
      expect(byteLength((call[0] as EthCallRequest).params[0].data as Hex)).toBeLessThanOrEqual(batchSize);
    }
  });

  it("packs a fallback with no stated batchSize as one chunk that halves on the initcode cap", async () => {
    const lens = mockPagedLens();
    const requestFn = vi.fn().mockImplementation(async (args: { params: readonly unknown[] }) => {
      if (isOverrideShaped(args.params)) return "0x";
      if (sentAddresses(args.params).length === 4) throw new Error("max initcode size exceeded");
      return lens(args);
    });

    const { result, field } = await withFacet(() => createTransport(requestFn).request(createRequest(four, OVERRIDE)));

    expect(decodeResults(result)).toEqual([1n, 2n, 3n, 4n]);
    expect(requestedIndices(requestFn)).toEqual([
      [1, 2, 3, 4],
      [1, 2, 3, 4],
      [1, 2],
      [3, 4],
    ]);
    expect(deliveries(requestFn)).toEqual(["override", "initcode", "initcode", "initcode"]);
    expect(field("splits_size")).toBe(1);
  });

  describe("the predicate's byte lines", () => {
    it("reads the same cap back off either delivery's frame", async () => {
      const CAP = 20_000_000;
      const three = [1, 2, 3].map(addr);
      // A node grants what its cap leaves after the delivery's intrinsic gas and the prologue, so
      // the same cap means a different frame in each delivery.
      const budgetFor = async (batch?: Record<string, unknown>) => {
        const probe = mockPagedLens();
        await createTransport(probe).request(createRequest(three, batch));
        const params = paramsOf(probe);
        const delivery = isOverrideShaped(params) ? "override" : "initcode";
        return CAP - intrinsicGasOf(params[0].data as Hex, delivery) - 100_000;
      };
      const initcode = mockPagedLens({ budget: await budgetFor() });
      const override = mockPagedLens({ budget: await budgetFor(OVERRIDE) });

      const a = await withFacet(() => createTransport(initcode).request(createRequest(three)));
      const b = await withFacet(() => createTransport(override).request(createRequest(three, OVERRIDE)));

      expect(a.field("gas_limit_observed")).toBe(CAP);
      expect(b.field("gas_limit_observed")).toBe(CAP);
    });

    it("declines a lone element whose bytes cannot clear the floor, without sending it", async () => {
      const probe = mockPagedLens();
      await createTransport(probe).request(createRequest([addr(1)]));
      const floor = floorGasOf(paramsOf(probe)[0].data as Hex);
      const admits = mockPagedLens();
      const refuses = mockPagedLens();

      // No stated item cost: a cap alone still runs the byte lines.
      const a = await withFacet(() => createTransport(admits, { gasLimit: floor }).request(createRequest([addr(1)])));
      const b = await withFacet(() =>
        createTransport(refuses, { gasLimit: floor - 1 }).request(createRequest([addr(1)])),
      );

      expect(admits).toHaveBeenCalledOnce();
      expect(a.field("elements_declined_oversize")).toBe(0);
      expect(refuses).not.toHaveBeenCalled();
      expect(decodePage(b.result)).toEqual({ results: [], skipped: [0] });
      expect(b.field("elements_declined_oversize")).toBe(1);
    });

    it("recovers one prologue from pages of very different sizes", async () => {
      // The lens's own prologue is a constant; what scales with the chunk is the envelope's copy of
      // the tuple, which the pool subtracts before reporting `fixed_gas`.
      const lens = () => mockPagedLens({ fixed: (_first, tupleBytes) => 100_000 + copyGas(tupleBytes, false) });
      const small = lens();
      const large = lens();

      const a = await withFacet(() => createTransport(small).request(createRequest([1, 2, 3, 4, 5].map(addr))));
      const b = await withFacet(() =>
        createTransport(large).request(createRequest([...Array(500).keys()].map((i) => addr(i + 1)))),
      );

      expect(a.field("fixed_gas")).toBe(100_000);
      expect(Math.abs(b.field("fixed_gas") - a.field("fixed_gas"))).toBeLessThan(500);
    });
  });
});
