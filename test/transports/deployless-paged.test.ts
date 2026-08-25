import type { Address, Hex } from "viem";
import {
  type AbiFunction,
  concat,
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
import { describe, expect, it, vi } from "vitest";

import { deployless } from "../../src/transports/deployless/index.js";
import { ETH_CALL_POLICY_ADDRESS } from "../../src/transports/state-overrides.js";
import type { EIP1193Parameters } from "../../src/types.js";
import { OK_SENTINEL, OOG_SENTINEL, unwrapDeploylessFactoryCall } from "../../src/utils/deployless/codec.envelope.js";
import { isDeploylessPartialResultError } from "../../src/utils/deployless/errors.js";

type EthCallRequest = EIP1193Parameters<import("viem").PublicRpcSchema, "eth_call">;

const TARGET_TO = "0x1111111111111111111111111111111111111111" as const;
const FACTORY = "0x2222222222222222222222222222222222222222" as const;
const FACTORY_DATA = "0xcafebabe" as const;

const pageAbi = parseAbiItem(
  "function page(address[] input) view returns (uint256[] results, uint256[] skipped)",
) as AbiFunction;

const addr = (n: number) => pad(toHex(n), { size: 20 });
const addrValue = (a: Address) => Number(BigInt(a));

function createRequest(addrs: readonly Address[], batch?: Record<string, unknown>): EthCallRequest {
  const targetData = concat([toFunctionSelector(pageAbi), encodeAbiParameters([{ type: "address[]" }], [addrs])]);
  const policy: Record<string, unknown> = { abi: pageAbi, paged: true };
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

function sentAddresses(data: Hex): readonly Address[] {
  const { targetData } = unwrapDeploylessFactoryCall(data);
  const [addrs] = decodeAbiParameters([{ type: "address[]" }], `0x${targetData.slice(10)}` as Hex);
  return addrs as readonly Address[];
}

function revertWith(data: Hex): Error & { data: Hex } {
  const err = new Error("execution reverted") as Error & { data: Hex };
  err.data = data;
  return err;
}

function revertWithPage(results: readonly bigint[], skipped: readonly number[]) {
  const encoded = encodeAbiParameters([{ type: "uint256[]" }, { type: "uint256[]" }], [results, skipped.map(BigInt)]);
  return revertWith(`${OK_SENTINEL}${encoded.slice(2)}` as Hex);
}

type LensBehavior = {
  /** Most elements the lens will attempt in one call before stopping for gas. */
  pageSize?: number;
  /** Address values the lens deterministically declines. */
  decline?: readonly number[];
  /** Address values that exhaust the frame when attempted, taking the whole call with them. */
  fatal?: readonly number[];
};

/**
 * A conforming paged lens: walks its input in index order, stops after `pageSize` attempts,
 * declines `decline` elements, and lets `fatal` elements kill the frame (no per-element cap).
 */
function mockPagedLens({ pageSize = Infinity, decline = [], fatal = [] }: LensBehavior = {}) {
  return vi.fn().mockImplementation(async (args: { params: readonly unknown[] }) => {
    const addrs = sentAddresses((args.params[0] as { data: Hex }).data);
    const results: bigint[] = [];
    const skipped: number[] = [];
    for (let i = 0; i < addrs.length && i < pageSize; i++) {
      const value = addrValue(addrs[i]!);
      if (fatal.includes(value)) throw revertWith(OOG_SENTINEL);
      if (decline.includes(value)) skipped.push(i);
      else results.push(BigInt(value));
    }
    throw revertWithPage(results, skipped);
  });
}

function createTransport(requestFn: ReturnType<typeof vi.fn>, gasLimit = 30_000_000) {
  return deployless(custom({ request: requestFn as never }), { gasLimit })({ retryCount: 0 } as never);
}

function decodeResults(result: unknown): bigint[] {
  const [values] = decodeAbiParameters([{ type: "uint256[]" }], result as Hex);
  return [...(values as readonly bigint[])];
}

/** Every element index each upstream call was asked about, one entry per call. */
function requestedIndices(requestFn: ReturnType<typeof vi.fn>): number[][] {
  return requestFn.mock.calls.map((call) =>
    sentAddresses((call[0] as EthCallRequest).params[0].data as Hex).map(addrValue),
  );
}

describe("deployless (paged)", () => {
  it("returns a dense array when the lens serves everything in one page", async () => {
    const requestFn = mockPagedLens();
    const transport = createTransport(requestFn);

    const result = await transport.request(createRequest([addr(1), addr(2), addr(3)]));

    expect(decodeResults(result)).toEqual([1n, 2n, 3n]);
    expect(requestFn).toHaveBeenCalledOnce();
  });

  it("hands back a dense U[], not the lens's two-array tuple", async () => {
    const requestFn = mockPagedLens([]);
    const transport = createTransport(requestFn);

    const result = await transport.request(createRequest([1, 2].map(addr)));

    // Paging is a lens→transport concern; callers see what an unpaged lens would have returned.
    expect(() => decodeFunctionResult({ abi: [pageAbi], functionName: "page", data: result as Hex })).toThrow();
    expect(decodeAbiParameters([{ type: "uint256[]" }], result as Hex)[0]).toEqual([1n, 2n]);
  });

  it("returns an empty array for empty input without an upstream call", async () => {
    const requestFn = mockPagedLens([]);
    const transport = createTransport(requestFn);

    const result = await transport.request(createRequest([]));

    expect(decodeAbiParameters([{ type: "uint256[]" }], result as Hex)[0]).toEqual([]);
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

    const error = await transport.request(createRequest([1, 2, 3, 4].map(addr))).catch((e) => e);

    expect(isDeploylessPartialResultError(error)).toBe(true);
    expect(error.missing).toEqual([1]);
    expect(decodeResults(error.data)).toEqual([1n, 3n, 4n]);
    const asked = requestedIndices(requestFn).flat();
    expect(asked.filter((v) => v === 2)).toHaveLength(1);
    expect(asked).toContain(4);
  });

  it("keeps sibling results when a single element exhausts the frame", async () => {
    const requestFn = mockPagedLens({ fatal: [3] });
    const transport = createTransport(requestFn);

    const error = await transport.request(createRequest([1, 2, 3, 4].map(addr))).catch((e) => e);

    expect(isDeploylessPartialResultError(error)).toBe(true);
    expect(error.missing).toEqual([2]);
    expect(decodeResults(error.data)).toEqual([1n, 2n, 4n]);
  });

  it("reports every unservable element, in ascending order", async () => {
    const requestFn = mockPagedLens({ decline: [2], fatal: [4] });
    const transport = createTransport(requestFn);

    const error = await transport.request(createRequest([1, 2, 3, 4].map(addr))).catch((e) => e);

    expect(error.missing).toEqual([1, 3]);
  });

  it("propagates an ordinary lens revert instead of treating it as unservable", async () => {
    const requestFn = vi.fn().mockRejectedValue(revertWith("0xdeadbeef"));
    const transport = createTransport(requestFn);

    await expect(transport.request(createRequest([addr(1)]))).rejects.toThrow(/execution reverted/);
  });

  describe("protocol violations", () => {
    it.each([
      ["makes no progress", [], [], /attempted 0 of 2 elements, expected 1\.\.2/],
      ["attempts more than it was given", [1n, 2n, 3n], [], /attempted 3 of 2 elements, expected 1\.\.2/],
      ["skips an index it never attempted", [1n], [5], /not strictly increasing below 2/],
      ["repeats a skipped index", [], [0, 0], /not strictly increasing below 2/],
      ["returns skipped indices out of order", [], [1, 0], /not strictly increasing below 2/],
    ])("throws when the lens %s", async (_name, results, skipped, expected) => {
      const requestFn = vi.fn().mockRejectedValue(revertWithPage(results as bigint[], skipped as number[]));
      const transport = createTransport(requestFn);

      const error = await transport.request(createRequest([addr(1), addr(2)])).catch((e) => e);

      expect(error.message).toMatch(expected);
      expect(isDeploylessPartialResultError(error)).toBe(false);
    });
  });
});
