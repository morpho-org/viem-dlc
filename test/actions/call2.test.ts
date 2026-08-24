import type { Address, Hex } from "viem";
import {
  type AbiFunction,
  BaseError,
  createPublicClient,
  custom,
  decodeAbiParameters,
  encodeAbiParameters,
  encodeFunctionData,
  pad,
  parseAbiItem,
  toHex,
} from "viem";
import { call } from "viem/actions";
import { describe, expect, it, vi } from "vitest";

import { policy } from "../../src/actions/call.js";
import { call2 } from "../../src/actions/call2.js";
import { deployless } from "../../src/transports/deployless/index.js";
import { failover } from "../../src/transports/failover/index.js";
import { OK_SENTINEL, unwrapDeploylessFactoryCall } from "../../src/utils/deployless/codec.envelope.js";
import { isDeploylessPartialResultError } from "../../src/utils/deployless/errors.js";

const TARGET_TO = "0x1111111111111111111111111111111111111111" as const;
const FACTORY = "0x2222222222222222222222222222222222222222" as const;
const FACTORY_DATA = "0xcafebabe" as const;

const pageAbi = parseAbiItem(
  "function page(address[] input) view returns (uint256[] results, uint256[] skipped)",
) as AbiFunction;

const addr = (n: number) => pad(toHex(n), { size: 20 });

/** Declines the given address values; serves everything else in one page. */
function mockPagedLens(decline: readonly number[]) {
  return vi.fn().mockImplementation(async (args: { params: readonly unknown[] }) => {
    const { targetData } = unwrapDeploylessFactoryCall((args.params[0] as { data: Hex }).data);
    const [addrs] = decodeAbiParameters([{ type: "address[]" }], `0x${targetData.slice(10)}` as Hex);
    const results: bigint[] = [];
    const skipped: bigint[] = [];
    (addrs as readonly Address[]).forEach((a, i) => {
      if (decline.includes(Number(BigInt(a)))) skipped.push(BigInt(i));
      else results.push(BigInt(a));
    });
    const err = new Error("execution reverted") as Error & { data: Hex };
    const encoded = encodeAbiParameters([{ type: "uint256[]" }, { type: "uint256[]" }], [results, skipped]);
    err.data = `${OK_SENTINEL}${encoded.slice(2)}` as Hex;
    throw err;
  });
}

function clientWith(requestFn: ReturnType<typeof vi.fn>) {
  return createPublicClient({
    transport: deployless(custom({ request: requestFn as never }), { gasLimit: 30_000_000 }),
  });
}

function callParameters(addrs: readonly Address[]) {
  return {
    factory: FACTORY,
    factoryData: FACTORY_DATA,
    to: TARGET_TO,
    data: encodeFunctionData({ abi: [pageAbi], functionName: "page", args: [addrs] }),
    stateOverride: [policy({ abi: pageAbi, paged: true })],
  } as const;
}

function decodeServed(data: Hex): readonly bigint[] {
  const [values] = decodeAbiParameters([{ type: "uint256[]" }], data);
  return values as readonly bigint[];
}

describe("call2", () => {
  it("returns a dense result and no missing indices when every element is served", async () => {
    const client = clientWith(mockPagedLens([]));

    const { data, missing } = await call2(client, callParameters([1, 2, 3].map(addr)));

    expect(decodeServed(data)).toEqual([1n, 2n, 3n]);
    expect(missing).toEqual([]);
  });

  it("returns the served complement plus the indices that were not", async () => {
    const client = clientWith(mockPagedLens([2]));

    const { data, missing } = await call2(client, callParameters([1, 2, 3].map(addr)));

    expect(decodeServed(data)).toEqual([1n, 3n]);
    expect(missing).toEqual([1]);
  });

  it("rethrows errors that are not partial results", async () => {
    const client = clientWith(vi.fn().mockRejectedValue(new Error("upstream exploded")));

    await expect(call2(client, callParameters([addr(1)]))).rejects.toThrow(/upstream exploded/);
  });
});

describe("strict call()", () => {
  it("throws, and the partial result is still recoverable under viem's CallExecutionError", async () => {
    const client = clientWith(mockPagedLens([2]));

    const error = await call(client, callParameters([1, 2, 3].map(addr))).catch((e) => e);

    // viem wraps in CallExecutionError, and `buildRequest` may wrap again beneath it — the
    // branded error must survive both, and a bare `walk()` would return the deepest cause.
    expect(error).toBeInstanceOf(BaseError);
    expect(error.name).toBe("CallExecutionError");
    expect(isDeploylessPartialResultError(error)).toBe(false);

    const found = error.walk(isDeploylessPartialResultError);
    expect(found).not.toBeNull();
    expect(found.missing).toEqual([1]);
    expect(decodeServed(found.data)).toEqual([1n, 3n]);
  });
});

describe("failover", () => {
  it("does not fall over to the next branch on a partial result", async () => {
    const primary = mockPagedLens([2]);
    const secondary = mockPagedLens([]);
    const client = createPublicClient({
      transport: failover([
        deployless(custom({ request: primary as never }), { gasLimit: 30_000_000 }),
        deployless(custom({ request: secondary as never }), { gasLimit: 30_000_000 }),
      ]),
    });

    const { missing } = await call2(client, callParameters([1, 2, 3].map(addr)));

    expect(missing).toEqual([1]);
    expect(secondary).not.toHaveBeenCalled();
  });

  it("still falls over when a branch fails for an ordinary reason", async () => {
    const primary = vi.fn().mockRejectedValue(new Error("connection refused"));
    const secondary = mockPagedLens([]);
    const client = createPublicClient({
      transport: failover([
        deployless(custom({ request: primary as never }), { gasLimit: 30_000_000 }),
        deployless(custom({ request: secondary as never }), { gasLimit: 30_000_000 }),
      ]),
    });

    const { data, missing } = await call2(client, callParameters([1, 2].map(addr)));

    expect(decodeServed(data)).toEqual([1n, 2n]);
    expect(missing).toEqual([]);
    expect(secondary).toHaveBeenCalled();
  });
});
