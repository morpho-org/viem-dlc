import type { Abi, AbiFunction, Address, Hex } from "viem";
import {
  createPublicClient,
  custom,
  decodeAbiParameters,
  encodeAbiParameters,
  fromHex,
  pad,
  parseAbiParameters,
  toFunctionSelector,
  toHex,
} from "viem";
import { describe, expect, it, vi } from "vitest";

import { readLens } from "../../src/actions/read-lens.js";
import { deployless } from "../../src/transports/deployless/index.js";
import { ETH_CALL_POLICY_ADDRESS } from "../../src/transports/state-overrides.js";
import {
  envelopeConfig,
  FACTORY_BYTECODE_REVERT,
  OK_SENTINEL,
  unwrapDeploylessFactoryCall,
} from "../../src/utils/deployless/codec.envelope.js";
import { arrayifiedAbi, resolveArrayFunction } from "../../src/utils/deployless/codec.inner.js";

const ADDRESS = "0x1111111111111111111111111111111111111111" as const;
const FACTORY = "0x2222222222222222222222222222222222222222" as const;
const FACTORY_DATA = "0xcafebabe" as const;
const OTHER = "0x4444444444444444444444444444444444444444" as const;

const lensAbi = [
  {
    type: "function",
    name: "healthOf",
    stateMutability: "view",
    inputs: [{ type: "address" }],
    outputs: [{ type: "uint256" }],
  },
  {
    type: "function",
    name: "priceOf",
    stateMutability: "view",
    inputs: [{ type: "address" }],
    outputs: [{ type: "uint256" }],
  },
] as const satisfies Abi;

const itemAbi = lensAbi[0] as AbiFunction;
const arrayAbi = arrayifiedAbi(itemAbi) as AbiFunction;
const ARRAY_SELECTOR = toFunctionSelector(arrayAbi);
const CONFIG = envelopeConfig(resolveArrayFunction(arrayAbi));

const addr = (n: number) => pad(toHex(n), { size: 20 });

const readLensArgs = {
  abi: lensAbi,
  functionName: "healthOf",
  address: ADDRESS,
  factory: FACTORY,
  factoryData: FACTORY_DATA,
} as const;

function pageHex(results: readonly bigint[], skipped: readonly number[]): Hex {
  return encodeAbiParameters([{ type: "uint256[]" }, { type: "uint256[]" }], [results, skipped.map(BigInt)]);
}

/**
 * Upstream stand-in for the envelope: asserts the wire form the transport built, then answers
 * with a sentinel-framed page that declines the element at `decline`.
 */
function mockEnvelope(decline: number) {
  return vi.fn().mockImplementation(async (args: { params: readonly unknown[] }) => {
    const data = (args.params[0] as { data: Hex }).data;
    const { target, targetData } = unwrapDeploylessFactoryCall(data);

    expect(target).toEqual({ address: ADDRESS, factory: FACTORY, factoryData: FACTORY_DATA });
    expect(targetData.slice(0, 10)).toBe(ARRAY_SELECTOR);
    const [, , , , config] = decodeAbiParameters(
      parseAbiParameters("address, bytes, address, bytes, uint256"),
      `0x${data.slice(FACTORY_BYTECODE_REVERT.length)}` as Hex,
    );
    expect(config).toBe(CONFIG);

    const [users] = decodeAbiParameters([{ type: "address[]" }], `0x${targetData.slice(10)}` as Hex);
    const results = (users as readonly Address[]).flatMap((u, i) => (i === decline ? [] : [BigInt(u)]));

    const err = new Error("execution reverted") as Error & { data: Hex };
    err.data = `${OK_SENTINEL}${pageHex(results, [decline]).slice(2)}` as Hex;
    throw err;
  });
}

function deploylessClient(requestFn: ReturnType<typeof vi.fn>) {
  return createPublicClient({ transport: deployless(custom({ request: requestFn as never })) });
}

describe("readLens", () => {
  it("reads the per-item function through the derived array-shaped fragment", async () => {
    const requestFn = mockEnvelope(1);
    const client = deploylessClient(requestFn);

    const { results, skipped } = await readLens(client, {
      ...readLensArgs,
      args: [addr(1), addr(2), addr(3)],
    });

    expect(results).toEqual([1n, 3n]);
    expect(skipped).toEqual([1]);
    // `skipped` is decoded from `uint256[]`, so the numbers are the action's own conversion.
    expect(skipped.map((k) => typeof k)).toEqual(["number"]);
    expect(requestFn).toHaveBeenCalledOnce();
  });

  it("appends the policy entry after the caller's own state overrides", async () => {
    const requestFn = vi.fn().mockResolvedValue(pageHex([7n], []));
    const client = createPublicClient({ transport: custom({ request: requestFn as never }) });

    const { results, skipped } = await readLens(client, {
      ...readLensArgs,
      args: [addr(1)],
      stateOverride: [{ address: OTHER, balance: 1n }],
    });

    expect({ results, skipped }).toEqual({ results: [7n], skipped: [] });

    const overrides = (requestFn.mock.calls[0]![0] as { params: readonly unknown[] }).params[2] as Record<
      string,
      { balance?: Hex; code?: Hex }
    >;
    const keys = Object.keys(overrides);
    expect(keys.map((k) => k.toLowerCase())).toEqual([OTHER.toLowerCase(), ETH_CALL_POLICY_ADDRESS.toLowerCase()]);
    expect(overrides[keys[0]!]!.balance).toBe("0x1");
    // The policy carries the fragment the action derived, not the per-item one the caller named.
    expect(JSON.parse(fromHex(overrides[keys[1]!]!.code!, "string")).abi).toEqual(arrayAbi);
  });

  it("throws when the abi has no function of that name", async () => {
    const client = deploylessClient(vi.fn());

    await expect(
      readLens(client, { ...readLensArgs, functionName: "missing", args: [addr(1)] } as never),
    ).rejects.toThrow(/no function named missing/);
  });

  it("throws when the abi overloads the function name", async () => {
    const overloaded = [
      ...lensAbi,
      {
        type: "function",
        name: "healthOf",
        stateMutability: "view",
        inputs: [{ type: "address" }, { type: "uint256" }],
        outputs: [{ type: "uint256" }],
      },
    ] as const satisfies Abi;
    const client = deploylessClient(vi.fn());

    await expect(readLens(client, { ...readLensArgs, abi: overloaded, args: [addr(1)] } as never)).rejects.toThrow(
      /more than one function named healthOf/,
    );
  });

  it("throws when the named function is not view or pure", async () => {
    const mutable = [
      {
        type: "function",
        name: "healthOf",
        stateMutability: "nonpayable",
        inputs: [{ type: "address" }],
        outputs: [{ type: "uint256" }],
      },
    ] as const satisfies Abi;
    const client = deploylessClient(vi.fn());

    await expect(readLens(client, { ...readLensArgs, abi: mutable, args: [addr(1)] } as never)).rejects.toThrow(
      /healthOf must be view or pure/,
    );
  });
});
