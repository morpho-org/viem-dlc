import type { Address, Hex } from "viem";
import {
  type AbiFunction,
  custom,
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

import { deployless } from "../../src/transports/deployless/index.js";
import { ETH_CALL_POLICY_ADDRESS } from "../../src/transports/state-overrides.js";
import type { EIP1193Parameters } from "../../src/types.js";

type EthCallRequest = EIP1193Parameters<import("viem").PublicRpcSchema, "eth_call">;

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

function policySentinel(abi: AbiFunction, opts?: { batchSize?: number; withCache?: boolean }) {
  return {
    [ETH_CALL_POLICY_ADDRESS]: {
      code: toHex(
        JSON.stringify({
          abi,
          batchSize: opts?.batchSize,
          ...(opts?.withCache ? { cache: { blobKey: "test-blob", ttl: 60_000 } } : {}),
        }),
      ),
    },
  };
}

function createRequest(
  addrs: readonly Address[],
  opts?: { batchSize?: number; abi?: AbiFunction; withCache?: boolean },
): EthCallRequest {
  const abi = opts?.abi ?? balancesOfAbi;
  return {
    method: "eth_call",
    params: [
      { data: buildDeploylessCall(buildTargetCalldata(abi, addrs)) },
      "latest",
      policySentinel(abi, { batchSize: opts?.batchSize, withCache: opts?.withCache }),
    ],
  };
}

function decodeSentAddresses(data: Hex): readonly Address[] {
  const argsHex = `0x${data.slice(deploylessCallViaFactoryBytecode.length)}` as Hex;
  const [, targetData] = decodeAbiParameters(DEPLOYLESS_CTOR_PARAMS, argsHex);
  const [addrs] = decodeAbiParameters([{ type: "address[]" }], `0x${targetData.slice(10)}` as Hex);
  return addrs as readonly Address[];
}

function mockBalancesOfFn() {
  return vi.fn().mockImplementation(async (args: { method: string; params: readonly unknown[] }) => {
    const data = (args.params[0] as { data: Hex }).data;
    const outputs = decodeSentAddresses(data).map((a) => BigInt(a));
    return encodeAbiParameters([{ type: "uint256[]" }], [outputs]);
  });
}

function createTransport(requestFn: ReturnType<typeof vi.fn>) {
  return deployless(custom({ request: requestFn }))({} as never);
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
    expect(requestFn).toHaveBeenCalledWith(req);
  });

  it("passes non-eth_call requests through unchanged", async () => {
    const requestFn = vi.fn().mockResolvedValue("0x64");
    const transport = createTransport(requestFn);

    const result = await transport.request({ method: "eth_blockNumber" });

    expect(result).toBe("0x64");
    expect(requestFn).toHaveBeenCalledWith({ method: "eth_blockNumber" });
  });

  it("batchSize splits marked deployless calls without exceeding the byte budget", async () => {
    const requestFn = mockBalancesOfFn();
    const transport = createTransport(requestFn);
    const req = createRequest([addr(1), addr(2), addr(3), addr(4), addr(5)], { batchSize: 1088 });

    const result = await transport.request(req);

    expect(requestFn.mock.calls.length).toBe(3);
    for (const [arg] of requestFn.mock.calls) {
      const data = (arg.params[0] as { data: Hex }).data;
      expect((data.length - 2) / 2).toBeLessThanOrEqual(1088);
    }

    const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
    expect(decoded).toEqual([addr(1), addr(2), addr(3), addr(4), addr(5)].map((a) => BigInt(a)));
  });

  it("returns immediately for empty input arrays", async () => {
    const requestFn = vi.fn();
    const transport = createTransport(requestFn);

    const result = await transport.request(createRequest([]));

    expect(requestFn).not.toHaveBeenCalled();
    const [decoded] = decodeAbiParameters([{ type: "uint256[]" }], result);
    expect(decoded).toEqual([]);
  });

  it("forwards block, cleaned stateOverride, and blockOverride upstream", async () => {
    const requestFn = mockBalancesOfFn();
    const transport = createTransport(requestFn);
    const extraOverride = { "0x4444444444444444444444444444444444444444": { balance: "0x1" } } as const;
    const blockOverride = { time: "0x1234" } as EthCallRequest["params"][3];
    const req: EthCallRequest = {
      method: "eth_call",
      params: [
        { data: buildDeploylessCall(buildTargetCalldata(balancesOfAbi, [addr(1)])) },
        "0x100" as Hex,
        { ...policySentinel(balancesOfAbi), ...extraOverride },
        blockOverride,
      ],
    };

    await transport.request(req);

    const call = requestFn.mock.calls[0]![0] as { params: readonly unknown[] };
    expect(call.params[1]).toBe("0x100");
    expect(call.params[2]).toEqual(extraOverride);
    expect(call.params[3]).toEqual(blockOverride);
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
    const requestFn = vi.fn().mockResolvedValue(encodeAbiParameters([{ type: "uint256[]" }], [[1n, 2n]]));
    const transport = createTransport(requestFn);

    await expect(transport.request(createRequest([addr(1), addr(2), addr(3)]))).rejects.toThrow(
      /returned 2.*expected 3/,
    );
  });

  it("ignores policy.cache and behaves like split-only mode", async () => {
    const addrs = [addr(1), addr(2), addr(3), addr(4), addr(5)];
    const requestFnWithoutCache = mockBalancesOfFn();
    const requestFnWithCache = mockBalancesOfFn();
    const transportWithoutCache = createTransport(requestFnWithoutCache);
    const transportWithCache = createTransport(requestFnWithCache);

    const withoutCache = await transportWithoutCache.request(createRequest(addrs, { batchSize: 1088 }));
    const withCache = await transportWithCache.request(createRequest(addrs, { batchSize: 1088, withCache: true }));

    expect(withCache).toEqual(withoutCache);
    expect(requestFnWithCache.mock.calls.length).toBe(requestFnWithoutCache.mock.calls.length);
    expect(requestFnWithCache.mock.calls.map(([arg]) => (arg.params[0] as { data: Hex }).data.toLowerCase())).toEqual(
      requestFnWithoutCache.mock.calls.map(([arg]) => (arg.params[0] as { data: Hex }).data.toLowerCase()),
    );
  });
});
