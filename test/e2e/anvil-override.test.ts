/**
 * The Anvil, Prague verification of docs/000016-tib-override-delivered-envelope.md, against a real
 * node. Skipped whole when `anvil` is not on PATH, so it costs an unequipped machine nothing.
 */
import { type ChildProcess, execFileSync, spawn } from "node:child_process";
import { createServer } from "node:net";

import type { Address, Hex } from "viem";
import {
  BaseError,
  concat,
  createPublicClient,
  createWalletClient,
  custom,
  getContractAddress,
  http,
  pad,
  toHex,
} from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { foundry } from "viem/chains";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { MAX_INITCODE_SIZE } from "../../src/actions/call.js";
import { readLens } from "../../src/actions/read-lens.js";
import { withLogging } from "../../src/observability.js";
import { deployless } from "../../src/transports/deployless/index.js";
import type { EnvelopeDelivery } from "../../src/utils/deployless/codec.envelope.js";
import {
  decodeEnvelopeRevert,
  deliveryParams,
  ENVELOPE_ADDRESS,
  encodeEnvelopeArgs,
  envelopeConfig,
  FACTORY_BYTECODE_REVERT,
} from "../../src/utils/deployless/codec.envelope.js";
import { arrayifiedAbi, arrayToWire, resolveArrayFunction } from "../../src/utils/deployless/codec.inner.js";
import { createStubLogger, findDotted } from "../helpers/logger.js";

import { ELEMENT_SIZE, FACTORY_BYTECODE, itemAbi, LENS_BYTECODE, lensAbi } from "./fixtures.js";

const ANVIL_ACCOUNT_0 = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80" as const;
const SALT = pad("0x01", { size: 32 });
const ELEMENTS = 1_000;
const GAS_LIMIT = 30_000_000;

const hasAnvil = (() => {
  try {
    execFileSync("anvil", ["--version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
})();

const solidity = resolveArrayFunction(arrayifiedAbi(itemAbi));
const config = envelopeConfig(solidity, false);

/** `n` elements of `item`'s input: `a = i + 1`, `mode = 0`, so the lens returns `2·(i + 1)`. */
const inputs = (n: number) => Array.from({ length: n }, (_, i) => ({ a: BigInt(i + 1), mode: 0n }));

/** EIP-7623's floor: the gas a message needs to start, whatever it goes on to spend. */
function floorGas(data: Hex): number {
  const bytes = data.length / 2 - 1;
  let zeros = 0;
  for (let at = 2; at < data.length; at += 2) if (data[at] === "0" && data[at + 1] === "0") zeros++;
  return 21_000 + 10 * zeros + 40 * (bytes - zeros);
}

/** The page's `nA`: how many elements the envelope adjudicated, the first word after the sentinel. */
function adjudicated(returnData: Hex): number {
  return Number(BigInt(returnData.slice(0, 66)));
}

/** The node's own message, as `classifyChunkError` reads it: viem's innermost cause. */
function upstreamMessage(error: unknown): string {
  return ((error instanceof BaseError ? error.walk() : error) as Error).message;
}

async function freePort(): Promise<number> {
  const server = createServer();
  return new Promise((resolve, reject) => {
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const port = (server.address() as { port: number }).port;
      server.close(() => resolve(port));
    });
  });
}

async function waitForRpc(url: string, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "eth_blockNumber", params: [] }),
      });
      if (res.ok) return;
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`anvil did not answer at ${url}`);
}

/** An http transport that keeps every request it forwards, to check the shape each chunk went out in. */
function recordingHttp(url: string) {
  const requests: { method: string; params?: readonly unknown[] }[] = [];
  const inner = http(url, { retryCount: 0 })({});
  const transport = custom(
    {
      request: (async (args: { method: string; params?: readonly unknown[] }) => {
        requests.push(args);
        return inner.request(args as never);
      }) as never,
    },
    { retryCount: 0 },
  );
  return { transport, requests, ethCalls: () => requests.filter((r) => r.method === "eth_call") };
}

describe.skipIf(!hasAnvil)("override-delivered envelope, against anvil", () => {
  let anvil: ChildProcess;
  let url: string;
  let factory: Address;
  let target: Address;
  let factoryData: Hex;
  let rpc: (params: unknown[]) => Promise<Hex>;

  beforeAll(async () => {
    const port = await freePort();
    url = `http://127.0.0.1:${port}`;
    anvil = spawn("anvil", ["--hardfork", "prague", "--port", String(port), "--silent"], { stdio: "ignore" });
    await waitForRpc(url);

    const transport = http(url);
    const publicClient = createPublicClient({ chain: foundry, transport });
    const wallet = createWalletClient({ chain: foundry, transport, account: privateKeyToAccount(ANVIL_ACCOUNT_0) });

    const hash = await wallet.deployContract({ abi: [], bytecode: FACTORY_BYTECODE });
    factory = (await publicClient.waitForTransactionReceipt({ hash })).contractAddress!;
    factoryData = concat([SALT, LENS_BYTECODE]);
    target = getContractAddress({ opcode: "CREATE2", from: factory, salt: SALT, bytecode: LENS_BYTECODE });
    rpc = (params) => publicClient.request({ method: "eth_call", params } as never) as Promise<Hex>;
  }, 60_000);

  afterAll(() => {
    anvil?.kill();
  });

  /** The envelope's argument tuple for `n` elements, the lens still counterfactual. */
  const args = (n: number): Hex => {
    const elements = inputs(n).map((x) => concat([pad(toHex(x.a), { size: 32 }), pad(toHex(x.mode), { size: 32 })]));
    return encodeEnvelopeArgs(
      {
        target: { address: target, factory, factoryData },
        targetData: arrayToWire({ mode: "static", size: ELEMENT_SIZE }, elements),
      },
      config,
    );
  };

  it("carries by override the chunk the initcode cap refuses", async () => {
    const tuple = args(ELEMENTS);
    expect(tuple.length / 2 - 1 + FACTORY_BYTECODE_REVERT.length / 2).toBeGreaterThan(MAX_INITCODE_SIZE);

    const refused = await rpc(deliveryParams("initcode", tuple, ["latest"], undefined)).catch((e) => e);
    expect(upstreamMessage(refused)).toMatch(/initcode/i);

    const error = await rpc(deliveryParams("override", tuple, ["latest"], undefined)).catch((e) => e);
    const page = decodeEnvelopeRevert(error);
    if (page?.kind !== "page") throw new Error(`no page by override: ${upstreamMessage(error)}`);
    expect(adjudicated(page.data)).toBe(ELEMENTS);
  });

  it("returns 0x when the state override is dropped, as an ignoring provider would", async () => {
    const [transaction, block] = deliveryParams("override", args(3), ["latest"], undefined);

    expect(await rpc([transaction, block])).toBe("0x");
  });

  it("refuses a chunk whose EIP-7623 floor exceeds the gas it is given", async () => {
    const tuple = args(ELEMENTS);
    const gas = toHex(floorGas(tuple) - 1);

    const error = await rpc(deliveryParams("override", tuple, ["latest"], gas)).catch((e) => e);

    // anvil answers `intrinsic gas too high -- GasFloorMoreThanGasLimit` under a JSON-RPC -32000,
    // which viem renders as "Missing or invalid parameters"; the phrase the packer halves on is on
    // the innermost cause, where `classifyChunkError` reads it.
    expect(upstreamMessage(error)).toMatch(/floor data gas|intrinsic gas/i);
  });

  /** Every element through a fresh deployless client on `transport`, the lens still counterfactual. */
  const readAll = (transport: Parameters<typeof deployless>[0], batch?: { envelope: EnvelopeDelivery }) =>
    readLens(createPublicClient({ transport: deployless(transport, { gasLimit: GAS_LIMIT }) }), {
      abi: lensAbi,
      functionName: "item",
      address: target,
      factory,
      factoryData,
      args: inputs(ELEMENTS),
      batch,
    });

  it("serves every element in one to-shaped chunk through the public client", async () => {
    const { transport, requests, ethCalls } = recordingHttp(url);
    const { logger, events } = createStubLogger();

    const page = await withLogging(() => readAll(transport, { envelope: "override" }), { logger });

    expect(page.skipped).toEqual([]);
    expect(page.results).toEqual(inputs(ELEMENTS).map((x) => x.a * 2n));
    expect(ethCalls()).toHaveLength(1);
    for (const call of ethCalls()) {
      const [transaction, , overrides] = call.params ?? [];
      const { to, data } = transaction as { to?: string; data: Hex };
      expect(to?.toLowerCase()).toBe(ENVELOPE_ADDRESS.toLowerCase());
      expect(data.startsWith(FACTORY_BYTECODE_REVERT)).toBe(false);
      expect((overrides as Record<string, { code?: Hex }>)[ENVELOPE_ADDRESS]?.code).toBe(FACTORY_BYTECODE_REVERT);
    }
    const field = (name: string) => findDotted(events[0]!.context, "viem-dlc-deployless", `eth_call.${name}`);
    expect(field("chunks_override")).toBe(1);
    expect(field("chunks_initcode")).toBe(0);
    expect(field("override_fallbacks")).toBe(0);
    expect(requests.filter((r) => r.method !== "eth_call")).toEqual([]);
  });

  it("needs more than one chunk for the same request without the option", async () => {
    const { transport, ethCalls } = recordingHttp(url);

    const page = await readAll(transport);

    expect(page.results).toEqual(inputs(ELEMENTS).map((x) => x.a * 2n));
    // With no `batchSize` the whole input goes out as one initcode chunk, which the node refuses
    // for its size; the packer halves until the pieces fit under the cap.
    expect(ethCalls().length).toBeGreaterThan(1);
    for (const call of ethCalls()) {
      const [transaction] = call.params ?? [];
      const { to, data } = transaction as { to?: string; data: Hex };
      expect(to).toBeUndefined();
      expect(data.startsWith(FACTORY_BYTECODE_REVERT)).toBe(true);
    }
  });
});
