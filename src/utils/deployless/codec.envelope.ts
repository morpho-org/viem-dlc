import type { Address, Hex } from "viem";
import { decodeAbiParameters, deploylessCallViaFactoryBytecode, encodeAbiParameters, parseAbiParameters } from "viem";

import { causeChain } from "../errors.js";

import type { ResolvedArrayFunction } from "./codec.inner.js";
import { flzCompress, flzDecompress } from "./flz.js";

/**
 * Viem's factory wrapper bytecode, lowercased to match normalized request hex.
 *
 * Inbound recognition only, and load-bearing: callers reach us through viem's own
 * `client.call({ factory, factoryData })`, which encodes with this constant, so
 * {@link unwrapDeploylessFactoryCall} must keep accepting it. We never emit it — it exfiltrates via
 * RETURN, which caps results at EIP-170's 24 KB, and it is a fixed viem export we cannot patch to
 * carry the {@link OOG_SENTINEL} branch.
 *
 * Constructor arg shape: `(address target, bytes targetData, address factory, bytes factoryData)`.
 */
const FACTORY_BYTECODE_RETURN_VIEM = deploylessCallViaFactoryBytecode.toLowerCase() as Hex;

/**
 * The envelope: our factory wrapper bytecode, REVERT-mode exfiltration.
 *
 * Viem's constructor-arg shape plus a trailing config word — `(address target, bytes targetData,
 * address factory, bytes factoryData, uint256 config)`, see {@link envelopeConfig}. `targetData` is
 * the wire form {@link arrayToWire} builds, its body FastLZ-compressed when the config word says so.
 * The envelope calls the lens's per-item function once per element in its own frame and appends one
 * record per adjudicated element to an outcome stream that it exfiltrates via REVERT, prefixed with
 * {@link OK_SENTINEL} — see {@link hexToPage} for the record format. Nothing is written before the
 * attempt that needs it, and every memory expansion is admitted against the fee schedule first.
 *
 * Source: ./Envelope.yul. Regenerate with `pnpm build:Envelope` and paste the output here.
 *
 * Behavior:
 *   1. If `target` already has code, REVERT with `CounterfactualDeployFailed(bytes)` (selector
 *      0x101bb98d, viem's deployless failure selector): resident code cannot be checked against
 *      `factoryData`. Else CALL(factory, factoryData). Drained of gas → REVERT {@link OOG_SENTINEL};
 *      call failed OR `target` still has no code → `CounterfactualDeployFailed`.
 *   2. Per element: STATICCALL(target, selector || element). Returned → success record; reverted →
 *      decline record; died of gas, or a dynamic result the frame cannot afford to keep → `~index`
 *      closes the stream and the page stops.
 *   3. REVERT with OK_SENTINEL || nA || records, with {@link MALFORMED_RESULT_SELECTOR} when a result
 *      does not fit the declared layout, or with {@link MALFORMED_INPUT_SELECTOR} when the wire does.
 */
export const FACTORY_BYTECODE_REVERT: Hex =
  "0x620005366080813803810191818303908239805190610029606082015182016040830151846104a9565b608081015190602081015101928351926020850151936040860151956060810191604081106104965760018660dd1c165f1461047657601f8501601f1916928892849283929101602001905b8181106103d257505003036103bf57935b60018460df1c1692831561038d575b601f19601f88880101169563f90a85b560e01b875260248701955f87526044880192838111610385575b5092975f9594602060da86901c1694600160de82901c16936001600160401b03808316861502946001600160e01b03198416949390929160409190911c16818e5b8c10610114575b8d808e8e60048301520390fd5b8a9b9c829b61034f575b8a8c019b818d016024018b8e8b8501602001838111610345575b5061c8c8905f908385119384610324575b5060230160051c60030201015a11156102ea575f601f198301526102e2575b508d818d60208501958b875280936102cf575b50602483918601015e019b888360045a9301818a5afa6001146101eb57602091925060061c015a11153d15166101bf576020818e60019352019c5b019a998e610100565b985050505050505050508495506001925060209150831981520191015f80808080808080808080610107565b508789156102bb575060403d106102b5573d91601f1983019260205f803e60205f511415601f8516176102af578201908b8211906103235f8e84610295575b508660051c60030201015a111561026957508360208085013e610260575b506001916020915b818460ff1b17815201019c6101b6565b99506001610248565b9d5050509950505050505050505050849550600192508319905291015f80808080808080808080610107565b61029f915061050c565b6102a88561050c565b038e61022a565b8f610520565b8d610520565b9150873d036102b557600191602091610250565b602493506020908185880152019261017b565b9a505f610168565b5050509c9b5050505050505050505050508293508015610315575b5f80808080808080808080610107565b505f1981526020016001610305565b60239192506103329061050c565b61033b8661050c565b039190508f610149565b925061c8c8610138565b83820385039b5060208c106103725781519b601f19018c11601f8d16171561011e575b8d633d62012160e21b5f5260045260245ffd5b92505f6100bf565b604085901c6001600160401b0316808804831190830288141517156100955750633d62012160e21b5f5260045260245ffd5b84633d62012160e21b5f5260045260245ffd5b91925092508051805f1a918260051c91821560011461045b5760078314928160011a600701811884021893611f008560020192856001011a9160081b160160018101908603906020811860208211021890815f5b8281015f190151818a01520183811061045457505050506002929183910101920101915b9188928492610075565b8290610426565b5082600193925081846002930151865201019201019161044a565b9050866040969296011461008657633d62012160e21b5f5260045260245ffd5b86633d62012160e21b5f5260045260245ffd5b9091813b156104cb575b63101bb98d60e01b5f5260206004525f60245260445ffd5b5f80915a9482602083519301915af19160051c5a11153d15168215166104fd573b1515166104fb578080806104b3565b565b633302f4d360e21b5f5260045ffd5b601f0160051c80800260091c906003020190565b63ace36ecd60e01b5f526004523d60245260445ffd";

/** Every wrapper we emit. {@link FACTORY_BYTECODE_RETURN_VIEM} is inbound-only. */
const OWN_FACTORY_BYTECODES = [FACTORY_BYTECODE_REVERT] as const;

/**
 * 4-byte magic prefix on revert data that means "this revert is a page, not a real revert". Equal
 * to `bytes4(keccak256("ViemDlcPage()"))`; it is also the response format's version, so a page in
 * an older format is simply not recognised.
 */
export const OK_SENTINEL: Hex = "0xf90a85b5";

/**
 * 4-byte revert data meaning "the counterfactual deploy ran out of gas". Equal to
 * `bytes4(keccak256("ViemDlcOutOfGas()"))`.
 *
 * A frame that exhausts its gas cannot report it; the envelope's caller sees only an empty-data
 * failure, indistinguishable from a bare `revert()`. The deploy runs in a child frame the envelope
 * survives, so a factory call that failed empty with the envelope drained to its EIP-150 remainder
 * (two frames deep: ~2/64) is substituted with this marker — the one prologue death that can be
 * reported. Detect it with {@link isOutOfGasRevert}.
 */
export const OOG_SENTINEL: Hex = "0xcc0bd34c";

/**
 * Selector of the envelope's `MalformedResult(uint256 index, uint256 returndataSize)` revert — a
 * per-item result that does not fit the declared output layout. A lens bug, never a size problem.
 */
export const MALFORMED_RESULT_SELECTOR: Hex = "0xace36ecd";

/**
 * Selector of the envelope's `MalformedInput(uint256 index)` revert — the wire's element `index`
 * (or, at `index == n`, the body as a whole) does not fit the layout the config word declares. The
 * client wrote the wire, so this is a codec bug, never a decline.
 */
export const MALFORMED_INPUT_SELECTOR: Hex = "0xf5880484";

/** Selector of `CounterfactualDeployFailed(bytes)`: `target` already had code, or the factory call failed (not of gas) or left no code. */
export const COUNTERFACTUAL_DEPLOY_FAILED_SELECTOR: Hex = "0x101bb98d";

const VIEM_CONSTRUCTOR_PARAMS = parseAbiParameters("address, bytes, address, bytes");
const DEPLOYLESS_CONSTRUCTOR_PARAMS = parseAbiParameters("address, bytes, address, bytes, uint256");

/** Addresses & data describing the deployless target — invariant across a batch. */
export type DeploylessTarget = {
  address: Address;
  factory: Address;
  factoryData: Hex;
};

/** A deployless factory call: its {@link DeploylessTarget} plus the per-call `targetData` bytes (clear, never compressed). */
export type DeploylessFactoryCall = {
  target: DeploylessTarget;
  targetData: Hex;
};

const COMPRESSED_BIT = 1n << 221n;

/**
 * Reverses {@link wrapDeploylessFactoryCall} structurally: `targetData` comes back as the clear wire
 * form, decompressed when the config word's compression bit is set. Also accepts the RETURN-mode
 * form that viem's stock `client.call({ factory, factoryData })` produces — see
 * {@link FACTORY_BYTECODE_RETURN_VIEM} — whose `targetData` is the ABI-encoded array-shaped call.
 */
export function unwrapDeploylessFactoryCall(data: Hex): DeploylessFactoryCall {
  const lower = data.toLowerCase();
  const prefix = [FACTORY_BYTECODE_RETURN_VIEM, ...OWN_FACTORY_BYTECODES].find((p) => lower.startsWith(p));
  if (!prefix) throw new Error("eth_call data is not a deployless factory wrapper");
  const argsHex = `0x${data.slice(prefix.length)}` as Hex;
  if (prefix === FACTORY_BYTECODE_RETURN_VIEM) {
    const [address, targetData, factory, factoryData] = decodeAbiParameters(VIEM_CONSTRUCTOR_PARAMS, argsHex);
    return { target: { address, factory, factoryData }, targetData };
  }
  const [address, wire, factory, factoryData, config] = decodeAbiParameters(DEPLOYLESS_CONSTRUCTOR_PARAMS, argsHex);
  const targetData = config & COMPRESSED_BIT ? withBody(wire, flzDecompress(bodyOf(wire))) : wire;
  return { target: { address, factory, factoryData }, targetData };
}

const WIRE_HEADER_HEX = 2 + 64 * 2;
const bodyOf = (wire: Hex) => `0x${wire.slice(WIRE_HEADER_HEX)}` as Hex;
const withBody = (wire: Hex, body: Hex) => `${wire.slice(0, WIRE_HEADER_HEX)}${body.slice(2)}` as Hex;

/**
 * The envelope's config word: the per-item selector in the top 32 bits, the input-dynamic,
 * output-dynamic and compressed bits at 223, 222 and 221, the input element stride at bit 64 and the
 * output element stride at bit 0 (static sizes; zero for a dynamic type).
 */
export function envelopeConfig({ itemSelector, inputLayout, outputLayout }: ResolvedArrayFunction, compress: boolean) {
  return (
    (BigInt(itemSelector) << 224n) |
    (BigInt(inputLayout.mode === "dynamic") << 223n) |
    (BigInt(outputLayout.mode === "dynamic") << 222n) |
    (compress ? COMPRESSED_BIT : 0n) |
    (BigInt(inputLayout.mode === "static" ? inputLayout.size : 0) << 64n) |
    BigInt(outputLayout.mode === "static" ? outputLayout.size : 0)
  );
}

/**
 * Builds a deployless factory `eth_call` payload from the clear wire form ({@link arrayToWire});
 * `config` is {@link envelopeConfig}, and `compress` must match its bit.
 */
export function wrapDeploylessFactoryCall(
  { target, targetData }: DeploylessFactoryCall,
  { compress, config }: { compress: boolean; config: bigint },
) {
  const wire = compress ? withBody(targetData, flzCompress(bodyOf(targetData))) : targetData;
  const args = encodeAbiParameters(DEPLOYLESS_CONSTRUCTOR_PARAMS, [
    target.address,
    wire,
    target.factory,
    target.factoryData,
    config,
  ]);
  return `${FACTORY_BYTECODE_REVERT}${args.slice(2)}` as Hex;
}

/**
 * True when `req` is one of our deployless `eth_call`s — the lens intentionally reverts to exfiltrate
 * its returndata. False for any other request.
 *
 * Use this to defeat per-call retries at the next transport boundary
 * (e.g. `requestFn(args, isRevertExpected(args) ? { retryCount: 0 } : undefined)`).
 */
export function isRevertExpected(req: { method: string; params?: readonly unknown[] }) {
  if (req.method !== "eth_call") return false;

  const [transaction] = req.params ?? [];
  if (!transaction || typeof transaction !== "object") return false;

  const data = (transaction as { data?: unknown }).data;
  if (typeof data !== "string") return false;

  const lower = data.toLowerCase();
  return OWN_FACTORY_BYTECODES.some((p) => lower.startsWith(p));
}

/**
 * Pulls the revert-data hex out of an error thrown by a viem `requestFn` and checks for
 * {@link OK_SENTINEL}.
 *
 * - `{ ok: true, returnData }` — sentinel present; `returnData` is the lens's payload.
 * - `{ ok: false }` — no revert data, or data does not begin with {@link OK_SENTINEL}.
 *   The caller should rethrow the original error.
 */
export function extractRevertData(e: unknown): { ok: true; returnData: Hex } | { ok: false } {
  for (const raw of revertDataCandidates(e)) {
    if (raw.slice(0, 10).toLowerCase() === OK_SENTINEL) {
      return { ok: true, returnData: `0x${raw.slice(10)}` as Hex };
    }
  }
  return { ok: false };
}

/**
 * True when `e` carries {@link OOG_SENTINEL} as its revert data — the envelope reporting that the
 * counterfactual deploy ran out of gas. Uses the same `cause`-chain walk as {@link extractRevertData}.
 *
 * The match is exact: the wrapper reverts with the bare 4-byte selector, so a lens error that
 * merely happens to start with those bytes is not mistaken for an out-of-gas.
 */
export function isOutOfGasRevert(e: unknown): boolean {
  return revertsExactly(e, OOG_SENTINEL);
}

/** True when `e` is the envelope's {@link MALFORMED_RESULT_SELECTOR} revert (selector plus two `uint256`s). */
export function isMalformedResultRevert(e: unknown): boolean {
  return revertsWithSelector(e, MALFORMED_RESULT_SELECTOR, 2 + 8 + 128);
}

/** True when `e` is the envelope's {@link MALFORMED_INPUT_SELECTOR} revert (selector plus one `uint256`). */
export function isMalformedInputRevert(e: unknown): boolean {
  return revertsWithSelector(e, MALFORMED_INPUT_SELECTOR, 2 + 8 + 64);
}

/** True when `e` is the envelope's {@link COUNTERFACTUAL_DEPLOY_FAILED_SELECTOR} revert. */
export function isCounterfactualDeployFailedRevert(e: unknown): boolean {
  return revertsWithSelector(e, COUNTERFACTUAL_DEPLOY_FAILED_SELECTOR);
}

function revertsWithSelector(e: unknown, selector: Hex, exactLength?: number): boolean {
  for (const raw of revertDataCandidates(e)) {
    if (exactLength !== undefined && raw.length !== exactLength) continue;
    if (raw.slice(0, 10).toLowerCase() === selector) return true;
  }
  return false;
}

function revertsExactly(e: unknown, sentinel: Hex): boolean {
  for (const raw of revertDataCandidates(e)) {
    if (raw.toLowerCase() === sentinel) return true;
  }
  return false;
}

/**
 * Walks `e`'s `cause` chain, yielding every revert-data hex it finds. Wrapped errors still surface
 * their inner `data` (e.g. Monad nests an `RpcRequestError` with `data: "0x..."` inside an
 * `InternalRpcError` whose own `data` is `undefined`), and both `data: Hex` and `data: { data: Hex }`
 * shapes are tolerated.
 */
function* revertDataCandidates(e: unknown): Generator<string> {
  for (const cur of causeChain(e)) {
    const data = (cur as { data?: unknown }).data;
    const raw =
      typeof data === "string"
        ? data
        : data && typeof (data as { data?: unknown }).data === "string"
          ? (data as { data: string }).data
          : undefined;
    if (raw) yield raw;
  }
}
