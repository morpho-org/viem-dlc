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
 * {@link OK_SENTINEL} — see {@link hexToPage} for the record format. Nothing is written or
 * decompressed before the attempt that needs it, and every memory expansion is admitted against
 * the fee schedule first.
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
  "0x6200093860a0903881900380830191833981519161002860608201518201604083015185610119565b60808101519060208101510180519160208201519360408301516060840191604086106101065760018460df1c16156100e6575b601f8019910116968752856020880152816040880152806060880152826080880152600160e088019383855260dd1c165f146100bc57506020945060a08601526101608501908160c08701525201016101008201526142a081019061017c565b92505050604001036100d4575061016081019061017c565b633d62012160e21b5f5260045260245ffd5b604084901c6001600160401b03168083048811908802831415171561005c575b86633d62012160e21b5f5260045260245ffd5b9091813b1561013b575b63101bb98d60e01b5f5260206004525f60245260445ffd5b5f80915a9482602083519301915af19160051c5a11153d151682151661016d573b15151661016b57808080610123565b565b633302f4d360e21b5f5260045ffd5b9063f90a85b560e01b8152602481015f8152604482016020840151935f915b858310610210575b506001858314608083015160dd1c16166101c5575b5082935060048301520390fd5b6060810151610120820151141560e082015160c08301511415179060a06101008201519101511415176101f857836101b8565b633d62012160e21b5f9081525f198501600452602490fd5b9093919294836101408601526102278287876105f8565b9791975a11156105d0575f601f198201528381116105c8575b506001600160e01b03196080870151166020820152602481019460e08701516001608089015160df1c166105b6575b6001608089015160dd1c161561059d57508592889560a08901519760c08a015160805260e08a0151966101008b0151995b8989608051031015610414579b60029c6142a08d0161012860805101116103d6575b8b81515f1a9e8f8060051c5f146103b05760051c60071483010111610398578d815160011a6007018160051c1860078260051c14029060051c189d8d825160078360051c146001011a611f008360081b16019061015f199060805103016001820111610380578f905f5b5f1981836080510301015181608051015260206001830118602060018401110260018301180182600201811061037b5750505060051c6007140160029081019d60805101016080525b9b6102a0565b61032c565b6101408f0151633d62012160e21b5f5260045260245ffd5b6101408d0151633d62012160e21b5f5260045260245ffd5b830101116103985760028e826001809401516080515201019d6080510101608052610375565b98978860809b929b510389825e88608051030197608051039003986120008c610160611fff196080510191015e6121608c01608052608051986102c2565b91949950978781949c979b98939b5e60a087015260805160c08701520160e0850152826020608086015160da1c160161012085019081510190525b6001600160401b036080850151166001608086015160de1c16150280935a92602085019101849003601f19018187515afa6001146104bf57602091925060061c015a11153d15166104ac57602081856001935201935b019161019b565b831981526020019260010191505f6101a3565b50816001608085015160de1c16156105865750905060403d10610580573d90601f1982019160205f803e60205f511415601f84161761057a5781018481115f8161055c575b6104b3908560051c60030201015a1115610545578360208085013e61053c575b506001916020915b818460ff1b1781520101936104a5565b93506001610524565b5050841981526020019360010192505f90506101a3565b506104b36105698761090e565b6105728461090e565b039050610504565b85610922565b83610922565b3d036105975760019160209161052c565b84610922565b97888180959a8994999a97989a5e0160e085015261044f565b9560209052602060448301960161026f565b92505f610240565b509392949150945081156105e5575b5f6101a3565b5f198352602090920191600191506105df565b9091925f936080830151604081901c6001600160401b0316959094905f19906001608087015160df1c166106d0575b505060208681608087015160da1c1601828101602401969092906001600160401b03166001608088015160de1c16150201018581116106c8575b505f918086116106a9575b5060230160051c600302016001608062012cc8830194015160dd1c1661068f5750565b601f850160051c60030261012c8602010162014ff0019150565b60239192506106b79061090e565b6106c08661090e565b03919061066c565b94505f610661565b91939495965f939193506001608088015160dd1c16156108fe576101208701515b60608801510392602084106108e6576001608089015160dd1c16156108d257620175795a11156108c757505095610100906020959495925f9160a08901519660c08a01519a8b98878c60e081015198899101519b5b031015610873576142a08c016101288e0111610842575b80515f1a8060051c5f1461081b578a600260078360051c148401011161039857908d918d825160011a6007018260051c1860078360051c14028260051c1893835160078460051c146001011a611f008460081b16019161015f199103016001820111610380578f5f5b8d8184840301015181830152602060018401186020600185011102600184011801908d8660020183106108145750505050600292916007849260051c1401019d0101995b999b87878e610746565b50906107c6565b908a6002838301011161039857906002818f9360019485820151905201019d01019961080a565b9594968c9786612000939e0387825e86890301958803900396611fff19016101608c015e6121608a019a8b9561075d565b929a9996959486929950859198509b939b5e60a088015260c08701520160e08501525f51955b601f19018611601f8716176108af575f80610627565b610140840151633d62012160e21b5f5260045260245ffd5b965096505050919050565b505091509493929160e08401515195610899565b610140880151633d62012160e21b5f5260045260245ffd5b604087015160e0880151036106f1565b601f0160051c80800260091c906003020190565b63ace36ecd60e01b5f526004523d60245260445ffd";

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
 * reported, and the one thing a smaller chunk cannot cure. Detect it with {@link isOutOfGasRevert}.
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
