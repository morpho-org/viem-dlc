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
  "0x620009aa61014090388190038083019183398151916100296060820151820160408301518561011a565b60808101519060208101510180519160208201519360408301516060840191604086106101075760018460df1c16156100e7575b601f8019910116968752856020880152816040880152806060880152826080880152600160e088019383855260dd1c165f146100bd57506020945060a08601526101608501908160c08701525201016101008201526142a081019061017d565b92505050604001036100d5575061016081019061017d565b633d62012160e21b5f5260045260245ffd5b604084901c6001600160401b03168083048811908802831415171561005d575b86633d62012160e21b5f5260045260245ffd5b9091813b1561013c575b63101bb98d60e01b5f5260206004525f60245260445ffd5b5f80915a9482602083519301915af19160051c5a11153d151682151661016e573b15151661016c57808080610124565b565b633302f4d360e21b5f5260045ffd5b60a05263f90a85b560e01b815260248101905f82526044810191602060a051015192608060a05101515f915b8583106101bf575b505082935060048301520390fd5b9093918061014060a05101526101d982858760a051610631565b90608052610100515a111561060c575f601f19820152828111610604575b506001600160e01b031985166020850152602484019360e060a051015160018760df1c166105f2575b60018760dd1c165f146105d4575084936080519560a0805101519760c060a051015160e05260e060a051015160c05261010060a0510151995b8860c05160e0510310156103cc576142a060a0510161012860e0510111610383575b89515f1a998a60051c5f14610359578b600260078d60051c148301011161033f57805160011a6007018b60051c1860078c60051c14028b60051c189a815160078260051c146001011a611f008260081b160161015f1960a05160e0510301600182011161033f578c5f5b81600201811061030c5750505060051c6007140160029081019a60e051010160e052610259565b60018184010190600281840301808311610337575b50815f198560e05103018260e051015e016102e5565b91505f610321565b61014060a0510151633d62012160e21b5f5260045260245ffd5b8b60028c8301011161033f5760028b8260018094015160e0515201019a60e051010160e052610259565b979660c05160e0510360c051825e60c05160e05103019660c05160e05103900397612000611fff1960e0510161016060a051015e61216060a0510160e05260e05160c05261027b565b9688939992949598919a5060c051905e60a08051015260e05160c060a051015260c0510160e060a051015261012060a0510160805160208960da1c1601815101809152602060a0510151600161014060a0510151011461059f575b505b5a9060de88901c600116156001600160401b03891602906020840190601f199085906080510103016020850160a051515afa60011461049f5760061c6020015a11153d151661048657602081856001935201935b019194906101a9565b8495506001925060209150831981520191015f806101b1565b5060de86901c600116156001600160401b038716810290610573575060403d1061056d573d90601f1982019160205f803e60205f511415601f8416176105675781018381115f81610549575b6104b3908560051c60030201015a111561052c578360208085013e610523575b506001916020915b818460ff1b17815201019361047d565b9250600161050b565b505090508495506001925060209150831981520191015f806101b1565b506104b361055686610980565b61055f84610980565b0390506104eb565b85610994565b83610994565b9060de87901c600116156001600160401b038816023d0361059957600191602091610513565b84610994565b606060a0510151141560e060a051015160c060a051015114151761010060a051015160a08051015114151761033f575f610427565b608097949697959192955181835e6080510160e060a0510152610429565b94602090526020604482019501610220565b91505f6101f7565b50919450508392508015610622575b5f806101b1565b505f198152602001600161061b565b5f1961010052604082901c6001600160401b0316949293905f60df84901c6001166106fb575b50508460208360da1c160160206024828701019560018060401b03851660018660de1c16150201018581116106f2575b50906001915f918087116106d3575b5060230160051c600302019162012cc883016101005260dd1c166106b75750565b601f840160051c60030261012c8502010162014ff00161010052565b60239192506106e190610980565b6106ea87610980565b039190610696565b94506001610687565b92919394909560018360dd1c1693845f14610970576101208801515b6060890151039460208610610958571561094557620175795a111561093b5750506020925f9160a08801519660c08901516101205260e0890151946101008a0151975b8787610120510310156108e5576142a08b016101286101205101116108a2575b89515f1a998a60051c5f146108755789600260078d60051c148301011161085d57805160011a6007018b60051c1860078c60051c14028b60051c189a815160078260051c146001011a611f008260081b16018d61015f19906101205103016001820111610845578c5f5b81600201811061080d5750505060051c6007140160029081019a6101205101016101205261075a565b60028160018186010193030180831161083d575b508161010051846101205103018261012051015e018d906107e4565b91505f610821565b6101408e0151633d62012160e21b5f5260045260245ffd5b6101408c0151633d62012160e21b5f5260045260245ffd5b8960028c8301011161085d5760028b82600180940151610120515201019a6101205101016101205261075a565b959485610120989298510386825e85610120510301946101205103900395612000611fff1961012051016101608c015e6121608a0161012052610120519561077a565b9580939992985087949a959791975e60a08401526101205160c08401520160e08201525f51955b601f19018611601f8716871517176109245780610657565b6101400151633d62012160e21b5f5260045260245ffd5b9594509550505050565b5050939290919460e0810151519561090c565b610140890151633d62012160e21b5f5260045260245ffd5b604088015160e089015103610717565b601f0160051c80800260091c906003020190565b63ace36ecd60e01b5f526004523d60245260445ffd";

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
const WIRE_HEADER_HEX = 2 + 64 * 2;
const bodyOf = (wire: Hex) => `0x${wire.slice(WIRE_HEADER_HEX)}` as Hex;
const withBody = (wire: Hex, body: Hex) => `${wire.slice(0, WIRE_HEADER_HEX)}${body.slice(2)}` as Hex;

/**
 * Reverses {@link wrapDeploylessFactoryCall} structurally: `targetData` comes back as the clear wire
 * form, decompressed when the config word's compression bit is set. Also accepts the RETURN-mode
 * form that viem's stock `client.call({ factory, factoryData })` produces — see
 * {@link FACTORY_BYTECODE_RETURN_VIEM} — whose `targetData` is the ABI-encoded array-shaped call.
 */
export function unwrapDeploylessFactoryCall(data: Hex): DeploylessFactoryCall {
  const lower = data.toLowerCase();
  if (lower.startsWith(FACTORY_BYTECODE_RETURN_VIEM)) {
    const args = `0x${data.slice(FACTORY_BYTECODE_RETURN_VIEM.length)}` as Hex;
    const [address, targetData, factory, factoryData] = decodeAbiParameters(VIEM_CONSTRUCTOR_PARAMS, args);
    return { target: { address, factory, factoryData }, targetData };
  }
  if (!lower.startsWith(FACTORY_BYTECODE_REVERT)) throw new Error("eth_call data is not a deployless factory wrapper");
  const args = `0x${data.slice(FACTORY_BYTECODE_REVERT.length)}` as Hex;
  const [address, wire, factory, factoryData, config] = decodeAbiParameters(DEPLOYLESS_CONSTRUCTOR_PARAMS, args);
  const targetData = config & COMPRESSED_BIT ? withBody(wire, flzDecompress(bodyOf(wire))) : wire;
  return { target: { address, factory, factoryData }, targetData };
}

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

  return data.toLowerCase().startsWith(FACTORY_BYTECODE_REVERT);
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
