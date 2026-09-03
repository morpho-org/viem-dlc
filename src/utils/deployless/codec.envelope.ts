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
  "0x6200099761012090388190038083019183398151916100296060820151820160408301518561011a565b60808101519060208101510180519160208201519360408301516060840191604086106101075760018460df1c16156100e7575b601f8019910116968752856020880152816040880152806060880152826080880152600160e088019383855260dd1c165f146100bd57506020945060a08601526101608501908160c08701525201016101008201526142a081019061017d565b92505050604001036100d5575061016081019061017d565b633d62012160e21b5f5260045260245ffd5b604084901c6001600160401b03168083048811908802831415171561005d575b86633d62012160e21b5f5260045260245ffd5b9091813b1561013c575b63101bb98d60e01b5f5260206004525f60245260445ffd5b5f80915a9482602083519301915af19160051c5a11153d151682151661016e573b15151661016c57808080610124565b565b633302f4d360e21b5f5260045ffd5b60e05263f90a85b560e01b8152602481015f815260448201602060e05101515f915b8183106101b4575b8480858560048301520390fd5b9093918061014060e05101526101cd828560e051610624565b909160c0525a11156105fd575f601f198201528281116105f5575b506001600160e01b0319608060e0510151166020850152602484019360e0805101516001608060e051015160df1c166105e3575b6001608060e051015160dd1c16156105c75750849360c05160805260a060e051015160c060e05101519060e0805101516101005261010060e051015160a0525b60805161010051830310156103be576142a060e05101610128830111610378575b80515f1a908160051c5f146103525760a051600260078460051c148301011161033857805160011a6007018260051c1860078360051c14028260051c1891815160078260051c146001011a611f008260081b160161015f1960e051860301600182011161033857602060018201186020600183011102600182011890815f5b82880381015f190151818901520185600201811061033157505050600292916007849260051c140101920101975b979061025c565b82906102fc565b61014060e0510151633d62012160e21b5f5260045260245ffd5b9060a051600282840101116103385760028183600180950151865201019201019761032a565b90956120009061010051880361010051825e610100518803019661010051810360805103608052611fff190161016060e051015e61216060e0510190816101005261027d565b90919692979493956080519061010051905e60a060e051015260c060e0510152608051610100510160e08051015260c0516020608060e051015160da1c160161012060e0510190815101809152602060e0510151600161014060e05101510114610592575b505b6001600160401b03608060e0510151166001608060e051015160de1c161502805a92602089019089601f199160c05101030160208a0160e051515afa6001146104a75750946020859660061c015a11153d151661049057602081856001935201935b0192935061019f565b60019250602091508319815201910190915f6101a7565b9050806001608060e051015160de1c161561057757505060403d10610571573d94601f1986019060205f803e60205f511415601f83161761056b57859681015f848211908161054d575b6104b3908560051c60030201015a1115610532578360208085013e610529575b506001916020915b818460ff1b178152010193610487565b92506001610511565b5050905060019250602091508319815201910190915f6101a7565b506104b361055a8661096d565b6105638461096d565b0390506104f1565b84610981565b82610981565b9590953d0361058c5760208596600192610519565b83610981565b606060e0510151141560e08051015160c060e051015114151761010060e051015160a060e0510151141517610338575f610423565b60c0969392949591965181835e60c0510160e080510152610425565b9460209052602060448201950161021c565b91505f6101e8565b5082945080915092919215610615575b90915f6101a7565b505f198152602001600161060d565b9091925f936080830151604081901c6001600160401b0316959094905f19906001608087015160df1c166106fc575b505060208681608087015160da1c1601828101602401969092906001600160401b03166001608088015160de1c16150201018581116106f4575b505f918086116106d5575b5060230160051c600302016001608062012cc8830194015160dd1c166106bb5750565b601f850160051c60030261012c8602010162014ff0019150565b60239192506106e39061096d565b6106ec8661096d565b039190610698565b94505f61068d565b91939495965f939193506001608088015160dd1c161561095d576101208701515b6060880151039260208410610945576001608089015160dd1c161561093157620175795a111561092657505095610100906020959495925f9160a08901519660c08a01519a8b98878c60e081015198899101519b5b0310156108cf576142a08c016101288e011161089e575b80515f1a8060051c5f14610877578a600260078360051c148401011161085f57908d918d825160011a6007018260051c1860078360051c14028260051c1893835160078460051c146001011a611f008460081b16019161015f199103016001820111610847578f5f5b8d8184840301015181830152602060018401186020600185011102600184011801908d8660020183106108405750505050600292916007849260051c1401019d0101995b999b87878e610772565b50906107f2565b6101408f0151633d62012160e21b5f5260045260245ffd5b6101408d0151633d62012160e21b5f5260045260245ffd5b908a6002838301011161085f57906002818f9360019485820151905201019d010199610836565b9594968c9786612000939e0387825e86890301958803900396611fff19016101608c015e6121608a019a8b95610789565b929a9996959486929950859198509b939b5e60a088015260c08701520160e08501525f51955b601f19018611601f87168715171761090e575f80610653565b610140840151633d62012160e21b5f5260045260245ffd5b965096505050919050565b505091509493929160e084015151956108f5565b610140880151633d62012160e21b5f5260045260245ffd5b604087015160e08801510361071d565b601f0160051c80800260091c906003020190565b63ace36ecd60e01b5f526004523d60245260445ffd";

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
