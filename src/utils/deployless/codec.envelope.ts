import type { Address, Hex, PublicRpcSchema } from "viem";
import { decodeAbiParameters, deploylessCallViaFactoryBytecode, encodeAbiParameters, parseAbiParameters } from "viem";

import type { EIP1193Parameters } from "../../types.js";
import { causeChain } from "../errors.js";
import type { Tail } from "../tuples.js";

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
 * One bytecode, two deliveries: run as initcode with the argument tuple trailing it, or placed at
 * {@link ENVELOPE_ADDRESS} by an `eth_call` state override and called with the tuple as calldata
 * (docs/000016-tib-override-delivered-envelope.md). {@link deliveryParams} builds either request.
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
 *   3. REVERT with OK_SENTINEL || nA || gas telemetry || records, with {@link MALFORMED_RESULT_SELECTOR} when a result
 *      does not fit the declared layout, or with {@link MALFORMED_INPUT_SELECTOR} when the wire does.
 */
export const FACTORY_BYTECODE_REVERT: Hex =
  "0x5a5f5262000a4260a09080380380918339365f833781519161002c60608201518201604083015185610158565b608081015160208201518201908151926020830151946040840151906060850192604087106101455760018560df1c1615610125575b601f90811992369101010116968752856020880152816040880152806060880152826080880152600160e08801938385526100b1828060401b038260401c16806101608c015260040182610988565b6101808a01526001600160e01b031981166101a08a015260dd1c16156100fb57506020945060a08601526101c08501908160c08701525201016101008201526143008101906101bb565b925050506040010361011357506101c08101906101bb565b633d62012160e21b5f5260045260245ffd5b604085901c6001600160401b031680840489119089028414151715610062575b87633d62012160e21b5f5260045260245ffd5b9091813b1561017a575b63101bb98d60e01b5f5260206004525f60245260445ffd5b5f80915a9482602083519301915af19160051c5a11153d15168215166101ac573b1515166101aa57808080610162565b565b633302f4d360e21b5f5260045ffd5b63a55835c360e01b8252905a602052602051602482019062015e0062015dff1982019111028152515f5103604482015260c481015f81526101fe60e483016109db565b5060208301519260808101519360018560de1c169160018060401b0386168315029560018160dd1c1660018260df1c161715965f945b84861061026c575b50508596508284148302610257575b50505060048301520390fd5b610264925f1901916103fa565b5f808061024b565b95979495600186146103ea5761028587838b86886104ae565b80156103c1575a908360208c0180928189515afa6001146103175750986020899a60061c015a11153d15166102ff57602081896001935201975b01955a80602051039060205260648a0181815101905260848a01818002815101905260a48a01805182116102f7575b50509850610234565b525f806102ee565b871981526020019660010195505f915081905061023c565b90508282156103ab575060403d106103a3573d99601f198b019160205f803e60205f511415601f84161761039b5761057b8b9c61035c8560051c6003029185016109db565b01015a1115610383575081816001936020808095013e5b818460ff1b1781520101976102bf565b93509150506001929550861990529401925f8061023c565b898688610a08565b878486610a08565b999050823d036103a3576020899a600192610373565b505050848697959450156103d6575f8061023c565b5f1985526001600487015285850360200186fd5b6103f5828a86610441565b610285565b6001606082015161040b8484610966565b14159260dd1c1661041f575b506101135750565b60e081015160c082015114159060a0610100820151910151141517175f610417565b915f939261016081015191600483019586918781116104a6575b5061018083015196850196610472602089016109db565b015a111561049e575060e06024925f859398526101a0810151602087015201938451928391015e019052565b955050505050565b96505f61045b565b93915f929593956101408601526101608501519260048401906101808701519760018560df1c16610716575b829083811161070e575b508301976104f460208a016109db565b015a111561070457505f9096526101a08501516020820152602481019060e08601519060018460df1c166106f3575b5060018360dd1c165f146106e057509491929490839160a08601519360c08701519760e088015193610100890151965b86868c0310156106af576141d88a018b1161067e575b80515f1a908160051c5f146106595788600260078460051c148301011161064157805160011a6007018260051c1860078360051c14028260051c1891815160078260051c146001011a611f008260081b16018d8d6101bf199103016001820111610629578d5f5b8560020181106105f757505050600292916007849260051c1401019b0101975b9799610553565b60018184010191600282880301808411610621575b5082908481035f19019083015e018e906105d0565b92505f61060c565b6101408d0151633d62012160e21b5f5260045260245ffd5b6101408b0151633d62012160e21b5f5260045260245ffd5b88600283830101116106415790600281838e600180960151905201019b0101976105f0565b9493958a9685612000939c0386825e85880301948703900395611fff19016101c08a015e6121c08801988994610569565b86602097949b929998508661012097949b965e60a085015260c08401520160e0820152019260da1c16018151019052565b90839250928160e0949695965e01910152565b60209283905260440191015f610523565b9650505050509050565b90505f6104e4565b929396809850879196955061072b9250610966565b606087015103906020821061094e5760018660dd1c165f1461093c576201a7795a1115610933576020935f9260a08901519760c08a015160805260e08a0151956101008b0151985b88886080510310156108ce576141d88c0160805111610891575b8a515f1a9a8a8c8060051c5f146108695760051c60071482016002011161062957805160011a6007018c60051c1860078d60051c14028c60051c189b8d825160078360051c146001011a611f008360081b1601906101bf199060805103016001820111610851578d5f5b81600201811061081e5750505060051c6007140160029081019b6080510101608052610773565b60018184010190600281840301808311610849575b50815f1985608051030182608051015e016107f7565b91505f610833565b6101408f0151633d62012160e21b5f5260045260245ffd5b8201600201116106295760028c826001809401516080515201019b6080510101608052610773565b9695866080999299510387825e8660805103019560805103900396612000611fff19608051016101c08d015e6121c08b016080526080519661078d565b949a9991968894995080939891965e60a089015260805160c08901520160e08701525f51935b601f19018411601f85168515171761091b5760248401906109158285610988565b976104da565b610140860151633d62012160e21b5f5260045260245ffd5b50505091509150565b95949190929360e086015151936108f4565b610140870151633d62012160e21b5f5260045260245ffd5b9060dd1c6001161561097a57610120015190565b60e060408201519101510390565b9190601f810160051c6003029062015ec882019360018160dd1c166109ac57505050565b62015ec8939450906020612328939260da1c169003601b810160051c600302906003190161012c020101010190565b90601f5f920160051c80800260091c906003020160405181116109fb5750565b9150604051820391604052565b90829060208301516001830114610a31575b5063ace36ecd60e01b5f526004523d60245260445ffd5b610a3a926103fa565b5f8181610a1a56";

/**
 * Where the envelope runs in either delivery. `CREATE` from the zero address at nonce 0 — an
 * `eth_call` without `from` is sent from the zero address, whose nonce is 0 on every chain — so
 * placing the code here by override leaves the factory's `msg.sender` exactly as creation delivery
 * has it. A caller's own override at this address is a protocol error.
 */
export const ENVELOPE_ADDRESS: Address = "0xBd770416a3345F91E4B34576cb804a576fa48EB1";

/** How a chunk reaches the node: the envelope as initcode, or as code placed by a state override. */
export type EnvelopeDelivery = "initcode" | "override";

/**
 * 4-byte magic prefix on revert data that means "this revert is a page, not a real revert". Equal
 * to `bytes4(keccak256("ViemDlcPage3()"))`; it is also the response format's version, so a page in
 * an older format is simply not recognised.
 */
export const OK_SENTINEL: Hex = "0xa55835c3";

/**
 * 4-byte revert data meaning "the counterfactual deploy ran out of gas". Equal to
 * `bytes4(keccak256("ViemDlcOutOfGas()"))`.
 *
 * A frame that exhausts its gas cannot report it; the envelope's caller sees only an empty-data
 * failure, indistinguishable from a bare `revert()`. The deploy runs in a child frame the envelope
 * survives, so a factory call that failed empty with the envelope drained to its EIP-150 remainder
 * (two frames deep: ~2/64) is substituted with this marker — the one prologue death that can be
 * reported, and the one thing a smaller chunk cannot cure.
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

/**
 * Selector of `CounterfactualDeployFailed(bytes)`: `target` already had code, or the factory call
 * failed (not of gas) or left no code.
 */
export const COUNTERFACTUAL_DEPLOY_FAILED_SELECTOR: Hex = "0x101bb98d";

const VIEM_CONSTRUCTOR_PARAMS = parseAbiParameters("address, bytes, address, bytes");
const DEPLOYLESS_CONSTRUCTOR_PARAMS = parseAbiParameters("address, bytes, address, bytes, uint256");

/** Addresses & data describing the deployless target — invariant across a batch. */
export type DeploylessTarget = {
  address: Address;
  factory: Address;
  factoryData: Hex;
};

/** A deployless factory call: its {@link DeploylessTarget} plus the per-call `targetData` bytes, always clear. */
export type DeploylessFactoryCall = {
  target: DeploylessTarget;
  targetData: Hex;
};

const COMPRESSED_BIT = 1n << 221n;
const WIRE_HEADER_HEX = 2 + 64 * 2;
const bodyOf = (wire: Hex) => `0x${wire.slice(WIRE_HEADER_HEX)}` as Hex;
const withBody = (wire: Hex, body: Hex) => `${wire.slice(0, WIRE_HEADER_HEX)}${body.slice(2)}` as Hex;

/**
 * Reverses {@link encodeEnvelopeArgs} behind the initcode: `targetData` comes back as the clear wire
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
 * The envelope's argument tuple from the clear wire form ({@link arrayToWire}), the body compressed
 * when `config` ({@link envelopeConfig}) says so. Trails the initcode in one delivery and is the
 * calldata in the other — see {@link deliveryParams}.
 */
export function encodeEnvelopeArgs({ target, targetData }: DeploylessFactoryCall, config: bigint): Hex {
  const wire = config & COMPRESSED_BIT ? withBody(targetData, flzCompress(bodyOf(targetData))) : targetData;
  return encodeAbiParameters(DEPLOYLESS_CONSTRUCTOR_PARAMS, [
    target.address,
    wire,
    target.factory,
    target.factoryData,
    config,
  ]);
}

const asInitcode = (args: Hex): Hex => `${FACTORY_BYTECODE_REVERT}${args.slice(2)}` as Hex;

type RpcEthCallParams = EIP1193Parameters<PublicRpcSchema, "eth_call">["params"];

/** An `eth_call`'s params behind the transaction: block selector, state override, block overrides. */
export type RestOfEthCallParams = Tail<RpcEthCallParams>;

/**
 * The outbound `eth_call` params for one chunk. `rest` is the caller's block selector and state
 * override as the transport cleaned them; by override the envelope's own code entry is added to the
 * third parameter, never to `rest`, which the cache keys from.
 */
export function deliveryParams(
  delivery: EnvelopeDelivery,
  args: Hex,
  rest: RestOfEthCallParams,
  gas: Hex | undefined,
): RpcEthCallParams {
  const gasField = gas === undefined ? {} : { gas };
  if (delivery === "initcode") {
    return [{ data: asInitcode(args), ...gasField }, ...rest] as RpcEthCallParams;
  }
  const [block, stateOverride, ...blockOverrides] = rest;
  return [
    { to: ENVELOPE_ADDRESS, data: args, ...gasField },
    block ?? "latest",
    { ...stateOverride, [ENVELOPE_ADDRESS]: { code: FACTORY_BYTECODE_REVERT } },
    ...blockOverrides,
  ] as RpcEthCallParams;
}

const ENVELOPE_ADDRESS_LOWER = ENVELOPE_ADDRESS.toLowerCase();

/** The entry a state override holds at {@link ENVELOPE_ADDRESS}, whatever the key's case. */
function envelopeOverrideEntry(stateOverride: unknown): unknown {
  if (!stateOverride || typeof stateOverride !== "object") return undefined;
  return Object.entries(stateOverride).find(([key]) => key.toLowerCase() === ENVELOPE_ADDRESS_LOWER)?.[1];
}

/** True when a caller's state override names {@link ENVELOPE_ADDRESS}, in any case. */
export function overridesEnvelopeAddress(stateOverride: unknown): boolean {
  return envelopeOverrideEntry(stateOverride) !== undefined;
}

/**
 * True when `req` is one of our deployless `eth_call`s in either delivery — the envelope's initcode
 * as `data`, or a call to {@link ENVELOPE_ADDRESS} with its code in the state override — where the
 * revert is the page. False for any other request, including other calls to that address.
 *
 * Use this to defeat per-call retries at the next transport boundary
 * (e.g. `requestFn(args, isRevertExpected(args) ? { retryCount: 0 } : undefined)`).
 */
export function isRevertExpected(req: { method: string; params?: readonly unknown[] }) {
  if (req.method !== "eth_call") return false;

  const [transaction] = req.params ?? [];
  if (!transaction || typeof transaction !== "object") return false;

  const { to, data } = transaction as { to?: unknown; data?: unknown };
  if (typeof data !== "string") return false;
  if (data.toLowerCase().startsWith(FACTORY_BYTECODE_REVERT)) return true;

  if (typeof to !== "string" || to.toLowerCase() !== ENVELOPE_ADDRESS_LOWER) return false;
  const code = (envelopeOverrideEntry(req.params?.[2]) as { code?: unknown } | undefined)?.code;
  return typeof code === "string" && code.toLowerCase() === FACTORY_BYTECODE_REVERT;
}

/** One of the envelope's own reverts, as {@link decodeEnvelopeRevert} reads it off a thrown error. */
export type EnvelopeRevert =
  | { kind: "page"; data: Hex }
  | { kind: "outOfGas" }
  | { kind: "malformedResult" }
  | { kind: "malformedInput" }
  | { kind: "counterfactualDeployFailed" };

/**
 * Reads the envelope's revert out of an error a viem `requestFn` threw, or `null` when the revert
 * data is not the envelope's: a page is {@link OK_SENTINEL} followed by the outcome stream;
 * {@link OOG_SENTINEL} matches exactly, so a lens error that merely starts with those bytes is not an
 * out-of-gas; the two `Malformed*` reverts carry exactly their declared arguments.
 */
export function decodeEnvelopeRevert(e: unknown): EnvelopeRevert | null {
  for (const raw of revertDataCandidates(e)) {
    const lower = raw.toLowerCase();
    const selector = lower.slice(0, 10);
    if (selector === OK_SENTINEL) return { kind: "page", data: `0x${raw.slice(10)}` as Hex };
    if (lower === OOG_SENTINEL) return { kind: "outOfGas" };
    if (selector === MALFORMED_RESULT_SELECTOR && raw.length === 2 + 8 + 128) return { kind: "malformedResult" };
    if (selector === MALFORMED_INPUT_SELECTOR && raw.length === 2 + 8 + 64) return { kind: "malformedInput" };
    if (selector === COUNTERFACTUAL_DEPLOY_FAILED_SELECTOR) return { kind: "counterfactualDeployFailed" };
  }
  return null;
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
