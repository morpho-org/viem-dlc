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
 *   3. REVERT with OK_SENTINEL || nA || gas telemetry || records, with {@link MALFORMED_RESULT_SELECTOR} when a result
 *      does not fit the declared layout, or with {@link MALFORMED_INPUT_SELECTOR} when the wire does.
 */
export const FACTORY_BYTECODE_REVERT: Hex =
  "0x5a5f5262000a3a60a0903881900380830191833981519161002b60608201518201604083015185610150565b608081015190602081015101805191602082015193604083015160608401916040861061013d5760018460df1c161561011d575b601f8019910116968752856020880152816040880152806060880152826080880152600160e08801938385526100a9828060401b038260401c16806101608c015260040182610980565b6101808a01526001600160e01b031981166101a08a015260dd1c16156100f357506020945060a08601526101c08501908160c08701525201016101008201526143008101906101b3565b925050506040010361010b57506101c08101906101b3565b633d62012160e21b5f5260045260245ffd5b604084901c6001600160401b03168083048811908802831415171561005f575b86633d62012160e21b5f5260045260245ffd5b9091813b15610172575b63101bb98d60e01b5f5260206004525f60245260445ffd5b5f80915a9482602083519301915af19160051c5a11153d15168215166101a4573b1515166101a25780808061015a565b565b633302f4d360e21b5f5260045ffd5b63a55835c360e01b8252905a602052602051602482019062015e0062015dff1982019111028152515f5103604482015260c481015f81526101f660e483016109d3565b5060208301519260808101519360018560de1c169160018060401b0386168315029560018160dd1c1660018260df1c161715965f945b848610610264575b5050859650828414830261024f575b50505060048301520390fd5b61025c925f1901916103f2565b5f8080610243565b95979495600186146103e25761027d87838b86886104a6565b80156103b9575a908360208c0180928189515afa60011461030f5750986020899a60061c015a11153d15166102f757602081896001935201975b01955a80602051039060205260648a0181815101905260848a01818002815101905260a48a01805182116102ef575b5050985061022c565b525f806102e6565b871981526020019660010195505f9150819050610234565b90508282156103a3575060403d1061039b573d99601f198b019160205f803e60205f511415601f8416176103935761057b8b9c6103548560051c6003029185016109d3565b01015a111561037b575081816001936020808095013e5b818460ff1b1781520101976102b7565b93509150506001929550861990529401925f80610234565b898688610a00565b878486610a00565b999050823d0361039b576020899a60019261036b565b505050848697959450156103ce575f80610234565b5f1985526001600487015285850360200186fd5b6103ed828a86610439565b61027d565b60016060820151610403848461095e565b14159260dd1c16610417575b5061010b5750565b60e081015160c082015114159060a0610100820151910151141517175f61040f565b915f9392610160810151916004830195869187811161049e575b506101808301519685019661046a602089016109d3565b015a1115610496575060e06024925f859398526101a0810151602087015201938451928391015e019052565b955050505050565b96505f610453565b93915f929593956101408601526101608501519260048401906101808701519760018560df1c1661070e575b8290838111610706575b508301976104ec60208a016109d3565b015a11156106fc57505f9096526101a08501516020820152602481019060e08601519060018460df1c166106eb575b5060018360dd1c165f146106d857509491929490839160a08601519360c08701519760e088015193610100890151965b86868c0310156106a7576141d88a018b11610676575b80515f1a908160051c5f146106515788600260078460051c148301011161063957805160011a6007018260051c1860078360051c14028260051c1891815160078260051c146001011a611f008260081b16018d8d6101bf199103016001820111610621578d5f5b8560020181106105ef57505050600292916007849260051c1401019b0101975b979961054b565b60018184010191600282880301808411610619575b5082908481035f19019083015e018e906105c8565b92505f610604565b6101408d0151633d62012160e21b5f5260045260245ffd5b6101408b0151633d62012160e21b5f5260045260245ffd5b88600283830101116106395790600281838e600180960151905201019b0101976105e8565b9493958a9685612000939c0386825e85880301948703900395611fff19016101c08a015e6121c08801988994610561565b86602097949b929998508661012097949b965e60a085015260c08401520160e0820152019260da1c16018151019052565b90839250928160e0949695965e01910152565b60209283905260440191015f61051b565b9650505050509050565b90505f6104dc565b9293968098508791969550610723925061095e565b60608701510390602082106109465760018660dd1c165f14610934576201a7795a111561092b576020935f9260a08901519760c08a015160805260e08a0151956101008b0151985b88886080510310156108c6576141d88c0160805111610889575b8a515f1a9a8a8c8060051c5f146108615760051c60071482016002011161062157805160011a6007018c60051c1860078d60051c14028c60051c189b8d825160078360051c146001011a611f008360081b1601906101bf199060805103016001820111610849578d5f5b8160020181106108165750505060051c6007140160029081019b608051010160805261076b565b60018184010190600281840301808311610841575b50815f1985608051030182608051015e016107ef565b91505f61082b565b6101408f0151633d62012160e21b5f5260045260245ffd5b8201600201116106215760028c826001809401516080515201019b608051010160805261076b565b9695866080999299510387825e8660805103019560805103900396612000611fff19608051016101c08d015e6121c08b0160805260805196610785565b949a9991968894995080939891965e60a089015260805160c08901520160e08701525f51935b601f19018411601f85168515171761091357602484019061090d8285610980565b976104d2565b610140860151633d62012160e21b5f5260045260245ffd5b50505091509150565b95949190929360e086015151936108ec565b610140870151633d62012160e21b5f5260045260245ffd5b9060dd1c6001161561097257610120015190565b60e060408201519101510390565b9190601f810160051c6003029062015ec882019360018160dd1c166109a457505050565b62015ec8939450906020612328939260da1c169003601b810160051c600302906003190161012c020101010190565b90601f5f920160051c80800260091c906003020160405181116109f35750565b9150604051820391604052565b90829060208301516001830114610a29575b5063ace36ecd60e01b5f526004523d60245260445ffd5b610a32926103f2565b5f8181610a1256";

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
