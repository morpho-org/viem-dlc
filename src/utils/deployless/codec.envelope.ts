import type { Address, Hex } from "viem";
import { decodeAbiParameters, deploylessCallViaFactoryBytecode, encodeAbiParameters, parseAbiParameters } from "viem";

import { causeChain } from "../errors.js";

import type { ResolvedArrayFunction } from "./codec.inner.js";
import { flzCompress } from "./flz.js";

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
 * Custom factory wrapper bytecode (REVERT-mode, no compression).
 *
 * Viem's constructor-arg shape plus a trailing config word — `(address target, bytes targetData,
 * address factory, bytes factoryData, uint256 config)`, see {@link envelopeConfig}. The envelope
 * paginates: it reads the element array out of `targetData` and calls the lens's per-item function
 * once per element in its own frame, depositing results into a `(U[] results, uint256[] skipped)`
 * slab that it exfiltrates via REVERT (prefixed with {@link OK_SENTINEL}), so results are not
 * subject to EIP-170's 24 KB and the frame's EIP-150 remainder never funds a copy.
 *
 * Source: ./RevertEnvelope.yul. Regenerate with `pnpm build:RevertEnvelope` and paste the output here.
 *
 * Behavior:
 *   1. If `target` already has code, REVERT with `CounterfactualDeployFailed(bytes)` (selector
 *      0x101bb98d, viem's deployless failure selector): resident code cannot be checked against
 *      `factoryData`. Else CALL(factory, factoryData). Drained of gas → REVERT {@link OOG_SENTINEL};
 *      call failed OR `target` still has no code → `CounterfactualDeployFailed`.
 *   2. Per element: STATICCALL(target, selector || element). Returned → deposited; reverted →
 *      index in `skipped`; died of gas → `~index` closes `skipped` and the page stops.
 *   3. REVERT with OK_SENTINEL || (results, skipped), or with {@link MALFORMED_RESULT_SELECTOR} when
 *      a result does not fit the declared layout.
 */
export const FACTORY_BYTECODE_REVERT: Hex =
  "0x620003bc60809080380390823980519060608101518101604082015190610026918461029b565b60208101518101906080015190805190604481015191810191601f19603f8401169460a086019163ffffffff60e01b86168352600160401b600190038660401c1695601f1987890160e3011696631580d19d60e01b885260048801604090528160de1c600116918360051b978884028a0160640197600160401b600190038316938360d91c602016850187028c019a8b016084015f9052601f0160051c600302601f850160051c86026003026101f40160061b01610190018c5260208c01996084018a5260640160408c015260200160608b015260808a01525f935f955f945b808610610139575b8a60208b8b8b8b8160051b8094518685015e6044860152848203916003198301602487015252010190fd5b9091929394966001810160c0028c51015a111561026e5761015c8289858f6102fe565b8015610259575a90868b891561022e5750505f809186885afa6101cc576020905b60061c015a11153d15166101a65760018189829360051b8d51015201975b019493929190610106565b945050505050602095965060019192198160051b86510152019085945f8080808061010e565b5096959760403d10610228573d601f19810160205f803e60205f511415601f821617878211176102225791600192918b60648f86956020863e8085036063190160059390931b01015201601f190199019661019b565b886103a6565b866103a6565b909186885afa6102405760209061017d565b50969597843d036102285760018581920199019661019b565b5060018189829360051b8d510152019761019b565b9694505050505060209596501561028c575b85945f8080808061010e565b90505f19835152600190610280565b9091813b156102bd575b63101bb98d60e01b5f5260206004525f60245260445ffd5b5f80915a9482602083519301915af19160051c5a11153d15168215166102ef573b1515166102ed578080806102a5565b565b633302f4d360e21b5f5260045ffd5b5f9194939260808201519360408301519060018660df1c165f1461037f579082916001606096959460051b83015183019687950151930190811061036e575b5050039260401c6001600160401b031683116103685750602484602060048596970152015e60240190565b93505050565b90915060051b810151015f8061033d565b50949560409490941c6001600160401b031694859491850201925060040190505e60040190565b63ace36ecd60e01b5f526004523d60245260445ffd";

/**
 * Custom factory wrapper bytecode (REVERT-mode, FLZ input compression).
 *
 * Same constructor-arg shape, but `targetData` must be FLZ-compressed by the caller
 * (via {@link flzCompress} in ./flz.ts). The wrapper decompresses it, then runs the same page
 * loop as {@link FACTORY_BYTECODE_REVERT}; the response is not compressed.
 *
 * Source: ./RevertEnvelopeCompressed.yul. Regenerate with `pnpm build:RevertEnvelopeCompressed`.
 */
export const FACTORY_BYTECODE_REVERT_COMPRESSED: Hex =
  "0x6200047e60808138038101918183039082398051916100296060830151830160408401518561035d565b6020820151820190601f801991011691815160208084019185940101905b8181106102b957505060800151918060248101519203810191601f19601f8401169460a086019163ffffffff60e01b86168352600160401b600190038660401c1695601f1987890160e3011696631580d19d60e01b885260048801604090528160de1c600116918360051b978884028a0160640197600160401b600190038316938360d91c602016850187028c019a8b016084015f9052601f0160051c600302601f850160051c86026003026101f40160061b01610190018c5260208c01996084018a5260440160408c015260608b015260808a01525f935f955f945b808610610157575b8a60208b8b8b8b8160051b8094518685015e6044860152848203916003198301602487015252010190fd5b9091929394966001810160c0028c51015a111561028c5761017a8289858f6103c0565b8015610277575a90868b891561024c5750505f809186885afa6101ea576020905b60061c015a11153d15166101c45760018189829360051b8d51015201975b019493929190610124565b945050505050602095965060019192198160051b86510152019085945f8080808061012c565b5096959760403d10610246573d601f19810160205f803e60205f511415601f821617878211176102405791600192918b60648f86956020863e8085036063190160059390931b01015201601f19019901966101b9565b88610468565b86610468565b909186885afa61025e5760209061019b565b50969597843d03610246576001858192019901966101b9565b5060018189829360051b8d51015201976101b9565b969450505050506020959650156102aa575b85945f8080808061012c565b90505f1983515260019061029e565b8095949593919351805f1a918260051c9182156001146103425760078314928160011a600701811884021893611f008560020192856001011a9160081b160160018101908603906020811860208211021890815f5b8281015f190151818a01520183811061033b57505050506002929183910101920101925b90949394610047565b829061030e565b50826001939250818460029301518652010192010192610332565b9091813b1561037f575b63101bb98d60e01b5f5260206004525f60245260445ffd5b5f80915a9482602083519301915af19160051c5a11153d15168215166103b1573b1515166103af57808080610367565b565b633302f4d360e21b5f5260045ffd5b5f9194939260808201519360408301519060018660df1c165f14610441579082916001606096959460051b830151830196879501519301908110610430575b5050039260401c6001600160401b0316831161042a5750602484602060048596970152015e60240190565b93505050565b90915060051b810151015f806103ff565b50949560409490941c6001600160401b031694859491850201925060040190505e60040190565b63ace36ecd60e01b5f526004523d60245260445ffd";

/** Every wrapper we emit. {@link FACTORY_BYTECODE_RETURN_VIEM} is inbound-only. */
const OWN_FACTORY_BYTECODES = [FACTORY_BYTECODE_REVERT, FACTORY_BYTECODE_REVERT_COMPRESSED] as const;

/**
 * 4-byte magic prefix on revert data that means "this revert is the lens's success payload,
 * not a real revert". Equal to `bytes4(keccak256("ViemDlcOk()"))`.
 */
export const OK_SENTINEL: Hex = "0x1580d19d";

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

/** A deployless factory call: its {@link DeploylessTarget} plus the per-call `targetData` bytes. */
export type DeploylessFactoryCall = {
  target: DeploylessTarget;
  targetData: Hex;
};

/**
 * Reverses {@link wrapDeploylessFactoryCall} structurally (`targetData` comes back as sent, still
 * compressed for the compressed envelope), and also accepts the RETURN-mode form that viem's stock
 * `client.call({ factory, factoryData })` produces — see {@link FACTORY_BYTECODE_RETURN_VIEM}.
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
  const [address, targetData, factory, factoryData] = decodeAbiParameters(DEPLOYLESS_CONSTRUCTOR_PARAMS, argsHex);
  return { target: { address, factory, factoryData }, targetData };
}

/**
 * The envelope's config word: the per-item selector in the top 32 bits, the input-dynamic and
 * output-dynamic bits at 223 and 222, the input element size at bit 64 and the output element
 * size at bit 0 (static strides, or declared maximum tail bytes for dynamic types).
 */
export function envelopeConfig(solidity: ResolvedArrayFunction): bigint {
  const { inputLayout, outputLayout } = solidity;
  const inputSize = inputLayout.mode === "dynamic" ? solidity.maxItemBytes! : inputLayout.size;
  const outputSize = outputLayout.mode === "dynamic" ? solidity.maxResultBytes! : outputLayout.size;
  return (
    (BigInt(solidity.itemSelector) << 224n) |
    (BigInt(inputLayout.mode === "dynamic") << 223n) |
    (BigInt(outputLayout.mode === "dynamic") << 222n) |
    (BigInt(inputSize) << 64n) |
    BigInt(outputSize)
  );
}

/** Builds a deployless factory `eth_call` payload; `config` is {@link envelopeConfig}. */
export function wrapDeploylessFactoryCall(
  { target, targetData }: DeploylessFactoryCall,
  { compress, config }: { compress: boolean; config: bigint },
) {
  const prefix = compress ? FACTORY_BYTECODE_REVERT_COMPRESSED : FACTORY_BYTECODE_REVERT;
  const encodedTargetData = compress ? flzCompress(targetData) : targetData;
  const args = encodeAbiParameters(DEPLOYLESS_CONSTRUCTOR_PARAMS, [
    target.address,
    encodedTargetData,
    target.factory,
    target.factoryData,
    config,
  ]);
  return `${prefix}${args.slice(2)}` as Hex;
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
