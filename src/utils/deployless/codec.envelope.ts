import type { Address, Hex } from "viem";
import {
  BaseError,
  decodeAbiParameters,
  deploylessCallViaFactoryBytecode,
  encodeAbiParameters,
  parseAbiParameters,
} from "viem";

import { flzCompress } from "./flz.js";

/**
 * Viem's factory wrapper bytecode (RETURN-mode), lowercased to match normalized request hex.
 *
 * Constructor arg shape: `(address target, bytes targetData, address factory, bytes factoryData)`.
 *
 * Behavior:
 *   1. CALL(factory, factoryData). If the call fails OR if `target` has no code afterward,
 *      REVERT with `CounterfactualDeployFailed(bytes)` (selector 0x101bb98d).
 *   2. CALL(target, targetData).
 *   3. On lens success: RETURN returndata.
 *   4. On lens revert:  REVERT with returndata verbatim.
 */
const FACTORY_BYTECODE_RETURN = deploylessCallViaFactoryBytecode.toLowerCase() as Hex;

/**
 * Custom factory wrapper bytecode (RETURN-mode, FLZ input compression).
 *
 * Same constructor-arg shape, but `targetData` must be FLZ-compressed by the caller
 * (via {@link flzCompress} in ./flz.ts).
 *
 * Source: ./ReturnEnvelopeCompressed.yul. Regenerate with `pnpm build:ReturnEnvelopeCompressed`.
 *
 * Behavior (additions over {@link FACTORY_BYTECODE_RETURN}):
 *   2b. flzDecompress(compressedTargetData) → targetData, then CALL(target, targetData).
 *   3b. On success: RETURN returndata (no output compression, no sentinel needed).
 */
const FACTORY_BYTECODE_RETURN_COMPRESSED: Hex =
  "0x6200011e803803905f395f51602051905f806040516060519082602083519301915af115813b15176101065781515992839160208083019184930101905b81811061006657838084035f808284828b5af1913d9261005f5782805f803e5ffd5b01815f823ef35b8091935051805f1a918260051c9182156001146100eb5760078314928160011a600701811884021893611f008560020192856001011a9160081b160160018101908603906020811860208211021890815f5b8281015f190151818a0152018381106100e457505050506002929183910101920101915b84929161003d565b82906100b8565b508260019392508184600293015186520101920101916100dc565b63101bb98d60e01b5f5260206004525f60245260445ffd";

/**
 * Custom factory wrapper bytecode (REVERT-mode, no compression).
 *
 * Identical constructor-arg shape to viem's wrapper — `(address target, bytes targetData, address factory,
 * bytes factoryData)` — so the same envelope codec drives both modes. The only difference: this wrapper
 * exfiltrates the lens's returndata via REVERT (prepended with {@link OK_SENTINEL}), instead of RETURN.
 * REVERT data is not subject to EIP-170, so lens results may exceed 24 KB.
 *
 * Source: ./RevertEnvelope.yul. Regenerate with `pnpm build:RevertEnvelope` and paste the output here.
 *
 * Behavior:
 *   1. CALL(factory, factoryData). If the call fails OR if `target` has no code afterward,
 *      REVERT with `CounterfactualDeployFailed(bytes)` (selector 0x101bb98d) — matches
 *      viem's deployless failure selector.
 *   2. CALL(target, targetData).
 *   3. On lens success: REVERT with OK_SENTINEL || returndata.
 *   4. On lens revert:  REVERT with returndata verbatim (no sentinel) — propagates the
 *      lens's revert to the caller as if the lens had been called directly.
 */
const FACTORY_BYTECODE_REVERT: Hex =
  "0x62000071803803905f395f51602051905f806040516060519082602083519301915af115813b15176059575f82819282602083519301915af13d90604457805f803e5ffd5b631580d19d60e01b5f52805f60043e6004015ffd5b63101bb98d60e01b5f5260206004525f60245260445ffd";

/**
 * Custom factory wrapper bytecode (REVERT-mode, FLZ input compression).
 *
 * Same constructor-arg shape, but `targetData` must be FLZ-compressed by the caller
 * (via {@link flzCompress} in ./flz.ts). The wrapper decompresses it before calling
 * the lens, then reverts with OK_SENTINEL || raw returndata.
 *
 * Source: ./RevertEnvelopeCompressed.yul. Regenerate with `pnpm build:RevertEnvelopeCompressed`.
 *
 * Behavior (additions over FACTORY_BYTECODE_REVERT):
 *   2b. flzDecompress(compressedTargetData) → targetData, then CALL(target, targetData).
 *   3b. On success: REVERT with OK_SENTINEL || returndata (no output compression).
 */
const FACTORY_BYTECODE_REVERT_COMPRESSED: Hex =
  "0x62000139803803905f395f51602051905f806040516060519082602083519301915af115813b1517610070575f808092602061004386519659978893849201610088565b9485925af1913d926100575782805f803e5ffd5b0190631580d19d60e01b8252805f600484013e60040190fd5b63101bb98d60e01b5f5260206004525f60245260445ffd5b9082908201915b82811061009d575090500390565b8051805f1a918260051c91821560011461011e5760078314928160011a600701811884021893611f008560020192856001011a9160081b160160018101908603906020811860208211021890815f5b8281015f190151818a01520183811061011757505050506002929183910101920101915b919061008f565b82906100ec565b5082600193925081846002930151865201019201019161011056";

/**
 * 4-byte magic prefix on revert data that means "this revert is the lens's success payload,
 * not a real revert". Equal to `bytes4(keccak256("ViemDlcOk()"))`.
 */
export const OK_SENTINEL: Hex = "0x1580d19d";

const DEPLOYLESS_CONSTRUCTOR_PARAMS = parseAbiParameters("address, bytes, address, bytes");

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
 * Exfiltration mode for the outer wrapper. `'revert'` lifts the EIP-170 24KB returndata cap,
 * but may be truncated by some RPC providers.
 */
export type DeploylessExfilMode = "return" | "revert";

/**
 * Reverses {@link wrapDeploylessFactoryCall}. Accepts data produced by either mode's wrapper
 * (RETURN or REVERT). viem's stock `client.call({ factory, factoryData })` produces the
 * RETURN form.
 */
export function unwrapDeploylessFactoryCall(data: Hex): DeploylessFactoryCall {
  const lower = data.toLowerCase();
  let argsStart: number;
  if (lower.startsWith(FACTORY_BYTECODE_RETURN)) {
    argsStart = FACTORY_BYTECODE_RETURN.length;
  } else if (lower.startsWith(FACTORY_BYTECODE_RETURN_COMPRESSED)) {
    argsStart = FACTORY_BYTECODE_RETURN_COMPRESSED.length;
  } else if (lower.startsWith(FACTORY_BYTECODE_REVERT)) {
    argsStart = FACTORY_BYTECODE_REVERT.length;
  } else if (lower.startsWith(FACTORY_BYTECODE_REVERT_COMPRESSED)) {
    argsStart = FACTORY_BYTECODE_REVERT_COMPRESSED.length;
  } else {
    throw new Error("eth_call data is not a deployless factory wrapper");
  }
  const argsHex = `0x${data.slice(argsStart)}` as Hex;
  const [address, targetData, factory, factoryData] = decodeAbiParameters(DEPLOYLESS_CONSTRUCTOR_PARAMS, argsHex);
  return { target: { address, factory, factoryData }, targetData };
}

/** Builds a deployless factory `eth_call` payload from its constituent parts. */
export function wrapDeploylessFactoryCall(
  { target, targetData }: DeploylessFactoryCall,
  { exfil, compress }: { exfil: DeploylessExfilMode; compress: boolean },
) {
  let prefix: Hex;
  let encodedTargetData: Hex;
  if (compress) {
    encodedTargetData = flzCompress(targetData);
    prefix = exfil === "revert" ? FACTORY_BYTECODE_REVERT_COMPRESSED : FACTORY_BYTECODE_RETURN_COMPRESSED;
  } else {
    encodedTargetData = targetData;
    prefix = exfil === "revert" ? FACTORY_BYTECODE_REVERT : FACTORY_BYTECODE_RETURN;
  }
  const args = encodeAbiParameters(DEPLOYLESS_CONSTRUCTOR_PARAMS, [
    target.address,
    encodedTargetData,
    target.factory,
    target.factoryData,
  ]);
  return `${prefix}${args.slice(2)}` as Hex;
}

/**
 * Pulls the revert-data hex out of an error thrown by a viem `requestFn` and checks for
 * {@link OK_SENTINEL}. Tolerates both `error.data: Hex` (most providers) and
 * `error.data: { data: Hex }` (some providers nest). Walks `BaseError` chains so
 * wrapped-transport setups still surface the data field.
 *
 * - `{ ok: true, returnData }` — sentinel present; `returnData` is the lens's payload.
 * - `{ ok: false }` — no revert data, or data does not begin with {@link OK_SENTINEL}.
 *   The caller should rethrow the original error.
 */
export function extractRevertData(e: unknown): { ok: true; returnData: Hex } | { ok: false } {
  // biome-ignore lint/complexity/noBannedTypes: `data` is legitimately any truthy type
  const hasData = (x: unknown): x is { data: {} } => {
    return !!x && typeof x === "object" && "data" in x && !!x.data;
  };

  const err = e instanceof BaseError ? (e.walk(hasData) ?? e.walk()) : e;
  if (!hasData(err)) return { ok: false };

  const raw =
    typeof err.data === "string"
      ? err.data
      : hasData(err.data) && typeof err.data.data === "string"
        ? err.data.data
        : undefined;
  if (!raw) return { ok: false };

  if (raw.slice(0, 10).toLowerCase() !== OK_SENTINEL) return { ok: false };
  return { ok: true, returnData: `0x${raw.slice(10)}` as Hex };
}
