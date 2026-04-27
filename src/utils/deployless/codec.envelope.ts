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
 *      REVERT with `DeploymentFailed(target)` (selector 0x9deffc1b).
 *   2. CALL(target, targetData).
 *   3. On lens success: RETURN returndata.
 *   4. On lens revert:  REVERT with returndata verbatim.
 */
const FACTORY_BYTECODE_RETURN = deploylessCallViaFactoryBytecode.toLowerCase() as Hex;

/**
 * Custom factory wrapper bytecode (RETURN-mode, FLZ wire compression).
 *
 * Same constructor-arg shape, but `targetData` must be FLZ-compressed by the caller
 * (via {@link flzCompress} in ./flz.ts).
 *
 * Source: ./ReturnEnvelopeCompressed.yul. Regenerate with `pnpm build:ReturnEnvelopeCompressed`.
 *
 * Behavior (additions over {@link FACTORY_BYTECODE_RETURN}):
 *   2b. flzDecompress(compressedTargetData) → targetData, then CALL(target, targetData).
 *   3b. On success: RETURN flzCompress(returndata). No sentinel needed.
 */
const FACTORY_BYTECODE_RETURN_COMPRESSED: Hex =
  "0x620003cd803803905f395f516020515f806040516060519082602083519301915af115823b1517610078575f80809360206100428551955996879384920161008b565b9586925af1913d926100565782805f803e5ffd5b6100759101825f823e8281016180005f8237618000810193849261013c565b90f35b50639deffc1b60e01b5f5260045260245ffd5b9082908201915b8281106100a0575090500390565b8051805f1a918260051c9182156001146101215760078314928160011a600701811884021893611f008560020192856001011a9160081b160160018101908603906020811860208211021890815f5b8281015f190151818a01520183811061011a57505050506002929183910101920101915b9190610092565b82906100ef565b50826001939250818460029301518652010192010191610113565b839290918201908260028101600c19840160031985015b818310610171575b5050505050908061016d93920361030b565b0390565b92975f9991959692979499505b610187886102b1565b97639e3779b9890260131c611fff166101b08c8260021b81015160e01c8d01928d8503916102c8565b80820391888110156102a05760010199611fff8311156101d3575b50505061017e565b6101dc826102b1565b146101e757806101cb565b90929a959199949796935b848a101561029757600193928961022993838d945f19860111610278575b506003600261022294950191016102de565b9283610355565b970161026f84820361025d5f1982016102576102475f1987016102b1565b639e3779b90260131c611fff1690565b896102c8565b610269610247846102b1565b876102c8565b01908195610153565b600261028f610222956003935f19818a030161030b565b945050610210565b9750509661015b565b995090929a959199949796936101f2565b51805f1a8160011a60081b179060021a60101b1790565b9060021b0190815160e01c1860e01b8151189052565b815f9493035b8085106102f057505050565b60019085849694015184840151185f1a1502920193916102e4565b91905b6020831015610336578215610330575f19830182535160018201520160010190565b50919050565b90602181601f602093538351600182015201910191601f19019161030e565b91905f19018060081c928360e0019160ff16915b6101078210156103ab57600782101560011461039357509260029360051b01825360018201530190565b60039450835360061901600183015360028201530190565b909280826003925360fd6001820153836002820153019261010519019061036956";

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
 *      REVERT with `DeploymentFailed(target)` (selector 0x9deffc1b) — matches viem's
 *      existing custom error.
 *   2. CALL(target, targetData).
 *   3. On lens success: REVERT with OK_SENTINEL || returndata.
 *   4. On lens revert:  REVERT with returndata verbatim (no sentinel) — propagates the
 *      lens's revert to the caller as if the lens had been called directly.
 */
const FACTORY_BYTECODE_REVERT: Hex =
  "0x6200006b803803905f395f51602051905f806040516060519082602083519301915af115813b15176059575f82819282602083519301915af13d90604457805f803e5ffd5b631580d19d60e01b5f52805f60043e6004015ffd5b639deffc1b60e01b5f5260045260245ffd";

/**
 * Custom factory wrapper bytecode (REVERT-mode, FLZ wire compression).
 *
 * Same constructor-arg shape, but `targetData` must be FLZ-compressed by the caller
 * (via {@link flzCompress} in ./flz.ts). The wrapper decompresses it before calling
 * the lens, then FLZ-compresses the returndata before reverting with the sentinel.
 *
 * Source: ./RevertEnvelopeCompressed.yul. Regenerate with `pnpm build:RevertEnvelopeCompressed`.
 *
 * Behavior (additions over FACTORY_BYTECODE_REVERT):
 *   2b. flzDecompress(compressedTargetData) → targetData, then CALL(target, targetData).
 *   3b. On success: REVERT with OK_SENTINEL || flzCompress(returndata).
 */
const FACTORY_BYTECODE_REVERT_COMPRESSED: Hex =
  "0x620003f1803803905f395f51602051905f806040516060519082602083519301915af115813b151761009d575f8080926020610043865196599788938492016100af565b9485925af1913d926100575782805f803e5ffd5b01815f823e609d618003610080848401936180005f863784618000810196618004820192610160565b9260158553608061800182015360d1618002820153015360040190fd5b639deffc1b60e01b5f5260045260245ffd5b9082908201915b8281106100c4575090500390565b8051805f1a918260051c9182156001146101455760078314928160011a600701811884021893611f008560020192856001011a9160081b160160018101908603906020811860208211021890815f5b8281015f190151818a01520183811061013e57505050506002929183910101920101915b91906100b6565b8290610113565b50826001939250818460029301518652010192010191610137565b839290918201908260028101600c19840160031985015b818310610195575b5050505050908061019193920361032f565b0390565b92975f9991959692979499505b6101ab886102d5565b97639e3779b9890260131c611fff166101d48c8260021b81015160e01c8d01928d8503916102ec565b80820391888110156102c45760010199611fff8311156101f7575b5050506101a2565b610200826102d5565b1461020b57806101ef565b90929a959199949796935b848a10156102bb57600193928961024d93838d945f1986011161029c575b50600360026102469495019101610302565b9283610379565b97016102938482036102815f19820161027b61026b5f1987016102d5565b639e3779b90260131c611fff1690565b896102ec565b61028d61026b846102d5565b876102ec565b01908195610177565b60026102b3610246956003935f19818a030161032f565b945050610234565b9750509661017f565b995090929a95919994979693610216565b51805f1a8160011a60081b179060021a60101b1790565b9060021b0190815160e01c1860e01b8151189052565b815f9493035b80851061031457505050565b60019085849694015184840151185f1a150292019391610308565b91905b602083101561035a578215610354575f19830182535160018201520160010190565b50919050565b90602181601f602093538351600182015201910191601f190191610332565b91905f19018060081c928360e0019160ff16915b6101078210156103cf5760078210156001146103b757509260029360051b01825360018201530190565b60039450835360061901600183015360028201530190565b909280826003925360fd6001820153836002820153019261010519019061038d56";

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

  const err = e instanceof BaseError ? e.walk() : e;
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
