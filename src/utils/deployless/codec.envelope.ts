import type { Address, Hex } from "viem";
import {
  BaseError,
  decodeAbiParameters,
  deploylessCallViaFactoryBytecode,
  encodeAbiParameters,
  parseAbiParameters,
} from "viem";

/** Viem's factory wrapper bytecode (RETURN-mode), lowercased to match normalized request hex. */
const FACTORY_BYTECODE_RETURN = deploylessCallViaFactoryBytecode.toLowerCase() as Hex;

/**
 * Custom factory wrapper bytecode (REVERT-mode).
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
  } else if (lower.startsWith(FACTORY_BYTECODE_REVERT)) {
    argsStart = FACTORY_BYTECODE_REVERT.length;
  } else {
    throw new Error("eth_call data is not a deployless factory wrapper");
  }
  const argsHex = `0x${data.slice(argsStart)}` as Hex;
  const [address, targetData, factory, factoryData] = decodeAbiParameters(DEPLOYLESS_CONSTRUCTOR_PARAMS, argsHex);
  return { target: { address, factory, factoryData }, targetData };
}

/** Builds a deployless factory `eth_call` payload from its constituent parts. */
export function wrapDeploylessFactoryCall({ target, targetData }: DeploylessFactoryCall, exfil: DeploylessExfilMode) {
  const args = encodeAbiParameters(DEPLOYLESS_CONSTRUCTOR_PARAMS, [
    target.address,
    targetData,
    target.factory,
    target.factoryData,
  ]);
  const prefix = exfil === "return" ? FACTORY_BYTECODE_RETURN : FACTORY_BYTECODE_REVERT;
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
