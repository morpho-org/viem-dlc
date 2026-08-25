import type { Address, Hex } from "viem";
import { decodeAbiParameters, deploylessCallViaFactoryBytecode, encodeAbiParameters, parseAbiParameters } from "viem";

import { causeChain } from "../errors.js";

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
 *   5. On lens OOG:     REVERT with {@link OOG_SENTINEL}.
 */
export const FACTORY_BYTECODE_REVERT: Hex =
  "0x620000a0803803905f395f516020519060405160605190823b15606c575b50505f80918351908260205a9601915af1903d9160565760061c5a1115811516604757805f803e5ffd5b633302f4d360e21b5f5260045ffd5b50631580d19d60e01b5f52805f60043e6004015ffd5b5f91829182602083519301915af115813b15176088575f80601d565b63101bb98d60e01b5f5260206004525f60245260445ffd";

/**
 * Custom factory wrapper bytecode (REVERT-mode, FLZ input compression).
 *
 * Same constructor-arg shape, but `targetData` must be FLZ-compressed by the caller
 * (via {@link flzCompress} in ./flz.ts). The wrapper decompresses it before calling
 * the lens, then reverts with OK_SENTINEL || raw returndata.
 *
 * Source: ./RevertEnvelopeCompressed.yul. Regenerate with `pnpm build:RevertEnvelopeCompressed`.
 *
 * Behavior (additions over {@link FACTORY_BYTECODE_REVERT}):
 *   2b. flzDecompress(compressedTargetData) → targetData, then CALL(target, targetData).
 *   3b. On success: REVERT with OK_SENTINEL || returndata (no output compression).
 */
export const FACTORY_BYTECODE_REVERT_COMPRESSED: Hex =
  "0x6200016b803803905f395f516020519060405160605190823b15610084575b5050610032826020935159948592016100ba565b5f808285825a965af1923d9361006757505060061c5a111581151661005857805f803e5ffd5b633302f4d360e21b5f5260045ffd5b631580d19d60e01b9101908152919050805f600484013e60040190fd5b5f91829182602083519301915af115813b15176100a2575f8061001e565b63101bb98d60e01b5f5260206004525f60245260445ffd5b9082908201915b8281106100cf575090500390565b8051805f1a918260051c9182156001146101505760078314928160011a600701811884021893611f008560020192856001011a9160081b160160018101908603906020811860208211021890815f5b8281015f190151818a01520183811061014957505050506002929183910101920101915b91906100c1565b829061011e565b5082600193925081846002930151865201019201019161014256";

/** Every wrapper we emit. {@link FACTORY_BYTECODE_RETURN_VIEM} is inbound-only. */
const OWN_FACTORY_BYTECODES = [FACTORY_BYTECODE_REVERT, FACTORY_BYTECODE_REVERT_COMPRESSED] as const;

/**
 * 4-byte magic prefix on revert data that means "this revert is the lens's success payload,
 * not a real revert". Equal to `bytes4(keccak256("ViemDlcOk()"))`.
 */
export const OK_SENTINEL: Hex = "0x1580d19d";

/**
 * 4-byte revert data meaning "the lens frame ran out of gas". Equal to
 * `bytes4(keccak256("ViemDlcOutOfGas()"))`.
 *
 * A lens frame that exhausts its gas cannot report it: EIP-150 gave the callee 63/64 and kept the
 * wrapper the remainder, so the wrapper survives but sees only an empty-data failure —
 * indistinguishable from a bare `revert()`, and no reason for a batcher to split the batch. Every
 * wrapper in {@link OWN_FACTORY_BYTECODES} therefore checks whether the frame was drained to that
 * 1/64 remainder and substitutes this marker. Detect it with {@link isOutOfGasRevert}.
 */
export const OOG_SENTINEL: Hex = "0xcc0bd34c";

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
 * Reverses {@link wrapDeploylessFactoryCall}, and also accepts the RETURN-mode form that viem's
 * stock `client.call({ factory, factoryData })` produces — see {@link FACTORY_BYTECODE_RETURN_VIEM}.
 */
export function unwrapDeploylessFactoryCall(data: Hex): DeploylessFactoryCall {
  const lower = data.toLowerCase();
  const prefix = [FACTORY_BYTECODE_RETURN_VIEM, ...OWN_FACTORY_BYTECODES].find((p) => lower.startsWith(p));
  if (!prefix) throw new Error("eth_call data is not a deployless factory wrapper");
  const argsStart = prefix.length;
  const argsHex = `0x${data.slice(argsStart)}` as Hex;
  const [address, targetData, factory, factoryData] = decodeAbiParameters(DEPLOYLESS_CONSTRUCTOR_PARAMS, argsHex);
  return { target: { address, factory, factoryData }, targetData };
}

/** Builds a deployless factory `eth_call` payload from its constituent parts. */
export function wrapDeploylessFactoryCall(
  { target, targetData }: DeploylessFactoryCall,
  { compress }: { compress: boolean },
) {
  const prefix = compress ? FACTORY_BYTECODE_REVERT_COMPRESSED : FACTORY_BYTECODE_REVERT;
  const encodedTargetData = compress ? flzCompress(targetData) : targetData;
  const args = encodeAbiParameters(DEPLOYLESS_CONSTRUCTOR_PARAMS, [
    target.address,
    encodedTargetData,
    target.factory,
    target.factoryData,
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
 * True when `e` carries {@link OOG_SENTINEL} as its revert data — the wrapper reporting that the
 * lens frame ran out of gas. Uses the same `cause`-chain walk as {@link extractRevertData}.
 *
 * The match is exact: the wrapper reverts with the bare 4-byte selector, so a lens error that
 * merely happens to start with those bytes is not mistaken for an out-of-gas.
 */
export function isOutOfGasRevert(e: unknown): boolean {
  for (const raw of revertDataCandidates(e)) {
    if (raw.toLowerCase() === OOG_SENTINEL) return true;
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
