import type { Chain } from "viem";
import { extendSchema } from "viem";

/**
 * How a chain's nodes run an `eth_call`, as far as gas goes: the frame a request that leaves `gas`
 * unspecified is given, and what happens to a `gas` above the provider's cap.
 *
 * geth gives an unspecified request the whole cap and clamps a higher `gas` to it, so nothing needs
 * sending and a page's `gas_limit_observed` reads the cap. Monad gives it a fixed default (8.1M,
 * promoted to a larger pool only when the call runs out of gas — which a paging envelope never does)
 * and rejects a `gas` above the cap, so the transports send their stated `gasLimit` as `gas` there.
 * Probed 2026-09-09.
 */
export type EthCallGas = {
  /** The frame an `eth_call` that leaves `gas` unspecified runs in. */
  gasWhenUnspecified: "providerCap" | "fixedDefault";
  /** What the node does with a `gas` above the provider's cap. */
  gasAboveCap: "clamped" | "rejected";
};

/**
 * EIP-3860's initcode limit, which a chain keeps unless it names its own. An initcode-delivered
 * chunk's bytes are the initcode, so this is what bounds one.
 */
export const EIP_3860_INITCODE_SIZE = 49_152;

/**
 * What this package needs to know about a chain beyond what viem's definition carries. Nothing is
 * assumed: a chain states every fact, and a fact that is missing when it is needed is an error.
 */
export type ChainFacts = {
  ethCall: EthCallGas;
  /** The largest initcode the chain's nodes accept ({@link EIP_3860_INITCODE_SIZE} unless raised). */
  maxInitcodeSize: number;
};

/** The chain field this package's facts are carried on. */
export const FACTS_KEY = "viemDlc" as const;

/** The schema a chain declares so it can carry {@link ChainFacts}. */
export type ViemDlcSchema = { [FACTS_KEY]: ChainFacts };

/**
 * Spread into a chain definition so the chain can carry its {@link ChainFacts}:
 *
 * ```ts
 * const chain = defineChain({ ...monad, ...chainConfig }).extend({ viemDlc: monadFacts });
 * ```
 */
export const chainConfig = { extendSchema: extendSchema<ViemDlcSchema>() };

/** Ethereum's rules: geth's `eth_call` frame, and EIP-3860's initcode limit. */
export const ethereumFacts: ChainFacts = {
  ethCall: { gasWhenUnspecified: "providerCap", gasAboveCap: "clamped" },
  maxInitcodeSize: EIP_3860_INITCODE_SIZE,
};

/** Monad's rules, probed 2026-09-09: a fixed default frame, a rejected `gas` above the cap, 256 KiB of initcode. */
export const monadFacts: ChainFacts = {
  ethCall: { gasWhenUnspecified: "fixedDefault", gasAboveCap: "rejected" },
  maxInitcodeSize: 262_144,
};

const GAS_WHEN_UNSPECIFIED = ["providerCap", "fixedDefault"];
const GAS_ABOVE_CAP = ["clamped", "rejected"];

/**
 * The facts `chain` carries, or `undefined` for a chain that carries none. A `viemDlc` entry that
 * isn't a complete {@link ChainFacts} throws rather than reading as absent, so a typo surfaces.
 *
 * viem's `Chain` is a closed type, so the field is reached by an `in` narrowing and checked here.
 */
export function chainFacts(chain: Chain | undefined): ChainFacts | undefined {
  if (chain === undefined || !(FACTS_KEY in chain)) return undefined;
  const carried: unknown = chain[FACTS_KEY];
  if (carried === undefined) return undefined;

  const bad = (why: string) => new Error(`[viem-dlc] ${describe(chain)} carries a malformed \`${FACTS_KEY}\`: ${why}`);
  if (typeof carried !== "object" || carried === null) throw bad("not an object");
  const { ethCall, maxInitcodeSize } = carried as Partial<ChainFacts>;
  if (typeof ethCall !== "object" || ethCall === null) throw bad("no `ethCall`");
  if (!GAS_WHEN_UNSPECIFIED.includes(ethCall.gasWhenUnspecified)) {
    throw bad(`\`ethCall.gasWhenUnspecified\` is not one of ${GAS_WHEN_UNSPECIFIED.join(", ")}`);
  }
  if (!GAS_ABOVE_CAP.includes(ethCall.gasAboveCap)) {
    throw bad(`\`ethCall.gasAboveCap\` is not one of ${GAS_ABOVE_CAP.join(", ")}`);
  }
  if (!Number.isSafeInteger(maxInitcodeSize) || (maxInitcodeSize as number) <= 0) {
    throw bad("`maxInitcodeSize` is not a positive integer");
  }
  return { ethCall, maxInitcodeSize: maxInitcodeSize as number };
}

/** The error a chain carrying no facts raises, at the point one of them is needed. */
export function missingChainFacts(chain: Chain | undefined, needed: string): Error {
  return new Error(
    `[viem-dlc] ${describe(chain)} carries no \`${FACTS_KEY}\` facts, and ${needed}. Attach them to the ` +
      "chain: `defineChain({ ...chain, ...chainConfig }).extend({ viemDlc: ethereumFacts })`, from " +
      "`@morpho-org/viem-dlc/chains`.",
  );
}

function describe(chain: Chain | undefined): string {
  if (chain === undefined) return "the client's chain is unset, which";
  return `chain ${chain.id}${chain.name ? ` (${chain.name})` : ""}`;
}
