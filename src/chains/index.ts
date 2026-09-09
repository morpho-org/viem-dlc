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
 * What this package knows about a chain beyond what viem's own definition carries. Internal for now:
 * the record's shape is expected to change as more facts are added, so it is not exported.
 */
export type ChainDefinition = {
  id: number;
  name: string;
  ethCall: EthCallGas;
};

const gethEthCall: EthCallGas = { gasWhenUnspecified: "providerCap", gasAboveCap: "clamped" };

export const mainnet: ChainDefinition = { id: 1, name: "Ethereum", ethCall: gethEthCall };
export const base: ChainDefinition = { id: 8453, name: "Base", ethCall: gethEthCall };
export const arbitrum: ChainDefinition = { id: 42_161, name: "Arbitrum One", ethCall: gethEthCall };
export const robinhood: ChainDefinition = { id: 4663, name: "Robinhood Chain", ethCall: gethEthCall };
export const monad: ChainDefinition = {
  id: 143,
  name: "Monad",
  ethCall: { gasWhenUnspecified: "fixedDefault", gasAboveCap: "rejected" },
};

export const chains: readonly ChainDefinition[] = [mainnet, base, arbitrum, robinhood, monad];

/**
 * The definition for `chainId`, or geth's behaviour for a chain this package has no entry for and
 * for a transport whose client has no chain.
 */
export function chainDefinition(chainId: number | undefined): ChainDefinition {
  return chains.find((c) => c.id === chainId) ?? { id: chainId ?? 0, name: "unknown", ethCall: gethEthCall };
}
