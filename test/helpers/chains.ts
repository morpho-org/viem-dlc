import { type Chain, defineChain } from "viem";

import { type ChainFacts, chainConfig, ethereumFacts, monadFacts } from "../../src/chains/index.js";

/** A chain shaped like viem's, carrying `facts` when given and nothing when not. */
function chainCarrying(facts: ChainFacts | undefined, id = 1): Chain {
  const base = defineChain({
    id,
    name: `Test chain ${id}`,
    nativeCurrency: { name: "Ether", symbol: "ETH", decimals: 18 },
    rpcUrls: { default: { http: ["http://localhost"] } },
    ...chainConfig,
  });
  return facts === undefined ? base : base.extend({ viemDlc: facts });
}

export const ethereumChain = (id = 1) => chainCarrying(ethereumFacts, id);
export const monadChain = (id = 143) => chainCarrying(monadFacts, id);
export const factlessChain = (id = 999_999) => chainCarrying(undefined, id);
