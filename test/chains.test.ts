import { describe, expect, it } from "vitest";

import { chainDefinition, chains, EIP_3860_INITCODE_SIZE, mainnet, monad } from "../src/chains/index.js";

describe("chains", () => {
  it("looks a definition up by id and defaults an unknown chain to geth's behaviour", () => {
    expect(chainDefinition(143)).toBe(monad);
    expect(chainDefinition(1)).toBe(mainnet);
    expect(chainDefinition(999_999)).toBeUndefined();
    expect(chainDefinition(undefined)).toBeUndefined();
  });

  it("has one entry per id", () => {
    expect(new Set(chains.map((c) => c.id)).size).toBe(chains.length);
  });

  it("carries an initcode limit for every chain, EIP-3860's unless the chain raised it", () => {
    expect(mainnet.maxInitcodeSize).toBe(EIP_3860_INITCODE_SIZE);
    expect(monad.maxInitcodeSize).toBeGreaterThan(EIP_3860_INITCODE_SIZE);
    for (const chain of chains) expect(chain.maxInitcodeSize).toBeGreaterThanOrEqual(EIP_3860_INITCODE_SIZE);
  });
});
