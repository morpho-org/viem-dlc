import { describe, expect, it } from "vitest";

import { chainDefinition, chains, mainnet, monad } from "../src/chains/index.js";

describe("chains", () => {
  it("looks a definition up by id and defaults an unknown chain to geth's behaviour", () => {
    expect(chainDefinition(143)).toBe(monad);
    expect(chainDefinition(1)).toBe(mainnet);
    expect(chainDefinition(999_999).ethCall).toEqual(mainnet.ethCall);
    expect(chainDefinition(undefined).ethCall).toEqual(mainnet.ethCall);
  });

  it("has one entry per id", () => {
    expect(new Set(chains.map((c) => c.id)).size).toBe(chains.length);
  });
});
