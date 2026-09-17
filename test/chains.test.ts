import { defineChain } from "viem";
import { describe, expect, it } from "vitest";

import { chainConfig, chainFacts, EIP_3860_INITCODE_SIZE, ethereumFacts, monadFacts } from "../src/chains/index.js";

import { ethereumChain, factlessChain } from "./helpers/chains.js";

describe("chain facts", () => {
  it("reads what a chain carries, and nothing from a chain carrying none", () => {
    expect(chainFacts(ethereumChain())).toEqual(ethereumFacts);
    expect(chainFacts(factlessChain())).toBeUndefined();
    expect(chainFacts(undefined)).toBeUndefined();
  });

  it("survives the round trip viem's own chain machinery makes", () => {
    // `extendSchema` is a type-level marker that returns `{}`, so only `.extend` actually attaches.
    const chain = defineChain({ ...ethereumChain(), ...chainConfig }).extend({ viemDlc: monadFacts });

    expect(chainFacts(chain)).toEqual(monadFacts);
  });

  it("states Ethereum's rules and Monad's raised initcode limit", () => {
    expect(ethereumFacts.maxInitcodeSize).toBe(EIP_3860_INITCODE_SIZE);
    expect(ethereumFacts.ethCall).toEqual({ gasWhenUnspecified: "providerCap", gasAboveCap: "clamped" });
    expect(monadFacts.maxInitcodeSize).toBeGreaterThan(EIP_3860_INITCODE_SIZE);
    expect(monadFacts.ethCall).toEqual({ gasWhenUnspecified: "fixedDefault", gasAboveCap: "rejected" });
  });

  it.each([
    ["not an object", "nope"],
    ["no `ethCall`", { maxInitcodeSize: 1 }],
    ["`ethCall.gasWhenUnspecified`", { ethCall: { gasAboveCap: "clamped" }, maxInitcodeSize: 1 }],
    ["`ethCall.gasAboveCap`", { ethCall: { gasWhenUnspecified: "providerCap" }, maxInitcodeSize: 1 }],
    ["`maxInitcodeSize`", { ethCall: ethereumFacts.ethCall, maxInitcodeSize: 0 }],
  ])("refuses a malformed entry rather than reading it as absent: %s", (why, carried) => {
    // An opt-in done wrong is a typo to surface, where an opt-out is a choice to respect.
    const chain = { ...factlessChain(), viemDlc: carried };

    expect(() => chainFacts(chain)).toThrow(why);
  });

  it("names the chain it could not read", () => {
    expect(() => chainFacts({ ...factlessChain(7), viemDlc: 1 })).toThrow(/chain 7/);
  });
});
