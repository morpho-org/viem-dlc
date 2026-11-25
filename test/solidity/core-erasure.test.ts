/**
 * TypeScript-only syntax is erased before anything is printed: annotations, generics, `as`,
 * `satisfies`, `!`, a `this` parameter, and types declared inside a body. So a module prints the same
 * Solidity from its `.ts` source and from the `.js` a compiler made of it.
 */
import { describe, expect, it } from "vitest";

import { describePairs, type ExecutedPair, IMPORTS } from "./helpers/core.js";
import { buildSnippet } from "./helpers/harness.js";

const typescript = `${IMPORTS}
import type { Abi } from "viem";
import { type Hex } from "viem";

type Amount = bigint;
interface Bounds { low: Amount; high: Amount }
declare const ambient: Amount;
export type { Amount, Bounds };

class Pair extends Struct({ low: uint128, high: uint128 }) {}

const SCALE = 1_000n as const;

const widen = internal(pure(uint128).returns(uint256), (x: Amount): Amount => uint256(x as Amount) satisfies Amount);
const element = internal(
  pure([uint128], uint256).returns(uint128),
  function (this: void, xs: readonly Amount[], i: Amount): Amount {
    type Index = number;
    interface Picked { index: Index; value: Amount }
    const index: Amount = i as Amount;
    const picked = xs[Number(index) as Index]!;
    return <Amount>picked;
  },
);

export const ErasureLens = contract("ErasureLens", {
  spread: pure(Pair).returns(uint256),
  pick: pure([uint128], uint256).returns(uint128),
  largest: pure([uint128]).returns(uint128),
}).implementation({
  spread(pair: Pair): Amount {
    const low: Amount = pair.low!;
    let high = (pair.high as Amount)!;
    const ordered = (low <= high) as boolean;
    if (!ordered!) high = low as Amount;
    return (widen<Amount>(high)! - widen(low as Amount)) * (SCALE satisfies Amount);
  },
  pick: (xs, i) => element(xs, i)!,
  largest(xs: readonly Amount[]) {
    let best = xs[0]!;
    for (let i: number = 1; i < xs.length; i++) if (xs[i]! > best!) best = xs[i]!;
    return best satisfies Amount as Amount;
  },
});

export const abi: Abi | undefined = undefined;
export const empty: Hex = "0x";
`;

// The same module as a compiler leaves it: every type gone, and nothing else changed.
const javascript = `${IMPORTS}

class Pair extends Struct({ low: uint128, high: uint128 }) {}

const SCALE = 1_000n;

const widen = internal(pure(uint128).returns(uint256), (x) => uint256(x));
const element = internal(
  pure([uint128], uint256).returns(uint128),
  function (xs, i) {
    const index = i;
    const picked = xs[Number(index)];
    return picked;
  },
);

export const ErasureLens = contract("ErasureLens", {
  spread: pure(Pair).returns(uint256),
  pick: pure([uint128], uint256).returns(uint128),
  largest: pure([uint128]).returns(uint128),
}).implementation({
  spread(pair) {
    const low = pair.low;
    let high = pair.high;
    const ordered = low <= high;
    if (!ordered) high = low;
    return (widen(high) - widen(low)) * SCALE;
  },
  pick: (xs, i) => element(xs, i),
  largest(xs) {
    let best = xs[0];
    for (let i = 1; i < xs.length; i++) if (xs[i] > best) best = xs[i];
    return best;
  },
});

export const abi = undefined;
export const empty = "0x";
`;

const pairs: Record<string, ExecutedPair> = {
  "a module full of types": {
    ts: typescript,
    solidity: `
      struct Pair {
          uint128 low;
          uint128 high;
      }

      contract ErasureLens {
          function spread(Pair calldata pair) external pure returns (uint256) {
              uint128 low = pair.low;
              uint128 high = pair.high;
              bool ordered = low <= high;
              if (!ordered) high = low;
              return (widen(high) - widen(low)) * 1000;
          }

          function pick(uint128[] calldata xs, uint256 i) external pure returns (uint128) {
              return element(xs, i);
          }

          function largest(uint128[] calldata xs) external pure returns (uint128) {
              uint128 best = xs[0];
              for (uint256 i = 1; i < xs.length; i++) if (xs[i] > best) best = xs[i];
              return best;
          }

          function widen(uint128 x) internal pure returns (uint256) {
              return uint256(x);
          }

          function element(uint128[] memory xs, uint256 i) internal pure returns (uint128) {
              uint256 index = i;
              uint128 picked = xs[index];
              return picked;
          }
      }
    `,
    calls: [
      { functionName: "spread", args: [{ low: 3n, high: 10n }] },
      { functionName: "spread", args: [{ low: 10n, high: 3n }] },
      { functionName: "pick", args: [[4n, 5n, 6n], 2n] },
      { functionName: "pick", args: [[4n, 5n, 6n], 3n], reverts: true },
      { functionName: "largest", args: [[4n, 9n, 6n]] },
      { functionName: "largest", args: [[]], reverts: true },
    ],
  },
};

describePairs("erasure of TypeScript-only syntax", pairs);

describe("erasure of TypeScript-only syntax: what is printed", () => {
  it("is the same from the TypeScript and from the JavaScript a compiler makes of it", async () => {
    const fromTypescript = await buildSnippet({ "lens.ts": typescript });
    const fromJavascript = await buildSnippet({ "lens.js": javascript });
    expect(fromTypescript.lenses[0]?.printed.solidity).toBe(fromJavascript.lenses[0]?.printed.solidity);
    expect(fromTypescript.lenses[0]?.artifact.bytecode).toBe(fromJavascript.lenses[0]?.artifact.bytecode);
  });

  it("has none of the types, and no trace of !, as or satisfies", async () => {
    const built = await buildSnippet(typescript);
    const solidity = built.lenses[0]?.printed.solidity ?? "";
    expect(solidity).toContain("uint128 picked = xs[index];");
    expect(solidity).toContain("return (widen(high) - widen(low)) * (1000);");
    expect(solidity).not.toMatch(/Amount|Index|Bounds|Picked|ambient|satisfies|\bas\b|readonly|this/);
    expect(solidity).not.toContain("]!");
  });
});
