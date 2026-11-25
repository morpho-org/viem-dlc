/**
 * The body of a struct class. A member that is only a type, which a compiler erases, is no member: the
 * class means the same read from its TypeScript and from the JavaScript made of it. A member that is there
 * when the module runs is refused.
 */
import { describe, it } from "vitest";

import { describePairs } from "./helpers/core.js";
import { expectRejection, positionIn } from "./helpers/harness.js";

const lens = (members: string) => `import { contract, pure, Struct, uint256 } from "@morpho-org/viem-dlc/solidity";
class Item extends Struct({ count: uint256 }) {${members}}
export const Lens = contract("Lens", { run: pure(Item).returns(uint256) }).implementation({
  run(item) { return item.count; },
});
`;

const solidity = `
  struct Item {
      uint256 count;
  }

  contract Lens {
      function run(Item calldata item) external pure returns (uint256) {
          return item.count;
      }
  }
`;
const calls = [{ functionName: "run", args: [{ count: 7n }] }];

describePairs("a struct class whose members are only types", {
  "a field declared again for its type": { ts: lens(" declare count: bigint; "), solidity, calls },
  "a field declared again and an index signature": {
    ts: lens("\n  declare readonly count: bigint;\n  [key: string]: unknown;\n"),
    solidity,
    calls,
  },
  "the same class as a compiler writes it": { ts: lens("\n"), solidity, calls },
});

describe("a struct class with a member that is there when the module runs", () => {
  const cases: readonly (readonly [name: string, members: string])[] = [
    ["a field with a value", " extra = 1n; "],
    ["a field with no value, which a compiler may keep", " count: bigint; "],
    ["a field declared for its type beside one that is kept", " declare count: bigint; extra: bigint; "],
    ["a method", " double() { return this.count * 2n; } "],
    ["a static block", " static {} "],
  ];
  for (const [name, members] of cases)
    it(`refuses ${name}`, async () => {
      const code = lens(members);
      await expectRejection(code, {
        message: "a struct class has no members",
        ...positionIn(code, `{${members}}`),
        source: "transform",
      });
    });
});
