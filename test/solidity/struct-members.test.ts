/**
 * The body of a struct class. A member that is only a type, which a compiler erases, is no member: the
 * class means the same read from its TypeScript and from the JavaScript made of it. A member that is there
 * when the module runs is refused.
 */
import { describe, expect, it } from "vitest";

import { describePairs } from "./helpers/core.js";
import { buildSnippet, expectRejection, positionIn } from "./helpers/harness.js";

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

// A struct class goes from the module with the lens once nothing else reads it. A decorator is code that
// runs when the class is declared, and would go with it.
describe("a struct class with a decorator", () => {
  const registered = (
    declaration: string,
  ) => `import { contract, pure, Struct, uint256 } from "@morpho-org/viem-dlc/solidity";
let registrations = 0;
function register(_value: unknown) { registrations++; }
${declaration} {}
export const observed = () => registrations;
export const Lens = contract("Lens", { run: pure(Item).returns(uint256) }).implementation({
  run(item) { return item.count; },
});
`;

  it.each([
    ["a decorator", "@register\nclass Item extends Struct({ count: uint256 })"],
    ["a decorator that is called", "@(() => register)()\nclass Item extends Struct({ count: uint256 })"],
    ["a decorator before its export", "@register\nexport class Item extends Struct({ count: uint256 })"],
    ["a decorator after its export", "export @register class Item extends Struct({ count: uint256 })"],
  ])("refuses %s, at the decorator", async (_what, declaration) => {
    const code = registered(declaration);
    await expectRejection(code, {
      message: "a struct class has no decorator",
      ...positionIn(code, declaration.slice(declaration.indexOf("@"))),
      source: "transform",
    });
  });

  it("builds the same class without one", async () => {
    const built = await buildSnippet(registered("class Item extends Struct({ count: uint256 })"));
    expect(built.lenses.map((lens) => lens.resolved.name)).toEqual(["Lens"]);
  });
});
