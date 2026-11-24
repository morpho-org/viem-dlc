/**
 * The type of a local: the type its initializer already has in the source. One function per row of
 * the rule, so each stays inside the EVM's stack.
 */
import { describe, expect, it } from "vitest";

import { requireAnvil } from "./helpers/anvil.js";
import { describePairs, type ExecutedPair, IMPORTS, MAX_UINT256, useEvm } from "./helpers/core.js";
import { compilePair, expectRejection, expectSameAst, expectSameExecution, positionIn } from "./helpers/harness.js";

const OWNER = "0x52908400098527886E0F7030069857D2E4169EE7";
const QUERY = { inner: { value: 7n, flags: [true, false] }, items: [10n, 20n], name: "a name", owner: OWNER };

const declarations = `
  const SCALE = 10n ** 6n;

  class Inner extends Struct({ value: uint64, flags: [bool] }) {}
  class Query extends Struct({ inner: Inner, items: [uint128], name: string, owner: address }) {}

  const half = internal(pure(uint128).returns(uint128), (x) => x / 2n);
`;
const structs = `
  struct Inner {
      uint64 value;
      bool[] flags;
  }

  struct Query {
      Inner inner;
      uint128[] items;
      string name;
      address owner;
  }
`;

const pairs: Record<string, ExecutedPair> = {
  "a parameter, a local, a field path, an index and a length: the declared type": {
    ts: `${IMPORTS}${declarations}
      export const DeclaredLens = contract("DeclaredLens", {
        values: pure(Query, uint128).returns(uint128, uint64, uint128, bool, uint256),
        references: pure(Query).returns(Inner, [uint128], string, address, [bool]),
      }).implementation({
        values(q, x) {
          const fromParameter = x;
          const fromLocal = fromParameter;
          const fromPath = q.inner.value;
          const fromIndex = q.items[0]!;
          const fromNestedIndex = q.inner.flags[1]!;
          const fromLength = q.items.length;
          return [fromLocal, fromPath, fromIndex, fromNestedIndex, fromLength];
        },
        // A reference type is copied with its type, and lives in memory.
        references(q) {
          const inner = q.inner;
          const items = q.items;
          const name = q.name;
          const whole = q;
          const flags = inner.flags;
          return [inner, items, name, whole.owner, flags];
        },
      });
    `,
    solidity: `${structs}
      contract DeclaredLens {
          function values(Query calldata q, uint128 x) external pure returns (uint128, uint64, uint128, bool, uint256) {
              uint128 fromParameter = x;
              uint128 fromLocal = fromParameter;
              uint64 fromPath = q.inner.value;
              uint128 fromIndex = q.items[0];
              bool fromNestedIndex = q.inner.flags[1];
              uint256 fromLength = q.items.length;
              return (fromLocal, fromPath, fromIndex, fromNestedIndex, fromLength);
          }

          function references(Query calldata q)
              external
              pure
              returns (Inner memory, uint128[] memory, string memory, address, bool[] memory)
          {
              Inner memory inner = q.inner;
              uint128[] memory items = q.items;
              string memory name = q.name;
              Query memory whole = q;
              bool[] memory flags = inner.flags;
              return (inner, items, name, whole.owner, flags);
          }
      }
    `,
    calls: [
      { functionName: "values", args: [QUERY, 5n] },
      { functionName: "values", args: [{ ...QUERY, items: [] }, 5n], reverts: true },
      { functionName: "references", args: [QUERY] },
    ],
  },
  "a call, a conversion, new Struct and array(T, n): the type declared or named": {
    ts: `${IMPORTS}${declarations}
      export const NamedTypeLens = contract("NamedTypeLens", {
        called: pure(uint128).returns(uint128, uint64, address, int256, bytes32, bytes4),
        built: pure(uint64, uint256).returns(Inner, Inner, [uint128], [[address]]),
      }).implementation({
        called(x) {
          const fromHelper = half(x);
          const narrowed = uint64(x);
          const nobody = address(0n);
          const negative = int256(-1n);
          const word = bytes32(uint256(x));
          const selector = bytes4("0x01020304");
          return [fromHelper, narrowed, nobody, negative, word, selector];
        },
        built(value, n) {
          const zero = new Inner();
          const flags = array(bool, n);
          const filled = new Inner({ value, flags });
          const amounts = array(uint128, n);
          const table = array([address], n);
          return [zero, filled, amounts, table];
        },
      });
    `,
    solidity: `
      struct Inner {
          uint64 value;
          bool[] flags;
      }

      contract NamedTypeLens {
          function called(uint128 x) external pure returns (uint128, uint64, address, int256, bytes32, bytes4) {
              uint128 fromHelper = half(x);
              uint64 narrowed = uint64(x);
              address nobody = address(0);
              int256 negative = int256(-1);
              bytes32 word = bytes32(uint256(x));
              bytes4 selector = bytes4(hex"01020304");
              return (fromHelper, narrowed, nobody, negative, word, selector);
          }

          function built(uint64 value, uint256 n)
              external
              pure
              returns (Inner memory, Inner memory, uint128[] memory, address[][] memory)
          {
              Inner memory zero;
              bool[] memory flags = new bool[](n);
              Inner memory filled = Inner({value: value, flags: flags});
              uint128[] memory amounts = new uint128[](n);
              address[][] memory table = new address[][](n);
              return (zero, filled, amounts, table);
          }

          function half(uint128 x) internal pure returns (uint128) {
              return x / 2;
          }
      }
    `,
    calls: [
      { functionName: "called", args: [9n] },
      { functionName: "called", args: [2n ** 128n - 1n] },
      { functionName: "built", args: [3n, 2n] },
      { functionName: "built", args: [0n, 0n] },
    ],
  },
  "a literal or a module constant: uint256, bool, string, address": {
    ts: `${IMPORTS}${declarations}
      export const LiteralTypeLens = contract("LiteralTypeLens", {
        literals: pure().returns(uint256, uint256, uint256, bool, bool, string, address),
      }).implementation({
        literals() {
          const integer = 5n;
          const unsuffixed = 5;
          const scale = SCALE;
          const yes = true;
          const no = false;
          const text = "text";
          const owner = "${OWNER}";
          return [integer, unsuffixed, scale, yes, no, text, owner];
        },
      });
    `,
    solidity: `
      contract LiteralTypeLens {
          function literals() external pure returns (uint256, uint256, uint256, bool, bool, string memory, address) {
              uint256 integer = 5;
              uint256 unsuffixed = 5;
              uint256 scale = 10 ** 6;
              bool yes = true;
              bool no = false;
              string memory text = "text";
              address owner = ${OWNER};
              return (integer, unsuffixed, scale, yes, no, text, owner);
          }
      }
    `,
    calls: [{ functionName: "literals" }],
  },
  "a comparison, &&, || and !: bool": {
    ts: `${IMPORTS}
      export const BoolLens = contract("BoolLens", {
        ordered: pure(uint256, uint256).returns(bool, bool, bool, bool),
        equal: pure(uint256, uint256).returns(bool, bool, bool, bool),
        logical: pure(bool, bool).returns(bool, bool, bool),
      }).implementation({
        ordered(x, y) {
          const less = x < y;
          const atMost = x <= y;
          const more = x > y;
          const atLeast = x >= y;
          return [less, atMost, more, atLeast];
        },
        // === is ==, and !== is !=.
        equal(x, y) {
          const same = x == y;
          const strictlySame = x === y;
          const different = x != y;
          const strictlyDifferent = x !== y;
          return [same, strictlySame, different, strictlyDifferent];
        },
        logical(a, b) {
          const both = a && b;
          const either = a || b;
          const neither = !either;
          return [both, either, neither];
        },
      });
    `,
    solidity: `
      contract BoolLens {
          function ordered(uint256 x, uint256 y) external pure returns (bool, bool, bool, bool) {
              bool less = x < y;
              bool atMost = x <= y;
              bool more = x > y;
              bool atLeast = x >= y;
              return (less, atMost, more, atLeast);
          }

          function equal(uint256 x, uint256 y) external pure returns (bool, bool, bool, bool) {
              bool same = x == y;
              bool strictlySame = x == y;
              bool different = x != y;
              bool strictlyDifferent = x != y;
              return (same, strictlySame, different, strictlyDifferent);
          }

          function logical(bool a, bool b) external pure returns (bool, bool, bool) {
              bool both = a && b;
              bool either = a || b;
              bool neither = !either;
              return (both, either, neither);
          }
      }
    `,
    calls: [
      { functionName: "ordered", args: [1n, 2n] },
      { functionName: "ordered", args: [2n, 2n] },
      { functionName: "ordered", args: [MAX_UINT256, 2n] },
      { functionName: "equal", args: [1n, 2n] },
      { functionName: "equal", args: [2n, 2n] },
      { functionName: "logical", args: [true, false] },
      { functionName: "logical", args: [true, true] },
      { functionName: "logical", args: [false, false] },
    ],
  },
  "a local keeps its type for the scope it is declared in": {
    ts: `${IMPORTS}${declarations}
      const divMod = internal(pure(uint64, uint64).returns(uint64, uint64), (x, y) => [x / y, x % y]);

      export const ScopeLens = contract("ScopeLens", {
        scoped: pure(uint64, bool).returns(uint256, bool, uint64),
        counted: pure(uint64, uint64).returns(uint64, uint256),
      }).implementation({
        // Two locals in one statement, and one name with a different type in each of three blocks.
        scoped(x, flag) {
          let total = 1n, seen = false;
          let last = x;
          if (flag) {
            const inBlock = x;
            const copy = inBlock;
            last = copy / 2n;
          } else {
            const inBlock = x > 3n;
            const copy = inBlock;
            seen = copy;
          }
          {
            const inBlock = SCALE;
            const copy = inBlock;
            total += copy;
          }
          return [total, seen, last];
        },
        // A local declared from several values, and the local of a for loop, are copied the same way.
        counted(x, y) {
          const [quotient, remainder] = divMod(x, y);
          const q = quotient;
          let steps = 0n;
          for (let i = remainder; i > 0n; i--) {
            const step = i;
            steps += step;
          }
          const i = steps;
          return [q, i];
        },
      });
    `,
    solidity: `
      contract ScopeLens {
          function scoped(uint64 x, bool flag) external pure returns (uint256, bool, uint64) {
              uint256 total = 1;
              bool seen = false;
              uint64 last = x;
              if (flag) {
                  uint64 inBlock = x;
                  uint64 copy = inBlock;
                  last = copy / 2;
              } else {
                  bool inBlock = x > 3;
                  bool copy = inBlock;
                  seen = copy;
              }
              {
                  uint256 inBlock = 10 ** 6;
                  uint256 copy = inBlock;
                  total += copy;
              }
              return (total, seen, last);
          }

          function counted(uint64 x, uint64 y) external pure returns (uint64, uint256) {
              (uint64 quotient, uint64 remainder) = divMod(x, y);
              uint64 q = quotient;
              uint256 steps = 0;
              for (uint64 i = remainder; i > 0; i--) {
                  uint64 step = i;
                  steps += step;
              }
              uint256 i = steps;
              return (q, i);
          }

          function divMod(uint64 x, uint64 y) internal pure returns (uint64, uint64) {
              return (x / y, x % y);
          }
      }
    `,
    calls: [
      { functionName: "scoped", args: [9n, true] },
      { functionName: "scoped", args: [9n, false] },
      { functionName: "scoped", args: [2n, false] },
      { functionName: "counted", args: [17n, 5n] },
      { functionName: "counted", args: [17n, 0n], reverts: true },
    ],
  },
};

describePairs("the type of a local", pairs);

describe("a local with no initializer is written with its zero value", () => {
  const ts = `${IMPORTS}
    class Pair extends Struct({ low: uint128, high: uint128 }) {}

    export const ZeroLens = contract("ZeroLens", {
      zero: pure(uint256).returns(uint256, bool, address, bytes32, string, Pair, [uint256]),
    }).implementation({
      zero(x) {
        let count = 0n;
        let found = false;
        let who = address(0n);
        let id = bytes32(0n);
        let text = "";
        const pair = new Pair();
        let values = array(uint256, 0n);
        if (x > 0n) {
          count = x;
          found = true;
          who = address(uint160(x));
          id = bytes32(x);
          text = "set";
          pair.high = uint128(x);
          values = array(uint256, x);
        }
        return [count, found, who, id, text, pair, values];
      },
    });
  `;
  const solidity = (locals: string) => `
    struct Pair {
        uint128 low;
        uint128 high;
    }

    contract ZeroLens {
        function zero(uint256 x)
            external
            pure
            returns (uint256, bool, address, bytes32, string memory, Pair memory, uint256[] memory)
        {
            ${locals}
            if (x > 0) {
                count = x;
                found = true;
                who = address(uint160(x));
                id = bytes32(x);
                text = "set";
                pair.high = uint128(x);
                values = new uint256[](x);
            }
            return (count, found, who, id, text, pair, values);
        }
    }
  `;
  // What the lens prints as: each zero value spelled out. A struct is the exception, as in Solidity.
  const written = solidity(`
    uint256 count = 0;
    bool found = false;
    address who = address(0);
    bytes32 id = bytes32(0);
    string memory text = "";
    Pair memory pair;
    uint256[] memory values = new uint256[](0);
  `);
  // The same function with locals that have no initializer, which the syntax has no form for.
  const bare = solidity(`
    uint256 count;
    bool found;
    address who;
    bytes32 id;
    string memory text;
    Pair memory pair;
    uint256[] memory values;
  `);
  const evm = useEvm();

  it("the printed Solidity spells each zero value", async () => {
    expectSameAst(await compilePair({ ts, solidity: written }));
  });

  it.skipIf(!requireAnvil())("and it gives the bytes the locals without initializers give", async () => {
    const outcomes = await expectSameExecution(evm(), await compilePair({ ts, solidity: bare }), [
      { functionName: "zero", args: [0n] },
      { functionName: "zero", args: [3n] },
      { functionName: "zero", args: [2n ** 128n] },
    ]);
    // The last asks for more memory than a call can pay for, on both sides.
    expect(outcomes.map((outcome) => outcome.status)).toEqual(["success", "success", "revert"]);
  });
});

describe("the type of a local: rejections", () => {
  const lens = (body: string, head = "") => `${IMPORTS}
class Pair extends Struct({ low: uint128, high: uint128 }) {}
const half = internal(pure(uint256).returns(uint256), (x) => x / 2n);
${head}
export const Lens = contract("Lens", { f: pure(uint256, Pair).returns(uint256) }).implementation({
  f(x, p) {
    ${body}
  },
});
`;
  const NAME_THE_TYPE =
    "this local's type cannot be copied from its initializer: name the type with a conversion, for example uint256(…)";

  for (const [what, initializer] of [
    ["arithmetic", "x * 2n"],
    ["a conditional", "x > 1n ? x : 1n"],
    ["a negation", "-x"],
    ["a complement", "~x"],
    ["an increment", "x++"],
    ["an assignment", "(x += 1n)"],
    ["a shift", "x << 1n"],
    ["a field the struct does not declare", "p.middle"],
    ["a field of a value that is not a struct", "x.low"],
    ["a helper that is not called", "half"],
    ["a type that is not called", "uint256"],
    ["a call that declares no type", "require(x > 0n)"],
  ] as const)
    it(`${what} has no type to copy`, async () => {
      const code = lens(`const y = ${initializer};\n    return uint256(y);`);
      // The error is at the initializer, inside the parentheses where it has them.
      const { line, column } = positionIn(code, `= ${initializer};`);
      await expectRejection(code, {
        message: NAME_THE_TYPE,
        line,
        column: column + (initializer.startsWith("(") ? 3 : 2),
      });
    });

  it("a local needs an initializer", async () => {
    const code = lens("let y;\n    y = x;\n    return y;");
    await expectRejection(code, {
      message: "a local needs an initializer; a local with none is written with its zero value",
      ...positionIn(code, "y;"),
    });
  });

  it("a local is declared with const or let", async () => {
    const code = lens("var y = x;\n    return y;");
    await expectRejection(code, { message: "a local is declared with const or let", ...positionIn(code, "var y") });
  });

  it("a local is used after it is declared", async () => {
    const code = lens("y = x;\n    let y = 0n;\n    return y;");
    await expectRejection(code, { message: '"y" is used before it is declared', ...positionIn(code, "y = x") });
  });

  it("a helper is not a value", async () => {
    const code = lens("return half;");
    await expectRejection(code, {
      message: '"half" is a helper and cannot be used as a value here',
      ...positionIn(code, "half;"),
    });
  });

  it("a struct is not a value", async () => {
    const code = lens("return x + Pair;");
    await expectRejection(code, {
      message: '"Pair" is a struct and cannot be used as a value here',
      ...positionIn(code, "Pair;"),
    });
  });

  it("solc checks a module constant that is not an integer, where it is used", async () => {
    const code = lens("const enabled = ENABLED;\n    return enabled ? x : 0n;", "const ENABLED = true;");
    await expectRejection(code, {
      message: "Type bool is not implicitly convertible to expected type uint256.",
      ...positionIn(code, "const enabled"),
      source: "solc",
    });
  });

  it("solc checks what a conversion that names a type is given, at the TypeScript position", async () => {
    const code = lens("const flag = bool(x * 2n);\n    return flag ? x : 0n;");
    await expectRejection(code, {
      message: 'Explicit type conversion not allowed from "uint256" to "bool".',
      ...positionIn(code, "bool(x * 2n)"),
      source: "solc",
    });
  });
});
