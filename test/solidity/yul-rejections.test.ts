/**
 * Typed Yul: what a block may not contain. Every rejection is checked for its message and for the
 * TypeScript line and column it is reported at.
 */
import { describe, expect, it } from "vitest";

import { expectRejection, positionIn } from "./helpers/harness.js";

/** A lens whose one function has this body. `head` is module-level code. */
const lens = (
  body: string,
  head = "",
) => `import { assembly, bytes32, contract, internal, pure, Struct, uint256 } from "@morpho-org/viem-dlc/solidity";
import * as yul from "@morpho-org/viem-dlc/solidity/yul";
import { add, mload, mstore } from "@morpho-org/viem-dlc/solidity/yul";
${head}
export const Lens = contract("Lens", { f: pure(uint256).returns(uint256) }).implementation({
  f(x) {
    ${body}
  },
});
`;

/** The same lens, with these statements inside an assembly block that is a statement. */
const block = (statements: string, head = "") =>
  lens(`let out = 0n;\n    assembly("memory-safe", () => {\n      ${statements}\n    });\n    return out;`, head);

/** [what is refused, the module, where the error points, the message] */
type Case = readonly [name: string, code: string, at: string, message: string];

const OPERATOR = "call the opcode";
const EXPRESSION = "an assembly block cannot contain this expression";
const STATEMENT = "an assembly block cannot contain this statement";
const LITERAL = "inside an assembly block a literal is an integer, written in decimal or in hexadecimal";
const CALL =
  "inside an assembly block a call is to an opcode imported from @morpho-org/viem-dlc/solidity/yul, or to a function declared in the block";
const SHAPE = 'an assembly block is written assembly("memory-safe", () => { … })';
const HELPER = "const helper = internal(pure(uint256).returns(uint256), (y) => y);";

const inTheBlock: readonly Case[] = [
  ["a local declared with var", block("var y = add(x, 1n);"), "var y", "a Yul local is declared with const or let"],
  ["a destructured local", block("const [a, b] = x;"), "[a, b]", "a Yul local must be a plain name"],
  ["a local with no initializer", block("let y;\n      y = x;"), "y;", "a Yul local needs an initializer"],
  [
    "a compound assignment",
    block("out += x;"),
    "out += x",
    "Yul has no operator +=: call the opcode, as in x = add(x, y)",
  ],
  ["an update", block("out++;"), "out++", "Yul has no operator ++: call the opcode, as in x = add(x, 1)"],
  ["an update inside an expression", block("out = add(out++, 1n);"), "out++", `Yul has no operator ++: ${OPERATOR}`],
  [
    "an assignment to a module constant",
    block("LIMIT = x;", "const LIMIT = 5n;"),
    "LIMIT = x",
    "inside an assembly block only a local can be assigned",
  ],
  [
    "an assignment to a field",
    block("out.value = x;"),
    "out.value",
    "inside an assembly block only a local can be assigned",
  ],
  [
    "an expression that is neither an assignment nor a call",
    block("x == 1n;"),
    "x == 1n",
    "this expression is not a statement: inside an assembly block a statement assigns or calls",
  ],
  ["an arithmetic operator", block("out = x + 1n;"), "x + 1n", `Yul has no operator +: ${OPERATOR}, as in add(a, b)`],
  ["a logical operator", block("out = x && out;"), "x && out", `Yul has no operator &&: ${OPERATOR}`],
  ["a unary operator", block("out = !x;"), "!x", `Yul has no operator !: ${OPERATOR}`],
  ["a conditional expression", block("out = x ? 1n : 2n;"), "x ? 1n", `${EXPRESSION} (ConditionalExpression)`],
  ["a field read", block("out = x.length;"), "x.length", `${EXPRESSION} (MemberExpression)`],
  [
    "a function that is not an arrow function",
    block("const twice = function (a) { return add(a, a); };"),
    "function (a)",
    `${EXPRESSION} (FunctionExpression)`,
  ],
  ["a string literal", block('mstore(0x00, "text");'), '"text"', LITERAL],
  ["a boolean literal", block("out = true;"), "true", LITERAL],
  ["a literal with a fraction", block("out = 1.5;"), "1.5", "this Number literal is not a safe integer"],
  ["a literal with an exponent", block("out = 1e3;"), "1e3", LITERAL],
  [
    "a constant that is an expression",
    block("out = WAD;", "const WAD = 10n ** 18n;"),
    "WAD;",
    '"WAD" is not a single integer literal. A constant named inside an assembly block must be one, because Yul has no operators',
  ],
  [
    "a constant that is a negated literal",
    block("out = MINUS_ONE;", "const MINUS_ONE = -1n;"),
    "MINUS_ONE;",
    '"MINUS_ONE" is not a single integer literal',
  ],
  [
    "a constant that names another constant",
    block("out = ALIAS;", "const ONE = 1n;\nconst ALIAS = ONE;"),
    "ALIAS;",
    '"ALIAS" is not a single integer literal',
  ],
  [
    "a constant that is a string",
    block("out = NAME;", 'const NAME = "feed";'),
    "NAME;",
    '"NAME" is not a single integer literal',
  ],
  [
    "an opcode used as a value",
    block("out = add;"),
    "add;",
    '"add" is not a value inside an assembly block. A value there is a local, an integer literal, or a module constant that is a single integer literal',
  ],
  [
    "a helper used as a value",
    block("out = helper;", HELPER),
    "helper;",
    '"helper" is not a value inside an assembly block',
  ],
  ["a call to a helper", block("out = helper(x);", HELPER), "helper(x)", CALL],
  ["a conversion", block("out = uint256(x);"), "uint256(x)", CALL],
  ["a call to something that is not a name", block("out = (x ? add : add)(x, x);"), "x ? add", CALL],
  [
    "an opcode the yul entry does not export",
    block("sstore(0n, x);", 'import { sstore } from "@morpho-org/viem-dlc/solidity/yul";'),
    "sstore(0n",
    '"sstore" is not an opcode @morpho-org/viem-dlc/solidity/yul exports',
  ],
  [
    "an opcode the yul entry does not export, through a namespace",
    block("yul.log0(0n, x);"),
    "yul.log0",
    '"log0" is not an opcode @morpho-org/viem-dlc/solidity/yul exports',
  ],
  ["a spread argument", block("out = add(...[x, x]);"), "...[x", "a call cannot spread its arguments"],
  ["a do loop", block("do {\n        out = x;\n      } while (x);"), "do {", `${STATEMENT} (DoWhileStatement)`],
  ["a bare block", block("{\n        out = x;\n      }"), "{\n        out", `${STATEMENT} (BlockStatement)`],
  [
    "a labelled loop",
    block("outer: while (x) {\n        break outer;\n      }"),
    "outer:",
    `${STATEMENT} (LabeledStatement)`,
  ],
  [
    "a for loop with no condition",
    block("for (;;) {\n        break;\n      }"),
    "for (;;)",
    "a for loop inside an assembly block needs a condition",
  ],
  [
    "two updates in the head of a for loop",
    block("for (let i = 0n; i < x; i = add(i, 1n), out = i) {}"),
    "i = add(i, 1n), out = i",
    "this expression is not a statement",
  ],
  [
    "a return that is not the last statement",
    block("if (x) {\n        return x;\n      }\n      out = x;"),
    "return x",
    "return word is the last statement of an assembly block, or of a function inside it",
  ],
  [
    "a return before the end of a function",
    block("const pick = (a) => {\n        if (a) {\n          return 1n;\n        }\n        return 2n;\n      };"),
    "return 1n",
    "return word is the last statement of an assembly block, or of a function inside it",
  ],
  [
    "a function with a destructured parameter",
    block("const first = ([a]) => a;"),
    "[a]",
    "a parameter must be a plain name",
  ],
  ["a function with a default", block("const orOne = (a = 1n) => a;"), "a = 1n", "a parameter must be a plain name"],
  [
    "an async function",
    block("const later = async (a) => a;"),
    "async (a)",
    "a function inside an assembly block cannot be async",
  ],
];

const atTheBlock: readonly Case[] = [
  [
    "a function whose value would take the name of a Solidity local",
    lens(
      'const r = assembly("memory-safe", () => {\n      const twice = (a) => add(a, a);\n      return twice(x);\n    });\n    return r;',
    ),
    "(a) => add",
    'a function inside an assembly block names its value "r" in Yul, and "r" is a local here: rename the local',
  ],
  [
    "a block that is a statement and returns a word",
    lens('assembly("memory-safe", () => {\n      return x;\n    });\n    return x;'),
    "return x;\n    });",
    "this block returns a word, so it is a value: write it as the initializer of a local, as in const x = assembly(…)",
  ],
  [
    "a block that is a statement and is an expression",
    lens('assembly("memory-safe", () => mload(x));\n    return x;'),
    "mload(x)",
    "this block returns a word, so it is a value",
  ],
  [
    "a block that is a value and returns no word",
    lens('const y = assembly("memory-safe", () => {\n      mstore(0x00, x);\n    });\n    return y;'),
    "{\n      mstore",
    "an assembly block used as a value ends in return word",
  ],
  [
    "a return with no word",
    lens('const y = assembly("memory-safe", () => {\n      mstore(0x00, x);\n      return;\n    });\n    return y;'),
    "return;",
    "return is followed by the word it gives",
  ],
  [
    "a callback that takes a parameter",
    lens('assembly("memory-safe", (p) => {\n      mstore(p, x);\n    });\n    return x;'),
    "p) =>",
    "the callback of an assembly block takes no parameters",
  ],
  [
    "an async callback",
    lens('assembly("memory-safe", async () => {\n      mstore(0x00, x);\n    });\n    return x;'),
    "async ()",
    "the callback of an assembly block cannot be async or a generator",
  ],
  [
    "a callback that is a generator",
    lens('assembly("memory-safe", function* () {\n      mstore(0x00, x);\n    });\n    return x;'),
    "function* ()",
    "the callback of an assembly block cannot be async or a generator",
  ],
  [
    "a type named with no flag",
    lens("const y = assembly(bytes32, () => mload(0x40));\n    return x;"),
    "assembly(bytes32",
    SHAPE,
  ],
  [
    "a block with no flag",
    lens("assembly(() => {\n      mstore(0x00, x);\n    });\n    return x;"),
    "assembly(()",
    SHAPE,
  ],
  [
    "a block with another flag",
    lens('assembly("unchecked", () => {\n      mstore(0x00, x);\n    });\n    return x;'),
    'assembly("unchecked"',
    SHAPE,
  ],
  [
    "a block whose callback is not written in place",
    lens('assembly("memory-safe", x);\n    return x;'),
    'assembly("memory-safe", x)',
    SHAPE,
  ],
  [
    "a block with too many arguments",
    lens('assembly(bytes32, "memory-safe", () => mload(0x40), x);\n    return x;'),
    "assembly(bytes32",
    SHAPE,
  ],
  [
    "a named type that is not elementary",
    lens(
      'const y = assembly(Pair, "memory-safe", () => mload(0x40));\n    return x;',
      "class Pair extends Struct({ a: uint256, b: uint256 }) {}",
    ),
    'Pair, "memory-safe"',
    "the type of an assembly block's value is an elementary type, such as bytes32 or string",
  ],
  [
    "a type named for a block that is a statement",
    lens('assembly(bytes32, "memory-safe", () => {\n      mstore(0x00, x);\n    });\n    return x;'),
    'bytes32, "memory-safe"',
    "a type is named only for an assembly block that is the initializer of a local",
  ],
  [
    "a block that is returned",
    lens('return assembly("memory-safe", () => mload(0x40));'),
    'assembly("memory-safe"',
    "an assembly block is a statement, or the initializer of a local as in const x = assembly(…)",
  ],
  [
    "a block that is assigned",
    lens('let y = 0n;\n    y = assembly("memory-safe", () => mload(0x40));\n    return y;'),
    'assembly("memory-safe"',
    "an assembly block is a statement, or the initializer of a local",
  ],
  [
    "a block that initializes the local of a for loop",
    lens('for (let i = assembly("memory-safe", () => mload(0x40)); i < x; i++) {}\n    return x;'),
    'assembly("memory-safe"',
    "an assembly block is not the initializer of a for loop's local: declare the local before the loop",
  ],
  [
    "an opcode called outside a block",
    lens("return add(x, 1n);"),
    "add(x",
    "add(…) is an opcode, and an opcode is called inside an assembly block",
  ],
];

describe("typed Yul: rejections", () => {
  describe("inside the block", () => {
    for (const [name, code, at, message] of inTheBlock)
      it(name, async () => {
        await expectRejection(code, { message, ...positionIn(code, at), source: "transform" });
      });
  });

  describe("at the block", () => {
    for (const [name, code, at, message] of atTheBlock)
      it(name, async () => {
        await expectRejection(code, { message, ...positionIn(code, at), source: "transform" });
      });
  });

  // The printer adds no checks of its own: what Yul refuses, solc refuses, at the TypeScript it was printed from.
  describe("from solc, at the TypeScript the block was printed from", () => {
    it("an opcode with the wrong number of arguments", async () => {
      const code = block("out = add(x);");
      const diagnostic = await expectRejection(code, {
        message: 'Function "add" expects 2 arguments but got 1.',
        ...positionIn(code, "add(x)"),
        source: "solc",
      });
      expect(diagnostic.lens).toBe("Lens");
    });

    it("a call whose value is dropped", async () => {
      const code = block("add(x, 1n);");
      await expectRejection(code, {
        message: "Top-level expressions are not supposed to return values",
        ...positionIn(code, "add(x, 1n)"),
        source: "solc",
      });
    });

    it("a function that reads a local of the block, which a Yul function cannot see", async () => {
      const code = block(
        "const base = mload(0x40);\n      const offset = (a) => add(base, a);\n      out = offset(x);",
      );
      await expectRejection(code, {
        message: 'Identifier "base" not found.',
        ...positionIn(code, "base, a"),
        source: "solc",
      });
    });

    it("a function that reads a Solidity local", async () => {
      const code = block("const offset = (a) => add(x, a);\n      out = offset(1n);");
      await expectRejection(code, {
        message: "Cannot access local Solidity variables from inside an inline assembly function.",
        ...positionIn(code, "x, a"),
        source: "solc",
      });
    });

    it("a Yul local with the name a function gives its value", async () => {
      const code = block("let r = x;\n      const twice = (a) => add(a, a);\n      out = twice(r);");
      await expectRejection(code, {
        message: "Variable name r already taken in this scope.",
        ...positionIn(code, "(a) => add"),
        source: "solc",
      });
    });
  });
});
