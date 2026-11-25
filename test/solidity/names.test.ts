/**
 * Names: a name the author wrote is printed into Solidity as it is, so it has to be a name there too.
 * Anything that is not a Solidity identifier, or that Solidity reserves, is refused where it is written.
 */
import { describe, expect, it } from "vitest";

import { findConfig, runSolc } from "../../src/solidity/build/index.js";
import { RESERVED_WORDS } from "../../src/solidity/build/names.js";

import { describePairs } from "./helpers/core.js";
import { expectRejection, positionIn } from "./helpers/harness.js";

const PACKAGE = "@morpho-org/viem-dlc/solidity";
const IMPORTS = `import {
  address, array, assembly, block, bytes, bytes32, contract, gasleft, iface, immutable, internal, keccak256, library,
  pure, Struct, uint256, view,
} from "${PACKAGE}";
import { add } from "${PACKAGE}/yul";`;

/** A lens whose one function has this body, after this module-level code. */
const lens = (body: string, head = "") => `${IMPORTS}
${head}
export const Lens = contract("Lens", { f: pure(uint256).returns(uint256) }).implementation({
  f(x) {
    ${body}
  },
});
`;

const IDENTIFIER = "is not a Solidity identifier";
const RESERVED = "is a reserved word in Solidity, so it cannot be used as a name";

/** [what is refused, the module, where the error points, the message] */
type Case = readonly [name: string, code: string, at: string, message: string];

describe("a name that is not a Solidity name is refused where it is written", () => {
  const cases: readonly Case[] = [
    [
      "an entry point whose quoted key holds a space",
      `${IMPORTS}
export const Lens = contract("Lens", { "quote ": pure().returns(uint256) }).implementation({
  "quote "() { return 7n; },
});`,
      '"quote ": pure()',
      `"quote " ${IDENTIFIER}`,
    ],
    [
      "an entry point named with a reserved word",
      `${IMPORTS}
export const Lens = contract("Lens", { delete: pure().returns(uint256) }).implementation({
  delete() { return 7n; },
});`,
      "delete: pure()",
      `"delete" ${RESERVED}`,
    ],
    [
      "a lens named with a reserved word",
      `${IMPORTS}
export const Lens = contract("mapping", { f: pure().returns(uint256) }).implementation({ f: () => 7n });`,
      '"mapping"',
      `"mapping" ${RESERVED}`,
    ],
    [
      "a struct field whose quoted key holds punctuation",
      lens("const o = new Pair();\n    return o.a;", 'class Pair extends Struct({ a: uint256, "b-c": uint256 }) {}'),
      '"b-c": uint256',
      `"b-c" ${IDENTIFIER}`,
    ],
    [
      "a struct field named with a reserved word",
      lens("const o = new Pair();\n    return o.a;", "class Pair extends Struct({ a: uint256, memory: uint256 }) {}"),
      "memory: uint256",
      `"memory" ${RESERVED}`,
    ],
    [
      "a struct named with a reserved word",
      lens("const o = new final();\n    return o.a;", "class final extends Struct({ a: uint256 }) {}"),
      "final extends",
      `"final" ${RESERVED}`,
    ],
    [
      "an interface function whose quoted key holds a space",
      lens(
        "return IFeed(address(0n)).price();",
        'const IFeed = iface({ price: view().returns(uint256), "last price": view() });',
      ),
      '"last price": view()',
      `"last price" ${IDENTIFIER}`,
    ],
    [
      "an interface named with a reserved word",
      lens("return alias(address(0n)).price();", "const alias = iface({ price: view().returns(uint256) });"),
      "alias = iface",
      `"alias" ${RESERVED}`,
    ],
    [
      "a library whose name holds a space",
      lens(
        "return Math.twice(x);",
        'const Math = library("Fixed Math", { twice: pure(uint256).returns(uint256) }).implementation({ twice: (a) => a * 2n });',
      ),
      '"Fixed Math"',
      `"Fixed Math" ${IDENTIFIER}`,
    ],
    [
      "a library function named with a reserved word",
      lens(
        "return Math.days(x);",
        'const Math = library("Math", { days: pure(uint256).returns(uint256) }).implementation({ days: (a) => a * 2n });',
      ),
      "days: pure",
      `"days" ${RESERVED}`,
    ],
    [
      "a helper named outside ASCII",
      lens("return café(x);", "const café = internal(pure(uint256).returns(uint256), (a) => a);"),
      "café = internal",
      `"café" ${IDENTIFIER}`,
    ],
    [
      "a helper named with a sized type",
      lens("return bytes7(x);", "const bytes7 = internal(pure(uint256).returns(uint256), (a) => a);"),
      "bytes7 = internal",
      `"bytes7" ${RESERVED}`,
    ],
    [
      "an immutable named with a reserved word",
      `${IMPORTS}
const weeks = immutable(uint256);
export const Lens = contract("Lens", { f: view().returns(uint256) }, { immutables: { weeks } }).implementation({
  f: () => weeks,
});`,
      "weeks = immutable",
      `"weeks" ${RESERVED}`,
    ],
    [
      "a parameter named with a reserved word",
      `${IMPORTS}
export const Lens = contract("Lens", { f: pure(uint256).returns(uint256) }).implementation({ f: (hours) => hours });`,
      "hours) =>",
      `"hours" ${RESERVED}`,
    ],
    [
      "a local named with a reserved word",
      lens("const seconds = x;\n    return seconds;"),
      "seconds = x",
      `"seconds" ${RESERVED}`,
    ],
    [
      "a local of a tuple named with a reserved word",
      lens(
        "const [of, rest] = both(x);\n    return of + rest;",
        "const both = internal(pure(uint256).returns(uint256, uint256), (a) => [a, a]);",
      ),
      "of, rest",
      `"of" ${RESERVED}`,
    ],
    ["a local named _", lens("const _ = x;\n    return x;"), "_ = x", `"_" ${RESERVED}`],
    [
      "a field of a struct literal whose quoted key holds a space",
      lens('const o = new Pair({ "a ": x });\n    return o.a;', "class Pair extends Struct({ a: uint256 }) {}"),
      '"a ": x',
      `"a " ${IDENTIFIER}`,
    ],
    [
      "a Yul local named outside ASCII",
      lens(
        'const out = assembly("memory-safe", () => {\n      const größe = add(x, 1n);\n      return größe;\n    });\n    return out;',
      ),
      "größe = add",
      `"größe" ${IDENTIFIER}`,
    ],
  ];
  for (const [name, code, at, message] of cases)
    it(`refuses ${name}`, async () => {
      await expectRejection(code, { message, ...positionIn(code, at), source: "transform" });
    });
});

describePairs("names that are names in both languages", {
  "a field named like a built-in, and names with $ and _": {
    ts: `${IMPORTS}
      class Note extends Struct({ msg: uint256, block: uint256, $value: uint256 }) {}
      const _twice$ = internal(pure(uint256).returns(uint256), (_x1) => _x1 * 2n);
      export const Notes = contract("Notes", { read: pure(Note).returns(uint256) }).implementation({
        read(note) {
          const $sum = uint256(note.msg + note.block);
          return _twice$($sum) + note.$value;
        },
      });
    `,
    solidity: `
      struct Note {
          uint256 msg;
          uint256 block;
          uint256 $value;
      }

      contract Notes {
          function read(Note calldata note) external pure returns (uint256) {
              uint256 $sum = uint256(note.msg + note.block);
              return _twice$($sum) + note.$value;
          }

          function _twice$(uint256 _x1) internal pure returns (uint256) {
              return _x1 * 2;
          }
      }
    `,
    calls: [{ functionName: "read", args: [{ msg: 3n, block: 4n, $value: 5n }] }],
  },
});

describe("the names the transform refuses are the names solc refuses", () => {
  const { solc } = findConfig(import.meta.filename);
  const compile = (sources: Record<string, string>) =>
    runSolc(
      {
        language: "Solidity",
        sources: Object.fromEntries(
          Object.entries(sources).map(([file, body]) => [
            file,
            { content: `// SPDX-License-Identifier: UNLICENSED\npragma solidity ^0.8.0;\n${body}` },
          ]),
        ),
        settings: { outputSelection: { "*": { "*": ["abi"] } } },
      },
      solc.version,
    ).errors as { severity: string; errorCode: string; sourceLocation?: { file: string } }[] | undefined;
  const reported = (errors: ReturnType<typeof compile>, severity: string, code?: string) =>
    new Set(
      (errors ?? [])
        .filter((error) => error.severity === severity && (code === undefined || error.errorCode === code))
        .map((error) => error.sourceLocation?.file),
    );

  const local = (word: string) => `contract C { function f() external pure { uint256 ${word} = 1; } }`;

  it("every reserved word is an error as the name of a local", () => {
    // solc refuses _ after parsing, and reports nothing of the kind while another source fails to parse.
    const words = [...RESERVED_WORDS, "uint8", "int256", "bytes1", "bytes32", "fixed128x18", "ufixed8x0"].filter(
      (word) => word !== "_",
    );
    const errors = compile(Object.fromEntries(words.map((word) => [word, local(word)])));
    expect(words.filter((word) => !reported(errors, "error").has(word))).toEqual([]);
    expect(reported(compile({ _: local("_") }), "error")).toEqual(new Set(["_"]));
  });
});
