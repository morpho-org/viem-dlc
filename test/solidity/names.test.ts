/**
 * Names: a name the author wrote is printed into Solidity as it is, so it has to be a name there too.
 * Anything that is not a Solidity identifier, or that Solidity reserves, is refused where it is written.
 */
import { describe, expect, it } from "vitest";

import { findConfig, runSolc } from "../../src/solidity/build/index.js";
import { RESERVED_WORDS, SOLIDITY_BUILTINS } from "../../src/solidity/build/names.js";

import { requireAnvil } from "./helpers/anvil.js";
import { describePairs, useEvm } from "./helpers/core.js";
import {
  compilePair,
  expectRejection,
  expectSameAst,
  expectSameExecution,
  type FixturePair,
  positionIn,
  type Snippet,
} from "./helpers/harness.js";
import { withConstructorArgs } from "./helpers/mocks.js";

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

const { solc } = findConfig(import.meta.filename);
/** Compiles Solidity sources with the pinned compiler, and returns what solc reported. */
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
      "a local named with a sized integer type",
      lens("const uint96 = x;\n    return uint96;"),
      "uint96 = x",
      `"uint96" ${RESERVED}`,
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
  "names that look like sized types and are of no size Solidity has": {
    ts: lens("const uint7 = x;\n    const int264 = uint7;\n    const bytes33 = int264;\n    return bytes33 + 1n;"),
    solidity: `
      contract Lens {
          function f(uint256 x) external pure returns (uint256) {
              uint256 uint7 = x;
              uint256 int264 = uint7;
              uint256 bytes33 = int264;
              return bytes33 + 1;
          }
      }
    `,
    calls: [{ functionName: "f", args: [5n] }],
  },
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

const ONE_NAME =
  "A name is printed in Solidity as it is written and is never changed, so two declarations of a lens cannot share one: rename one of them";

const BUILTIN = (name: string) =>
  `"${name}" is a Solidity built-in, and a declaration named "${name}" would hide it: choose another name`;

describe("a printed name reaches the declaration the TypeScript names", () => {
  const cases: readonly (readonly [
    name: string,
    code: Snippet,
    at: string,
    message: string,
    file?: string,
    others?: Readonly<Record<string, string>>,
  ])[] = [
    [
      "an immutable imported under another name, where a parameter has the name it is declared with",
      {
        "lens.ts": `${IMPORTS}
import { r as total } from "./values.js";
export const Lens = contract("Lens", { run: view(uint256).returns(uint256) }, { immutables: { total } }).implementation({
  run(r) { return total + r; },
});`,
        "values.ts": `import { immutable, uint256 } from "${PACKAGE}";\nexport const r = immutable(uint256);\n`,
      },
      "total + r",
      'this is "r" in Solidity, the name it is declared with, and a parameter or local named "r" hides it here',
    ],
    [
      "an immutable imported under another name, where a local has the name it is declared with",
      {
        "lens.ts": `${IMPORTS}
import { r as total } from "./values.js";
export const Lens = contract("Lens", { run: view(uint256).returns(uint256) }, { immutables: { total } }).implementation({
  run(x) {
    const r = x;
    return total + r;
  },
});`,
        "values.ts": `import { immutable, uint256 } from "${PACKAGE}";\nexport const r = immutable(uint256);\n`,
      },
      "total + r",
      'this is "r" in Solidity, the name it is declared with, and a parameter or local named "r" hides it here',
    ],
    [
      "a helper with the name of an entry point, which is given no second name",
      `${IMPORTS}
const read = internal(pure().returns(uint256), () => 4n);
export const Lens = contract("Lens", { read: pure(uint256).returns(uint256) }).implementation({
  read(read_2) { return read() + read_2; },
});`,
      "internal(pure().returns(uint256), () => 4n)",
      `"read" is also the name of an entry point. ${ONE_NAME}`,
    ],
    [
      "a struct with the name of the contract",
      `${IMPORTS}
class Naming extends Struct({ a: uint256 }) {}
export const Lens = contract("Naming", { f: pure(Naming).returns(uint256) }).implementation({ f: (o) => o.a });`,
      "class Naming",
      `"Naming" is also the name of contract Naming. ${ONE_NAME}`,
    ],
    [
      "an interface with the name of a struct",
      `import { address, contract, iface, Struct, uint256, view } from "${PACKAGE}";
import { Feed as IFeed } from "./feed.js";
class Feed extends Struct({ at: address }) {}
export const Lens = contract("Lens", { run: view(Feed).returns(uint256) }).implementation({
  run(feed) { return IFeed(feed.at).price(); },
});`,
      "class Feed",
      `"Feed" is also the name of an interface. ${ONE_NAME}`,
      undefined,
      {
        "feed.ts": `import { iface, uint256, view } from "${PACKAGE}";\nexport const Feed = iface({ price: view().returns(uint256) });\n`,
      },
    ],
    [
      "a function of a library with the name of a struct",
      `${IMPORTS}
class Pair extends Struct({ a: uint256, b: uint256 }) {}
const Pairs = library("Pairs", { Pair: pure(uint256).returns(Pair) }).implementation({
  Pair: (a) => new Pair({ a, b: a + 1n }),
});
export const Lens = contract("Lens", { f: pure(uint256).returns(uint256) }).implementation({
  f(x) { return Pairs.Pair(x).a; },
});`,
      "(a) => new Pair",
      `"Pair" is also the name of a struct. ${ONE_NAME}`,
    ],
    [
      "a helper with the name of a function of a library",
      `${IMPORTS}
const Halves = library("Halves", { half: pure(uint256).returns(uint256) }).implementation({ half: (a) => a / 2n });
const half = internal(pure(uint256).returns(uint256), (a) => Halves.half(a));
export const Lens = contract("Lens", { f: pure(uint256).returns(uint256) }).implementation({
  f(x) { return half(x); },
});`,
      "internal(pure(uint256).returns(uint256), (a) => Halves",
      `"half" is also the name of a function of library Halves. ${ONE_NAME}`,
    ],
    [
      "a parameter named block, where block is imported under another name",
      `import { contract, Struct, uint256, view, block as chain } from "${PACKAGE}";
class Counter extends Struct({ number: uint256 }) {}
export const Lens = contract("Lens", { run: view(Counter).returns(uint256) }).implementation({
  run(block) { return chain.number; },
});`,
      "block) {",
      BUILTIN("block"),
    ],
    ["a local named after a built-in", lens("const msg = x;\n    return msg;"), "msg = x", BUILTIN("msg")],
    [
      "a helper named gasleft",
      `import { contract, gasleft as gasRemaining, internal, pure, uint256, view } from "${PACKAGE}";
const gasleft = internal(pure().returns(uint256), () => 7n);
export const Lens = contract("Lens", { read: view().returns(uint256) }).implementation({
  read() { return gasRemaining() > 100n ? gasleft() : 0n; },
});`,
      "gasleft = internal",
      BUILTIN("gasleft"),
    ],
    [
      "a helper of another module named keccak256",
      {
        "lens.ts": `import { bytes, bytes32, contract, keccak256 as digest, pure } from "${PACKAGE}";
import { keccak256 as fakeHash } from "./helper.js";
export const Lens = contract("Lens", { run: pure(bytes).returns(bytes32, bytes32) }).implementation({
  run(data) { return [digest(data), fakeHash(data)]; },
});`,
        "helper.ts": `import { bytes, bytes32, internal, pure } from "${PACKAGE}";
export const keccak256 = internal(pure(bytes).returns(bytes32), (_data) => bytes32(0n));
`,
      },
      "keccak256 = internal",
      BUILTIN("keccak256"),
      "helper.ts",
    ],
    [
      "an entry point named after a built-in",
      `${IMPORTS}
export const Lens = contract("Lens", { tx: pure().returns(uint256) }).implementation({ tx: () => 7n });`,
      "tx: pure()",
      BUILTIN("tx"),
    ],
    [
      "a library function named after a built-in",
      lens(
        "return Math.require(x);",
        'const Math = library("Math", { require: pure(uint256).returns(uint256) }).implementation({ require: (a) => a });',
      ),
      "require: pure",
      BUILTIN("require"),
    ],
    [
      "a struct named after a built-in",
      lens("const o = new abi();\n    return o.a;", "class abi extends Struct({ a: uint256 }) {}"),
      "abi extends",
      BUILTIN("abi"),
    ],
  ];
  for (const [name, code, at, message, file, others] of cases)
    it(`refuses ${name}`, async () => {
      const text = typeof code === "string" ? code : (code[file ?? "lens.ts"] as string);
      await expectRejection(typeof code === "string" && others ? { "lens.ts": code, ...others } : code, {
        message,
        ...positionIn(text, at),
        source: "transform",
        ...(file ? { file } : {}),
      });
    });

  it("every built-in is hidden by a declaration of its name, as solc warns", () => {
    const names = [...SOLIDITY_BUILTINS];
    const errors = compile(
      Object.fromEntries(
        names.map((name) => [name, `contract C { function ${name}() internal pure returns (uint256) { return 1; } }`]),
      ),
    );
    expect(names.filter((name) => !reported(errors, "warning", "2319").has(name))).toEqual([]);
  });
});

describePairs("built-ins and declarations under other names", {
  "block imported under another name, beside a struct with a field named number": {
    ts: `
      import { contract, Struct, uint256, view, block as chain } from "${PACKAGE}";
      class Counter extends Struct({ number: uint256 }) {}
      export const Blocks = contract("Blocks", { run: view(Counter).returns(uint256, uint256) }).implementation({
        run(counter) { return [chain.number, counter.number]; },
      });
    `,
    solidity: `
      struct Counter {
          uint256 number;
      }

      contract Blocks {
          function run(Counter calldata counter) external view returns (uint256, uint256) {
              return (block.number, counter.number);
          }
      }
    `,
    calls: [{ functionName: "run", args: [{ number: 999n }] }],
  },
  "gasleft imported under another name, beside a helper": {
    ts: `
      import { contract, gasleft as gasRemaining, internal, pure, uint256, view } from "${PACKAGE}";
      const localGas = internal(pure().returns(uint256), () => 7n);
      export const Gas = contract("Gas", { read: view().returns(uint256) }).implementation({
        read() { return gasRemaining() > 100n ? localGas() : 0n; },
      });
    `,
    solidity: `
      contract Gas {
          function read() external view returns (uint256) {
              return gasleft() > 100 ? localGas() : 0;
          }

          function localGas() internal pure returns (uint256) {
              return 7;
          }
      }
    `,
    calls: [{ functionName: "read" }],
  },
  "keccak256 imported under another name, beside a helper imported under another name": {
    ts: {
      "lens.ts": `
        import { bytes, bytes32, contract, keccak256 as digest, pure } from "${PACKAGE}";
        import { syntheticHash as fakeHash } from "./helper.js";
        export const Hashes = contract("Hashes", { run: pure(bytes).returns(bytes32, bytes32) }).implementation({
          run(data) { return [digest(data), fakeHash(data)]; },
        });
      `,
      "helper.ts": `
        import { bytes, bytes32, internal, pure } from "${PACKAGE}";
        export const syntheticHash = internal(pure(bytes).returns(bytes32), (_data) => bytes32(0n));
      `,
    },
    solidity: `
      contract Hashes {
          function run(bytes calldata data) external pure returns (bytes32, bytes32) {
              return (keccak256(data), syntheticHash(data));
          }

          function syntheticHash(bytes memory _data) internal pure returns (bytes32) {
              return bytes32(0);
          }
      }
    `,
    calls: [{ functionName: "run", args: ["0x12"] }],
  },
});

describe("an immutable imported under another name", () => {
  const evm = useEvm();
  const pair: FixturePair = {
    ts: {
      "lens.ts": `${IMPORTS}
        import { r as total } from "./values.js";
        export const Totals = contract("Totals", { run: view(uint256).returns(uint256, uint256) }, { immutables: { total } }).implementation({
          run(x) { return [total, x]; },
        });
      `,
      "values.ts": `import { immutable, uint256 } from "${PACKAGE}";\nexport const r = immutable(uint256);\n`,
    },
    solidity: `
      contract Totals {
          uint256 immutable r;

          constructor(uint256 r_) {
              r = r_;
          }

          function run(uint256 x) external view returns (uint256, uint256) {
              return (r, x);
          }
      }
    `,
  };

  it("the printed Solidity is the handwritten Solidity", async () => {
    expectSameAst(await compilePair(pair));
  });

  it.skipIf(!requireAnvil())("both read the value the constructor was given", async () => {
    const [read] = await expectSameExecution(evm(), withConstructorArgs(await compilePair(pair), [51n]), [
      { functionName: "run", args: [7n] },
    ]);
    expect(read).toEqual({
      status: "success",
      data: `0x${51n.toString(16).padStart(64, "0")}${7n.toString(16).padStart(64, "0")}`,
    });
  });
});

describe("the value a try returns, beside an assigned target that names an immutable", () => {
  const evm = useEvm();
  const pair: FixturePair = {
    ts: {
      "lens.ts": `${IMPORTS}
        import { r as index } from "./slot.js";
        export const Slots = contract(
          "Slots",
          { one: pure().returns(uint256), read: view().returns(uint256) },
          { immutables: { index } },
        ).implementation({
          one() { return 1n; },
          read() {
            const slots = array(uint256, 2n);
            try { slots[Number(index)] = this.one(); } catch {}
            return slots[0];
          },
        });
      `,
      "slot.ts": `import { immutable, uint256 } from "${PACKAGE}";\nexport const r = immutable(uint256);\n`,
    },
    solidity: `
      contract Slots {
          uint256 immutable r;

          constructor(uint256 r_) {
              r = r_;
          }

          function one() external pure returns (uint256) {
              return 1;
          }

          function read() external view returns (uint256) {
              uint256[] memory slots = new uint256[](2);
              try this.one() returns (uint256 r_) {
                  slots[r] = r_;
              } catch {}
              return slots[0];
          }
      }
    `,
  };

  it("the printed Solidity is the handwritten Solidity", async () => {
    expectSameAst(await compilePair(pair));
  });

  it.skipIf(!requireAnvil())("both write the returned value at the immutable's index", async () => {
    const word = (value: bigint) => ({ status: "success", data: `0x${value.toString(16).padStart(64, "0")}` });
    const compiled = await compilePair(pair);
    const read = [{ functionName: "read" }];
    expect(await expectSameExecution(evm(), withConstructorArgs(compiled, [0n]), read)).toEqual([word(1n)]);
    expect(await expectSameExecution(evm(), withConstructorArgs(compiled, [1n]), read)).toEqual([word(0n)]);
  });
});
