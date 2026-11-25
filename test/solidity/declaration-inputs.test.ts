/**
 * What a declaration is given: every argument and every key is read, or is refused where it is written.
 * An argument past the ones a builder takes, or a key written a second time, would otherwise be dropped
 * without a word, and the Solidity would be compiled from part of what the TypeScript says.
 */
import { describe, it } from "vitest";

import { expectRejection, positionIn } from "./helpers/harness.js";

const PACKAGE = "@morpho-org/viem-dlc/solidity";
const IMPORTS = `import { bool, contract, iface, immutable, internal, library, pure, Struct, uint256, view } from "${PACKAGE}";`;
const READ = "{ read: pure().returns(uint256) }";
const lens = (declaration: string, signatures: string, implementation: string) =>
  `${IMPORTS}\n${declaration}\nexport const Lens = contract("Lens", ${signatures}).implementation(${implementation});\n`;

/** [what is refused, the module, the message, where the error points] */
type Case = readonly [name: string, code: string, message: string, at: string];

const refuses = (cases: readonly Case[]) => {
  for (const [name, code, message, at] of cases)
    it(`refuses ${name}`, async () => {
      await expectRejection(code, { message, ...positionIn(code, at), source: "transform" });
    });
};

describe("an argument a builder does not take", () => {
  refuses([
    [
      "a third argument of internal",
      lens("const four = internal(pure().returns(uint256), () => 4n, 111);", READ, "{ read: () => four() }"),
      "internal(…) takes 2 arguments, and this is one more",
      "111",
    ],
    [
      "a fourth argument of contract",
      lens("", `${READ}, {}, 222`, "{ read: () => 1n }"),
      "contract(…) takes 3 arguments, and this is one more",
      "222",
    ],
    [
      "a second argument of a contract's implementation",
      lens("", READ, "{ read: () => 1n }, 333"),
      ".implementation(…) takes 1 argument, and this is one more",
      "333",
    ],
    [
      "a third argument of library",
      lens(
        'const Maths = library("Maths", { four: pure().returns(uint256) }, 444).implementation({ four: () => 4n });',
        READ,
        "{ read: () => Maths.four() }",
      ),
      "library(…) takes 2 arguments, and this is one more",
      "444",
    ],
    [
      "a second argument of a library's implementation",
      lens(
        'const Maths = library("Maths", { four: pure().returns(uint256) }).implementation({ four: () => 4n }, 555);',
        READ,
        "{ read: () => Maths.four() }",
      ),
      ".implementation(…) takes 1 argument, and this is one more",
      "555",
    ],
    [
      "a second argument of iface",
      lens(
        "const Feed = iface({ price: view().returns(uint256) }, 666);",
        "{ read: view(Feed).returns(uint256) }",
        "{ read: (feed) => feed.price() }",
      ),
      "iface(…) takes 1 argument, and this is one more",
      "666",
    ],
    [
      "a second argument of immutable",
      lens(
        "const SCALE = immutable(uint256, 777);",
        "{ read: view().returns(uint256) }, { immutables: { scale: SCALE } }",
        "{ read: () => SCALE }",
      ),
      "immutable(…) takes 1 argument, and this is one more",
      "777",
    ],
    [
      "a second argument of Struct",
      lens(
        "class Pair extends Struct({ left: uint256 }, 888) {}",
        "{ read: pure(Pair).returns(uint256) }",
        "{ read: (pair) => pair.left }",
      ),
      "Struct(…) takes 1 argument, and this is one more",
      "888",
    ],
  ]);
});

describe("a key written twice", () => {
  refuses([
    [
      "two implementations of one entry point",
      lens("", READ, "{\n  read() { return 11n; },\n  read() { return 22n; },\n}"),
      '"read" is written twice in the implementation of contract Lens',
      "read() { return 22n; }",
    ],
    [
      "two signatures of one entry point",
      lens("", "{ read: pure().returns(uint256), read: view().returns(uint256) }", "{ read: () => 1n }"),
      '"read" is written twice in the declaration of contract Lens',
      "read: view()",
    ],
    [
      "two implementations of one library function",
      lens(
        'const Maths = library("Maths", { four: pure().returns(uint256) }).implementation({ four: () => 4n, "four": () => 5n });',
        READ,
        "{ read: () => Maths.four() }",
      ),
      '"four" is written twice in the implementation of library Maths',
      '"four": () => 5n',
    ],
    [
      "two signatures of one library function",
      lens(
        'const Maths = library("Maths", { four: pure().returns(uint256), four: view().returns(uint256) }).implementation({ four: () => 4n });',
        READ,
        "{ read: () => Maths.four() }",
      ),
      '"four" is written twice in the declaration of library Maths',
      "four: view()",
    ],
    [
      "two fields of one name in a struct",
      lens(
        "class Pair extends Struct({ left: uint256, left: bool }) {}",
        "{ read: pure(Pair).returns(uint256) }",
        "{ read: (pair) => pair.left }",
      ),
      '"left" is written twice in the fields of a struct',
      "left: bool",
    ],
    [
      "two functions of one name in an interface",
      lens(
        "const Feed = iface({ price: view().returns(uint256), price: pure().returns(uint256) });",
        "{ read: view(Feed).returns(uint256) }",
        "{ read: (feed) => feed.price() }",
      ),
      '"price" is written twice in the functions of an interface',
      "price: pure()",
    ],
    [
      "two immutables under one key",
      lens(
        "const SCALE = immutable(uint256);\nconst OTHER = immutable(uint256);",
        "{ read: view().returns(uint256) }, { immutables: { scale: SCALE, scale: OTHER } }",
        "{ read: () => SCALE + OTHER }",
      ),
      '"scale" is written twice in the immutables of a contract',
      "scale: OTHER",
    ],
    [
      "one option twice",
      lens(
        "const SCALE = immutable(uint256);",
        "{ read: view().returns(uint256) }, { immutables: {}, immutables: { scale: SCALE } }",
        "{ read: () => SCALE }",
      ),
      '"immutables" is written twice in the options of contract(…)',
      "immutables: { scale",
    ],
  ]);
});
