/**
 * What the transform refuses about the outside world, each with its message and its TypeScript position,
 * and what solc refuses there, reported at the TypeScript it was printed from.
 */
import { describe, it } from "vitest";

import { expectRejection, positionIn } from "./helpers/harness.js";

const HEAD = `import {
  abi, address, block, bytes, codeSize, contract, gasleft, iface, immutable, internal, keccak256, library, pure,
  require, staticcall, string, Struct, uint160, uint256, view,
} from "@morpho-org/viem-dlc/solidity";

const IFeed = iface({ price: view(address).returns(uint256), latest: view(address).returns(uint256, uint256) });
const FEED = immutable(IFeed);
const WAD = 10n ** 18n;
class Quote extends Struct({ price: uint256 }) {}
const double = internal(pure(uint256).returns(uint256), (x) => x * 2n);
const Fixed = library("Fixed", { half: pure(uint256).returns(uint256) }).implementation({ half: (x) => x / 2n });
`;

/** A lens whose entry point `f(feed, x, data)` has this body. */
const lens = (body: string, { head = "", options = "{ immutables: { FEED } }" } = {}) => `${HEAD}${head}
export const Lens = contract(
  "Lens",
  { f: view(address, uint256, bytes).returns(uint256), g: view(uint256).returns(uint256) },
  ${options},
).implementation({
  f(feed, x, data) {
    ${body}
  },
  g: (x) => x,
});
`;

/** Refused by the transform with this message, at the first occurrence of `at`. */
const refuses = async (code: string, message: string, at: string) =>
  expectRejection(code, { message, ...positionIn(code, at), source: "transform" });

const THIS = "this only calls an entry point: this.f(…)";
const INTERFACE = "an interface is used as I(address) for a handle, I.f.selector for a selector, or I.f.encode(…)";
const HANDLE = "The options of a call are written where the call is made: I(address, { gas: … }).f(…)";
const OPTIONS = "the options of a call are written { gas: … }";
const TRY = "the block of a try holds one external call, bare or assigned";
const NOT_COPIED = "this local's type cannot be copied from its initializer";

describe("rejections: this", () => {
  it("as a value", async () => {
    await refuses(lens("return uint256(uint160(address(this))) + x;"), THIS, "this)");
  });

  it("as the object of anything but a call", async () => {
    await refuses(lens("return this.g == this.g ? x : 0n;"), THIS, "this.g ==");
  });
});

describe("rejections: immutables", () => {
  it("one a body reads and the contract does not list", async () => {
    await refuses(
      lens("return FEED.price(feed) + x;", { options: "{}" }),
      '"FEED" is not listed in the immutables of Lens',
      "FEED.price(feed)",
    );
  });

  it("one the contract lists twice", async () => {
    await refuses(
      lens("return FEED.price(feed) + x;", { options: "{ immutables: { first: FEED, second: FEED } }" }),
      '"second" lists an immutable the contract already lists',
      "FEED }",
    );
  });
});

describe("rejections: declarations", () => {
  it("an immutables entry that does not name an immutable", async () => {
    await refuses(
      lens("return x;", { options: "{ immutables: { WAD } }" }),
      '"WAD" must name a module-level immutable(…)',
      "WAD }",
    );
  });

  it("an immutable of something that is not a type", async () => {
    await refuses(
      lens("return x;", { head: "const NAME = immutable(WAD);", options: "{ immutables: { NAME } }" }),
      "this is not a type",
      "WAD);",
    );
  });

  it("an interface whose functions are not written in place", async () => {
    await refuses(
      lens("return IOther(feed).price(feed);", {
        head: "const functions = { price: view(address).returns(uint256) };\nconst IOther = iface(functions);",
      }),
      "the functions of an interface must be an object literal written in place",
      "functions);",
    );
  });

  it("an interface function that is not a signature", async () => {
    await refuses(
      lens("return IOther(feed).price(feed);", { head: "const IOther = iface({ price: uint256 });" }),
      "a signature is written view(…) or pure(…)",
      "uint256 });",
    );
  });

  const library = (declaration: string) =>
    lens("return Other.f(x);", { head: `const NAME = "Other";\nconst Other = ${declaration};` });

  it("a library whose name is not a string literal", async () => {
    await refuses(
      library("library(NAME, { f: pure(uint256).returns(uint256) }).implementation({ f: (y) => y })"),
      "the first argument of library(…) must be a string literal: its name",
      "NAME, {",
    );
  });

  it("a library whose functions are not written in place", async () => {
    await refuses(
      library('library("Other", { f: pure(uint256).returns(uint256) }).implementation(Fixed)'),
      "library(name, { … }).implementation({ … }) takes object literals written in place",
      'library("Other"',
    );
  });

  it("a library that declares a function and does not implement it", async () => {
    await refuses(
      library('library("Other", { f: pure(uint256).returns(uint256) }).implementation({})'),
      'library Other declares "f" and does not implement it',
      "f: pure(uint256).returns(uint256) }).implementation({})",
    );
  });

  it("a library that implements a function it does not declare", async () => {
    await refuses(
      library('library("Other", { f: pure(uint256).returns(uint256) }).implementation({ f: (y) => y, h: (y) => y })'),
      'library Other implements "h" and does not declare its signature',
      "h: (y)",
    );
  });
});

describe("rejections: libraries", () => {
  const library = (signature: string, body: string) =>
    lens("return Outside.f(x);", {
      head: `const Outside = library("Outside", { f: ${signature} }).implementation({ f: ${body} });`,
    });

  it("a library function that calls a helper of the contract", async () => {
    await refuses(
      library("pure(uint256).returns(uint256)", "(y) => double(y)"),
      'a library function cannot use "double"',
      "double(y)",
    );
  });

  it("a library function that reads an immutable of the contract", async () => {
    await refuses(
      library("view(uint256).returns(uint256)", "(y) => y + FEED.price(address(0))"),
      'a library function cannot use "FEED"',
      "FEED.price(address(0))",
    );
  });
});

describe("rejections: interfaces and handles", () => {
  it("a handle made with options and not called there", async () => {
    await refuses(
      lens("const capped = IFeed(feed, { gas: 1n });\n    return capped.price(feed);"),
      HANDLE,
      "IFeed(feed",
    );
  });

  it.each([
    ["no address", "IFeed()"],
    ["more than an address and options", "IFeed(feed, { gas: 1n }, x)"],
  ])("a handle made from %s", async (_, handle) => {
    await refuses(lens(`return ${handle}.price(feed);`), HANDLE, handle);
  });

  it.each([
    ["another key", "{ value: 1n }"],
    ["two keys", "{ gas: 1n, gas: 2n }"],
    ["no key", "{}"],
    ["a computed key", '{ ["gas"]: 1n }'],
    ["a method", "{ gas() {} }"],
    ["an accessor", "{ get gas() { return 1n; } }"],
    ["a spread", "{ ...x }"],
    ["something that is not an object literal", "x"],
  ])("call options with %s", async (_, options) => {
    await refuses(lens(`return IFeed(feed, ${options}).price(feed);`), OPTIONS, `${options}).price`);
    await refuses(lens(`staticcall(feed, data, ${options});\n    return x;`), OPTIONS, `${options});`);
  });

  it("an interface as a value", async () => {
    await refuses(lens("return IFeed.price(feed) + x;"), INTERFACE, "IFeed.price(feed)");
  });

  it("a function of an interface as a value", async () => {
    await refuses(lens("return IFeed.price == IFeed.latest ? x : 0n;"), INTERFACE, "IFeed.price ==");
  });

  it("a member of a function of an interface other than its selector", async () => {
    await refuses(lens("return uint256(IFeed.price.length) + x;"), INTERFACE, "IFeed.price.length");
  });

  it("a call on a function of an interface other than encode", async () => {
    await refuses(lens("return uint256(IFeed.price.decode(data)) + x;"), INTERFACE, "IFeed.price.decode");
  });

  it("a call under a computed name", async () => {
    await refuses(
      lens('return FEED["price"](feed) + x;'),
      "a lens body can call helpers, interface functions",
      "FEED[",
    );
  });

  it("a call that spreads its arguments", async () => {
    await refuses(lens("return FEED.price(...[feed]) + x;"), "a call cannot spread its arguments", "...[feed]");
  });

  it("a library as a value", async () => {
    await refuses(
      lens("return Fixed == Fixed ? x : 0n;"),
      '"Fixed" is a library and cannot be used as a value here',
      "Fixed ==",
    );
  });
});

describe("rejections: built-ins", () => {
  it.each([
    ["abi.encodePacked", "abi.encodePacked(x)", "a lens cannot call abi.encodePacked"],
    ["block.timestamp, called", "block.timestamp()", "a lens cannot call block.timestamp"],
    ["abi.decode without its types", "abi.decode(data)", "abi.decode takes the data and the types to read"],
    ["abi.decode with types not listed in place", "abi.decode(data, uint256)", "abi.decode takes the data"],
    ["abi.decode with a third argument", "abi.decode(data, [uint256], x)", "abi.decode takes the data"],
    ["staticcall without calldata", "staticcall(feed)", "staticcall takes the target, the calldata and, optionally"],
    ["staticcall with a fourth argument", "staticcall(feed, data, { gas: 1n }, x)", "staticcall takes the target"],
    ["codeSize of nothing", "codeSize()", "codeSize takes one account"],
    ["codeSize of two accounts", "codeSize(feed, feed)", "codeSize takes one account"],
    ["a declaration built-in", "view(x)", "a lens body cannot call view"],
    ["a built-in that is not a function", "abi(x)", "a lens body cannot call abi"],
  ])("%s", async (_, call, message) => {
    await refuses(lens(`${call};\n    return x;`), message, call);
  });

  it.each([
    ["a member block does not have", "block.coinbase", "a lens cannot read block.coinbase"],
    ["a member of abi, not called", "abi.encode", "a lens cannot read abi.encode"],
    ["a built-in function, not called", "keccak256", '"keccak256" cannot be used as a value here'],
    ["a built-in object", "block", '"block" cannot be used as a value here'],
  ])("%s", async (_, value, message) => {
    await refuses(lens(`return ${value} == ${value} ? x : 0n;`), message, `${value} ==`);
  });

  it("an opcode outside assembly", async () => {
    const head = 'import { add } from "@morpho-org/viem-dlc/solidity/yul";';
    await refuses(
      lens("add(x, 1n);\n    return x;", { head }),
      "add(…) is an opcode, and an opcode is called inside an assembly block",
      "add(x, 1n)",
    );
    await refuses(lens("const y = add(x, 1n);\n    return y;", { head }), NOT_COPIED, "add(x, 1n)");
  });

  it("abi.decode of something that is not a type", async () => {
    await refuses(lens("return abi.decode(data, [x]);"), "this is not a type", "x]");
  });

  it("abi.decode with a hole in its types", async () => {
    await refuses(
      lens("abi.decode(data, [uint256, , address]);\n    return x;"),
      "a list of types has no holes",
      "[uint256",
    );
  });

  it.each([
    ["a constant", "WAD"],
    ["a struct", "Quote"],
    ["an immutable", "FEED"],
    ["a library", "Fixed"],
  ])("a call to %s", async (kind, name) => {
    await refuses(lens(`${name}(x);\n    return x;`), `"${name}" is ${kind} and cannot be called`, `${name}(x)`);
  });
});

describe("rejections: try and catch", () => {
  const attempt = (block: string, rest = "catch {}") => lens(`try ${block} ${rest}\n    return x;`);

  it("a finally", async () => {
    await refuses(attempt("{ x = FEED.price(feed); }", "catch {} finally { x = 1n; }"), TRY, "try {");
  });

  it("no catch", async () => {
    await refuses(attempt("{ x = FEED.price(feed); }", "finally { x = 1n; }"), TRY, "try {");
  });

  it("a catch that binds the error", async () => {
    await refuses(attempt("{ x = FEED.price(feed); }", "catch (error) {}"), "catch binds nothing in a lens", "error)");
  });

  it("a local of the catch block, read after the block", async () => {
    const code = attempt("{ x = FEED.price(feed); }", "catch { const fallen = 1n; x = fallen; }\n    x += fallen;");
    await refuses(code, '"fallen" is not a module-level name', "fallen;\n    return");
  });

  it.each([
    ["nothing", "{ ; }"],
    ["two statements", "{ x = FEED.price(feed); x = FEED.price(feed); }"],
    ["a declaration", "{ const price = FEED.price(feed); }"],
    ["a statement that is not an expression", "{ if (x > 0n) x = FEED.price(feed); }"],
    ["an expression that is not a call", "{ x++; }"],
    ["a compound assignment", "{ x += FEED.price(feed); }"],
    ["a call to a helper", "{ x = double(x); }"],
    ["a call to a library", "{ x = Fixed.half(x); }"],
    ["a call to a built-in", "{ x = gasleft(); }"],
    ["a call to a member of a built-in", "{ abi.encode(x); }"],
    ["an encoded call", "{ IFeed.price.encode(feed); }"],
    ["a call under a computed name", '{ x = FEED["price"](feed); }'],
  ])("a block that holds %s", async (_, block) => {
    await refuses(attempt(block), TRY, block);
  });

  it("a block that holds a call to a built-in named through a namespace", async () => {
    const code = lens("try { x = sol.gasleft(); } catch {}\n    return x;", {
      head: 'import * as sol from "@morpho-org/viem-dlc/solidity";',
    });
    await refuses(code, TRY, "{ x = sol.gasleft(); }");
  });

  it("an assigned call that returns two values", async () => {
    await refuses(attempt("{ x = FEED.latest(feed); }"), "a try assigns the one value its call returns", "FEED.latest");
  });

  it("an assigned call whose function is not declared in the source", async () => {
    await refuses(
      attempt("{ x = (x > 0n ? FEED : IFeed(feed)).price(feed); }"),
      "the call must be to a declared function with exactly one return value",
      "(x > 0n",
    );
  });
});

describe("rejections: the type of a local initialized by a call", () => {
  it.each([
    ["an interface", "FEED.nope(feed)", 'interface IFeed declares no function "nope"'],
    ["a handle made in place", "IFeed(feed).nope(feed)", 'interface IFeed declares no function "nope"'],
    ["a library", "Fixed.nope(x)", 'library Fixed declares no function "nope"'],
    ["the lens", "this.nope(x)", 'contract Lens declares no function "nope"'],
  ])("a function %s does not declare", async (_, call, message) => {
    await refuses(lens(`const y = ${call};\n    return y;`), message, call);
  });

  it.each([
    ["an interface function", "FEED.latest(feed)"],
    ["staticcall", "staticcall(feed, data)"],
    ["abi.decode", "abi.decode(data, [uint256, address])"],
  ])("%s that returns two values is not one local", async (_, initializer) => {
    await refuses(
      lens(`const y = ${initializer};\n    return x;`),
      "a local takes one value and this call returns 2. Several values are declared together",
      initializer,
    );
  });

  it.each([
    ["a call through something with no declared type", "(x > 0n ? FEED : IFeed(feed)).price(feed)"],
    ["a function of an interface", "IFeed.price"],
    ["a member of a function of an interface other than its selector", "IFeed.price.length"],
    ["a member block does not have", "block.coinbase"],
    ["a call on a function of an interface other than encode", "IFeed.price.decode(data)"],
    ["abi.decode without its types", "abi.decode(data)"],
    ["a call to a member abi does not have", "abi.encodePacked(x)"],
    ["a call to a built-in that returns nothing", "require(x > 0n)"],
    ["a call to a struct", "Quote(x)"],
    ["a call under a computed name", 'FEED["price"](feed)'],
  ])("%s has no type to copy", async (_, initializer) => {
    await refuses(lens(`const y = ${initializer};\n    return x;`), NOT_COPIED, initializer);
  });
});

describe("rejections: solc's, at the TypeScript they were printed from", () => {
  const solc = async (code: string, message: string, at: string) =>
    expectRejection(code, { message, ...positionIn(code, at), source: "solc" });

  it("an argument of the wrong type in a call through a handle", async () => {
    await solc(lens("return FEED.price(x);"), "Invalid implicit conversion from uint256 to address requested", "x);");
  });

  it("a function the interface does not declare", async () => {
    await solc(lens("FEED.nope(feed);\n    return x;"), 'Member "nope" not found', "FEED.nope(feed)");
  });

  it("an entry point the lens does not have", async () => {
    await solc(lens("this.nope(x);\n    return x;"), 'Member "nope" not found', "this.nope(x)");
  });

  it("gas that is not a number", async () => {
    await solc(
      lens("return IFeed(feed, { gas: feed }).price(feed);"),
      "Type address is not implicitly convertible to expected type uint256",
      "feed }).price",
    );
  });

  it("an immutable of a type that cannot be one", async () => {
    await solc(
      lens("return x;", { head: "const NAME = immutable(string);", options: "{ immutables: { NAME } }" }),
      "Immutable variables cannot have a non-value type",
      "immutable(string)",
    );
  });

  it("a helper of the contract a library reaches through a call the transform does not follow", async () => {
    await solc(
      lens("return Outside.f(x);", {
        head: `const Outside = library("Outside", { f: view(uint256).returns(uint256) }).implementation({ f: (y) => this.g(y) });`,
      }),
      'Member "g" not found',
      "this.g(y)",
    );
  });
});
