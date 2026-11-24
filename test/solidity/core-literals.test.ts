/**
 * The spelling of a literal: integers, strings, hex literals and address literals. Respelling a literal
 * is not arithmetic, so no value is computed.
 */
import { describe, expect, it } from "vitest";

import { describePairs, type ExecutedPair, IMPORTS } from "./helpers/core.js";
import { buildSnippet, expectRejection, positionIn } from "./helpers/harness.js";

const OWNER = "0x52908400098527886E0F7030069857D2E4169EE7";
const WORD = "0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

const pairs: Record<string, ExecutedPair> = {
  "integer literals": {
    ts: `${IMPORTS}
      export const IntegerLens = contract("IntegerLens", {
        decimal: pure(uint256).returns(uint256),
        separated: pure().returns(uint256, uint256, uint256),
        hexadecimal: pure().returns(uint256, uint256, uint256, uint8),
        largest: pure().returns(uint256),
        exact: pure().returns(uint256),
      }).implementation({
        // With the n suffix and without it.
        decimal: (x) => x + 0n + 7 + 1000000n,
        // A numeric separator is dropped.
        separated: () => [60_000, 1_000_000n, 0xff_ffn],
        // The digits of a hexadecimal literal are kept as written.
        hexadecimal: () => [0x1fn, 0xDEADbeefn, 0X0an, 0x00ff],
        largest: () => 115792089237316195423570985008687907853269984665640564039457584007913129639935n,
        // An expression made only of literals is exact in Solidity, and it is printed, never computed.
        exact: () => (7n / 2n) * 2n,
      });
    `,
    solidity: `
      contract IntegerLens {
          function decimal(uint256 x) external pure returns (uint256) {
              return x + 0 + 7 + 1000000;
          }

          function separated() external pure returns (uint256, uint256, uint256) {
              return (60000, 1000000, 0xffff);
          }

          function hexadecimal() external pure returns (uint256, uint256, uint256, uint8) {
              return (0x1f, 0xDEADbeef, 0x0a, 0x00ff);
          }

          function largest() external pure returns (uint256) {
              return 115792089237316195423570985008687907853269984665640564039457584007913129639935;
          }

          function exact() external pure returns (uint256) {
              return 7 / 2 * 2;
          }
      }
    `,
    calls: [
      { functionName: "decimal", args: [1n] },
      { functionName: "separated" },
      { functionName: "hexadecimal" },
      { functionName: "largest" },
      { functionName: "exact" },
    ],
  },
  "string literals, inside printable ASCII and outside it": {
    // The snippets are raw, so a backslash below is a backslash of the TypeScript and of the Solidity.
    ts: String.raw`${IMPORTS}
      export const StringLens = contract("StringLens", {
        plain: pure().returns(string, string, string),
        escaped: pure().returns(string, string),
        control: pure().returns(string),
        accented: pure().returns(string, string),
        astral: pure().returns(string),
        lineBreaks: pure().returns(string),
        checked: pure(uint256).returns(uint256),
      }).implementation({
        plain: () => ["", "text", 'single quoted, with a " in it'],
        escaped: () => ["a \"quoted\" word and a back\\slash", "it's"],
        // Control characters are written as escapes: Solidity takes none of them raw.
        control: () => "tab\there\nnew line\r\0\x01\x1f\x7f end",
        // Anything outside ASCII makes the literal a unicode literal.
        accented: () => ["héllo wörld", "\u00e9 written as an escape"],
        astral: () => "日本語 and 😀 and \u{1F600}",
        // Solidity's scanner ends a line on these three, so they are escaped inside a unicode literal.
        lineBreaks: () => "next\u0085line\u2028separator\u2029paragraph ✓",
        checked(x) {
          require(x > 0n, "x is zéro");
          if (x > 10n) revert("x > 10 ⚠");
          return x;
        },
      });
    `,
    solidity: String.raw`
      contract StringLens {
          function plain() external pure returns (string memory, string memory, string memory) {
              return ("", "text", 'single quoted, with a " in it');
          }

          function escaped() external pure returns (string memory, string memory) {
              return ("a \"quoted\" word and a back\\slash", "it's");
          }

          function control() external pure returns (string memory) {
              return "tab\there\nnew line\r\x00\x01\x1f\x7f end";
          }

          function accented() external pure returns (string memory, string memory) {
              return (unicode"héllo wörld", unicode"é written as an escape");
          }

          function astral() external pure returns (string memory) {
              return unicode"日本語 and 😀 and 😀";
          }

          function lineBreaks() external pure returns (string memory) {
              return unicode"next\u0085line\u2028separator\u2029paragraph ✓";
          }

          function checked(uint256 x) external pure returns (uint256) {
              require(x > 0, unicode"x is zéro");
              if (x > 10) revert(unicode"x > 10 ⚠");
              return x;
          }
      }
    `,
    calls: [
      { functionName: "plain" },
      { functionName: "escaped" },
      { functionName: "control" },
      { functionName: "accented" },
      { functionName: "astral" },
      { functionName: "lineBreaks" },
      { functionName: "checked", args: [5n] },
      { functionName: "checked", args: [0n], reverts: true },
      { functionName: "checked", args: [11n], reverts: true },
    ],
  },
  "hex literals and address literals": {
    ts: `${IMPORTS}
      const OWNER = "${OWNER}";

      export const HexLens = contract("HexLens", {
        selector: pure().returns(bytes4, bytes4),
        word: pure().returns(bytes32),
        raw: pure().returns(bytes, bytes, bytes),
        owner: pure().returns(address, address),
        isOwner: pure(address).returns(bool),
        notHex: pure().returns(string, string, string),
      }).implementation({
        // A string of 0x and an even number of hexadecimal digits is a hex literal.
        selector() {
          const named = bytes4("0x313ce567");
          return ["0x313ce567", named];
        },
        word: () => "${WORD}",
        raw: () => ["0x", "0xDEADbeef00", "0x00"],
        // With 40 digits it is an address literal, and a local declared from one is an address.
        owner() {
          const holder = "${OWNER}";
          return [holder, OWNER];
        },
        isOwner: (who) => who == "${OWNER}",
        // Anything else is a string: an odd number of digits, a digit that is not hexadecimal, a capital X.
        notHex: () => ["0x123", "0xzz", "0X12"],
      });
    `,
    solidity: `
      contract HexLens {
          function selector() external pure returns (bytes4, bytes4) {
              bytes4 named = bytes4(hex"313ce567");
              return (hex"313ce567", named);
          }

          function word() external pure returns (bytes32) {
              return hex"${WORD.slice(2)}";
          }

          function raw() external pure returns (bytes memory, bytes memory, bytes memory) {
              return (hex"", hex"DEADbeef00", hex"00");
          }

          function owner() external pure returns (address, address) {
              address holder = ${OWNER};
              return (holder, ${OWNER});
          }

          function isOwner(address who) external pure returns (bool) {
              return who == ${OWNER};
          }

          function notHex() external pure returns (string memory, string memory, string memory) {
              return ("0x123", "0xzz", "0X12");
          }
      }
    `,
    calls: [
      { functionName: "selector" },
      { functionName: "word" },
      { functionName: "raw" },
      { functionName: "owner" },
      { functionName: "isOwner", args: [OWNER] },
      { functionName: "isOwner", args: ["0x0000000000000000000000000000000000000001"] },
      { functionName: "notHex" },
    ],
  },
};

describePairs("literals", pairs);

describe("literals: what is printed", () => {
  it("is the literal respelled, with nothing computed", async () => {
    const built = await buildSnippet(pairs["integer literals"]?.ts ?? "");
    const solidity = built.lenses[0]?.printed.solidity;
    expect(solidity).toContain("return ((x + 0) + 7) + 1000000;");
    expect(solidity).toContain("return (60000, 1000000, 0xffff);");
    expect(solidity).toContain("return (0x1f, 0xDEADbeef, 0x0a, 0x00ff);");
    expect(solidity).toContain("return (7 / 2) * 2;");
  });

  it("is an escape for every character Solidity does not take raw", async () => {
    const built = await buildSnippet(pairs["string literals, inside printable ASCII and outside it"]?.ts ?? "");
    const solidity = built.lenses[0]?.printed.solidity;
    expect(solidity).toContain(String.raw`return "tab\x09here\x0anew line\x0d\x00\x01\x1f\x7f end";`);
    expect(solidity).toContain(String.raw`"a \"quoted\" word and a back\\slash"`);
    expect(solidity).toContain(String.raw`unicode"next\u0085line\u2028separator\u2029paragraph ✓"`);
    expect(solidity).toContain('unicode"日本語 and 😀 and 😀"');
  });
});

describe("literals: rejections", () => {
  const lens = (body: string, returns = "uint256") => `${IMPORTS}
export const Lens = contract("Lens", { f: pure(uint256).returns(${returns}) }).implementation({
  f(x) {
    ${body}
  },
});
`;

  it("an octal literal", async () => {
    const code = lens("return x + 0o17n;");
    await expectRejection(code, {
      message: "an integer literal is written in decimal or in hexadecimal",
      ...positionIn(code, "0o17n"),
    });
  });

  it("a binary literal", async () => {
    const code = lens("return x + 0b101n;");
    await expectRejection(code, {
      message: "an integer literal is written in decimal or in hexadecimal",
      ...positionIn(code, "0b101n"),
    });
  });

  it("a literal with an exponent", async () => {
    const code = lens("return x + 1e18;");
    await expectRejection(code, {
      message: "an integer literal is written in decimal or in hexadecimal",
      ...positionIn(code, "1e18"),
    });
  });

  it("a literal with a fraction", async () => {
    const code = lens("return x + 1.5;");
    await expectRejection(code, {
      message: "an integer literal is written in decimal or in hexadecimal",
      ...positionIn(code, "1.5"),
    });
  });

  it("null", async () => {
    const code = lens("return x + null;");
    await expectRejection(code, { message: "a lens has no such literal", ...positionIn(code, "null") });
  });

  it("a regular expression", async () => {
    const code = lens("return x + /x/;");
    await expectRejection(code, { message: "a lens has no such literal", ...positionIn(code, "/x/") });
  });

  it("a template literal", async () => {
    const code = lens("return `text`;", "string");
    await expectRejection(code, {
      message: "cannot contain this expression (TemplateLiteral)",
      ...positionIn(code, "`text`"),
    });
  });

  it("a string that holds half of a surrogate pair", async () => {
    const code = lens(String.raw`return "broken \ud83d";`, "string");
    await expectRejection(code, {
      message: "a string literal is well-formed Unicode",
      ...positionIn(code, '"broken'),
    });
  });

  it("a local declared from a hex literal has no type to copy", async () => {
    const code = lens('const selector = "0x313ce567";\n    return x;');
    await expectRejection(code, { message: "name the type with a conversion", ...positionIn(code, '"0x313ce567"') });
  });

  it("solc checks the checksum of an address literal, at the TypeScript position", async () => {
    const code = lens('return "0x52908400098527886e0f7030069857d2e4169ee7";', "address");
    await expectRejection(code, {
      message: "This looks like an address but has an invalid checksum.",
      ...positionIn(code, '"0x5290'),
      source: "solc",
    });
  });

  it("solc reports an error that follows a unicode literal at the right column", async () => {
    const code = lens('const text = "日本語 😀"; const narrow = uint8(1n); narrow = x;\n    return x;');
    await expectRejection(code, {
      message: "Type uint256 is not implicitly convertible to expected type uint8.",
      ...positionIn(code, "x;\n"),
      source: "solc",
    });
  });
});
