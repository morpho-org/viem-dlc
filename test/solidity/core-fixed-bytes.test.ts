/**
 * Conversions to the fixed-size bytes types, spelled as in Solidity: from another fixed-size bytes
 * type, from `bytes`, from an integer of the same size and from a literal. The literal is a quoted
 * hexadecimal string, or zero: solc reads the bytes of any other number off its spelling, which a
 * compiler is free to change.
 */
import { beforeAll, describe, expect, it } from "vitest";

import { describePairs, type ExecutedPair, IMPORTS } from "./helpers/core.js";
import { emitJavaScript } from "./helpers/emit.js";
import { buildSnippet, expectRejection, positionIn } from "./helpers/harness.js";

const WORD = "0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

const pairs: Record<string, ExecutedPair> = {
  "conversions to bytes4 and to bytes32": {
    ts: `${IMPORTS}
      import { uint32 } from "@morpho-org/viem-dlc/solidity";
      export const BytesLens = contract("BytesLens", {
        prefix: pure(bytes32).returns(bytes4),
        widen: pure(bytes4).returns(bytes32),
        same: pure(bytes4, bytes32).returns(bytes4, bytes32),
        head: pure(bytes).returns(bytes4, bytes32),
        literal: pure().returns(bytes4, bytes32, bytes4, bytes32),
        integer: pure(uint32, uint256).returns(bytes4, bytes32),
      }).implementation({
        // A wider type is cut to its first bytes, and a narrower one is padded on the right.
        prefix: (word) => bytes4(word),
        widen: (selector) => bytes32(selector),
        same: (selector, word) => [bytes4(selector), bytes32(word)],
        head: (data) => [bytes4(data), bytes32(data)],
        // Zero is every byte zero in any spelling, as a Number and as a bigint.
        literal: () => [bytes4("0x313ce567"), bytes32(0), bytes4(0x00_00n), bytes32(Number(0n))],
        integer: (small, large) => [bytes4(small), bytes32(large)],
      });
    `,
    solidity: `
      contract BytesLens {
          function prefix(bytes32 word) external pure returns (bytes4) {
              return bytes4(word);
          }

          function widen(bytes4 selector) external pure returns (bytes32) {
              return bytes32(selector);
          }

          function same(bytes4 selector, bytes32 word) external pure returns (bytes4, bytes32) {
              return (bytes4(selector), bytes32(word));
          }

          function head(bytes calldata data) external pure returns (bytes4, bytes32) {
              return (bytes4(data), bytes32(data));
          }

          function literal() external pure returns (bytes4, bytes32, bytes4, bytes32) {
              return (bytes4(hex"313ce567"), bytes32(0), bytes4(0x0000), bytes32(0));
          }

          function integer(uint32 small, uint256 large) external pure returns (bytes4, bytes32) {
              return (bytes4(small), bytes32(large));
          }
      }
    `,
    calls: [
      { functionName: "prefix", args: [WORD] },
      { functionName: "widen", args: ["0x313ce567"] },
      { functionName: "same", args: ["0x313ce567", WORD] },
      { functionName: "head", args: [WORD] },
      { functionName: "head", args: ["0x0102"] },
      { functionName: "literal" },
      { functionName: "integer", args: [0x313ce567, 2n ** 255n + 1n] },
    ],
  },
};

describePairs("conversions to fixed-size bytes", pairs);

const NUMBER = (type: string) =>
  `a number converted to ${type} is read by its spelling, which a compiler may change. Write the bytes as a quoted hexadecimal string: ${type}("0x…")`;

describe("a number converted to fixed-size bytes", () => {
  it.each([
    { what: "a Number in hexadecimal", type: "bytes4", argument: "0x313ce567" },
    { what: "a Number with separators", type: "bytes4", argument: "0x00_00_00_01" },
    { what: "a bigint in hexadecimal", type: "bytes4", argument: "0x00000001n" },
    { what: "a bigint in decimal", type: "bytes32", argument: "1n" },
    { what: "a number in parentheses", type: "bytes4", argument: "(0x00000001n)" },
    { what: "a number inside Number(…), which prints as the number", type: "bytes4", argument: "Number(0x00000001n)" },
    { what: "a module constant that is a number", type: "bytes4", argument: "SELECTOR" },
    { what: "a module constant that is another one", type: "bytes4", argument: "SAME" },
  ])("is refused at the conversion's argument: $what", async ({ type, argument }) => {
    const code = `${IMPORTS}
const SELECTOR = 0x00000001n;
const SAME = SELECTOR;
export const Lens = contract("Lens", { f: pure().returns(${type}) }).implementation({
  f: () => ${type}(${argument}),
});
`;
    await expectRejection(code, { message: NUMBER(type), ...positionIn(code, `${argument}),`) });
  });

  describe("in a helper as tsc emits it for ES2020", () => {
    const lens = `${IMPORTS}
import { pick } from "./helper.js";
export const Lens = contract("Lens", { read: pure().returns(bytes4) }).implementation({ read: () => pick() });
`;
    const helper = (argument: string) => `import { bytes4, internal, pure } from "@morpho-org/viem-dlc/solidity";
export const pick = internal(pure().returns(bytes4), () => bytes4(${argument}));
`;
    const sources = { number: helper("0x00_00_00_01"), quoted: helper('"0x00000001"') };
    let emitted: typeof sources;
    beforeAll(() => {
      emitted = emitJavaScript(sources, "es2020");
    });

    it("is spelled another way, and is refused from the source and from the emitted JavaScript alike", async () => {
      expect(emitted.number).toContain("bytes4(1)");
      await expectRejection(
        { "lens.ts": lens, "helper.ts": sources.number },
        { message: NUMBER("bytes4"), file: "helper.ts", ...positionIn(sources.number, "0x00_00_00_01") },
      );
      await expectRejection(
        { "lens.ts": lens, "helper.js": emitted.number },
        { message: NUMBER("bytes4"), file: "helper.js", ...positionIn(emitted.number, "1)") },
      );
    });

    it("written as a quoted string, prints the same Solidity from both", async () => {
      const fromSource = await buildSnippet({ "lens.ts": lens, "helper.ts": sources.quoted });
      const fromEmitted = await buildSnippet({ "lens.ts": lens, "helper.js": emitted.quoted });
      expect(fromSource.lenses[0]?.printed.solidity).toContain('return bytes4(hex"00000001");');
      expect(fromEmitted.lenses[0]?.printed.solidity).toBe(fromSource.lenses[0]?.printed.solidity);
    });
  });
});

describe("conversions to fixed-size bytes: rejections", () => {
  it("solc refuses an address, at the TypeScript position", async () => {
    const code = `${IMPORTS}
export const Lens = contract("Lens", { f: pure(address).returns(bytes4) }).implementation({
  f: (a) => bytes4(a),
});
`;
    await expectRejection(code, {
      message: 'Explicit type conversion not allowed from "address" to "bytes4".',
      ...positionIn(code, "bytes4(a)"),
      source: "solc",
    });
  });
});
