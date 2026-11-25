/**
 * Conversions to the fixed-size bytes types, spelled as in Solidity: from another fixed-size bytes
 * type, from `bytes`, from an integer of the same size and from a literal. The editor accepts the same
 * ones: see types/conversions.ts.
 */
import { describe, it } from "vitest";

import { describePairs, type ExecutedPair, IMPORTS } from "./helpers/core.js";
import { expectRejection, positionIn } from "./helpers/harness.js";

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
        literal: pure().returns(bytes4, bytes32, bytes4),
        integer: pure(uint32, uint256).returns(bytes4, bytes32),
      }).implementation({
        // A wider type is cut to its first bytes, and a narrower one is padded on the right.
        prefix: (word) => bytes4(word),
        widen: (selector) => bytes32(selector),
        same: (selector, word) => [bytes4(selector), bytes32(word)],
        head: (data) => [bytes4(data), bytes32(data)],
        literal: () => [bytes4("0x313ce567"), bytes32(0), bytes4(0x313ce567)],
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

          function literal() external pure returns (bytes4, bytes32, bytes4) {
              return (bytes4(hex"313ce567"), bytes32(0), bytes4(0x313ce567));
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
