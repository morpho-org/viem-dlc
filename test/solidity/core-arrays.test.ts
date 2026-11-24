/**
 * Arrays and data locations: `[T]` types, indexing, `.length`, `array(T, n)`, and where a
 * reference type lives. A reference-type parameter of an external function is `calldata` and
 * everything else is `memory`.
 */
import { describe, expect, it } from "vitest";

import { requireAnvil } from "./helpers/anvil.js";
import { describePairs, type ExecutedPair, IMPORTS, MAX_UINT256, useEvm } from "./helpers/core.js";
import {
  buildSnippet,
  compilePair,
  expectRejection,
  expectSameAst,
  expectSameExecution,
  positionIn,
} from "./helpers/harness.js";

const ALICE = "0x00000000000000000000000000000000000A11cE";
const BOB = "0x0000000000000000000000000000000000000B0b";
const WORD = `0x${"11".repeat(32)}`;

const pairs: Record<string, ExecutedPair> = {
  "array types, indexing, length and array(T, n)": {
    ts: `${IMPORTS}
      class Basket extends Struct({ owner: address, amounts: [uint256], tags: [bytes32] }) {}
      class Summary extends Struct({ sum: uint256, count: uint256, largest: uint256, doubled: [uint256] }) {}

      // A helper's array is in memory. An index held in a bigint goes through Number.
      const total = internal(pure([uint256]).returns(uint256), (xs) => {
        let sum = 0n;
        for (let i = 0n; i < xs.length; i++) sum += xs[Number(i)]!;
        return sum;
      });

      export const ArrayLens = contract("ArrayLens", {
        summarize: pure(Basket).returns(Summary),
        pick: pure([uint256], uint256).returns(uint256),
        last: pure([uint256]).returns(uint256),
        rowSums: pure([[uint256]]).returns([uint256]),
        owners: pure([Basket]).returns([address]),
        triangle: pure(uint256).returns([[uint256]]),
        fresh: pure(uint256).returns([Basket]),
      }).implementation({
        summarize(basket) {
          const o = new Summary();
          const amounts = basket.amounts;
          const count = amounts.length;
          o.count = count;
          o.sum = total(amounts);
          o.doubled = array(uint256, count);
          for (let i = 0; i < count; i++) {
            const amount = amounts[i]!;
            if (amount > o.largest) o.largest = amount;
            o.doubled[i] = amount * 2n;
          }
          return o;
        },
        // An index is any expression, and Number and BigInt print as their argument.
        pick(xs, i) {
          const first = xs[0]!;
          const picked = xs[Number(i)]!;
          const next = xs[Number(i + 1n)]!;
          return first + picked + next + BigInt(xs.length) * 2n;
        },
        last: (xs) => xs[xs.length - 1]!,
        // An array of arrays, and a row copied to memory as a local.
        rowSums(rows) {
          const sums = array(uint256, rows.length);
          for (let i = 0; i < rows.length; i++) {
            const row = rows[i]!;
            sums[i] = total(row) + rows[i]![0]!;
          }
          return sums;
        },
        // An array of structs, and a struct copied to memory as a local.
        owners(baskets) {
          const out = array(address, baskets.length);
          for (let i = 0n; i < baskets.length; i++) {
            const basket = baskets[Number(i)]!;
            out[Number(i)] = basket.owner;
          }
          return out;
        },
        triangle(n) {
          const rows = array([uint256], n);
          for (let i = 0n; i < n; i++) {
            rows[Number(i)] = array(uint256, i + 1n);
            rows[Number(i)]![Number(i)] = i;
          }
          return rows;
        },
        fresh(n) {
          const baskets = array(Basket, n);
          if (n > 0n) baskets[0]!.amounts = array(uint256, n);
          return baskets;
        },
      });
    `,
    solidity: `
      struct Basket {
          address owner;
          uint256[] amounts;
          bytes32[] tags;
      }

      struct Summary {
          uint256 sum;
          uint256 count;
          uint256 largest;
          uint256[] doubled;
      }

      contract ArrayLens {
          function summarize(Basket calldata basket) external pure returns (Summary memory) {
              Summary memory o;
              uint256[] memory amounts = basket.amounts;
              uint256 count = amounts.length;
              o.count = count;
              o.sum = total(amounts);
              o.doubled = new uint256[](count);
              for (uint256 i = 0; i < count; i++) {
                  uint256 amount = amounts[i];
                  if (amount > o.largest) o.largest = amount;
                  o.doubled[i] = amount * 2;
              }
              return o;
          }

          function pick(uint256[] calldata xs, uint256 i) external pure returns (uint256) {
              uint256 first = xs[0];
              uint256 picked = xs[i];
              uint256 next = xs[i + 1];
              return first + picked + next + xs.length * 2;
          }

          function last(uint256[] calldata xs) external pure returns (uint256) {
              return xs[xs.length - 1];
          }

          function rowSums(uint256[][] calldata rows) external pure returns (uint256[] memory) {
              uint256[] memory sums = new uint256[](rows.length);
              for (uint256 i = 0; i < rows.length; i++) {
                  uint256[] memory row = rows[i];
                  sums[i] = total(row) + rows[i][0];
              }
              return sums;
          }

          function owners(Basket[] calldata baskets) external pure returns (address[] memory) {
              address[] memory out = new address[](baskets.length);
              for (uint256 i = 0; i < baskets.length; i++) {
                  Basket memory basket = baskets[i];
                  out[i] = basket.owner;
              }
              return out;
          }

          function triangle(uint256 n) external pure returns (uint256[][] memory) {
              uint256[][] memory rows = new uint256[][](n);
              for (uint256 i = 0; i < n; i++) {
                  rows[i] = new uint256[](i + 1);
                  rows[i][i] = i;
              }
              return rows;
          }

          function fresh(uint256 n) external pure returns (Basket[] memory) {
              Basket[] memory baskets = new Basket[](n);
              if (n > 0) baskets[0].amounts = new uint256[](n);
              return baskets;
          }

          function total(uint256[] memory xs) internal pure returns (uint256) {
              uint256 sum = 0;
              for (uint256 i = 0; i < xs.length; i++) sum += xs[i];
              return sum;
          }
      }
    `,
    calls: [
      { functionName: "summarize", args: [{ owner: ALICE, amounts: [3n, 9n, 4n], tags: [] }] },
      { functionName: "summarize", args: [{ owner: ALICE, amounts: [], tags: [WORD] }] },
      { functionName: "summarize", args: [{ owner: ALICE, amounts: [MAX_UINT256], tags: [] }], reverts: true },
      { functionName: "pick", args: [[5n, 6n, 7n], 1n] },
      // The index is past the end: Panic(0x32), from the same read on both sides.
      { functionName: "pick", args: [[5n, 6n, 7n], 2n], reverts: true },
      { functionName: "pick", args: [[], 0n], reverts: true },
      { functionName: "last", args: [[1n, 2n, 3n]] },
      { functionName: "last", args: [[]], reverts: true },
      { functionName: "rowSums", args: [[[1n, 2n], [3n], [4n, 5n, 6n]]] },
      { functionName: "rowSums", args: [[[1n], []]], reverts: true },
      {
        functionName: "owners",
        args: [
          [
            { owner: ALICE, amounts: [1n], tags: [] },
            { owner: BOB, amounts: [], tags: [] },
          ],
        ],
      },
      { functionName: "triangle", args: [0n] },
      { functionName: "triangle", args: [4n] },
      { functionName: "fresh", args: [0n] },
      { functionName: "fresh", args: [3n] },
      // More memory than a call can pay for: Panic(0x41).
      { functionName: "fresh", args: [MAX_UINT256], reverts: true },
    ],
  },
  "calldata for a reference-type parameter of an external function, memory everywhere else": {
    ts: `${IMPORTS}
      class Note extends Struct({ title: string, body: bytes, marks: [uint8] }) {}

      // Helper parameters and return values are in memory, so the calldata they are given is copied.
      const pickTitle = internal(pure(Note, string, bool).returns(string), (note, other, own) =>
        own ? note.title : other,
      );
      const marksOf = internal(pure(Note).returns([uint8]), (note) => note.marks);

      export const LocationLens = contract("LocationLens", {
        echo: pure(bytes, string, [uint8], Note).returns(bytes, string, [uint8], Note),
        title: pure(Note, string, bool).returns(string),
        marks: pure([Note], uint256).returns([uint8]),
        scalar: pure(uint256, address, bool, bytes32, bytes4).returns(bytes4, bytes32, bool, address, uint256),
      }).implementation({
        // Every reference type as a parameter, as a local and as a return value.
        echo(data, text, marks, note) {
          const dataCopy = data;
          const textCopy = text;
          const marksCopy = marks;
          const noteCopy = note;
          return [dataCopy, textCopy, marksCopy, noteCopy];
        },
        title: (note, other, own) => pickTitle(note, other, own),
        marks(notes, i) {
          const found = marksOf(notes[Number(i)]!);
          return found;
        },
        // A value type has no location, wherever it is.
        scalar(n, who, flag, word, selector) {
          const m = n;
          return [selector, word, flag, who, m];
        },
      });
    `,
    solidity: `
      struct Note {
          string title;
          bytes body;
          uint8[] marks;
      }

      contract LocationLens {
          function echo(bytes calldata data, string calldata text, uint8[] calldata marks, Note calldata note)
              external
              pure
              returns (bytes memory, string memory, uint8[] memory, Note memory)
          {
              bytes memory dataCopy = data;
              string memory textCopy = text;
              uint8[] memory marksCopy = marks;
              Note memory noteCopy = note;
              return (dataCopy, textCopy, marksCopy, noteCopy);
          }

          function title(Note calldata note, string calldata other, bool own) external pure returns (string memory) {
              return pickTitle(note, other, own);
          }

          function marks(Note[] calldata notes, uint256 i) external pure returns (uint8[] memory) {
              uint8[] memory found = marksOf(notes[i]);
              return found;
          }

          function scalar(uint256 n, address who, bool flag, bytes32 word, bytes4 selector)
              external
              pure
              returns (bytes4, bytes32, bool, address, uint256)
          {
              uint256 m = n;
              return (selector, word, flag, who, m);
          }

          function pickTitle(Note memory note, string memory other, bool own) internal pure returns (string memory) {
              return own ? note.title : other;
          }

          function marksOf(Note memory note) internal pure returns (uint8[] memory) {
              return note.marks;
          }
      }
    `,
    calls: [
      {
        functionName: "echo",
        args: ["0xdeadbeef", "some text", [1, 2, 3], { title: "a title", body: "0x00ff", marks: [7] }],
      },
      { functionName: "echo", args: ["0x", "", [], { title: "", body: "0x", marks: [] }] },
      { functionName: "title", args: [{ title: "own", body: "0x", marks: [] }, "other", true] },
      { functionName: "title", args: [{ title: "own", body: "0x", marks: [] }, "other", false] },
      { functionName: "marks", args: [[{ title: "t", body: "0x01", marks: [4, 5] }], 0n] },
      { functionName: "marks", args: [[{ title: "t", body: "0x01", marks: [4, 5] }], 1n], reverts: true },
      { functionName: "scalar", args: [1n, ALICE, true, WORD, "0x01020304"] },
    ],
  },
};

describePairs("arrays and data locations", pairs);

describe("a named return parameter is a local and a return", () => {
  const ts = `${IMPORTS}
    class Stats extends Struct({ count: uint256, sum: uint256, squares: [uint256] }) {}

    export const NamedLens = contract("NamedLens", {
      stats: pure([uint256]).returns(Stats),
      squares: pure(uint256).returns([uint256]),
      sum: pure([uint256]).returns(uint256),
    }).implementation({
      stats(xs) {
        const o = new Stats();
        o.count = xs.length;
        o.squares = array(uint256, xs.length);
        for (let i = 0; i < xs.length; i++) {
          o.sum += xs[i]!;
          o.squares[i] = xs[i]! * xs[i]!;
        }
        return o;
      },
      squares(n) {
        const out = array(uint256, n);
        for (let i = 0n; i < n; i++) out[Number(i)] = i * i;
        return out;
      },
      sum(xs) {
        let total = 0n;
        for (let i = 0; i < xs.length; i++) total += xs[i]!;
        return total;
      },
    });
  `;
  const stats = `
    struct Stats {
        uint256 count;
        uint256 sum;
        uint256[] squares;
    }
  `;
  // What the lens prints as: each named return parameter is a local, and the function ends on a return.
  const written = `${stats}
    contract NamedLens {
        function stats(uint256[] calldata xs) external pure returns (Stats memory) {
            Stats memory o;
            o.count = xs.length;
            o.squares = new uint256[](xs.length);
            for (uint256 i = 0; i < xs.length; i++) {
                o.sum += xs[i];
                o.squares[i] = xs[i] * xs[i];
            }
            return o;
        }

        function squares(uint256 n) external pure returns (uint256[] memory) {
            uint256[] memory out = new uint256[](n);
            for (uint256 i = 0; i < n; i++) out[i] = i * i;
            return out;
        }

        function sum(uint256[] calldata xs) external pure returns (uint256) {
            uint256 total = 0;
            for (uint256 i = 0; i < xs.length; i++) total += xs[i];
            return total;
        }
    }
  `;
  // The same lens written with named return parameters, which the syntax has no form for.
  const named = `${stats}
    contract NamedLens {
        function stats(uint256[] calldata xs) external pure returns (Stats memory o) {
            o.count = xs.length;
            o.squares = new uint256[](xs.length);
            for (uint256 i = 0; i < xs.length; i++) {
                o.sum += xs[i];
                o.squares[i] = xs[i] * xs[i];
            }
        }

        function squares(uint256 n) external pure returns (uint256[] memory out) {
            out = new uint256[](n);
            for (uint256 i = 0; i < n; i++) out[i] = i * i;
        }

        function sum(uint256[] calldata xs) external pure returns (uint256 total) {
            for (uint256 i = 0; i < xs.length; i++) total += xs[i];
        }
    }
  `;
  const calls = [
    { functionName: "stats", args: [[1n, 2n, 3n]] },
    { functionName: "stats", args: [[]] },
    { functionName: "stats", args: [[2n ** 128n]] },
    { functionName: "squares", args: [5n] },
    { functionName: "squares", args: [0n] },
    { functionName: "sum", args: [[4n, 5n]] },
    { functionName: "sum", args: [[MAX_UINT256, 1n]] },
  ];
  const evm = useEvm();

  it("the printed Solidity is the local and the return", async () => {
    expectSameAst(await compilePair({ ts, solidity: written }));
  });

  it.skipIf(!requireAnvil())("and it gives the bytes the named return parameters give", async () => {
    const outcomes = await expectSameExecution(evm(), await compilePair({ ts, solidity: named }), calls);
    expect(outcomes.map((outcome) => outcome.status)).toEqual([
      "success",
      "success",
      "revert",
      "success",
      "success",
      "success",
      "revert",
    ]);
  });
});

describe("arrays: what is printed", () => {
  it("Number and BigInt print as their argument, parenthesized where an operator needs it", async () => {
    const built = await buildSnippet(`${IMPORTS}
      export const Lens = contract("Lens", { f: pure([uint256], uint256).returns(uint256) }).implementation({
        f: (xs, i) => xs[Number(i + 1n)]! * BigInt(Number(i) + xs.length) + Number(BigInt(i)),
      });
    `);
    expect(built.lenses[0]?.printed.solidity).toContain("return (xs[i + 1] * (i + xs.length)) + i;");
  });

  it("a local declared through Number or BigInt takes the type of the argument", async () => {
    const built = await buildSnippet(`${IMPORTS}
      export const Lens = contract("Lens", { f: pure([uint128], uint64).returns(uint256) }).implementation({
        f(xs, i) {
          const count = BigInt(xs.length);
          const at = Number(i);
          const picked = xs[Number(at)]!;
          return picked + count;
        },
      });
    `);
    const solidity = built.lenses[0]?.printed.solidity;
    expect(solidity).toContain("uint256 count = xs.length;");
    expect(solidity).toContain("uint64 at = i;");
    expect(solidity).toContain("uint128 picked = xs[at];");
  });
});

describe("arrays: rejections", () => {
  const lens = (body: string) => `${IMPORTS}
export const Lens = contract("Lens", { f: pure([uint256], bytes).returns(uint256) }).implementation({
  f(xs, data) {
    ${body}
  },
});
`;

  it("array(T, n) takes a type and a length", async () => {
    const code = lens("const out = array(uint256);\n    return out.length;");
    await expectRejection(code, {
      message: "an array is made with array(T, n)",
      ...positionIn(code, "array(uint256)"),
    });
  });

  it("array(T, n) takes nothing after the length", async () => {
    const code = lens("xs = array(uint256, 1n, 2n);\n    return xs.length;");
    await expectRejection(code, { message: "an array is made with array(T, n)", ...positionIn(code, "array(") });
  });

  it("array(T, n) does not spread its arguments", async () => {
    const code = lens("xs = array(uint256, ...xs);\n    return xs.length;");
    await expectRejection(code, { message: "an array is made with array(T, n)", ...positionIn(code, "array(") });
  });

  it("the first argument of array(T, n) is a type", async () => {
    const code = lens("const out = array(xs, 1n);\n    return out.length;");
    await expectRejection(code, { message: "this is not a type", ...positionIn(code, "xs, 1n") });
  });

  it("a helper is not a type", async () => {
    const code = `${IMPORTS}
const one = internal(pure().returns(uint256), () => 1n);
export const Lens = contract("Lens", { f: pure().returns(uint256) }).implementation({
  f() {
    const out = array(one, 1n);
    return out.length;
  },
});
`;
    await expectRejection(code, { message: "this is not a type", ...positionIn(code, "one, 1n") });
  });

  it("an array type names one element type", async () => {
    const code = lens("const out = array([uint256, uint128], 1n);\n    return out.length;");
    await expectRejection(code, {
      message: "an array type is written [T], with one element type",
      ...positionIn(code, "[uint256, uint128]"),
    });
  });

  it("Number takes one argument", async () => {
    const code = lens("return xs[Number()]!;");
    await expectRejection(code, { message: "Number(x) takes one argument", ...positionIn(code, "Number()") });
  });

  it("BigInt takes one argument", async () => {
    const code = lens("return BigInt(xs.length, 2n);");
    await expectRejection(code, { message: "BigInt(x) takes one argument", ...positionIn(code, "BigInt(") });
  });

  it("Number does not spread its argument", async () => {
    const code = lens("return xs[Number(...xs)]!;");
    await expectRejection(code, { message: "Number(x) takes one argument", ...positionIn(code, "Number(") });
  });

  it("Number is not a value", async () => {
    const code = lens("return xs[0]! + Number;");
    await expectRejection(code, { message: "Number is written as a call", ...positionIn(code, "Number;") });
  });

  it("Number alone is not a statement", async () => {
    const code = lens("Number(xs[0]!);\n    return 1n;");
    await expectRejection(code, { message: "this expression is not a statement", ...positionIn(code, "xs[0]!);") });
  });

  it("arithmetic on an element has no type to copy", async () => {
    const code = lens("const y = xs[0]! + 1n;\n    return y;");
    await expectRejection(code, { message: "name the type with a conversion", ...positionIn(code, "xs[0]! + 1n") });
  });

  it("an element of bytes has no type to copy", async () => {
    const code = lens("const y = data[0]!;\n    return uint256(uint8(y));");
    await expectRejection(code, { message: "name the type with a conversion", ...positionIn(code, "data[0]") });
  });

  it("an array has no member but length", async () => {
    const code = lens("const y = xs.size;\n    return y;");
    await expectRejection(code, { message: "name the type with a conversion", ...positionIn(code, "xs.size") });
  });

  it("solc checks an index, at the TypeScript position", async () => {
    const code = lens("return xs[data]!;");
    await expectRejection(code, {
      message: "Type bytes calldata is not implicitly convertible to expected type uint256.",
      ...positionIn(code, "data]"),
      source: "solc",
    });
  });
});
