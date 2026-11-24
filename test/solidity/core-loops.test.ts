/**
 * Loops: `while`, `do…while`, `for`, `break` and `continue`, each printed as the same Solidity statement.
 */
import { describe, it } from "vitest";

import { describePairs, type ExecutedPair, IMPORTS, MAX_UINT256 } from "./helpers/core.js";
import { expectRejection, positionIn } from "./helpers/harness.js";

const pairs: Record<string, ExecutedPair> = {
  "while, break and continue": {
    ts: `${IMPORTS}
      export const WhileLens = contract("WhileLens", {
        steps: pure(uint256, uint256).returns(uint256),
        log2: pure(uint256).returns(uint256),
      }).implementation({
        steps(x, limit) {
          let count = 0n;
          while (x != 1n) {
            if (count == limit) break;
            count++;
            if (x % 2n == 0n) {
              x /= 2n;
              continue;
            }
            x = 3n * x + 1n;
          }
          return count;
        },
        // A loop whose body is one statement, not a block.
        log2(x) {
          let n = 0n;
          while ((x >>= 1n) > 0n) n++;
          return n;
        },
      });
    `,
    solidity: `
      contract WhileLens {
          function steps(uint256 x, uint256 limit) external pure returns (uint256) {
              uint256 count = 0;
              while (x != 1) {
                  if (count == limit) break;
                  count++;
                  if (x % 2 == 0) {
                      x /= 2;
                      continue;
                  }
                  x = 3 * x + 1;
              }
              return count;
          }

          function log2(uint256 x) external pure returns (uint256) {
              uint256 n = 0;
              while ((x >>= 1) > 0) n++;
              return n;
          }
      }
    `,
    calls: [
      { functionName: "steps", args: [6n, 100n] },
      { functionName: "steps", args: [6n, 3n] },
      { functionName: "steps", args: [1n, 0n] },
      { functionName: "steps", args: [0n, 5n] },
      // An odd number so large that 3x + 1 overflows.
      { functionName: "steps", args: [MAX_UINT256, 10n], reverts: true },
      { functionName: "log2", args: [0n] },
      { functionName: "log2", args: [1n] },
      { functionName: "log2", args: [1024n] },
      { functionName: "log2", args: [MAX_UINT256] },
    ],
  },
  "do…while": {
    ts: `${IMPORTS}
      export const DoLens = contract("DoLens", {
        digits: pure(uint256).returns(uint256),
        firstMultiple: pure(uint256, uint256).returns(uint256),
        skipEven: pure(uint256).returns(uint256),
      }).implementation({
        digits(x) {
          let n = 0n;
          do {
            n++;
            x /= 10n;
          } while (x != 0n);
          return n;
        },
        // The body runs once before the test, and here it is one statement.
        firstMultiple(step, floor) {
          let x = 0n;
          do x += step;
          while (x < floor);
          return x;
        },
        skipEven(n) {
          let sum = 0n;
          let i = 0n;
          do {
            i++;
            if (i % 2n == 0n) continue;
            if (i > 9n) break;
            sum += i;
          } while (i < n);
          return sum;
        },
      });
    `,
    solidity: `
      contract DoLens {
          function digits(uint256 x) external pure returns (uint256) {
              uint256 n = 0;
              do {
                  n++;
                  x /= 10;
              } while (x != 0);
              return n;
          }

          function firstMultiple(uint256 step, uint256 floor) external pure returns (uint256) {
              uint256 x = 0;
              do x += step; while (x < floor);
              return x;
          }

          function skipEven(uint256 n) external pure returns (uint256) {
              uint256 sum = 0;
              uint256 i = 0;
              do {
                  i++;
                  if (i % 2 == 0) continue;
                  if (i > 9) break;
                  sum += i;
              } while (i < n);
              return sum;
          }
      }
    `,
    calls: [
      { functionName: "digits", args: [0n] },
      { functionName: "digits", args: [9n] },
      { functionName: "digits", args: [12345n] },
      { functionName: "firstMultiple", args: [7n, 0n] },
      { functionName: "firstMultiple", args: [7n, 30n] },
      { functionName: "firstMultiple", args: [MAX_UINT256, MAX_UINT256] },
      { functionName: "firstMultiple", args: [MAX_UINT256 - 1n, MAX_UINT256], reverts: true },
      { functionName: "skipEven", args: [0n] },
      { functionName: "skipEven", args: [6n] },
      { functionName: "skipEven", args: [100n] },
    ],
  },
  "for, with each part present and absent": {
    ts: `${IMPORTS}
      const bump = internal(pure(uint256).returns(uint256), (i) => i + 1n);

      export const ForLens = contract("ForLens", {
        triangle: pure(uint256).returns(uint256),
        factorial: pure(uint256).returns(uint256),
        evens: pure(uint256).returns(uint256),
        countdown: pure(uint256).returns(uint256),
        firstSquareAbove: pure(uint256).returns(uint256),
        grid: pure(uint256, uint256).returns(uint256),
        forever: pure(uint256).returns(uint256),
      }).implementation({
        triangle(n) {
          let sum = 0n;
          for (let i = 1n; i <= n; i++) {
            sum += i;
          }
          return sum;
        },
        // One statement as the body, and a counter written without the n suffix.
        factorial(n) {
          let product = uint256(1n);
          for (let i = 2; i <= n; i++) product *= i;
          return product;
        },
        // The init assigns a local declared before the loop, and the update is a compound assignment.
        evens(n) {
          let sum = 0n;
          let i = 0n;
          for (i = 2n; i <= n; i += 2n) sum += i;
          return sum + i;
        },
        // No init, and a prefix update.
        countdown(n) {
          let visited = 0n;
          for (; n > 0n; --n) visited++;
          return visited;
        },
        // No test: the loop ends on a break. The update calls a helper.
        firstSquareAbove(floor) {
          let found = 0n;
          for (let i = 0n; ; i = bump(i)) {
            if (i * i <= floor) continue;
            found = i * i;
            break;
          }
          return found;
        },
        // A loop in a loop, each with its own counter of the same name in its own scope.
        grid(rows, columns) {
          let cells = 0n;
          for (let i = 0n; i < rows; i++) {
            for (let j = 0n; j < columns; j++) cells += i * columns + j;
          }
          for (let i = 0n; i < columns; i++) cells++;
          return cells;
        },
        // No part at all.
        forever(limit) {
          let i = 0n;
          for (;;) {
            if (i == limit) return i;
            i++;
          }
        },
      });
    `,
    solidity: `
      contract ForLens {
          function triangle(uint256 n) external pure returns (uint256) {
              uint256 sum = 0;
              for (uint256 i = 1; i <= n; i++) {
                  sum += i;
              }
              return sum;
          }

          function factorial(uint256 n) external pure returns (uint256) {
              uint256 product = uint256(1);
              for (uint256 i = 2; i <= n; i++) product *= i;
              return product;
          }

          function evens(uint256 n) external pure returns (uint256) {
              uint256 sum = 0;
              uint256 i = 0;
              for (i = 2; i <= n; i += 2) sum += i;
              return sum + i;
          }

          function countdown(uint256 n) external pure returns (uint256) {
              uint256 visited = 0;
              for (; n > 0; --n) visited++;
              return visited;
          }

          function firstSquareAbove(uint256 floor) external pure returns (uint256) {
              uint256 found = 0;
              for (uint256 i = 0; ; i = bump(i)) {
                  if (i * i <= floor) continue;
                  found = i * i;
                  break;
              }
              return found;
          }

          function grid(uint256 rows, uint256 columns) external pure returns (uint256) {
              uint256 cells = 0;
              for (uint256 i = 0; i < rows; i++) {
                  for (uint256 j = 0; j < columns; j++) cells += i * columns + j;
              }
              for (uint256 i = 0; i < columns; i++) cells++;
              return cells;
          }

          function forever(uint256 limit) external pure returns (uint256) {
              uint256 i = 0;
              for (;;) {
                  if (i == limit) return i;
                  i++;
              }
          }

          function bump(uint256 i) internal pure returns (uint256) {
              return i + 1;
          }
      }
    `,
    calls: [
      { functionName: "triangle", args: [0n] },
      { functionName: "triangle", args: [100n] },
      { functionName: "factorial", args: [0n] },
      { functionName: "factorial", args: [20n] },
      // 58! is past 2^256.
      { functionName: "factorial", args: [58n], reverts: true },
      { functionName: "evens", args: [1n] },
      { functionName: "evens", args: [11n] },
      { functionName: "countdown", args: [0n] },
      { functionName: "countdown", args: [17n] },
      { functionName: "firstSquareAbove", args: [0n] },
      { functionName: "firstSquareAbove", args: [99n] },
      { functionName: "grid", args: [0n, 4n] },
      { functionName: "grid", args: [3n, 4n] },
      { functionName: "forever", args: [0n] },
      { functionName: "forever", args: [12n] },
    ],
  },
};

describePairs("loops", pairs);

describe("loops: rejections", () => {
  const lens = (body: string) => `${IMPORTS}
export const Lens = contract("Lens", { f: pure(uint256).returns(uint256) }).implementation({
  f(x) {
    ${body}
  },
});
`;

  it("a for loop declares one local", async () => {
    const code = lens("for (let i = 0n, j = 0n; i < x; i++) j++;\n    return x;");
    await expectRejection(code, { message: "a for loop declares one local", ...positionIn(code, "let i = 0n, j") });
  });

  it("the init of a for loop is a declaration or a statement", async () => {
    const code = lens("for (x; x < 3n; x++) {}\n    return x;");
    await expectRejection(code, { message: "this expression is not a statement", ...positionIn(code, "x; x < 3n") });
  });

  it("the update of a for loop is one statement, not several joined by a comma", async () => {
    const code = lens("let j = 0n;\n    for (let i = 0n; i < x; i++, j++) {}\n    return j;");
    await expectRejection(code, { message: "this expression is not a statement", ...positionIn(code, "i++, j++") });
  });

  it("a local of a for loop is declared with const or let", async () => {
    const code = lens("for (var i = 0n; i < x; i++) {}\n    return x;");
    await expectRejection(code, { message: "a local is declared with const or let", ...positionIn(code, "var i") });
  });

  it("for…in has no Solidity form", async () => {
    const code = lens("for (const i in [x]) return x;\n    return x;");
    await expectRejection(code, {
      message: "cannot contain this statement (ForInStatement)",
      ...positionIn(code, "for ("),
    });
  });

  it("a label has no Solidity form", async () => {
    const code = lens("outer: while (x > 0n) {\n      break outer;\n    }\n    return x;");
    await expectRejection(code, {
      message: "cannot contain this statement (LabeledStatement)",
      ...positionIn(code, "outer:"),
    });
  });

  it("the empty statement is not the body of a loop", async () => {
    const code = lens("while (x++ < 3n);\n    return x;");
    await expectRejection(code, { message: "an empty one is written {}", ...positionIn(code, ";\n    return") });
  });

  it("the empty statement is not the body of an if", async () => {
    const code = lens("if (x > 3n);\n    else x = 3n;\n    return x;");
    await expectRejection(code, { message: "an empty one is written {}", ...positionIn(code, ";\n    else") });
  });
});
