/**
 * Struct literals: `new T({ a, b })` is Solidity's `T({a: a, b: b})`, the fields by name.
 */
import { describe, it } from "vitest";

import { describePairs, type ExecutedPair, IMPORTS, MAX_UINT256 } from "./helpers/core.js";
import { expectRejection, positionIn } from "./helpers/harness.js";

const pairs: Record<string, ExecutedPair> = {
  "a struct built from its fields": {
    ts: `${IMPORTS}
      const ONE = 1n;

      class Point extends Struct({ x: uint256, y: uint256 }) {}
      class Segment extends Struct({ from: Point, to: Point, label: string, weights: [uint128], closed: bool }) {}

      // A field named alone takes the local of that name.
      const point = internal(pure(uint256, uint256).returns(Point), (x, y) => new Point({ x, y }));
      const squaredLength = internal(pure(Segment).returns(uint256), (s) => {
        const dx = uint256(s.to.x - s.from.x);
        const dy = uint256(s.to.y - s.from.y);
        return dx * dx + dy * dy;
      });

      export const StructLens = contract("StructLens", {
        segment: pure(uint256, uint256, uint256).returns(Segment),
        mirrored: pure(Point).returns(Point),
        lengthSquared: pure(Point, Point).returns(uint256),
        points: pure(uint256).returns([Point]),
      }).implementation({
        // As a local, with fields that are calls, struct literals, a string, an array and a comparison.
        segment(x, y, step) {
          const s = new Segment({
            from: point(x, y),
            to: new Point({ y: y + step, x: x + step }),
            label: "diagonal",
            weights: array(uint128, step),
            closed: step == 0n,
          });
          return s;
        },
        // As a returned value, with a key written as a string and a module constant as a value.
        mirrored: (p) => new Point({ "x": p.y, y: p.x + ONE }),
        // As an argument, and as an operand.
        lengthSquared(a, b) {
          return squaredLength(new Segment({ from: a, to: b, label: "", weights: array(uint128, 0n), closed: false }))
            + new Point({ x: a.x, y: 0n }).y;
        },
        // As the value of an assignment to an element.
        points(n) {
          const out = array(Point, n);
          for (let i = 0n; i < n; i++) out[Number(i)] = new Point({ x: i, y: i * i });
          return out;
        },
      });
    `,
    solidity: `
      struct Point {
          uint256 x;
          uint256 y;
      }

      struct Segment {
          Point from;
          Point to;
          string label;
          uint128[] weights;
          bool closed;
      }

      contract StructLens {
          function segment(uint256 x, uint256 y, uint256 step) external pure returns (Segment memory) {
              Segment memory s = Segment({
                  from: point(x, y),
                  to: Point({y: y + step, x: x + step}),
                  label: "diagonal",
                  weights: new uint128[](step),
                  closed: step == 0
              });
              return s;
          }

          function mirrored(Point calldata p) external pure returns (Point memory) {
              return Point({x: p.y, y: p.x + 1});
          }

          function lengthSquared(Point calldata a, Point calldata b) external pure returns (uint256) {
              return squaredLength(Segment({from: a, to: b, label: "", weights: new uint128[](0), closed: false}))
                  + Point({x: a.x, y: 0}).y;
          }

          function points(uint256 n) external pure returns (Point[] memory) {
              Point[] memory out = new Point[](n);
              for (uint256 i = 0; i < n; i++) out[i] = Point({x: i, y: i * i});
              return out;
          }

          function point(uint256 x, uint256 y) internal pure returns (Point memory) {
              return Point({x: x, y: y});
          }

          function squaredLength(Segment memory s) internal pure returns (uint256) {
              uint256 dx = uint256(s.to.x - s.from.x);
              uint256 dy = uint256(s.to.y - s.from.y);
              return dx * dx + dy * dy;
          }
      }
    `,
    calls: [
      { functionName: "segment", args: [1n, 2n, 3n] },
      { functionName: "segment", args: [0n, 0n, 0n] },
      { functionName: "segment", args: [MAX_UINT256, 0n, 1n], reverts: true },
      { functionName: "mirrored", args: [{ x: 4n, y: 9n }] },
      { functionName: "mirrored", args: [{ x: MAX_UINT256, y: 9n }], reverts: true },
      {
        functionName: "lengthSquared",
        args: [
          { x: 1n, y: 1n },
          { x: 4n, y: 5n },
        ],
      },
      {
        functionName: "lengthSquared",
        args: [
          { x: 4n, y: 5n },
          { x: 1n, y: 1n },
        ],
        reverts: true,
      },
      { functionName: "points", args: [0n] },
      { functionName: "points", args: [4n] },
    ],
  },
};

describePairs("struct literals", pairs);

describe("struct literals: rejections", () => {
  const lens = (body: string) => `${IMPORTS}
class Point extends Struct({ x: uint256, y: uint256 }) {}
const origin = internal(pure().returns(Point), () => new Point({ x: 0n, y: 0n }));
export const Lens = contract("Lens", { f: pure(uint256, Point).returns(Point) }).implementation({
  f(x, p) {
    ${body}
  },
});
`;

  it("a struct is built from one object literal", async () => {
    const code = lens("return new Point(x, x);");
    await expectRejection(code, {
      message: "a struct is built from an object literal of its fields",
      ...positionIn(code, "new Point(x, x)"),
    });
  });

  it("a struct is not built from a value that holds its fields", async () => {
    const code = lens("return new Point(p);");
    await expectRejection(code, {
      message: "a struct is built from an object literal of its fields",
      ...positionIn(code, "new Point(p)"),
    });
  });

  it("a struct literal takes nothing after its fields", async () => {
    const code = lens("return new Point({ x, y: x }, x);");
    await expectRejection(code, {
      message: "a struct is built from an object literal of its fields",
      ...positionIn(code, "new Point({ x, y: x }, x)"),
    });
  });

  it("a struct literal cannot spread another struct", async () => {
    const code = lens("return new Point({ ...p, x });");
    await expectRejection(code, {
      message: "a field of a struct literal is written name: value",
      ...positionIn(code, "...p"),
    });
  });

  it("a field of a struct literal has a plain name", async () => {
    const code = lens('const key = "x";\n    return new Point({ [key]: x, y: x });');
    await expectRejection(code, {
      message: "a field of a struct literal is written name: value",
      ...positionIn(code, "[key]"),
    });
  });

  it("a field of a struct literal is not a method", async () => {
    const code = lens("return new Point({ x, y() {} });");
    await expectRejection(code, {
      message: "a field of a struct literal is written name: value",
      ...positionIn(code, "y() {}"),
    });
  });

  it("a field of a struct literal is not an accessor", async () => {
    const code = lens("return new Point({ x, get y() { return x; } });");
    await expectRejection(code, {
      message: "a field of a struct literal is written name: value",
      ...positionIn(code, "get y()"),
    });
  });

  it("the zero value of a struct is only the initializer of a local", async () => {
    const code = lens("return new Point();");
    await expectRejection(code, {
      message: "new T() is the zero value of a local",
      ...positionIn(code, "new Point()"),
    });
  });

  it("new builds a struct and nothing else, as an expression", async () => {
    const code = lens("return new origin({ x });");
    await expectRejection(code, { message: "new builds a struct", ...positionIn(code, "new origin(") });
  });

  it("new builds a struct and nothing else, as the initializer of a local", async () => {
    const code = lens("const o = new uint256();\n    return p;");
    await expectRejection(code, { message: "new builds a struct", ...positionIn(code, "new uint256()") });
  });

  it("solc checks the fields, at the TypeScript position", async () => {
    const code = lens("return new Point({ x });");
    await expectRejection(code, {
      message: "Wrong argument count for struct constructor: 1 arguments given but expected 2.",
      ...positionIn(code, "new Point({ x })"),
      source: "solc",
    });
  });

  it("solc checks the name of a field, at the TypeScript position", async () => {
    const code = lens("return new Point({ x, z: x });");
    await expectRejection(code, {
      message: 'Named argument "z" does not match function declaration.',
      ...positionIn(code, "new Point({ x, z: x })"),
      source: "solc",
    });
  });

  it("solc checks the type of a field, at the TypeScript position", async () => {
    const code = lens("return new Point({ x, y: p });");
    await expectRejection(code, {
      message:
        /Invalid type for argument in function call\. Invalid implicit conversion from struct Point calldata to uint256/,
      ...positionIn(code, "p }"),
      source: "solc",
    });
  });
});
