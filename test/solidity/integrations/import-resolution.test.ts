/**
 * Import resolution in the core: a lens reaches its declarations through named imports, renamed
 * re-exports, `export *`, namespace imports and namespace re-exports, and every file on the way is
 * reported. How each host's own resolver and its "@/" alias are used is covered by the host suites.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { type Evm, requireAnvil, startAnvil } from "../helpers/anvil.js";
import {
  buildSnippet,
  compilePair,
  expectRejection,
  expectSameAst,
  expectSameExecution,
  type FixturePair,
  positionIn,
} from "../helpers/harness.js";

const MAX_UINT256 = 2n ** 256n - 1n;

/** A library of four modules behind one entry, `lib.ts`, that only re-exports. */
const library = {
  "lib.ts": `
    export * from "./math.js";
    export { scale as scaleUp } from "./scale.js";
    export * as units from "./units.js";
    export { ONE, Amounts } from "./units.js";
  `,
  "math.ts": `
    import { internal, pure, uint256 } from "@morpho-org/viem-dlc/solidity";

    export const half = internal(pure(uint256).returns(uint256), (x) => x / 2n);
  `,
  "scale.ts": `
    import { internal, pure, uint256 } from "@morpho-org/viem-dlc/solidity";

    const scale = internal(pure(uint256).returns(uint256), (x) => x * 3n);

    export { scale };
  `,
  "units.ts": `
    import { internal, pure, Struct, uint256 } from "@morpho-org/viem-dlc/solidity";

    export const WAD = 10n ** 18n;
    export const ONE = 1n;
    export class Amounts extends Struct({ raw: uint256, scaled: uint256 }) {}
    export const toWad = internal(pure(uint256).returns(uint256), (x) => x * WAD);
  `,
};

const pairs = {
  "a renamed re-export, export *, a namespace import and a namespace re-export": {
    ts: {
      "main.ts": `
        import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";

        import * as lib from "./lib.js";
        import { Amounts, scaleUp as triple } from "./lib.js";

        export const ImportLens = contract("ImportLens", {
          run: pure(uint256).returns(Amounts),
        }).implementation({
          run(x) {
            const o = new Amounts();
            const tripled = triple(x);
            const halved = lib.half(tripled);
            o.raw = halved + lib.ONE;
            o.scaled = lib.units.toWad(halved) + lib.units.WAD;
            return o;
          },
        });
      `,
      ...library,
    },
    solidity: `
      struct Amounts {
          uint256 raw;
          uint256 scaled;
      }

      contract ImportLens {
          function run(uint256 x) external pure returns (Amounts memory) {
              Amounts memory o;
              uint256 tripled = scale(x);
              uint256 halved = half(tripled);
              o.raw = halved + 1;
              o.scaled = toWad(halved) + 10 ** 18;
              return o;
          }

          // Declarations follow their modules, in the order the lens first reaches each: the struct's first.
          function toWad(uint256 x) internal pure returns (uint256) {
              return x * 10 ** 18;
          }

          function scale(uint256 x) internal pure returns (uint256) {
              return x * 3;
          }

          function half(uint256 x) internal pure returns (uint256) {
              return x / 2;
          }
      }
    `,
    calls: [
      { functionName: "run", args: [7n] },
      { functionName: "run", args: [0n] },
      { functionName: "run", args: [MAX_UINT256] },
    ],
  },
  "one declaration reached by two paths, and two declarations that share a name": {
    ts: {
      "main.ts": `
        import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";

        import { half as halfThroughEntry } from "./lib.js";
        import { half } from "./math.js";
        import { scale as otherScale } from "./other.js";
        import { scale } from "./scale.js";

        export const NamesLens = contract("NamesLens", {
          run: pure(uint256).returns(uint256),
        }).implementation({
          run: (x) => half(halfThroughEntry(scale(x))) + otherScale(x),
        });
      `,
      "other.ts": `
        import { internal, pure, uint256 } from "@morpho-org/viem-dlc/solidity";

        export const scale = internal(pure(uint256).returns(uint256), (x) => x * 5n);
      `,
      ...library,
    },
    solidity: `
      contract NamesLens {
          function run(uint256 x) external pure returns (uint256) {
              return half(half(scale(x))) + scale_2(x);
          }

          function half(uint256 x) internal pure returns (uint256) {
              return x / 2;
          }

          function scale(uint256 x) internal pure returns (uint256) {
              return x * 3;
          }

          function scale_2(uint256 x) internal pure returns (uint256) {
              return x * 5;
          }
      }
    `,
    calls: [
      { functionName: "run", args: [8n] },
      { functionName: "run", args: [MAX_UINT256] },
    ],
  },
  "the package's names through a module of the application": {
    ts: {
      "main.ts": `
        import { contract } from "@morpho-org/viem-dlc/solidity";

        import * as sol from "./dsl.js";
        import { pure, uint256 as word } from "./dsl.js";

        const double = sol.internal(pure(word).returns(sol.uint256), (x) => x * 2n);

        export const AliasLens = contract("AliasLens", {
          run: sol.pure(sol.uint128).returns(word),
        }).implementation({
          run: (x) => double(word(x)),
        });
      `,
      "dsl.ts": `
        export * from "@morpho-org/viem-dlc/solidity";
      `,
    },
    solidity: `
      contract AliasLens {
          function run(uint128 x) external pure returns (uint256) {
              return double(uint256(x));
          }

          function double(uint256 x) internal pure returns (uint256) {
              return x * 2;
          }
      }
    `,
    calls: [{ functionName: "run", args: [2n ** 128n - 1n] }],
  },
} satisfies Record<string, FixturePair & { calls: { functionName: string; args: bigint[] }[] }>;

describe("imports: fixture pairs", () => {
  let evm: Evm;
  beforeAll(async () => {
    if (requireAnvil()) evm = await startAnvil();
  });
  afterAll(() => evm?.stop());

  for (const [name, pair] of Object.entries(pairs)) {
    it(`${name}: the same AST as the handwritten Solidity`, async () => {
      expectSameAst(await compilePair(pair));
    });

    it.skipIf(!requireAnvil())(`${name}: the same return and revert bytes`, async () => {
      const outcomes = await expectSameExecution(evm, await compilePair(pair), pair.calls);
      expect(outcomes.some((outcome) => outcome.status === "success")).toBe(true);
    });
  }

  it("the overflowing call of the first pair reverts in both", async () => {
    if (!requireAnvil()) return;
    const pair = pairs["a renamed re-export, export *, a namespace import and a namespace re-export"];
    const [outcome] = await expectSameExecution(evm, await compilePair(pair), [
      { functionName: "run", args: [MAX_UINT256] },
    ]);
    expect(outcome?.status).toBe("revert");
  });
});

describe("imports: what is reported", () => {
  it("every file on the way to a declaration is a dependency, the ones that only re-export included", async () => {
    const built = await buildSnippet(
      pairs["a renamed re-export, export *, a namespace import and a namespace re-export"].ts,
    );
    const [lens] = built.lenses;
    const names = (files: readonly string[]) => files.map((file) => file.split("/").at(-1)).sort();
    expect(names(lens?.resolved.files ?? [])).toEqual(["lib.ts", "main.ts", "math.ts", "scale.ts", "units.ts"]);
    expect(names(built.dependencies)).toEqual(["lib.ts", "math.ts", "scale.ts", "solidity.config.json", "units.ts"]);
  });
});

describe("imports: rejections", () => {
  const main = (imports: string, body: string) => `
import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
${imports}

export const Lens = contract("Lens", {
  run: pure(uint256).returns(uint256),
}).implementation({
  run(x) {
    ${body}
  },
});
`;

  it("an import the host cannot resolve, at the import", async () => {
    const code = main('import { half } from "./nowhere.js";', "return half(x);");
    await expectRejection(code, {
      message: /^cannot resolve ".\/nowhere.js" from .*lens\.ts$/,
      ...positionIn(code, "half }"),
      source: "transform",
    });
  });

  it("a name the module does not export, through export *, at the import", async () => {
    const code = main('import { quarter } from "./lib.js";', "return quarter(x);");
    await expectRejection(
      { "lens.ts": code, ...library },
      { message: /^"quarter" is not exported by .*lib\.ts$/, ...positionIn(code, "quarter }") },
    );
  });

  it("a name no module of an export * cycle exports", async () => {
    const code = main('import { quarter } from "./a.js";', "return quarter(x);");
    await expectRejection(
      { "lens.ts": code, "a.ts": 'export * from "./b.js";', "b.ts": 'export * from "./a.js";' },
      { message: /^"quarter" is not exported by .*a\.ts$/, ...positionIn(code, "quarter }") },
    );
  });

  it("a re-export of a module that cannot be resolved, at the import that needs it", async () => {
    const code = main('import { half } from "./entry.js";', "return half(x);");
    await expectRejection(
      { "lens.ts": code, "entry.ts": 'export { half } from "./gone.js";' },
      { message: /^cannot resolve ".\/gone.js" from .*entry\.ts$/, ...positionIn(code, "half }") },
    );
  });

  it("a namespace import used as a value, at the use", async () => {
    const code = main('import * as lib from "./lib.js";', "return lib;");
    await expectRejection(
      { "lens.ts": code, ...library },
      { message: "a namespace import can only be used as namespace.name", ...positionIn(code, "lib;") },
    );
  });

  it("a namespace re-export used as a value, at the use", async () => {
    const code = main('import * as lib from "./lib.js";', "return lib.units;");
    await expectRejection(
      { "lens.ts": code, ...library },
      { message: "a namespace import can only be used as namespace.name", ...positionIn(code, "lib.units;") },
    );
  });

  it("a name a namespace does not have, at the use", async () => {
    const code = main('import * as lib from "./lib.js";', "return lib.units.quarter(x);");
    await expectRejection(
      { "lens.ts": code, ...library },
      { message: /^"quarter" is not exported by .*units\.ts$/, ...positionIn(code, "lib.units.quarter") },
    );
  });

  it("a default export that is an expression, at the import", async () => {
    const code = main('import half from "./half.js";', "return half(x);");
    await expectRejection(
      {
        "lens.ts": code,
        "half.ts": `
          import { internal, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
          export default internal(pure(uint256).returns(uint256), (x) => x / 2n);
        `,
      },
      { message: "a default export that is an expression cannot be used by a lens", ...positionIn(code, "half from") },
    );
  });

  it("an error inside an imported module is reported in that module", async () => {
    const code = main('import { rate } from "./rates.js";', "return x * rate;");
    const rates = "export let rate = 2n;\n";
    await expectRejection(
      { "lens.ts": code, "rates.ts": rates },
      { message: "declared with let", file: "rates.ts", ...positionIn(rates, "rate = 2n") },
    );
  });

  it("contract(…) reached through a module of the application is not a lens", async () => {
    await expect(
      buildSnippet({
        "lens.ts": `
          import { pure, uint256 } from "@morpho-org/viem-dlc/solidity";
          import { contract } from "./dsl.js";

          export const Lens = contract("Lens", { run: pure(uint256).returns(uint256) }).implementation({
            run: (x) => x,
          });
        `,
        "dsl.ts": 'export { contract } from "@morpho-org/viem-dlc/solidity";',
      }),
    ).rejects.toThrow("the snippet holds no lens");
  });
});
