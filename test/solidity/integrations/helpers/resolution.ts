/**
 * One fixture for "a lens reads the module the application imports", written into a fixture project
 * and run under each host.
 *
 * The lens returns four constants, each imported in a way where a resolver of the build's own could
 * pick another file than the host does:
 *
 * - `./unit.js`, which exists beside a `unit.ts` holding another value;
 * - `fixture-units`, whose `exports` answer by condition;
 * - `fixture-dual`, whose `exports` answer `import` and `require` differently;
 * - `./only.js`, which exists only as `only.ts`: the one import a host may not resolve as written. A
 *   fixture for a host that refuses the import in an application module too is written without it.
 *
 * The lens module exports the first three again, so a script can print what the host's own import of
 * each gave. What the lens must return is that value, and the tests also state it as a number.
 */
import { encodeFunctionData, type Hex } from "viem";
import { expect } from "vitest";

import { SOLIDITY_HEADER } from "../../../../src/solidity/build/index.js";
import type { Evm } from "../../helpers/anvil.js";
import { compileSolidity, expectSameAst } from "../../helpers/harness.js";

import type { Project } from "./project.js";

/** What each file of the fixture exports. */
export const VALUES = {
  "unit.js": 17n,
  "unit.ts": 29n,
  "only.ts": 41n,
  "fixture-units, by default": 43n,
  "fixture-units, under the condition 'fixture'": 47n,
  "fixture-units, under the condition 'react-server'": 53n,
  "fixture-dual, imported": 59n,
  "fixture-dual, required": 61n,
} as const;

const packageFiles = (
  name: string,
  exportName: string,
  exports: Record<string, string>,
  files: Record<string, bigint>,
) => ({
  [`node_modules/${name}/package.json`]: `${JSON.stringify(
    { name, version: "1.0.0", type: "module", exports: { types: "./index.d.ts", ...exports } },
    null,
    2,
  )}\n`,
  [`node_modules/${name}/index.d.ts`]: `export declare const ${exportName}: bigint;\n`,
  ...Object.fromEntries(
    Object.entries(files).map(([file, value]) => [
      `node_modules/${name}/${file}`,
      `export const ${exportName} = ${value}n;\n`,
    ]),
  ),
});

/** `fixture-dual`, with its two answers in either order. */
export const dualPackage = (first: "import" | "require") => {
  const answers = { import: "./import.js", require: "./require.js" };
  return packageFiles(
    "fixture-dual",
    "DUAL",
    first === "import" ? answers : { require: answers.require, import: answers.import },
    { "import.js": VALUES["fixture-dual, imported"], "require.js": VALUES["fixture-dual, required"] },
  );
};

const lensModule = (sourceOnly: boolean) => `import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
import { DUAL } from "fixture-dual";
import { SCALE } from "fixture-units";

${sourceOnly ? 'import { ONLY } from "./only.js";\n' : ""}import { UNIT } from "./unit.js";

// Exported again: what the host's own import of each gives can then be read beside the lens.
export { DUAL, SCALE, UNIT };

export const ResolvedLens = contract("ResolvedLens", {
  sibling: pure().returns(uint256),
  conditional: pure().returns(uint256),
  dual: pure().returns(uint256),${sourceOnly ? "\n  sourceOnly: pure().returns(uint256)," : ""}
}).implementation({
  sibling: () => UNIT,
  conditional: () => SCALE,
  dual: () => DUAL,${sourceOnly ? "\n  sourceOnly: () => ONLY," : ""}
});
`;

export interface ResolutionFixture {
  /** Which of its two answers `fixture-dual` lists first. */
  dual?: "import" | "require";
  /** Whether the lens reads `./only.js` too. */
  sourceOnly?: boolean;
}

/** The expression a script evaluates to report the lens and the host's own imports, as JSON. */
export const REPORT =
  "JSON.stringify({ bytecode: ResolvedLens.bytecode, imported: { sibling: String(UNIT), conditional: String(SCALE), dual: String(DUAL) } })";

/** Writes the fixture's modules under `dir` of the project, and its packages into `node_modules`. */
export function writeResolutionFixture(
  project: Project,
  dir: string,
  { dual = "require", sourceOnly = true }: ResolutionFixture = {},
): void {
  const files = {
    [`${dir}/lens.ts`]: lensModule(sourceOnly),
    [`${dir}/unit.js`]: `export const UNIT = ${VALUES["unit.js"]}n;\n`,
    [`${dir}/unit.ts`]: `export const UNIT = ${VALUES["unit.ts"]}n;\n`,
    [`${dir}/only.ts`]: `export const ONLY = ${VALUES["only.ts"]}n;\n`,
    ...packageFiles(
      "fixture-units",
      "SCALE",
      { "react-server": "./react-server.js", fixture: "./fixture.js", default: "./default.js" },
      {
        "react-server.js": VALUES["fixture-units, under the condition 'react-server'"],
        "fixture.js": VALUES["fixture-units, under the condition 'fixture'"],
        "default.js": VALUES["fixture-units, by default"],
      },
    ),
    ...dualPackage(dual),
  };
  for (const [file, content] of Object.entries(files)) project.write(file, content);
}

export interface Reported {
  bytecode: Hex;
  imported: { sibling: string; conditional: string; dual: string };
}

/** The Solidity a person writes for the lens, given what `fixture-units` resolves to. */
export const handwritten = (conditional: bigint, sourceOnly = true): string => `${SOLIDITY_HEADER}
contract ResolvedLens {
    function sibling() external pure returns (uint256) {
        return ${VALUES["unit.js"]};
    }

    function conditional() external pure returns (uint256) {
        return ${conditional};
    }

    function dual() external pure returns (uint256) {
        return ${VALUES["fixture-dual, imported"]};
    }
${
  sourceOnly
    ? `
    function sourceOnly() external pure returns (uint256) {
        return ${VALUES["only.ts"]};
    }
`
    : ""
}}
`;

/**
 * The lens a host served returns, on every call, the bytes of the handwritten Solidity, and the first
 * three are the values the host's own imports gave. When the host can print the Solidity it compiled,
 * that is the handwritten Solidity too.
 */
export async function expectHostResolution(
  evm: Evm,
  reported: Reported,
  conditional: bigint,
  { printed, sourceOnly = true }: { printed?: string; sourceOnly?: boolean } = {},
): Promise<void> {
  const expected = compileSolidity(handwritten(conditional, sourceOnly), "ResolvedLens");
  if (printed !== undefined)
    expectSameAst({
      name: "ResolvedLens",
      printed,
      ported: compileSolidity(printed, "ResolvedLens"),
      handwritten: expected,
    });

  const served = await evm.deploy(reported.bytecode);
  const reference = await evm.deploy(expected.bytecode);
  const returned: Record<string, string> = {};
  for (const functionName of [
    "sibling",
    "conditional",
    "dual",
    ...(sourceOnly ? (["sourceOnly"] as const) : []),
  ] as const) {
    const data = encodeFunctionData({ abi: expected.abi, functionName });
    const outcome = await evm.call(served, data);
    expect(outcome, functionName).toEqual(await evm.call(reference, data));
    returned[functionName] = String(BigInt(outcome.data));
  }
  expect(returned).toEqual({ ...reported.imported, ...(sourceOnly && { sourceOnly: String(VALUES["only.ts"]) }) });
  expect(reported.imported).toEqual({
    sibling: String(VALUES["unit.js"]),
    conditional: String(conditional),
    dual: String(VALUES["fixture-dual, imported"]),
  });
}
