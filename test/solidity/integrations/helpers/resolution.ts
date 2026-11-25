/**
 * One fixture for "a lens reads the module the application imports", written into a fixture project
 * and run under each host.
 *
 * The lens returns constants, each imported in a way where a build could read another file than the
 * host loads, and that names one file all the same:
 *
 * - `./unit.js`, which exists beside a `unit.ts` holding another value;
 * - `fixture-shared`, whose `exports` give one file to `import` and to `require` alike;
 * - `fixture-reused`, whose `exports` name one file for the package, and name that file again, for
 *   `require` only, under another subpath;
 * - `#scale`, which the project's `imports` map to the package `fixture-scale`;
 * - `./only.js`, which exists only as `only.ts`: the one import a host may not resolve as written. A
 *   fixture for a host that refuses the import in an application module too is written without it.
 *
 * The lens module exports the first four again, so a script can print what the host's own import of
 * each gave. What the lens must return is that value, and the tests also state it as a number.
 *
 * {@link REFUSED} lists the imports that do not name one file. Each is refused at the import, under
 * every host and whatever conditions it resolves with.
 */
import { encodeFunctionData, type Hex } from "viem";
import { expect } from "vitest";

import { SOLIDITY_HEADER } from "../../../../src/solidity/build/index.js";
import type { Evm } from "../../helpers/anvil.js";
import { compileSolidity, expectSameAst, positionIn } from "../../helpers/harness.js";

import type { Project } from "./project.js";

/** What each file of the fixture exports. */
export const VALUES = {
  "unit.js": 17n,
  "unit.ts": 29n,
  "only.ts": 41n,
  "fixture-shared": 43n,
  "fixture-reused": 47n,
  "fixture-reused/other, imported": 61n,
  "fixture-scale": 53n,
} as const;

const packageFiles = (name: string, exportName: string, exports: unknown, files: Record<string, bigint>) => ({
  [`node_modules/${name}/package.json`]: `${JSON.stringify({ name, version: "1.0.0", type: "module", exports }, null, 2)}\n`,
  [`node_modules/${name}/index.d.ts`]: `export declare const ${exportName}: bigint;\n`,
  ...Object.fromEntries(
    Object.entries(files).map(([file, value]) => [
      `node_modules/${name}/${file}`,
      `export const ${exportName} = ${value}n;\n`,
    ]),
  ),
});

/** Adds entries to the `imports` of the project's own manifest. */
const addImports = (project: Project, imports: Record<string, unknown>): void => {
  const manifest = JSON.parse(project.read("package.json")) as { imports?: Record<string, unknown> };
  project.write(
    "package.json",
    `${JSON.stringify({ ...manifest, imports: { ...manifest.imports, ...imports } }, null, 2)}\n`,
  );
};

const lensModule = (sourceOnly: boolean) => `import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
import { SCALE } from "#scale";
import { REUSED } from "fixture-reused";
import { SHARED } from "fixture-shared";

${sourceOnly ? 'import { ONLY } from "./only.js";\n' : ""}import { UNIT } from "./unit.js";

// Exported again: what the host's own import of each gives can then be read beside the lens.
export { REUSED, SCALE, SHARED, UNIT };

export const ResolvedLens = contract("ResolvedLens", {
  sibling: pure().returns(uint256),
  shared: pure().returns(uint256),
  reused: pure().returns(uint256),
  mapped: pure().returns(uint256),${sourceOnly ? "\n  sourceOnly: pure().returns(uint256)," : ""}
}).implementation({
  sibling: () => UNIT,
  shared: () => SHARED,
  reused: () => REUSED,
  mapped: () => SCALE,${sourceOnly ? "\n  sourceOnly: () => ONLY," : ""}
});
`;

export interface ResolutionFixture {
  /** Whether the lens reads `./only.js` too. */
  sourceOnly?: boolean;
}

/** The expression a script evaluates to report the lens and the host's own imports, as JSON. */
export const REPORT =
  "JSON.stringify({ bytecode: ResolvedLens.bytecode, imported: { sibling: String(UNIT), shared: String(SHARED), reused: String(REUSED), mapped: String(SCALE) } })";

/** The names a script imports from the lens module to evaluate {@link REPORT}. */
export const REPORTED_NAMES = "REUSED, ResolvedLens, SCALE, SHARED, UNIT";

/** Writes the fixture's modules under `dir` of the project, and its packages into `node_modules`. */
export function writeResolutionFixture(
  project: Project,
  dir: string,
  { sourceOnly = true }: ResolutionFixture = {},
): void {
  const files = {
    [`${dir}/lens.ts`]: lensModule(sourceOnly),
    [`${dir}/unit.js`]: `export const UNIT = ${VALUES["unit.js"]}n;\n`,
    [`${dir}/unit.ts`]: `export const UNIT = ${VALUES["unit.ts"]}n;\n`,
    [`${dir}/only.ts`]: `export const ONLY = ${VALUES["only.ts"]}n;\n`,
    ...packageFiles(
      "fixture-shared",
      "SHARED",
      { types: "./index.d.ts", require: "./index.js", import: "./index.js" },
      { "index.js": VALUES["fixture-shared"] },
    ),
    ...packageFiles(
      "fixture-reused",
      "REUSED",
      {
        ".": { types: "./index.d.ts", default: "./index.js" },
        "./other": { types: "./index.d.ts", require: "./index.js", import: "./other.js" },
      },
      { "index.js": VALUES["fixture-reused"], "other.js": VALUES["fixture-reused/other, imported"] },
    ),
    ...packageFiles(
      "fixture-scale",
      "SCALE",
      { types: "./index.d.ts", default: "./index.js" },
      { "index.js": VALUES["fixture-scale"] },
    ),
  };
  for (const [file, content] of Object.entries(files)) project.write(file, content);
  addImports(project, { "#scale": "fixture-scale" });
}

export interface Reported {
  bytecode: Hex;
  imported: { sibling: string; shared: string; reused: string; mapped: string };
}

/** The Solidity a person writes for the lens. */
export const handwritten = (sourceOnly = true): string => `${SOLIDITY_HEADER}
contract ResolvedLens {
    function sibling() external pure returns (uint256) {
        return ${VALUES["unit.js"]};
    }

    function shared() external pure returns (uint256) {
        return ${VALUES["fixture-shared"]};
    }

    function reused() external pure returns (uint256) {
        return ${VALUES["fixture-reused"]};
    }

    function mapped() external pure returns (uint256) {
        return ${VALUES["fixture-scale"]};
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
 * four are the values the host's own imports gave. When the host can print the Solidity it compiled,
 * that is the handwritten Solidity too.
 */
export async function expectHostResolution(
  evm: Evm,
  reported: Reported,
  { printed, sourceOnly = true }: { printed?: string; sourceOnly?: boolean } = {},
): Promise<void> {
  const expected = compileSolidity(handwritten(sourceOnly), "ResolvedLens");
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
    "shared",
    "reused",
    "mapped",
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
    shared: String(VALUES["fixture-shared"]),
    reused: String(VALUES["fixture-reused"]),
    mapped: String(VALUES["fixture-scale"]),
  });
}

const RULE = "A lens reads a module only through an import that names one file under every condition.";

/** One import that does not name one file: what to install for it, and what the build says. */
interface RefusedImport {
  name: string;
  specifier: string;
  files: Record<string, string>;
  imports?: Record<string, unknown>;
  /** The error, given the project's directory. */
  message(root: string): string;
}

const twoAnswers = (name: string, exports: Record<string, string>) =>
  packageFiles(
    name,
    "RATE",
    { types: "./index.d.ts", ...exports },
    Object.fromEntries(Object.values(exports).map((file, index) => [file.slice(2), 71n + BigInt(index)])),
  );
const oneAnswer = (name: string, value: bigint) =>
  packageFiles(name, "RATE", { types: "./index.d.ts", default: "./index.js" }, { "index.js": value });

/**
 * The imports every host refuses. The last is the one a build gets wrong without a word when it takes
 * the answer of a resolver that is not told whether a module is imported or required: the two packages
 * are both installed, and each names one file.
 */
export const REFUSED: readonly RefusedImport[] = [
  {
    name: "a package whose exports answer by condition",
    specifier: "fixture-conditional",
    files: twoAnswers("fixture-conditional", {
      "react-server": "./server.js",
      fixture: "./fixture.js",
      default: "./default.js",
    }),
    message: (root) =>
      `the entry "." of "exports" in ${root}/node_modules/fixture-conditional/package.json names "./server.js" and "./fixture.js" and "./default.js", by condition. ${RULE}`,
  },
  {
    name: "a package whose exports answer import and require differently",
    specifier: "fixture-dual",
    files: twoAnswers("fixture-dual", { require: "./require.js", import: "./import.js" }),
    message: (root) =>
      `the entry "." of "exports" in ${root}/node_modules/fixture-dual/package.json names "./require.js" and "./import.js", by condition. ${RULE}`,
  },
  {
    name: "a # import that names one package to import and another to require",
    specifier: "#rate",
    files: { ...oneAnswer("fixture-rate-required", 79n), ...oneAnswer("fixture-rate-imported", 83n) },
    imports: {
      "#rate": { types: "fixture-rate-imported", require: "fixture-rate-required", import: "fixture-rate-imported" },
    },
    message: (root) =>
      `the entry "#rate" of "imports" in ${root}/package.json names "fixture-rate-required" and "fixture-rate-imported", by condition. ${RULE}`,
  },
];

/**
 * Writes a lens module at `file` of the project that reads a constant through a refused import, and
 * returns the line a host must report: the error, at the import, in tsc's format.
 */
export function writeRefusedImport(project: Project, file: string, refused: RefusedImport): string {
  const code = `import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
import { RATE } from "${refused.specifier}";

export { RATE };

export const RefusedLens = contract("RefusedLens", { rate: pure().returns(uint256) }).implementation({
  rate: () => RATE,
});
`;
  project.write(file, code);
  for (const [name, content] of Object.entries(refused.files)) project.write(name, content);
  if (refused.imports) addImports(project, refused.imports);
  const { line, column } = positionIn(code, `"${refused.specifier}"`);
  return `${file}:${line}:${column} - error: ${refused.message(project.dir)}`;
}
