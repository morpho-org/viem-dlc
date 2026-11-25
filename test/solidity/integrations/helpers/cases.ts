/**
 * One fixture per kind of thing solc reports, as edits to the fixture project. Every integration is run
 * on each, and must report it at the TypeScript position given here.
 *
 * tsc accepts some of these and not others. That is beside the point: a build runs solc, not tsc, and
 * solc is where these are caught.
 */
import { positionIn } from "../../helpers/harness.js";

export interface DiagnosticCase {
  /** The kind of diagnostic, as the test's name. */
  kind: string;
  /** Files of the fixture project to overwrite, by path. */
  files: Readonly<Record<string, string>>;
  severity: "error" | "warning";
  message: string;
  /** solc's category for it. */
  solcType: string;
  /** The file the diagnostic is reported in, and the text it is reported at. */
  file: string;
  at: string;
}

const LENS = "src/lens.ts";
const MATH = "src/lib/math.ts";
const CONFIG = "solidity.config.json";

const lensModule = (body: string) => `import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";

export const QuoteLens = contract("QuoteLens", {
  quote: pure(uint256).returns(uint256),
}).implementation({
  quote(assets) {
${body}
  },
});
`;

export const DIAGNOSTIC_CASES: readonly DiagnosticCase[] = [
  {
    // In a helper's module, not the lens's: the position has to name the right file.
    kind: "a type error, in a helper's module",
    files: {
      [MATH]: `import { internal, pure, uint128, uint256 } from "@morpho-org/viem-dlc/solidity";

export const mulDivDown = internal(pure(uint256, uint256, uint256).returns(uint128), (x, y, d) => {
  return (x * y) / d;
});
`,
    },
    severity: "error",
    solcType: "TypeError",
    message:
      "Return argument type uint256 is not implicitly convertible to expected type (type of first return variable) uint128.",
    file: MATH,
    at: "(x * y) / d",
  },
  {
    // A function inside an assembly block reads a local of the block, which Yul does not give it.
    kind: "a declaration error",
    files: {
      [LENS]: `import { assembly, contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
import { add, mload } from "@morpho-org/viem-dlc/solidity/yul";

export const QuoteLens = contract("QuoteLens", {
  quote: pure(uint256).returns(uint256),
}).implementation({
  quote(assets) {
    const net = assembly("memory-safe", () => {
      const base = mload(0x40);
      const offset = (a: bigint) => add(base, a);
      return offset(assets);
    });
    return net;
  },
});
`,
    },
    severity: "error",
    solcType: "DeclarationError",
    message: 'Identifier "base" not found.',
    file: LENS,
    at: "base, a",
  },
  {
    // A word only Yul reserves: the transform leaves those to solc, which stops at it while parsing.
    kind: "a syntax error",
    files: {
      [LENS]: `import { assembly, contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";

export const QuoteLens = contract("QuoteLens", {
  quote: pure(uint256).returns(uint256),
}).implementation({
  quote(assets) {
    const out = assembly("memory-safe", () => {
      const leave = assets;
      return leave;
    });
    return out;
  },
});
`,
    },
    severity: "error",
    solcType: "ParserError",
    message: "Expected identifier but got 'leave'",
    file: LENS,
    at: "leave = assets",
  },
  {
    // A warning is reported and does not stop the build.
    kind: "a warning",
    files: { [LENS]: lensModule("    const unusedLocal = assets;\n    return assets;") },
    severity: "warning",
    solcType: "Warning",
    message: "Unused local variable.",
    file: LENS,
    at: "const unusedLocal",
  },
  {
    // solc gives no source location for a setting it refuses, so it is reported against the lens.
    kind: "an error without a location",
    files: {
      [CONFIG]: `${JSON.stringify({ solc: { version: "0.8.36", settings: { evmVersion: "atlantis" } } }, null, 2)}\n`,
    },
    severity: "error",
    solcType: "JSONError",
    message: "Invalid EVM version requested.",
    file: LENS,
    at: 'contract("QuoteLens"',
  },
];

/** `src/lens.ts:8:17 - error: …`, as every integration prints it somewhere in its output. */
export const expectedLine = (diagnosticCase: DiagnosticCase, fileText: string): string => {
  const { line, column } = positionIn(fileText, diagnosticCase.at);
  return `${diagnosticCase.file}:${line}:${column} - ${diagnosticCase.severity}: ${diagnosticCase.message}`;
};
