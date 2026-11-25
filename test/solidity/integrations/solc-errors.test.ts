/**
 * solc's diagnostics in the core: what each kind becomes before any integration prints it. That every
 * integration then prints it at the same TypeScript position is covered by the host suites, which run
 * the cases of `./helpers/cases.ts`.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { afterAll, describe, expect, it } from "vitest";

import {
  build,
  findConfig,
  type LensDiagnostic,
  LensError,
  mapSolcDiagnostics,
  type PrintedLens,
  type SolcDiagnostic,
  type SolidityConfig,
} from "../../../src/solidity/build/index.js";
import { buildSnippet, positionIn } from "../helpers/harness.js";

const FILE = path.join(import.meta.dirname, "..", "__fixtures__", "lens.ts");

const lens = (
  body: string,
  declarations = "",
) => `import { assembly, contract, pure, Struct, uint128, uint256 } from "@morpho-org/viem-dlc/solidity";
${declarations}
export const Lens = contract("Lens", {
  run: pure(uint256).returns(uint256),
}).implementation({
  run(x) {
    ${body}
  },
});
`;

const temporary: string[] = [];
afterAll(() => {
  for (const dir of temporary) fs.rmSync(dir, { recursive: true, force: true });
});

/** The repository's configuration with a cache directory of the test's own, and optionally other settings. */
const configWith = (solc: Partial<SolidityConfig["solc"]> = {}): SolidityConfig => {
  const cacheDir = fs.mkdtempSync(path.join(os.tmpdir(), "viem-dlc-cache-"));
  temporary.push(cacheDir);
  const config = findConfig(FILE);
  return { ...config, cacheDir, solc: { ...config.solc, ...solc } };
};

/** Builds one module from memory, and returns what was thrown and what was warned. */
const diagnose = async (code: string, config = configWith()) => {
  const warnings: LensDiagnostic[] = [];
  let errors: readonly LensDiagnostic[] = [];
  try {
    await build(
      code,
      FILE,
      { resolve: () => undefined, addDependency: () => {}, warn: (warning) => warnings.push(warning) },
      { config },
    );
  } catch (cause) {
    if (!(cause instanceof LensError)) throw cause;
    errors = cause.diagnostics;
  }
  return { errors, warnings };
};

describe("solc's diagnostics, at TypeScript positions", () => {
  it("a type error: at the expression", async () => {
    const code = lens("const narrow = uint128(1n);\n    narrow = x;\n    return narrow;");
    const { errors } = await diagnose(code);
    expect(errors).toEqual([
      {
        file: FILE,
        ...positionIn(code, "x;"),
        severity: "error",
        source: "solc",
        lens: "Lens",
        code: "7407",
        message: "Type uint256 is not implicitly convertible to expected type uint128.",
      },
    ]);
  });

  it("a declaration error: at the name", async () => {
    // A function inside an assembly block reads a local of the block, which Yul does not give it.
    const code = lens(
      'const out = assembly("memory-safe", () => {\n      const base = x;\n      const get = () => base;\n      return get();\n    });\n    return out;',
    );
    const { errors } = await diagnose(code);
    expect(errors[0]).toMatchObject({
      ...positionIn(code, "base;\n      return"),
      severity: "error",
      source: "solc",
      code: "8198",
      message: 'Identifier "base" not found.',
    });
  });

  it("a syntax error: at the statement", async () => {
    // A word only Yul reserves: the transform leaves those to solc, which stops at it while parsing.
    const code = lens(
      'const out = assembly("memory-safe", () => {\n      const leave = x;\n      return leave;\n    });\n    return out;',
    );
    const { errors } = await diagnose(code);
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatchObject({
      ...positionIn(code, "const leave"),
      source: "solc",
      code: "2314",
      message: "Expected identifier but got 'leave'",
    });
  });

  it("several errors of one compile: each at its own position", async () => {
    const code = lens(
      "const a = uint128(1n);\n    const b = uint128(2n);\n    a = x;\n    b = x + 1n;\n    return a + b;",
    );
    const { errors } = await diagnose(code);
    expect(errors.map(({ line, column, code: solcCode }) => ({ line, column, code: solcCode }))).toEqual([
      { ...positionIn(code, "x;"), code: "7407" },
      { ...positionIn(code, "x + 1n"), code: "7407" },
    ]);
  });

  it("a warning: reported to the host, and the build goes on", async () => {
    const code = lens("const unused = x;\n    return x;");
    const { errors, warnings } = await diagnose(code);
    expect(errors).toEqual([]);
    expect(warnings).toEqual([
      {
        file: FILE,
        ...positionIn(code, "const unused"),
        severity: "warning",
        source: "solc",
        lens: "Lens",
        code: "2072",
        message: "Unused local variable.",
      },
    ]);
  });

  it("a warning is reported again when the lens comes from the cache", async () => {
    const code = lens("const unused = x;\n    return x;");
    const config = configWith();
    const cold = await diagnose(code, config);
    expect(fs.readdirSync(config.cacheDir)).toHaveLength(1);
    const warm = await diagnose(code, config);
    expect(warm.warnings).toEqual(cold.warnings);
    expect(warm.warnings).toHaveLength(1);
  });

  it("an error without a location: against the lens", async () => {
    const code = lens("return x;");
    const { errors } = await diagnose(code, configWith({ settings: { evmVersion: "atlantis" } }));
    expect(errors).toEqual([
      {
        file: FILE,
        ...positionIn(code, 'contract("Lens"'),
        severity: "error",
        source: "solc",
        lens: "Lens",
        message: "Invalid EVM version requested.",
      },
    ]);
  });

  it("a solc other than the pinned one: against the lens, and never a substitute", async () => {
    const code = lens("return x;");
    const { errors } = await diagnose(code, configWith({ version: "0.8.19" }));
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatchObject({ ...positionIn(code, 'contract("Lens"'), source: "transform", lens: "Lens" });
    expect(errors[0]?.message).toMatch(
      /^the lens Lens needs compiling: the repository pins solc 0\.8\.19 and solc 0\.8\.\d+\+\S+ is installed$/,
    );
  });
});

describe("mapSolcDiagnostics", () => {
  const code = lens("const first = x;\n    return first;");
  const diagnostic = (
    range: SolcDiagnostic["range"],
    severity: SolcDiagnostic["severity"] = "error",
  ): SolcDiagnostic => ({
    severity,
    type: "TypeError",
    message: "made up",
    formattedMessage: "made up",
    ...(range ? { range } : {}),
  });

  // A printed lens made by hand, to give solc's byte offsets something to disagree with: "é" is one
  // UTF-16 unit and two UTF-8 bytes, so every node after it sits one byte later for solc.
  const solidity = '// header\nstring s = unicode"é"; uint256 a = b;';
  const printed: PrintedLens = {
    solidity,
    map: {
      entries: [
        { start: solidity.indexOf("b;"), end: solidity.indexOf("b;") + 1, file: "other.ts", line: 7, column: 3 },
        { start: solidity.indexOf("uint256"), end: solidity.length, file: FILE, line: 5, column: 9 },
        { start: solidity.indexOf("string"), end: solidity.indexOf("uint256") - 1, file: FILE, line: 4, column: 9 },
      ],
    },
  };
  const bytes = (needle: string) => Buffer.byteLength(solidity.slice(0, solidity.indexOf(needle)), "utf8");

  const map = async (diagnostics: SolcDiagnostic[]) => {
    const [built] = (await buildSnippet(code)).lenses;
    if (!built) throw new Error("the snippet holds no lens");
    return mapSolcDiagnostics(diagnostics, printed, built.resolved).map(({ file, line, column, severity }) => ({
      file: path.basename(file),
      line,
      column,
      severity,
    }));
  };

  it("reads solc's ranges as UTF-8 bytes, and takes the innermost node that covers one", async () => {
    // Read as UTF-16 offsets, the same numbers would name the ";" after "b", and the statement around
    // it would be reported instead.
    expect(bytes("b;")).toBe(solidity.indexOf("b;") + 1);
    expect(await map([diagnostic({ start: bytes("b;"), end: bytes("b;") + 1 })])).toEqual([
      { file: "other.ts", line: 7, column: 3, severity: "error" },
    ]);
    // A range over more than one node: the innermost node that covers all of it.
    expect(await map([diagnostic({ start: bytes("a ="), end: bytes("b;") + 1 })])).toEqual([
      { file: "lens.ts", line: 5, column: 9, severity: "error" },
    ]);
  });

  it("a range no printed node covers is reported against the lens, as one without a range is", async () => {
    const atLens = { file: "lens.ts", ...positionIn(code, 'contract("Lens"'), severity: "error" };
    // The header: printed, and inside no node.
    const outside = { start: 0, end: bytes("string") - 1 };
    expect(await map([diagnostic(outside), diagnostic(undefined)])).toEqual([atLens, atLens]);
  });

  it("solc's info is a warning", async () => {
    expect(
      (await map([diagnostic(undefined, "info"), diagnostic(undefined, "warning")])).map((d) => d.severity),
    ).toEqual(["warning", "warning"]);
  });
});
