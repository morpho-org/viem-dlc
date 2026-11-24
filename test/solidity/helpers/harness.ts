/**
 * The fixture-pair harness: how a construct of the DSL is proven.
 *
 * A fixture pair is a TypeScript snippet and the Solidity a person wrote for it. The transform does not
 * decide what anything means; it prints structure. So the one question is whether the Solidity it
 * prints is the Solidity the author wrote, and that has a direct test:
 *
 * - {@link expectSameAst}: solc's AST of the printed Solidity equals the AST of the handwritten one,
 *   ignoring node ids, source offsets and redundant parentheses.
 * - {@link expectSameExecution}: both return the same bytes, or revert with the same bytes, on fixed inputs.
 * - {@link expectRejection}: a snippet the transform refuses is refused with this message at this
 *   TypeScript line and column.
 *
 * Add one pair per row of the design's tables, in a `*.test.ts` beside this directory.
 */
import path from "node:path";
import { fileURLToPath } from "node:url";

import { type Abi, encodeFunctionData, type Hex } from "viem";
import { expect } from "vitest";

import type { LensDiagnostic, SolcDiagnostic } from "../../../src/solidity/build/index.js";
import { build, findConfig, LensError, runSolc, SOLIDITY_HEADER } from "../../../src/solidity/build/index.js";

import type { CallOutcome, Evm } from "./anvil.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
/** Where a snippet pretends to live: under this repository, so its `solidity.config.json` applies. */
const FIXTURE_DIR = path.join(HERE, "..", "__fixtures__");

/** A module's text, or several modules by file name. The first is the one holding the lens. */
export type Snippet = string | Readonly<Record<string, string>>;

const filesOf = (snippet: Snippet): Map<string, string> =>
  new Map(
    Object.entries(typeof snippet === "string" ? { "lens.ts": snippet } : snippet).map(([name, code]) => [
      path.join(FIXTURE_DIR, name),
      code,
    ]),
  );

/** Runs the build half on a snippet, without the rewrite. The modules exist only in memory. */
export async function buildSnippet(snippet: Snippet) {
  const files = filesOf(snippet);
  const [entry] = files.keys();
  if (entry === undefined) throw new Error("a snippet needs at least one module");
  const built = await build(
    files.get(entry) as string,
    entry,
    {
      resolve(specifier, importer) {
        const base = path.resolve(path.dirname(importer), specifier);
        return [base, base.replace(/\.js$/, ".ts"), `${base}.ts`].find((candidate) => files.has(candidate));
      },
      addDependency: () => {},
      readFile(file) {
        const code = files.get(file);
        if (code === undefined) throw new Error(`the snippet has no module ${file}`);
        return code;
      },
    },
    { cache: false },
  );
  if (!built) throw new Error("the snippet holds no lens");
  return built;
}

export interface CompiledSolidity {
  abi: Abi;
  bytecode: Hex;
  /** solc's AST of the source unit, as it comes. */
  ast: unknown;
}

/** Compiles a Solidity source with the repository's pinned compiler and settings, AST included. */
export function compileSolidity(solidity: string, contractName: string): CompiledSolidity {
  const config = findConfig(path.join(FIXTURE_DIR, "lens.ts"));
  const output = runSolc(
    {
      language: "Solidity",
      sources: { "Fixture.sol": { content: solidity } },
      settings: {
        ...config.solc.settings,
        outputSelection: { "*": { "": ["ast"], "*": ["abi", "evm.bytecode.object"] } },
      },
    },
    config.solc.version,
  ) as {
    errors?: SolcDiagnostic[];
    sources?: Record<string, { ast: unknown }>;
    contracts?: Record<string, Record<string, { abi: Abi; evm: { bytecode: { object: string } } }>>;
  };
  const errors = (output.errors ?? []).filter((error) => error.severity === "error");
  if (errors.length)
    throw new Error(
      `solc rejected the Solidity:\n${errors.map((error) => error.formattedMessage).join("\n")}\n${solidity}`,
    );
  const contract = output.contracts?.["Fixture.sol"]?.[contractName];
  if (!contract) throw new Error(`the Solidity has no contract named ${contractName}`);
  return {
    abi: contract.abi,
    bytecode: `0x${contract.evm.bytecode.object}`,
    ast: output.sources?.["Fixture.sol"]?.ast,
  };
}

/** Node ids, references to node ids, and source offsets: what two equal programs may differ in. */
const IGNORED_AST_KEYS = new Set([
  "id",
  "src",
  "nameLocation",
  "nameLocations",
  "memberLocation",
  "scope",
  "referencedDeclaration",
  "overloadedDeclarations",
  "exportedSymbols",
  "linearizedBaseContracts",
  "contractDependencies",
  "usedErrors",
  "usedEvents",
  "functionReturnParameters",
  "baseFunctions",
  "absolutePath",
  "typeIdentifier",
]);

/**
 * solc's AST without what {@link IGNORED_AST_KEYS} lists, and without redundant parentheses. solc keeps
 * `(e)` as a tuple of one component; the printer parenthesizes every nested operator and a person
 * writes only the parentheses precedence needs, so both are compared by the tree the parentheses make.
 */
export function normalizeAst(node: unknown): unknown {
  if (Array.isArray(node)) return node.map(normalizeAst);
  if (typeof node !== "object" || node === null) return node;
  const record = node as Record<string, unknown>;
  if (record.nodeType === "TupleExpression" && record.isInlineArray === false) {
    const components = record.components as unknown[];
    if (components.length === 1) return normalizeAst(components[0]);
  }
  return Object.fromEntries(
    Object.entries(record)
      .filter(([key]) => !IGNORED_AST_KEYS.has(key))
      .map(([key, value]) => [
        key,
        // The ids of the locals a statement declares: which positions declare one is kept, the ids are not.
        key === "assignments" && Array.isArray(value)
          ? value.map((id) => (id === null ? null : "declared"))
          : normalizeAst(value),
      ]),
  );
}

export interface FixturePair {
  /** The TypeScript: one module, or several by file name with the lens's first. */
  ts: Snippet;
  /** The Solidity a person wrote. The standard header is added when it has no pragma. */
  solidity: string;
}

export interface CompiledPair {
  name: string;
  /** What the printer printed for `ts`. */
  printed: string;
  ported: CompiledSolidity;
  handwritten: CompiledSolidity;
}

/** Prints the snippet's lens and compiles both sides of the pair the same way. */
export async function compilePair(pair: FixturePair): Promise<CompiledPair> {
  const built = await buildSnippet(pair.ts);
  const [lens] = built.lenses;
  if (!lens || built.lenses.length !== 1) throw new Error("a fixture pair holds exactly one lens");
  const name = lens.resolved.name;
  const handwritten = /^\s*pragma solidity/m.test(pair.solidity)
    ? pair.solidity
    : `${SOLIDITY_HEADER}\n${pair.solidity}`;
  return {
    name,
    printed: lens.printed.solidity,
    ported: compileSolidity(lens.printed.solidity, name),
    handwritten: compileSolidity(handwritten, name),
  };
}

/** The printed Solidity is the handwritten Solidity, as solc parses and types them. */
export function expectSameAst(pair: CompiledPair): void {
  expect(normalizeAst(pair.ported.ast), `printed:\n${pair.printed}`).toEqual(normalizeAst(pair.handwritten.ast));
}

export interface FixtureCall {
  functionName: string;
  args?: readonly unknown[];
}

/**
 * Both sides give the same bytes on every call: the same return data, or the same revert data. Returns
 * the outcomes, so a test can also assert what they are.
 */
export async function expectSameExecution(
  evm: Evm,
  pair: CompiledPair,
  calls: readonly FixtureCall[],
): Promise<CallOutcome[]> {
  const ported = await evm.deploy(pair.ported.bytecode);
  const handwritten = await evm.deploy(pair.handwritten.bytecode);
  const outcomes: CallOutcome[] = [];
  for (const call of calls) {
    const data = encodeFunctionData({
      abi: pair.handwritten.abi,
      functionName: call.functionName,
      args: call.args ?? [],
    });
    const expected = await evm.call(handwritten, data);
    const actual = await evm.call(ported, data);
    expect(actual, `${call.functionName}(${(call.args ?? []).join(", ")})`).toEqual(expected);
    outcomes.push(actual);
  }
  return outcomes;
}

export interface ExpectedRejection {
  message: string | RegExp;
  /** 1-based, in the snippet's first module unless `file` names another. */
  line: number;
  column: number;
  file?: string;
  source?: LensDiagnostic["source"];
}

/** The transform refuses the snippet, with this message at this TypeScript position. */
export async function expectRejection(snippet: Snippet, expected: ExpectedRejection): Promise<LensDiagnostic> {
  let error: unknown;
  try {
    await buildSnippet(snippet);
  } catch (cause) {
    error = cause;
  }
  if (!(error instanceof LensError))
    throw new Error(`expected a LensError, got ${error === undefined ? "a successful build" : String(error)}`, {
      cause: error,
    });
  const [diagnostic] = error.diagnostics;
  if (!diagnostic) throw new Error("the LensError carries no diagnostic");
  if (typeof expected.message === "string") expect(diagnostic.message).toContain(expected.message);
  else expect(diagnostic.message).toMatch(expected.message);
  expect({ file: path.basename(diagnostic.file), line: diagnostic.line, column: diagnostic.column }).toEqual({
    file: expected.file ?? path.basename([...filesOf(snippet).keys()][0] as string),
    line: expected.line,
    column: expected.column,
  });
  if (expected.source) expect(diagnostic.source).toBe(expected.source);
  return diagnostic;
}

/** The 1-based line and column of the first occurrence of `needle` in a snippet's module. */
export function positionIn(code: string, needle: string): { line: number; column: number } {
  const offset = code.indexOf(needle);
  if (offset < 0) throw new Error(`the snippet does not contain ${needle}`);
  const before = code.slice(0, offset).split("\n");
  return { line: before.length, column: (before.at(-1) ?? "").length + 1 };
}
