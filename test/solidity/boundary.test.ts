/**
 * The import boundaries of `src/solidity`.
 *
 * - The `solidity` subpath and the rest of the package do not import each other, so a user who only
 *   wants caching pays for nothing here.
 * - The runtime half (`src/solidity` without `build/`) imports only viem and itself: no parser, no
 *   compiler, no Node built-in, nothing from the build half.
 * - Only the build half imports solc and the TypeScript parser, which are optional peer dependencies.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { parseSync } from "oxc-parser";
import { describe, expect, it } from "vitest";

const SRC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../src");
const SOLIDITY = path.join(SRC, "solidity");
const BUILD = path.join(SOLIDITY, "build");

const inside = (dir: string, file: string) => file === dir || file.startsWith(dir + path.sep);

const sources = (dir: string): string[] =>
  fs
    .readdirSync(dir, { withFileTypes: true })
    .flatMap((entry) =>
      entry.isDirectory()
        ? sources(path.join(dir, entry.name))
        : /\.[cm]?ts$/.test(entry.name)
          ? [path.join(dir, entry.name)]
          : [],
    );

/** Every specifier a file imports, exports from, requires or imports dynamically, types included. */
const specifiersOf = (file: string): string[] => {
  const found: string[] = [];
  const visit = (node: unknown): void => {
    if (Array.isArray(node)) {
      for (const child of node) visit(child);
      return;
    }
    if (typeof node !== "object" || node === null) return;
    const record = node as Record<string, unknown>;
    const literal = (value: unknown) => (value as { value?: unknown } | null)?.value;
    if (
      ["ImportDeclaration", "ExportNamedDeclaration", "ExportAllDeclaration", "ImportExpression"].includes(
        record.type as string,
      )
    ) {
      const source = literal(record.source);
      if (typeof source === "string") found.push(source);
    }
    if (record.type === "TSImportEqualsDeclaration") {
      const source = literal((record.moduleReference as { expression?: unknown }).expression);
      if (typeof source === "string") found.push(source);
    }
    if (record.type === "CallExpression" && (record.callee as { name?: string }).name === "require") {
      const source = literal((record.arguments as unknown[])[0]);
      if (typeof source === "string") found.push(source);
    }
    for (const key in record) if (key !== "parent") visit(record[key]);
  };
  const parsed = parseSync(file, fs.readFileSync(file, "utf8"));
  expect(parsed.errors).toEqual([]);
  visit(parsed.program);
  return found;
};

const imports = sources(SRC).map((file) => ({
  file,
  specifiers: specifiersOf(file).map((specifier) =>
    specifier.startsWith(".")
      ? { specifier, target: path.resolve(path.dirname(file), specifier) }
      : { specifier, target: undefined },
  ),
}));
const relative = (file: string) => path.relative(SRC, file);
const packageOf = (specifier: string) =>
  specifier
    .split("/")
    .slice(0, specifier.startsWith("@") ? 2 : 1)
    .join("/");

const BUILD_ONLY = ["solc", "oxc-parser", "oxc-resolver"];

describe("src/solidity import boundaries", () => {
  it("finds the files it is about", () => {
    expect(imports.some(({ file }) => inside(BUILD, file))).toBe(true);
    expect(imports.some(({ file }) => inside(SOLIDITY, file) && !inside(BUILD, file))).toBe(true);
    expect(imports.some(({ file }) => !inside(SOLIDITY, file))).toBe(true);
  });

  it("nothing outside src/solidity imports it", () => {
    const offenders = imports
      .filter(({ file }) => !inside(SOLIDITY, file))
      .flatMap(({ file, specifiers }) =>
        specifiers
          .filter(({ target }) => target && inside(SOLIDITY, target))
          .map(({ specifier }) => `${relative(file)} -> ${specifier}`),
      );
    expect(offenders).toEqual([]);
  });

  it("src/solidity imports nothing from the rest of the package", () => {
    const offenders = imports
      .filter(({ file }) => inside(SOLIDITY, file))
      .flatMap(({ file, specifiers }) =>
        specifiers
          .filter(({ target }) => target && !inside(SOLIDITY, target))
          .map(({ specifier }) => `${relative(file)} -> ${specifier}`),
      );
    expect(offenders).toEqual([]);
  });

  it("the runtime half imports only viem and itself", () => {
    const offenders = imports
      .filter(({ file }) => inside(SOLIDITY, file) && !inside(BUILD, file))
      .flatMap(({ file, specifiers }) =>
        specifiers
          .filter(({ specifier, target }) => (target ? inside(BUILD, target) : packageOf(specifier) !== "viem"))
          .map(({ specifier }) => `${relative(file)} -> ${specifier}`),
      );
    expect(offenders).toEqual([]);
  });

  it("only the build half imports solc and the parser", () => {
    const users = imports
      .filter(({ specifiers }) => specifiers.some(({ specifier }) => BUILD_ONLY.includes(packageOf(specifier))))
      .map(({ file }) => file);
    expect(users.length).toBeGreaterThan(0);
    expect(users.filter((file) => !inside(BUILD, file)).map(relative)).toEqual([]);
  });

  it("the parser is called from one module", () => {
    const callers = imports
      .filter(({ file }) => fs.readFileSync(file, "utf8").match(/import \{[^}]*\bparseSync\b[^}]*\} from "oxc-parser"/))
      .map(({ file }) => relative(file));
    expect(callers).toEqual([path.join("solidity", "build", "module.ts")]);
  });
});
