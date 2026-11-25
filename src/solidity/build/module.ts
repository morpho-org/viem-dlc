/**
 * Parse: one file's text to a {@link SourceModule}. The only module that calls the TypeScript parser.
 *
 * It records what the file declares at module level and what it imports and exports, and evaluates
 * nothing. TypeScript and TSX are parsed as such; a package's built `.js` is parsed the same way.
 */
import { parseSync, type Span, type Statement } from "oxc-parser";

import { type AnyNode, is, keyName } from "./ast.js";
import { LensError, LINE_TERMINATOR } from "./errors.js";
import type { ModuleBinding, ModuleExport, SourceModule } from "./types.js";

const lineStartsOf = (code: string): number[] => {
  const starts = [0];
  for (const terminator of code.matchAll(LINE_TERMINATOR)) starts.push(terminator.index + terminator[0].length);
  return starts;
};

export function parseModule(file: string, code: string): SourceModule {
  const parsed = parseSync(file, code, { sourceType: "module", preserveParens: true });
  const lineStarts = lineStartsOf(code);
  const bindings = new Map<string, ModuleBinding>();
  const exports = new Map<string, ModuleExport>();
  const starExports: string[] = [];
  const module: SourceModule = { file, code, program: parsed.program, lineStarts, bindings, exports, starExports };

  const [error] = parsed.errors;
  if (error) {
    const offset = error.labels[0]?.start ?? 0;
    let line = 0;
    while (line + 1 < lineStarts.length && (lineStarts[line + 1] ?? 0) <= offset) line++;
    throw new LensError([
      {
        file,
        line: line + 1,
        column: offset - (lineStarts[line] ?? 0) + 1,
        severity: "error",
        source: "transform",
        message: `cannot parse this module: ${error.message}`,
      },
    ]);
  }

  const declare = (node: Span, statement: Statement, exported: boolean) => {
    if (is(node, "VariableDeclaration")) {
      if (node.declare) return;
      for (const declarator of node.declarations) {
        if (!is(declarator.id, "Identifier")) continue;
        const name = declarator.id.name;
        bindings.set(name, {
          kind: "variable",
          name,
          declarationKind: node.kind,
          init: declarator.init,
          node: declarator,
          statement,
          single: node.declarations.length === 1,
          exported,
        });
        if (exported) exports.set(name, { kind: "local", local: name });
      }
    } else if (is(node, "ClassDeclaration") || is(node, "FunctionDeclaration")) {
      if (!node.id || node.declare) return;
      const name = node.id.name;
      bindings.set(
        name,
        is(node, "ClassDeclaration")
          ? { kind: "class", name, node, statement, exported }
          : { kind: "function", name, node, statement, exported },
      );
      if (exported) exports.set(name, { kind: "local", local: name });
    }
  };

  for (const statement of parsed.program.body) {
    if (is(statement, "ImportDeclaration")) {
      if (statement.importKind === "type") continue;
      for (const specifier of statement.specifiers) {
        if (is(specifier, "ImportSpecifier") && specifier.importKind === "type") continue;
        const imported = is(specifier, "ImportDefaultSpecifier")
          ? "default"
          : is(specifier, "ImportNamespaceSpecifier")
            ? "*"
            : (keyName(specifier.imported) ?? "");
        bindings.set(specifier.local.name, {
          kind: "import",
          name: specifier.local.name,
          source: statement.source.value,
          imported,
          node: specifier,
          statement,
        });
      }
    } else if (is(statement, "ExportNamedDeclaration")) {
      if (statement.exportKind === "type") continue;
      if (statement.declaration) declare(statement.declaration, statement, true);
      for (const specifier of statement.specifiers) {
        if (specifier.exportKind === "type") continue;
        const exported = keyName(specifier.exported);
        const local = keyName(specifier.local);
        if (exported === undefined || local === undefined) continue;
        exports.set(
          exported,
          statement.source
            ? { kind: "reexport", source: statement.source.value, imported: local }
            : { kind: "local", local },
        );
      }
    } else if (is(statement, "ExportAllDeclaration")) {
      if (statement.exportKind === "type") continue;
      const exported = keyName(statement.exported);
      if (exported === undefined) starExports.push(statement.source.value);
      else exports.set(exported, { kind: "reexport", source: statement.source.value, imported: "*" });
    } else if (is(statement, "ExportDefaultDeclaration")) {
      const declaration = statement.declaration as AnyNode;
      if (declaration.type === "ClassDeclaration" || declaration.type === "FunctionDeclaration") {
        declare(declaration, statement, false);
        const name = keyName(declaration.id as Span | null);
        if (name !== undefined) exports.set("default", { kind: "local", local: name });
      } else if (!declaration.type.startsWith("TS")) {
        exports.set("default", { kind: "default", node: statement.declaration as never });
      }
    } else declare(statement, statement, false);
  }
  for (const entry of exports.values()) {
    if (entry.kind !== "local") continue;
    const binding = bindings.get(entry.local);
    if (binding && binding.kind !== "import") binding.exported = true;
  }
  return module;
}
