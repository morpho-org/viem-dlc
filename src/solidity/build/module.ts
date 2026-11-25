/**
 * Parse: one file's text to a {@link SourceModule}. The only module that calls the TypeScript parser.
 *
 * It records what the file declares at module level and what it imports and exports, and evaluates
 * nothing. TypeScript and TSX are parsed as such; a package's built `.js` is parsed the same way.
 *
 * A binding is recorded when the transform can read it: an import, or a name declared alone by a
 * statement of the module's own level. An export is recorded whatever it exports, because which names a
 * module exports decides what an `export *` of it passes on.
 */
import { parseSync, type Span, type Statement } from "oxc-parser";

import { type AnyNode, is, isErased, keyName, patternNames, unwrap } from "./ast.js";
import { LensError, LINE_TERMINATOR } from "./errors.js";
import type { ModuleBinding, ModuleExport, SourceModule } from "./types.js";

const lineStartsOf = (code: string): number[] => {
  const starts = [0];
  for (const terminator of code.matchAll(LINE_TERMINATOR)) starts.push(terminator.index + terminator[0].length);
  return starts;
};

export function parseModule(file: string, code: string): SourceModule {
  // Every node carries its range and TypeScript's fields, whatever the file's extension: the scope analyzer reads both.
  const parsed = parseSync(file, code, { sourceType: "module", preserveParens: true, range: true, astType: "ts" });
  const lineStarts = lineStartsOf(code);
  const bindings = new Map<string, ModuleBinding>();
  const exports = new Map<string, ModuleExport>();
  const starExports: string[] = [];
  const module: SourceModule = { file, code, program: parsed.program, lineStarts, bindings, exports, starExports };

  const [error] = parsed.errors;
  if (error) {
    const offset = error.labels[0]?.start ?? 0;
    let line = 0;
    /* v8 ignore next -- @preserve: the index is in range, and the fallback after ?? is there for the type checker */
    while (line + 1 < lineStarts.length && (lineStarts[line + 1] ?? 0) <= offset) line++;
    /* v8 ignore next -- @preserve: the index is in range, and the fallback after ?? is there for the type checker */
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
    if (isErased(node)) return;
    if (is(node, "VariableDeclaration")) {
      for (const declarator of node.declarations) {
        if (!is(declarator.id, "Identifier")) {
          // A name declared by destructuring is no binding a lens reads. Exported, it is still an export.
          if (exported)
            for (const name of patternNames(declarator.id)) exports.set(name, { kind: "local", local: name });
          continue;
        }
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
      if (!node.id) return;
      const name = node.id.name;
      bindings.set(
        name,
        is(node, "ClassDeclaration")
          ? { kind: "class", name, node, statement, exported }
          : { kind: "function", name, node, statement, exported },
      );
      if (exported) exports.set(name, { kind: "local", local: name });
    } else if (exported) {
      // An enum, a namespace or an import alias: a value the module exports, and no binding a lens reads.
      // A namespace written A.B exports A.
      let id = (node as AnyNode).id as AnyNode;
      while (is(id, "TSQualifiedName")) id = id.left as AnyNode;
      const name = id.name as string;
      exports.set(name, { kind: "local", local: name });
    }
  };

  for (const statement of parsed.program.body) {
    if (is(statement, "ImportDeclaration")) {
      if (statement.importKind === "type") continue;
      for (const specifier of statement.specifiers) {
        if (is(specifier, "ImportSpecifier") && specifier.importKind === "type") continue;
        /* v8 ignore next -- @preserve: an imported name is an identifier or a string, so the fallback after ?? is there for the type checker */
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
        /* v8 ignore next -- @preserve: an exported name and the local it names are each an identifier or a string */
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
      } else if (!isErased(unwrap(declaration))) {
        // A default export that is a name is that binding, as `export { name as default }` is.
        const value = unwrap(declaration);
        exports.set(
          "default",
          is(value, "Identifier")
            ? { kind: "local", local: value.name }
            : { kind: "default", node: statement.declaration as never },
        );
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
