/**
 * Rewrite: the module as production gets it.
 *
 * Each lens expression becomes a call to the runtime entry's `compiledLens` with the lens's name, ABI
 * and bytecode. What only the removed bodies used goes too: the lens's own declarations that nothing
 * else in the module reads, and the import specifiers that are then unused. An exported declaration
 * stays, because another module may read it.
 *
 * "Reads" is decided by binding, not by spelling: the scope analysis says which identifiers use a
 * module-level binding, as a value or as a type. A property key, a label, or a parameter or local of the
 * same name is not among them, and a name read where its own scope does not yet hide it is.
 *
 * Every edit keeps the line count, so the rest of the file keeps its line numbers without a source map.
 */
import type { Span } from "oxc-parser";

import { type AnyNode, is, unwrap } from "./ast.js";
import { SOLIDITY_SPECIFIER } from "./builtins.js";
import { LINE_TERMINATOR } from "./errors.js";
import { usesOf } from "./scopes.js";
import type { BuiltLens, ModuleBinding, SourceModule } from "./types.js";

type Edit = readonly [start: number, end: number, text: string];

/** The line terminators of a text, each as it is written. */
const lineBreaks = (text: string): string[] => text.match(LINE_TERMINATOR) ?? [];

export function rewrite(module: SourceModule, lenses: readonly BuiltLens[]): string {
  const { code } = module;
  if (!lenses.length) return code;
  const edits: Edit[] = [];
  const removed: (readonly [number, number])[] = [];
  const inRemoved = (offset: number) => removed.some(([start, end]) => offset >= start && offset < end);

  // ---- the lens expressions ------------------------------------------------------------------------------
  let wrapper = "__viemDlcCompiledLens";
  while (module.bindings.has(wrapper) || code.includes(wrapper)) wrapper += "_";
  const first = lenses[0] as BuiltLens;
  const callee = unwrap(first.resolved.site.declaration.callee);
  const root = is(callee, "MemberExpression") ? unwrap(callee.object) : callee;
  const packageImport = module.bindings.get((root as AnyNode).name as string);
  /* v8 ignore next -- @preserve: a lens is only found through the name an import of the package gives, so this guards a broken caller */
  if (packageImport?.kind !== "import") throw new Error("rewrite: the lens's contract is not an import");
  // On the line of the import that brought `contract`, so no line moves. The leading `;` ends that import
  // when it is written without one.
  edits.push([
    packageImport.statement.end,
    packageImport.statement.end,
    `;import { compiledLens as ${wrapper} } from "${SOLIDITY_SPECIFIER}";`,
  ]);
  for (const { resolved, artifact } of lenses) {
    const { node } = resolved.site;
    const compiled = {
      name: resolved.name,
      abi: artifact.abi,
      bytecode: artifact.bytecode,
      immutables: resolved.immutables.map((immutable) => immutable.key),
    };
    edits.push([node.start, node.end, `/*#__PURE__*/ ${wrapper}(${JSON.stringify(compiled)})`]);
    removed.push([node.start, node.end]);
  }

  // ---- what only the removed bodies used -----------------------------------------------------------------
  /** True when something outside what is removed uses the binding. */
  const live = (binding: ModuleBinding) => usesOf(module, binding).some((use) => !inRemoved(use.start));

  // Only the lenses' own declarations are candidates: their initializers are stub calls and literals,
  // so removing one cannot remove a side effect.
  const declared = new Set<Span>();
  for (const { resolved } of lenses)
    for (const declaration of resolved.declarations) if (declaration.module === module) declared.add(declaration.node);
  const candidates: Exclude<ModuleBinding, { kind: "import" }>[] = [];
  for (const binding of module.bindings.values()) {
    if (binding.kind === "import" || binding.kind === "function" || binding.exported) continue;
    if (
      binding.kind === "class"
        ? declared.has(binding.node)
        : binding.init && (declared.has(binding.init) || declared.has(unwrap(binding.init)))
    )
      candidates.push(binding);
  }
  const gone = new Set<ModuleBinding>();
  for (let changed = true; changed; ) {
    changed = false;
    for (const binding of candidates) {
      if (gone.has(binding) || live(binding)) continue;
      gone.add(binding);
      changed = true;
      removed.push([binding.node.start, binding.node.end]);
    }
  }
  // A statement goes when every name it declares goes. Otherwise each declarator that goes takes one
  // comma with it, and the statement keeps declaring the others.
  const goneNodes = new Set<Span>([...gone].map((binding) => binding.node));
  for (const statement of new Set([...gone].map((binding) => binding.statement))) {
    const declarators = is(statement, "VariableDeclaration") ? statement.declarations : [];
    const goes = declarators.map((declarator) => goneNodes.has(declarator));
    if (goes.every(Boolean)) {
      edits.push([statement.start, statement.end, ""]);
      continue;
    }
    for (let first = 0; first < declarators.length; first++) {
      if (!goes[first]) continue;
      let last = first;
      while (goes[last + 1]) last++;
      const next = declarators[last + 1];
      edits.push(
        next
          ? [(declarators[first] as Span).start, next.start, ""]
          : [(declarators[first - 1] as Span).end, (declarators[last] as Span).end, ""],
      );
      first = last;
    }
  }

  // An import goes when a removed range used it and nothing left does.
  for (const statement of module.program.body) {
    if (!is(statement, "ImportDeclaration") || statement.importKind === "type") continue;
    const kept = statement.specifiers.filter((specifier) => {
      const binding = module.bindings.get(specifier.local.name);
      // An import of a type is no binding of the module, and is left for the compiler that erases it.
      if (binding?.node !== specifier) return true;
      const uses = usesOf(module, binding);
      return uses.every((use) => !inRemoved(use.start)) || uses.some((use) => !inRemoved(use.start));
    });
    if (kept.length === statement.specifiers.length) continue;
    const text = (node: Span) => code.slice(node.start, node.end);
    const named = kept.filter((specifier) => is(specifier, "ImportSpecifier")).map(text);
    const head = [
      ...kept.filter((specifier) => !is(specifier, "ImportSpecifier")).map(text),
      ...(named.length ? [`{ ${named.join(", ")} }`] : []),
    ];
    edits.push([
      statement.start,
      statement.end,
      kept.length ? `import ${head.join(", ")} from ${code.slice(statement.source.start, statement.end)}` : "",
    ]);
  }

  // ---- apply, last edit first, padding each with the line breaks it removed -------------------------------------
  let out = code;
  for (const [start, end, text] of edits.sort((a, b) => b[0] - a[0] || b[1] - a[1])) {
    // The terminators the replaced text held, as they were written, so a module keeps the kind it uses.
    const padding = lineBreaks(code.slice(start, end)).slice(lineBreaks(text).length).join("");
    out = out.slice(0, start) + text + padding + out.slice(end);
  }
  return out;
}
