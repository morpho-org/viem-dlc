/**
 * Rewrite: the module as production gets it.
 *
 * Each lens expression becomes a call to the runtime entry's `compiledLens` with the lens's name, ABI
 * and bytecode. What only the removed bodies used goes too: the lens's own declarations that nothing
 * else in the module reads, and the import specifiers that are then unused. An exported declaration
 * stays, because another module may read it.
 *
 * "Reads" is decided by binding, not by spelling: a property key, a label, or a parameter or local of
 * the same name is not a use of the module's declaration. Where a position is not known to be one of
 * those, it counts as a use, so a doubt keeps a declaration and never removes one that is read.
 *
 * Every edit keeps the line count, so the rest of the file keeps its line numbers without a source map.
 */
import type { Span } from "oxc-parser";

import { type AnyNode, is, patternNames, unwrap } from "./ast.js";
import { SOLIDITY_SPECIFIER } from "./builtins.js";
import type { BuiltLens, ModuleBinding, SourceModule } from "./types.js";

type Edit = readonly [start: number, end: number, text: string];

const lineBreaks = (text: string): number => text.split("\n").length - 1;

const isNode = (value: unknown): value is AnyNode =>
  typeof value === "object" && value !== null && typeof (value as AnyNode).type === "string";

/** The children that name something other than a variable, unless the node is `computed`. */
const NAME_KEYS: Readonly<Record<string, readonly string[]>> = {
  MemberExpression: ["property"],
  Property: ["key"],
  MethodDefinition: ["key"],
  PropertyDefinition: ["key"],
  AccessorProperty: ["key"],
  TSPropertySignature: ["key"],
  TSMethodSignature: ["key"],
  TSEnumMember: ["id"],
  TSQualifiedName: ["right"],
  LabeledStatement: ["label"],
  BreakStatement: ["label"],
  ContinueStatement: ["label"],
  ExportSpecifier: ["exported"],
  ExportAllDeclaration: ["exported"],
  MetaProperty: ["meta", "property"],
  JSXAttribute: ["name"],
  JSXMemberExpression: ["property"],
};

/** The statements that declare a value under their `id`. A type of the same name hides no value. */
const VALUE_DECLARATIONS = new Set([
  "FunctionDeclaration",
  "ClassDeclaration",
  "TSDeclareFunction",
  "TSEnumDeclaration",
  "TSModuleDeclaration",
]);
/** The blocks whose `var`s are their own: a function's body is found by its `params`. */
const VAR_SCOPES = new Set(["BlockStatement", "StaticBlock", "TSModuleBlock"]);

/** The names the statements of one block declare for that block. */
const declaredBy = (statements: readonly AnyNode[], into: string[]): void => {
  for (const statement of statements) {
    const declaration = statement.type.startsWith("Export") ? statement.declaration : statement;
    if (!isNode(declaration)) continue;
    if (declaration.type === "VariableDeclaration")
      for (const declarator of declaration.declarations as AnyNode[]) patternNames(declarator.id as Span, into);
    else if (VALUE_DECLARATIONS.has(declaration.type)) patternNames(declaration.id as Span | null, into);
  }
};

/** The names of the `var`s under a node that are not another function's, static block's or namespace's. */
const hoisted = (node: unknown, into: string[]): void => {
  if (Array.isArray(node)) for (const item of node) hoisted(item, into);
  if (!isNode(node) || Array.isArray(node.params) || node.type === "StaticBlock" || node.type === "TSModuleBlock")
    return;
  if (node.type === "VariableDeclaration" && node.kind === "var")
    for (const declarator of node.declarations as AnyNode[]) patternNames(declarator.id as Span, into);
  for (const key in node) if (key !== "parent") hoisted(node[key], into);
};

/** The names a node declares for everything under it: the scopes of a strict-mode module. */
const scopeOf = (node: AnyNode): string[] => {
  const names: string[] = [];
  // A function, and a function's type: its parameters and, inside itself, its own name.
  if (Array.isArray(node.params)) for (const parameter of node.params as Span[]) patternNames(parameter, names);
  if (Array.isArray(node.params) || node.type.startsWith("Class")) patternNames(node.id as Span | null, names);
  // A block: what its statements declare, and every `var` below it, which the function around it holds.
  if (VAR_SCOPES.has(node.type)) {
    declaredBy(node.body as AnyNode[], names);
    hoisted(node.body, names);
  }
  if (node.type === "SwitchStatement")
    for (const switchCase of node.cases as AnyNode[]) declaredBy(switchCase.consequent as AnyNode[], names);
  if (node.type === "CatchClause") patternNames(node.param as Span | null, names);
  if (node.type.startsWith("For"))
    for (const head of [node.init, node.left]) if (isNode(head)) declaredBy([head], names);
  return names;
};

type Reference = Span & { name: string };

/**
 * Every identifier under a node that can be a use of a module-level binding, TypeScript types included:
 * a name used only as a type still counts as used. Left out are the names of {@link NAME_KEYS} and the
 * identifiers that a scope between the module and the identifier declares.
 */
const references = (node: unknown, into: Reference[], scopes: readonly string[][] = []): void => {
  if (Array.isArray(node)) {
    for (const item of node) references(item, into, scopes);
    return;
  }
  if (!isNode(node)) return;
  if (node.type === "Identifier" || node.type === "JSXIdentifier") {
    if (!scopes.some((scope) => scope.includes(node.name as string))) into.push(node as Span as Reference);
    return;
  }
  const names = node.computed === true ? [] : (NAME_KEYS[node.type] ?? []);
  // `export { a as b } from "…"` names the other module's bindings, not this one's.
  const skipped = node.type === "ExportNamedDeclaration" && node.source ? ["specifiers"] : names;
  const declared = scopeOf(node);
  const inner = declared.length ? [...scopes, declared] : scopes;
  for (const key in node) if (key !== "parent" && !skipped.includes(key)) references(node[key], into, inner);
};

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
  const uses: Reference[] = [];
  for (const statement of module.program.body) if (!is(statement, "ImportDeclaration")) references(statement, uses);
  const live = (name: string, own?: Span) =>
    uses.some((use) => use.name === name && use !== own && !inRemoved(use.start));

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
      if (gone.has(binding) || live(binding.name, binding.node.id ?? undefined)) continue;
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
  const touched = new Set(uses.filter((use) => inRemoved(use.start)).map((use) => use.name));
  for (const statement of module.program.body) {
    if (!is(statement, "ImportDeclaration") || statement.importKind === "type") continue;
    const kept = statement.specifiers.filter(
      (specifier) =>
        (is(specifier, "ImportSpecifier") && specifier.importKind === "type") ||
        !touched.has(specifier.local.name) ||
        live(specifier.local.name),
    );
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
    const padding = "\n".repeat(Math.max(0, lineBreaks(code.slice(start, end)) - lineBreaks(text)));
    out = out.slice(0, start) + text + padding + out.slice(end);
  }
  return out;
}
