/**
 * Scopes: the names bound around a node, between the module's own scope and the node.
 *
 * The transform reads module-level bindings. A name means its nearest binding, so the one question about
 * any other binding is whether it is nearer: a parameter or a local of the code around a node hides the
 * module-level name it shares.
 */
import type { Span } from "oxc-parser";

import { type AnyNode, children, is, patternNames } from "./ast.js";

const FUNCTIONS = new Set(["FunctionDeclaration", "FunctionExpression", "ArrowFunctionExpression"]);
const BLOCKS = new Set(["BlockStatement", "StaticBlock", "TSModuleBlock"]);
/** Declarations that bind their own name where they are written. */
const NAMED = new Set([
  "FunctionDeclaration",
  "ClassDeclaration",
  "TSEnumDeclaration",
  "TSModuleDeclaration",
  "TSImportEqualsDeclaration",
]);

const bind = (pattern: Span | null | undefined, into: Set<string>): void => {
  for (const name of patternNames(pattern)) into.add(name);
};

/** The names a list of statements declares in the scope the statements are written in. */
const declared = (statements: readonly AnyNode[], into: Set<string>): void => {
  for (const written of statements) {
    const exported = written.type.startsWith("Export") ? (written.declaration as AnyNode | null | undefined) : null;
    const statement = exported ?? written;
    if (is(statement, "VariableDeclaration"))
      for (const declarator of statement.declarations) bind(declarator.id, into);
    else if (NAMED.has(statement.type)) bind(statement.id as Span | null, into);
  }
};

/** The names declared with `var` anywhere under a node, which belong to the function around them. */
const hoistedVars = (node: AnyNode, into: Set<string>): void => {
  if (is(node, "VariableDeclaration") && node.kind === "var")
    for (const declarator of node.declarations) bind(declarator.id, into);
  for (const child of children(node)) if (!FUNCTIONS.has(child.type)) hoistedVars(child, into);
};

/** The names a node binds for what is written inside it. */
const scopeOf = (node: AnyNode): Set<string> => {
  const names = new Set<string>();
  if (FUNCTIONS.has(node.type)) {
    for (const parameter of node.params as Span[]) bind(parameter, names);
    bind(node.id as Span | null, names);
    if (node.body) hoistedVars(node.body as AnyNode, names);
  } else if (BLOCKS.has(node.type)) declared(node.body as AnyNode[], names);
  else if (is(node, "ForStatement")) declared(node.init ? [node.init as unknown as AnyNode] : [], names);
  else if (is(node, "ForInStatement") || is(node, "ForOfStatement")) declared([node.left as unknown as AnyNode], names);
  else if (is(node, "CatchClause")) bind(node.param, names);
  else if (is(node, "SwitchStatement"))
    declared(
      node.cases.flatMap((switchCase) => switchCase.consequent as unknown as AnyNode[]),
      names,
    );
  else if (is(node, "ClassDeclaration") || is(node, "ClassExpression")) bind(node.id, names);
  return names;
};

/**
 * The names bound by each scope between the module's own and `target`, a node of `program`, outermost
 * first. The module's own bindings are not among them: a name that is in none of these scopes is the
 * module's, or nobody's.
 */
export const enclosingScopes = (program: AnyNode, target: Span): ReadonlySet<string>[] => {
  const scopes: Set<string>[] = [];
  const around = (node: AnyNode) =>
    children(node).find((child) => child.start <= target.start && target.end <= child.end);
  for (let node = around(program); node && node !== target; node = around(node)) scopes.push(scopeOf(node));
  return scopes;
};

/** True when a scope between the module's own and `target` binds `name`, so the name there is not the module's. */
export const isHidden = (program: AnyNode, target: Span, name: string): boolean =>
  enclosingScopes(program, target).some((scope) => scope.has(name));
