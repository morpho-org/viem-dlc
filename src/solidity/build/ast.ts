/**
 * Small helpers over the parser's tree, shared by every stage. Type-only: nothing here calls the parser.
 */
import type { Node, Span } from "oxc-parser";

/** A node read structurally, for generic walks. */
export type AnyNode = Span & { readonly type: string; readonly [key: string]: unknown };

/** Every node interface whose `type` can be T. Several interfaces share one `type`, and some hold several. */
type NodeOf<T extends Node["type"]> = Node extends infer N
  ? N extends { type: infer U }
    ? T extends U
      ? N
      : never
    : never
  : never;

/** Narrows a node by its `type`. */
export const is = <T extends Node["type"]>(node: Span | null | undefined, type: T): node is NodeOf<T> =>
  node != null && (node as AnyNode).type === type;

const WRAPPERS = new Set([
  "ParenthesizedExpression",
  "TSAsExpression",
  "TSSatisfiesExpression",
  "TSNonNullExpression",
  "TSTypeAssertion",
  "TSInstantiationExpression",
]);

/**
 * Strips what the transform does not read: parentheses and TypeScript's `as`, `satisfies`, `!` and
 * `<T>` wrappers. Parentheses are printed from the tree's shape, never copied from the source, so a
 * module prints the same from `.ts` and from built `.js`.
 */
export const unwrap = (node: Span): AnyNode => {
  let current = node as AnyNode;
  while (WRAPPERS.has(current.type)) current = current.expression as AnyNode;
  return current;
};

/** TypeScript nodes that hold a value. Every other `TS…` node is a type, and names nothing at run time. */
const VALUE_NODES = new Set([
  "TSAsExpression",
  "TSSatisfiesExpression",
  "TSNonNullExpression",
  "TSTypeAssertion",
  "TSInstantiationExpression",
  "TSParameterProperty",
  "TSModuleDeclaration",
  "TSModuleBlock",
  "TSEnumDeclaration",
  "TSEnumBody",
  "TSEnumMember",
  "TSExportAssignment",
  "TSImportEqualsDeclaration",
  "TSExternalModuleReference",
  "TSQualifiedName",
]);

/**
 * True for what is only TypeScript, and is gone from the JavaScript a compiler emits: a type, a type
 * alias, an interface, an overload signature, and anything declared with `declare`. No stage reads
 * one, so a module means the same read from `.ts` source and from built `.js`.
 */
export const isErased = (node: Span): boolean => {
  const { type, declare } = node as AnyNode;
  return declare === true || (type.startsWith("TS") && !VALUE_NODES.has(type));
};

/** True for a statement that prints as nothing: the empty statement, and one that is only TypeScript. */
export const printsNothing = (statement: Span): boolean => is(statement, "EmptyStatement") || isErased(statement);

/** True for a `this` parameter, which is a TypeScript annotation and is erased with the others. */
export const isThisParameter = (parameter: Span): boolean => is(parameter, "Identifier") && parameter.name === "this";

/** Keys of a node that hold TypeScript types, labels or back-pointers, which no stage reads. */
const SKIPPED_KEYS = new Set([
  "type",
  "start",
  "end",
  "range",
  "parent",
  "typeAnnotation",
  "typeArguments",
  "typeParameters",
  "superTypeArguments",
  "returnType",
  "implements",
  "label",
]);

/**
 * The child nodes of a node, in source order: what is left of them in JavaScript. The keys of
 * {@link SKIPPED_KEYS}, the nodes that are only TypeScript and a `this` parameter are not among them, so
 * nothing that walks a tree reads a name a compiler erases.
 */
export const children = (node: AnyNode): AnyNode[] => {
  const out: AnyNode[] = [];
  for (const key in node) {
    if (SKIPPED_KEYS.has(key)) continue;
    const value = node[key];
    for (const item of Array.isArray(value) ? value : [value])
      if (isNode(item) && !isErased(item) && !(key === "params" && isThisParameter(item))) out.push(item);
  }
  return out;
};

const isNode = (value: unknown): value is AnyNode =>
  typeof value === "object" && value !== null && typeof (value as AnyNode).type === "string";

/** Visits a node and everything under it that {@link children} gives. */
export const walk = (node: AnyNode, visit: (node: AnyNode) => void): void => {
  visit(node);
  for (const child of children(node)) walk(child, visit);
};

/** The name of a plain property or specifier key: an identifier or a string literal. */
export const keyName = (key: Span | null | undefined): string | undefined => {
  if (is(key, "Identifier")) return key.name;
  if (is(key, "Literal") && typeof key.value === "string") return key.value;
  return undefined;
};

/** The names a binding pattern declares: `x`, `[a, , c]`, `{ a, b: c }`, `...rest`, `x = 1`. */
export const patternNames = (pattern: Span | null | undefined, into: string[] = []): string[] => {
  const node = pattern as AnyNode | null | undefined;
  if (!node) return into;
  switch (node.type) {
    case "Identifier":
      into.push(node.name as string);
      break;
    case "ArrayPattern":
      for (const element of node.elements as (Span | null)[]) patternNames(element, into);
      break;
    case "ObjectPattern":
      for (const property of node.properties as AnyNode[])
        patternNames(property.type === "RestElement" ? property : (property.value as Span), into);
      break;
    case "AssignmentPattern":
      patternNames(node.left as Span, into);
      break;
    case "RestElement":
      patternNames(node.argument as Span, into);
      break;
  }
  return into;
};
