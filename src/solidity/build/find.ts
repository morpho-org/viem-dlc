/**
 * Find: the lenses of one module.
 *
 * A lens is `contract(name, functions, options?).implementation({ … })` where `contract` is imported
 * from the package directly, at any nesting depth. Its parts are literals written
 * in place, and anything else is a located error.
 *
 * What `contract` is bound to is read from the scope analysis, not from its spelling: where a parameter
 * or a local is named like the imported `contract`, or like the namespace it is read off, the call is
 * ordinary code and is left as it is.
 *
 * Every other use of the package's `contract` is a located error: a declaration kept in a variable and
 * implemented later, `contract` passed around or exported under another name, or read off the package's
 * namespace in any way but `namespace.contract`, would not be compiled, and would throw on first use.
 * The namespace itself is only ever the object of `namespace.name`, so no such read goes unseen, and
 * no module passes the package's names on with `export *`, which the parse refuses.
 */
import { type AnyNode, is, keyName, unwrap, walk } from "./ast.js";
import { SOLIDITY_SPECIFIER } from "./builtins.js";
import { fail, failOnExtraArgument } from "./errors.js";
import { refusedName } from "./names.js";
import { boundTo } from "./scopes.js";
import type { LensSite, SourceModule } from "./types.js";

/** True when a module's text can hold a lens. Checked before parsing, so other files cost nothing. */
export const mentionsPackage = (code: string): boolean => code.includes(SOLIDITY_SPECIFIER);

const NOT_A_LENS =
  "contract is written in one expression with its implementation, as contract(name, functions).implementation({ … }): used in any other way, a lens is not compiled";

export function find(module: SourceModule): LensSite[] {
  const sites: LensSite[] = [];
  const program = module.program as unknown as AnyNode;

  /** The export of the package a name is bound to where it is written: `"*"` for a namespace import. */
  const imported = (node: AnyNode): string | undefined => {
    const bound = is(node, "Identifier") || is(node, "JSXIdentifier") ? boundTo(module, node) : undefined;
    return bound?.kind === "module" && bound.binding?.kind === "import" && bound.binding.source === SOLIDITY_SPECIFIER
      ? bound.binding.imported
      : undefined;
  };
  /** The namespace import of the package that `node` reads `name` off, when it is written `namespace.name`. */
  const namespaceOf = (node: AnyNode, name?: string): AnyNode | undefined => {
    if (!is(node, "MemberExpression") || node.computed || (name !== undefined && node.property.name !== name))
      return undefined;
    const object = unwrap(node.object);
    return imported(object) === "*" ? object : undefined;
  };

  /** The uses of `contract` that are the start of a lens. */
  const used = new Set<AnyNode>();
  walk(program, (node) => {
    if (!is(node, "CallExpression")) return;
    const callee = unwrap(node.callee);
    if (!is(callee, "MemberExpression") || callee.computed || callee.property.name !== "implementation") return;
    const declaration = unwrap(callee.object);
    if (!is(declaration, "CallExpression")) return;
    const factory = unwrap(declaration.callee);
    if (imported(factory) !== "contract" && !namespaceOf(factory, "contract")) return;
    used.add(factory);

    const [nameNode, functionsNode, optionsNode] = declaration.arguments.map(unwrap);
    if (!is(nameNode, "Literal") || typeof nameNode.value !== "string")
      return fail(
        module,
        nameNode ?? declaration,
        "the first argument of contract(…) must be a string literal: the lens's name",
      );
    const name = nameNode.value;
    const refused = refusedName(name);
    if (refused) return fail(module, nameNode, refused);
    if (!is(functionsNode, "ObjectExpression"))
      return fail(
        module,
        functionsNode ?? declaration,
        "the second argument of contract(…) must be an object literal written in place: the entry points' signatures",
        name,
      );
    if (optionsNode !== undefined && !is(optionsNode, "ObjectExpression"))
      return fail(
        module,
        optionsNode,
        "the third argument of contract(…) must be an object literal written in place",
        name,
      );
    const implementation = node.arguments[0] && unwrap(node.arguments[0]);
    if (!is(implementation, "ObjectExpression"))
      return fail(module, implementation ?? node, ".implementation(…) takes an object literal written in place", name);
    failOnExtraArgument(module, declaration, 3, "contract(…)", name);
    failOnExtraArgument(module, node, 1, ".implementation(…)", name);

    sites.push({ node, declaration, name, functions: functionsNode, options: optionsNode, implementation });
  });

  /** The reads of the package's namespace that are written `namespace.name`. */
  const namespaceReads = new Set<AnyNode>();
  walk(program, (node) => {
    // `export { contract } from "…"` hands the package's contract to another module, where it is not found.
    if (is(node, "ExportNamedDeclaration") && node.source?.value === SOLIDITY_SPECIFIER && node.exportKind !== "type")
      for (const specifier of node.specifiers)
        if (specifier.exportKind !== "type" && keyName(specifier.local) === "contract")
          fail(module, specifier, NOT_A_LENS);
    const namespace = namespaceOf(node);
    if (namespace) {
      namespaceReads.add(namespace);
      if (namespaceOf(node, "contract") && !used.has(node)) fail(module, node, NOT_A_LENS);
    }
    const name = imported(node);
    if (name === "contract" && !used.has(node)) fail(module, node, NOT_A_LENS);
    if (name === "*" && !namespaceReads.has(node))
      fail(
        module,
        node,
        "a namespace import of the package is only read as namespace.name: read in any other way, a contract taken from it is not seen, and its lens is not compiled",
      );
  });
  return sites;
}
