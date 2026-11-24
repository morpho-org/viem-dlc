/**
 * Find: the lenses of one module.
 *
 * A lens is `contract(name, functions, options?).implementation({ … })` where `contract` is imported
 * from the package directly, at any nesting depth. Its parts are literals written
 * in place, and anything else is a located error.
 *
 * Detection is by import binding, not by scope analysis: a local that shadows the imported `contract`
 * at the call would be taken for it.
 */
import { type AnyNode, is, unwrap, walk } from "./ast.js";
import { SOLIDITY_SPECIFIER } from "./builtins.js";
import { fail } from "./errors.js";
import type { LensSite, SourceModule } from "./types.js";

/** True when a module's text can hold a lens. Checked before parsing, so other files cost nothing. */
export const mentionsPackage = (code: string): boolean => code.includes(SOLIDITY_SPECIFIER);

/** True when `callee` is the package's export `name`, imported by name or read off a namespace import. */
export const isPackageExport = (module: SourceModule, callee: AnyNode, name: string): boolean => {
  if (is(callee, "Identifier")) {
    const binding = module.bindings.get(callee.name);
    return binding?.kind === "import" && binding.source === SOLIDITY_SPECIFIER && binding.imported === name;
  }
  if (is(callee, "MemberExpression") && !callee.computed && callee.property.name === name) {
    const object = unwrap(callee.object);
    if (!is(object, "Identifier")) return false;
    const binding = module.bindings.get(object.name);
    return binding?.kind === "import" && binding.source === SOLIDITY_SPECIFIER && binding.imported === "*";
  }
  return false;
};

const IDENTIFIER = /^[A-Za-z_$][A-Za-z0-9_$]*$/;

export function find(module: SourceModule): LensSite[] {
  const sites: LensSite[] = [];
  walk(module.program as unknown as AnyNode, (node) => {
    if (!is(node, "CallExpression")) return;
    const callee = unwrap(node.callee);
    if (!is(callee, "MemberExpression") || callee.computed || callee.property.name !== "implementation") return;
    const declaration = unwrap(callee.object);
    if (!is(declaration, "CallExpression") || !isPackageExport(module, unwrap(declaration.callee), "contract")) return;

    const [nameNode, functionsNode, optionsNode] = declaration.arguments.map(unwrap);
    if (!is(nameNode, "Literal") || typeof nameNode.value !== "string")
      return fail(
        module,
        nameNode ?? declaration,
        "the first argument of contract(…) must be a string literal: the lens's name",
      );
    const name = nameNode.value;
    if (!IDENTIFIER.test(name)) return fail(module, nameNode, `"${name}" is not a Solidity identifier`);
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

    sites.push({ node, declaration, name, functions: functionsNode, options: optionsNode, implementation });
  });
  return sites;
}
