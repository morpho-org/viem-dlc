/**
 * Find: the lenses of one module.
 *
 * A lens is `contract(name, functions, options?).implementation({ … })` where `contract` is imported
 * from the package directly, at any nesting depth. Its parts are literals written
 * in place, and anything else is a located error.
 *
 * A name means its nearest binding: where a parameter or a local is named like the imported `contract`,
 * or like the namespace it is read off, the call is ordinary code and is left as it is.
 *
 * Every other use of the package's `contract` is a located error: a declaration kept in a variable and
 * implemented later, or `contract` passed around under another name, would not be compiled, and would
 * throw on first use.
 */
import { type AnyNode, is, SKIPPED_KEYS, unwrap, walk } from "./ast.js";
import { SOLIDITY_SPECIFIER } from "./builtins.js";
import { fail, failOnExtraArgument } from "./errors.js";
import { refusedName } from "./names.js";
import { isHidden } from "./scopes.js";
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
]);

export function find(module: SourceModule): LensSite[] {
  const sites: LensSite[] = [];
  const program = module.program as unknown as AnyNode;
  /** The uses of `contract` that are the start of a lens. */
  const used = new Set<AnyNode>();
  walk(program, (node) => {
    if (!is(node, "CallExpression")) return;
    const callee = unwrap(node.callee);
    if (!is(callee, "MemberExpression") || callee.computed || callee.property.name !== "implementation") return;
    const declaration = unwrap(callee.object);
    if (!is(declaration, "CallExpression") || !isPackageExport(module, unwrap(declaration.callee), "contract")) return;
    const factory = unwrap(declaration.callee);
    const imported = is(factory, "MemberExpression") ? unwrap(factory.object) : factory;
    if (isHidden(program, imported, imported.name as string)) return;
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

  const others = (node: AnyNode): void => {
    for (const key in node) {
      if (SKIPPED_KEYS.has(key)) continue;
      const value = node[key];
      for (const child of (Array.isArray(value) ? value : [value]) as (AnyNode | null)[]) {
        if (typeof child?.type !== "string" || (child.type.startsWith("TS") && !VALUE_NODES.has(child.type))) continue;
        // A name that is declared, a property, a key, or a specifier of an import or an export is not a use.
        const names = key === "id" || ((key === "property" || key === "key") && !node.computed);
        if (
          !names &&
          !node.type.endsWith("Specifier") &&
          !used.has(child) &&
          isPackageExport(module, child, "contract")
        ) {
          const imported = is(child, "MemberExpression") ? unwrap(child.object) : child;
          if (!isHidden(program, imported, imported.name as string))
            fail(
              module,
              child,
              "contract is written in one expression with its implementation, as contract(name, functions).implementation({ … }): used in any other way, a lens is not compiled",
            );
        }
        others(child);
      }
    }
  };
  others(program);
  return sites;
}
