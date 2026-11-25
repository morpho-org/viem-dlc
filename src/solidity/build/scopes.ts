/**
 * Scopes: what a name written in a module is bound to.
 *
 * The lexical scopes are not worked out here. A standard scope analyzer reads a module's tree once and
 * says, for every identifier that uses a name, which declaration the language binds it to. A default
 * parameter beside a `var` of its function's body, a `var` hoisted out of a block, the static block of a
 * class and the cases of a `switch` are the analyzer's business, and every stage asks it instead of
 * comparing spellings.
 *
 * What the transform needs to know of a use is one of three things: it is bound to a binding of the
 * module's own scope, to a parameter or a local of some function or block inside the module, or to
 * nothing the module declares.
 */
import { analyze, type Variable } from "@typescript-eslint/scope-manager";
import type { Span } from "oxc-parser";

import { isErased } from "./ast.js";
import { LensError } from "./errors.js";
import type { ModuleBinding, SourceModule } from "./types.js";

/** What a use of a name is bound to. */
export type Bound =
  /**
   * A binding of the module's own scope. `binding` is undefined for one the transform does not read: a
   * name declared by destructuring, a `var` in a block, an enum, a namespace, a name declared twice. A
   * type alias, an interface or a `declare` of the same name is no second declaration: a compiler erases it.
   */
  | { readonly kind: "module"; readonly binding: ModuleBinding | undefined }
  /** A parameter or a local. `scope` is the function, block or class that declares it. */
  | { readonly kind: "local"; readonly scope: Span }
  /** Nothing the module declares: a global, or no binding at all. */
  | { readonly kind: "global" };

interface Use {
  readonly identifier: Span;
  readonly variable: Variable | null;
  /** True for a use that reads or writes a value, which is what is left of it in JavaScript. */
  readonly value: boolean;
  /** True for the write a declaration makes to the name it declares: `const x = 1`. */
  readonly declares: boolean;
}

interface Analysis {
  /** Every use of a name, by its identifier node. */
  readonly uses: ReadonlyMap<Span, Use>;
  /** The variables of the module's own scope, by name. */
  readonly moduleScope: ReadonlyMap<string, Variable>;
}

const analyses = new WeakMap<SourceModule, Analysis>();

const analysisOf = (module: SourceModule): Analysis => {
  const known = analyses.get(module);
  if (known) return known;
  let manager: ReturnType<typeof analyze>;
  try {
    // No library globals: a name the module does not declare stays unbound, and is never taken for one it does.
    manager = analyze(module.program as never, { sourceType: "module", lib: [] });
    /* v8 ignore start -- @preserve: the analyzer reads every tree the parser accepts, so this guards a tree it would not */
  } catch (cause) {
    throw new LensError([
      {
        file: module.file,
        line: 1,
        column: 1,
        severity: "error",
        source: "transform",
        message: `cannot work out what the names of this module are bound to: ${cause instanceof Error ? cause.message : String(cause)}`,
      },
    ]);
  }
  /* v8 ignore stop */
  const uses = new Map<Span, Use>();
  let moduleScope: ReadonlyMap<string, Variable> = new Map();
  for (const scope of manager.scopes) {
    if (scope.type === "module") moduleScope = scope.set;
    for (const reference of scope.references)
      uses.set(reference.identifier as unknown as Span, {
        identifier: reference.identifier as unknown as Span,
        variable: reference.resolved,
        value: reference.isValueReference,
        declares: reference.init === true,
      });
  }
  const analysis: Analysis = { uses, moduleScope };
  analyses.set(module, analysis);
  return analysis;
};

/**
 * What the name `identifier` writes is bound to where it is written, in JavaScript. Undefined when the
 * identifier uses no value: it is a property name, a key, a label, a name being declared, or a use
 * that is only a type.
 */
export const boundTo = (module: SourceModule, identifier: Span): Bound | undefined => {
  const analysis = analysisOf(module);
  const use = analysis.uses.get(identifier);
  if (!use?.value) return undefined;
  const { variable } = use;
  if (!variable) return { kind: "global" };
  if (analysis.moduleScope.get(variable.name) !== variable)
    return { kind: "local", scope: variable.scope.block as unknown as Span };
  const binding = module.bindings.get(variable.name);
  const [declared, ...redeclared] = variable.defs.filter((def) => !isErased(def.node as unknown as Span));
  const read = binding && declared?.node === (binding.node as unknown) && !redeclared.length;
  return { kind: "module", binding: read ? binding : undefined };
};

/**
 * Every identifier that uses the module-level binding `binding`, as a value or as a type, other than
 * the declaration's own write to the name it declares.
 */
export const usesOf = (module: SourceModule, binding: ModuleBinding): Span[] => {
  const analysis = analysisOf(module);
  const variable = analysis.moduleScope.get(binding.name);
  /* v8 ignore next -- @preserve: the parser and the analyzer read one tree, so a binding of one is a variable of the other */
  if (!variable) throw new Error(`scopes: "${binding.name}" is not a variable of the module's scope`);
  const uses: Span[] = [];
  for (const use of analysis.uses.values()) if (use.variable === variable && !use.declares) uses.push(use.identifier);
  return uses;
};
