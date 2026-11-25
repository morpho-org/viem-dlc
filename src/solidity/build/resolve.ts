/**
 * Resolve: every name a lens uses, to a module-level declaration.
 *
 * The transform parses modules and never runs them, so everything a lens uses must be readable from
 * source. A name in a body, a signature or a constant resolves to a module-level binding: a package
 * export, a struct, an interface, a helper, an immutable, a constant, a library, or an import of one of
 * those, followed through re-exports, `export *` and namespace imports with the host's resolver.
 * Anything else stops the build with the file, line and column.
 *
 * What a name is bound to where it is written is read from the scope analysis of its module. A name
 * bound to a module-level binding the transform does not read is an error, and never a global.
 *
 * An `export *` is followed only where it is the one `export *` of its module that offers the name.
 * How a JavaScript engine links two that offer it is not worked out here: the import is refused.
 *
 * What a declaration is, is read from the syntax of its initializer and nothing else: no initializer is
 * evaluated, and no TypeScript type is read.
 */
import type { Span } from "oxc-parser";

import { type AnyNode, children, is, keyName, unwrap } from "./ast.js";
import {
  BODY_BUILTINS,
  DECLARATION_BUILTINS,
  GLOBAL_BUILTINS,
  isElementaryTypeName,
  SOLIDITY_SPECIFIER,
  YUL_SPECIFIER,
} from "./builtins.js";
import { fail, failOnExtraArgument } from "./errors.js";
import { refusedName } from "./names.js";
import { YUL_OPCODES } from "./print-yul.js";
import { boundTo } from "./scopes.js";
import type {
  ConstantDeclaration,
  FunctionBody,
  HelperDeclaration,
  ImmutableDeclaration,
  InterfaceDeclaration,
  LensSite,
  LibraryDeclaration,
  ModuleBinding,
  ModuleLoader,
  Reference,
  ResolvedDeclaration,
  ResolvedFunction,
  ResolvedLens,
  ResolvedSignature,
  SourceModule,
  StructDeclaration,
  TypeRef,
} from "./types.js";

type Mutable<T> = { -readonly [K in keyof T]: T[K] };

/** What a module-level name stands for. A namespace is only ever the object of `namespace.name`. */
type Value =
  | Reference
  | { readonly kind: "namespace"; readonly module: SourceModule }
  | { readonly kind: "package"; readonly module: "solidity" | "yul" };

/** Gathers the declarations one function, signature or declaration names directly. */
type Uses = ResolvedDeclaration[];

/** The operators a constant a lens reads may use: where solc's literal arithmetic and `BigInt` agree. */
const CONSTANT_BINARY = new Set(["+", "-", "*", "**", "%", "<<", ">>", "&", "|", "^"]);
const CONSTANT_UNARY = new Set(["-", "~"]);

const NAMES =
  "A lens can name its own parameters and locals, and module-level imports, constants, structs, interfaces, immutables, libraries and internal(…) helpers";

const unread = (name: string): string =>
  `"${name}" is a module-level binding a lens cannot read: it is declared by destructuring, by a var inside a block, more than once, or as an enum or a namespace. ${NAMES}, each declared alone by a statement of the module's own level.`;

export function resolve(site: LensSite, module: SourceModule, loader: ModuleLoader): ResolvedLens {
  const lens = site.name;
  const references = new Map<Span, Reference>();
  /** Keyed by the binding's own node, so a declaration reached twice is one declaration. */
  const declarations = new Map<Span, Mutable<ResolvedDeclaration>>();
  const busy = new Set<Span>();
  const moduleRank = new Map<SourceModule, number>();
  const files = new Set<string>([module.file]);

  const error = (m: SourceModule, node: Span, message: string): never => fail(m, node, message, lens);

  /** Stops the build when a name cannot be printed as a Solidity name. A field or an interface function is a `member`. */
  const named = (m: SourceModule, at: Span, name: string, member = false): void => {
    const refused = refusedName(name, member ? "member" : "scoped");
    if (refused) error(m, at, refused);
  };

  const takes = (m: SourceModule, call: { readonly arguments: readonly Span[] }, most: number, what: string): void =>
    failOnExtraArgument(m, call, most, what, lens);

  // ---- names ------------------------------------------------------------------------------------------
  /** The module `source` names when imported from `m`. `user` and `at` are where the name is used, for errors. */
  const target = (m: SourceModule, source: string, user: SourceModule, at: Span): SourceModule => {
    const file = loader.resolve(source, m.file);
    if (file === undefined) return error(user, at, `cannot resolve "${source}" from ${m.file}`);
    files.add(file);
    return loader.load(file);
  };

  const packageExport = (pkg: "solidity" | "yul", name: string, m: SourceModule, at: Span): Value => {
    if (name === "*") return { kind: "package", module: pkg };
    if (pkg === "yul") return { kind: "builtin", module: "yul", name };
    if (isElementaryTypeName(name) || DECLARATION_BUILTINS.has(name) || BODY_BUILTINS.has(name))
      return { kind: "builtin", module: "solidity", name };
    return error(m, at, `"${name}" is not something a lens can use from ${SOLIDITY_SPECIFIER}`);
  };

  /** What `m` gets by importing `imported` from `source`. `user` and `at` are where it is used, for errors. */
  const importedValue = (m: SourceModule, source: string, imported: string, user: SourceModule, at: Span): Value => {
    if (source === SOLIDITY_SPECIFIER) return packageExport("solidity", imported, user, at);
    if (source === YUL_SPECIFIER) return packageExport("yul", imported, user, at);
    const from = target(m, source, user, at);
    return imported === "*" ? { kind: "namespace", module: from } : exportedValue(from, imported, user, at);
  };

  /** The exports being followed, as `file\0name`: modules may export each other's names in a circle. */
  const exporting = new Set<string>();

  /**
   * True when `from` has an export named `name`: one it writes, or one an `export *` of it passes on.
   * Every export counts, whatever it exports, and none is followed to what it names. A module that is
   * already being asked for the name is where the search came from, and offers nothing more.
   */
  const offers = (from: SourceModule, name: string, user: SourceModule, at: Span, seen: Set<SourceModule>): boolean => {
    if (seen.has(from) || exporting.has(`${from.file}\0${name}`)) return false;
    seen.add(from);
    return from.exports.has(name) || from.starExports.some((source) => passesOn(from, source, name, user, at, seen));
  };

  /** True when the `export * from "source"` of `from` passes on a name `name`. */
  const passesOn = (
    from: SourceModule,
    source: string,
    name: string,
    user: SourceModule,
    at: Span,
    seen: Set<SourceModule>,
  ): boolean =>
    source === YUL_SPECIFIER ? YUL_OPCODES.has(name) : offers(target(from, source, user, at), name, user, at, seen);

  /**
   * What the `export *` declarations of `from` give as `name`: nothing for `default`, and otherwise what the
   * one declaration that offers the name gives. Two that offer it are refused, where they lead to one
   * binding too: which binding each leads to is not compared.
   */
  const starredValue = (from: SourceModule, name: string, user: SourceModule, at: Span): Value | undefined => {
    if (name === "default") return undefined;
    const [source, ...others] = from.starExports.filter((candidate) =>
      passesOn(from, candidate, name, user, at, new Set()),
    );
    if (source === undefined) return undefined;
    if (others.length)
      return error(
        user,
        at,
        `"${name}" is offered by more than one export * of ${from.file}: from "${[source, ...others].join('" and from "')}". The build does not work out whether they lead to one declaration. Export it by name there, as export { ${name} } from "…", or import it from the module that declares it`,
      );
    return importedValue(from, source, name, user, at);
  };

  /** The value `from` exports as `name`, or undefined when it exports no such name. */
  const exportedBy = (from: SourceModule, name: string, user: SourceModule, at: Span): Value | undefined => {
    const key = `${from.file}\0${name}`;
    if (exporting.has(key)) return error(user, at, `"${name}" is exported by ${from.file} in terms of itself`);
    exporting.add(key);
    try {
      const entry = from.exports.get(name);
      if (entry?.kind === "local") return valueOfName(from, entry.local, user, at);
      if (entry?.kind === "reexport") return importedValue(from, entry.source, entry.imported, user, at);
      if (entry?.kind === "default")
        return error(
          user,
          at,
          "a default export that is an expression cannot be used by a lens; export it under a name",
        );
      return starredValue(from, name, user, at);
    } finally {
      exporting.delete(key);
    }
  };

  /** The value `from` exports as `name`. `user` and `at` are where it is asked for, for errors. */
  const exportedValue = (from: SourceModule, name: string, user: SourceModule, at: Span): Value =>
    exportedBy(from, name, user, at) ??
    error(
      user,
      at,
      name === "default" && from.starExports.length > 0
        ? `"default" is not exported by ${from.file}: export * does not pass a default export on`
        : `"${name}" is not exported by ${from.file}`,
    );

  /**
   * The value of a name written in `m`, by what the scope analysis binds it to there. Undefined for a
   * parameter or a local declared inside `within`, and for an identifier that uses no value: a property
   * name, a key, or a name being declared.
   *
   * A binding of the code around a lens is a capture, which a lens compiled ahead of time cannot make. A
   * module-level binding the transform does not read is an error too, whatever it is named: only a name
   * that nothing in the module declares can be a global.
   */
  const valueOfIdentifier = (
    m: SourceModule,
    node: Span & { readonly name: string },
    within?: Span,
  ): Value | undefined => {
    const bound = boundTo(m, node);
    if (!bound) return undefined;
    const { name } = node;
    if (bound.kind === "global") {
      if (GLOBAL_BUILTINS.has(name)) return { kind: "builtin", module: "global", name };
      return error(m, node, `"${name}" is not a module-level name. ${NAMES}.`);
    }
    if (bound.kind === "local") {
      if (within && bound.scope.start >= within.start && bound.scope.end <= within.end) return undefined;
      return error(
        m,
        node,
        `"${name}" is a parameter or a local of the code around this lens, which a lens cannot capture. ${NAMES}.`,
      );
    }
    return bound.binding ? valueOfBinding(m, bound.binding) : error(m, node, unread(name));
  };

  /** The value of the module-level name `name` of `m`, which `m` exports. `user` and `at` are where it is used. */
  const valueOfName = (m: SourceModule, name: string, user: SourceModule, at: Span): Value => {
    const binding = m.bindings.get(name);
    return binding ? valueOfBinding(m, binding) : error(user, at, unread(name));
  };

  const valueOfBinding = (m: SourceModule, binding: ModuleBinding): Value =>
    binding.kind === "import"
      ? importedValue(m, binding.source, binding.imported, m, binding.node)
      : { kind: "declaration", declaration: declarationOf(m, binding) };

  /**
   * The value an expression names without being evaluated: a name, or `namespace.name`. Undefined for any
   * other expression.
   */
  const staticValue = (m: SourceModule, node: AnyNode): Value | undefined => {
    if (is(node, "Identifier")) return valueOfIdentifier(m, node);
    if (is(node, "MemberExpression") && !node.computed) {
      const object = unwrap(node.object);
      if (!is(object, "Identifier") && !is(object, "MemberExpression")) return undefined;
      const base = staticValue(m, object);
      if (base?.kind === "namespace") return exportedValue(base.module, node.property.name, m, node);
      if (base?.kind === "package") return packageExport(base.module, node.property.name, m, node);
    }
    return undefined;
  };

  const asReference = (value: Value, m: SourceModule, node: Span): Reference =>
    value.kind === "namespace" || value.kind === "package"
      ? error(m, node, "a namespace import can only be used as namespace.name")
      : value;

  const use = (uses: Uses, reference: Reference, node: Span): Reference => {
    references.set(node, reference);
    if (reference.kind === "declaration" && !uses.includes(reference.declaration)) uses.push(reference.declaration);
    return reference;
  };

  const isBuiltin = (value: Value | undefined, name: string): boolean =>
    value?.kind === "builtin" && value.module === "solidity" && value.name === name;

  // ---- types and signatures ----------------------------------------------------------------------------
  const typeRef = (m: SourceModule, raw: Span, uses: Uses): TypeRef => {
    const node = unwrap(raw);
    if (is(node, "ArrayExpression")) {
      const [element] = node.elements;
      if (node.elements.length !== 1 || !element || is(element, "SpreadElement"))
        return error(m, node, "an array type is written [T], with one element type");
      return { kind: "array", element: typeRef(m, element, uses) };
    }
    const value = staticValue(m, node);
    if (value) {
      const reference = asReference(value, m, node);
      if (reference.kind === "builtin" && reference.module === "solidity" && isElementaryTypeName(reference.name)) {
        use(uses, reference, node);
        return { kind: "elementary", name: reference.name };
      }
      if (reference.kind === "declaration") {
        const { declaration } = reference;
        if (declaration.kind === "struct" || declaration.kind === "interface") {
          use(uses, reference, node);
          return declaration.kind === "struct" ? { kind: "struct", declaration } : { kind: "interface", declaration };
        }
      }
    }
    return error(m, node, "this is not a type. A type is an elementary type, a struct class, an interface, or [T]");
  };

  const signature = (m: SourceModule, raw: Span, uses: Uses): ResolvedSignature => {
    const node = unwrap(raw);
    const malformed = (): never =>
      error(m, node, "a signature is written view(…) or pure(…), optionally followed by .returns(…)");
    if (!is(node, "CallExpression")) return malformed();
    const callee = unwrap(node.callee);
    let base = node;
    let outputNodes: readonly Span[] = [];
    if (is(callee, "MemberExpression") && !callee.computed && callee.property.name === "returns") {
      const inner = unwrap(callee.object);
      if (!is(inner, "CallExpression")) return malformed();
      base = inner;
      outputNodes = node.arguments;
    }
    const builder = staticValue(m, unwrap(base.callee));
    const mutability = isBuiltin(builder, "view") ? "view" : isBuiltin(builder, "pure") ? "pure" : malformed();
    return {
      mutability,
      inputs: base.arguments.map((argument) => typeRef(m, argument, uses)),
      outputs: outputNodes.map((argument) => typeRef(m, argument, uses)),
      origin: { module: m, node },
    };
  };

  // ---- bodies ----------------------------------------------------------------------------------------------
  const functionLiteral = (m: SourceModule, raw: Span | undefined, at: Span, what: string): FunctionBody => {
    const node = raw && unwrap(raw);
    if (!is(node, "ArrowFunctionExpression") && !is(node, "FunctionExpression"))
      return error(m, node ?? at, `${what} must be a function written in place`);
    if (node.async || node.generator) return error(m, node, "a lens function cannot be async or a generator");
    return node;
  };

  /**
   * Records what every name under `node` resolves to, unless the function `fn` that holds it declares the
   * name itself. Which is which is read from the scope analysis; a property name and a key use no name.
   */
  const scan = (m: SourceModule, node: AnyNode, fn: Span, uses: Uses): void => {
    if (is(node, "Identifier")) {
      const value = valueOfIdentifier(m, node, fn);
      if (value) use(uses, asReference(value, m, node), node);
      return;
    }
    if (is(node, "MemberExpression") && !node.computed) {
      // A path through namespaces, such as `math.mulDivDown` or `lib.math.mulDivDown`, is one name.
      let root = unwrap(node.object);
      while (is(root, "MemberExpression") && !root.computed) root = unwrap(root.object);
      const base = is(root, "Identifier") ? valueOfIdentifier(m, root, fn) : undefined;
      const value = base?.kind === "namespace" || base?.kind === "package" ? staticValue(m, node) : undefined;
      if (value) {
        use(uses, asReference(value, m, node), node);
        return;
      }
    }
    for (const child of children(node)) scan(m, child, fn, uses);
  };

  const scanFunction = (m: SourceModule, fn: FunctionBody, uses: Uses): void =>
    scan(m, fn as unknown as AnyNode, fn, uses);

  /** The functions of a contract or a library: each declared signature with its implementation. */
  const functionsOf = (
    m: SourceModule,
    signatures: AnyNode,
    implementation: AnyNode,
    owner: string,
  ): ResolvedFunction[] => {
    const bodies = new Map<string, AnyNode>();
    for (const property of implementation.properties as AnyNode[]) {
      const name = is(property, "Property") && !property.computed ? keyName(property.key) : undefined;
      if (name === undefined || !is(property, "Property") || property.kind !== "init")
        return error(m, property, `the implementation of ${owner} can only have plain functions under plain keys`);
      if (bodies.has(name))
        return error(m, property.key, `"${name}" is written twice in the implementation of ${owner}`);
      bodies.set(name, property);
    }
    const functions: ResolvedFunction[] = [];
    const declared = new Set<string>();
    for (const property of signatures.properties as AnyNode[]) {
      const name = is(property, "Property") && !property.computed ? keyName(property.key) : undefined;
      if (name === undefined || !is(property, "Property") || property.kind !== "init" || property.method)
        return error(m, property, `the declaration of ${owner} can only have signatures under plain keys`);
      if (declared.has(name))
        return error(m, property.key, `"${name}" is written twice in the declaration of ${owner}`);
      declared.add(name);
      named(m, property.key, name);
      const implemented = bodies.get(name);
      if (!implemented) return error(m, property, `${owner} declares "${name}" and does not implement it`);
      bodies.delete(name);
      const dependencies: Uses = [];
      const resolved = signature(m, property.value, dependencies);
      const body = functionLiteral(m, implemented.value as Span, implemented, `the implementation of "${name}"`);
      scanFunction(m, body, dependencies);
      functions.push({ name, signature: resolved, body, module: m, dependencies });
    }
    for (const [name, property] of bodies)
      return error(m, property, `${owner} implements "${name}" and does not declare its signature`);
    return functions;
  };

  // ---- declarations --------------------------------------------------------------------------------------------
  /**
   * The entries of an object literal. With `members`, each key is printed as a Solidity name, and must be one.
   * A key is written once: JavaScript would keep the last of two, without a word.
   */
  const plainEntries = (
    m: SourceModule,
    raw: Span | undefined,
    at: Span,
    what: string,
    members = false,
  ): [string, AnyNode][] => {
    const node = raw && unwrap(raw);
    if (!is(node, "ObjectExpression"))
      return error(m, node ?? at, `${what} must be an object literal written in place`);
    const written = new Set<string>();
    return node.properties.map((property): [string, AnyNode] => {
      const name = is(property, "Property") && !property.computed ? keyName(property.key) : undefined;
      if (name === undefined || !is(property, "Property") || property.kind !== "init" || property.method)
        return error(m, property, `${what} can only have plain keys`);
      if (written.has(name)) return error(m, property.key, `"${name}" is written twice in ${what}`);
      written.add(name);
      if (members) named(m, property.key, name, true);
      return [name, property.value as unknown as AnyNode];
    });
  };

  const register = <D extends Mutable<ResolvedDeclaration>>(m: SourceModule, key: Span, declaration: D): D => {
    if (!moduleRank.has(m)) moduleRank.set(m, moduleRank.size);
    declarations.set(key, declaration);
    return declaration;
  };

  const constantExpression = (m: SourceModule, raw: Span, uses: Uses): void => {
    const node = unwrap(raw);
    function refused(what: string): never {
      return error(
        m,
        node,
        `${what} cannot be part of a constant a lens reads. Such a constant is built only from bigint literals, booleans, strings, other such constants, and the operators + - * ** % << >> & | ^ ~`,
      );
    }
    if (is(node, "Literal")) {
      // JavaScript and a lens both read the constant, so it holds only what means the same to both.
      if (typeof node.value === "number")
        error(
          m,
          node,
          "a Number literal cannot be part of a constant a lens reads: JavaScript computes with Numbers as floats, and in 32 bits where it shifts, and solc computes exactly, so the constant would mean two different values. Write it as a bigint, with the n suffix",
        );
      if (typeof node.value === "string" && node.value.startsWith("0x"))
        error(
          m,
          node,
          "a string that starts with 0x cannot be a constant a lens reads: a body can read it as a hex literal or an address, and JavaScript reads it as text, so the constant would mean two different values. Write the literal in the body",
        );
      if (!["bigint", "boolean", "string"].includes(typeof node.value)) refused("this literal");
      return;
    }
    if (is(node, "Identifier") || is(node, "MemberExpression")) {
      const value = staticValue(m, node);
      if (value?.kind !== "declaration" || value.declaration.kind !== "constant") refused("this name");
      use(uses, value, node);
      return;
    }
    if (is(node, "UnaryExpression")) {
      if (!CONSTANT_UNARY.has(node.operator)) refused(`the operator ${node.operator}`);
      constantExpression(m, node.argument, uses);
      return;
    }
    if (is(node, "BinaryExpression")) {
      if (node.operator === "/") {
        // The operator is the one slash between the operands that is not in a comment.
        const between = m.code.slice(node.left.end, node.right.start);
        const comments = /\/\*[\s\S]*?\*\/|\/\/.*/g;
        const at = node.left.end + between.replace(comments, (comment) => " ".repeat(comment.length)).indexOf("/");
        error(
          m,
          { start: at, end: at + 1 },
          "a constant a lens reads cannot use /: solc divides literals exactly and JavaScript's BigInt truncates, so the constant would mean two different values. Divide inside the body, where the division is Solidity's",
        );
      }
      if (!CONSTANT_BINARY.has(node.operator)) refused(`the operator ${node.operator}`);
      constantExpression(m, node.left, uses);
      constantExpression(m, node.right, uses);
      return;
    }
    if (is(node, "CallExpression"))
      refused("a call (a hash or any other computed value is written inside a body, where solc computes it)");
    refused("this expression");
  };

  const declarationOf = (m: SourceModule, binding: Exclude<ModuleBinding, { kind: "import" }>): ResolvedDeclaration => {
    const key = binding.node;
    const known = declarations.get(key);
    if (known) return known;
    if (busy.has(key)) return error(m, key, `"${binding.name}" is defined in terms of itself`);
    const base = { name: binding.name, module: m, dependencies: [] as Uses };

    if (binding.kind === "function")
      return error(m, key, `"${binding.name}" is a plain function; wrap it in internal(…) to call it from a lens`);

    if (binding.kind === "class") {
      const superClass = binding.node.superClass && unwrap(binding.node.superClass);
      if (!is(superClass, "CallExpression") || !isBuiltin(staticValue(m, unwrap(superClass.callee)), "Struct"))
        return error(m, key, `class "${binding.name}" does not extend Struct({ … })`);
      takes(m, superClass, 1, "Struct(…)");
      // A member that is only a type is erased by a compiler, and is no member.
      const members = binding.node.body.body.filter(
        (member) => !is(member, "TSIndexSignature") && !(is(member, "PropertyDefinition") && member.declare),
      );
      if (members.length) return error(m, binding.node.body, "a struct class has no members");
      named(m, binding.node.id ?? key, binding.name);
      const struct = register<Mutable<StructDeclaration>>(m, key, { ...base, kind: "struct", node: key, fields: [] });
      struct.fields = plainEntries(m, superClass.arguments[0], superClass, "the fields of a struct", true).map(
        ([name, node]) => ({ name, type: typeRef(m, node, struct.dependencies as Uses), origin: { module: m, node } }),
      );
      return struct;
    }

    if (binding.declarationKind !== "const")
      return error(
        m,
        key,
        `"${binding.name}" is declared with ${binding.declarationKind}; a lens can only read a module-level const`,
      );
    if (!binding.init) return error(m, key, `"${binding.name}" has no initializer`);
    const init = unwrap(binding.init);

    if (is(init, "CallExpression")) {
      const callee = unwrap(init.callee);
      const called = staticValue(m, callee);
      if (isBuiltin(called, "internal") || isBuiltin(called, "iface") || isBuiltin(called, "immutable"))
        named(m, binding.node.id, binding.name);
      if (isBuiltin(called, "internal")) {
        takes(m, init, 2, "internal(…)");
        const helper = register<Mutable<HelperDeclaration>>(m, key, {
          ...base,
          kind: "helper",
          node: init,
          signature: undefined as never,
          body: undefined as never,
        });
        helper.signature = signature(m, init.arguments[0] ?? init, helper.dependencies as Uses);
        helper.body = functionLiteral(m, init.arguments[1], init, "the second argument of internal(…)");
        scanFunction(m, helper.body, helper.dependencies as Uses);
        return helper;
      }
      if (isBuiltin(called, "iface")) {
        takes(m, init, 1, "iface(…)");
        const declared = register<Mutable<InterfaceDeclaration>>(m, key, {
          ...base,
          kind: "interface",
          node: init,
          functions: [],
        });
        declared.functions = plainEntries(m, init.arguments[0], init, "the functions of an interface", true).map(
          ([name, node]) => ({ name, signature: signature(m, node, declared.dependencies as Uses) }),
        );
        return declared;
      }
      if (isBuiltin(called, "immutable")) {
        takes(m, init, 1, "immutable(…)");
        const declared = register<Mutable<ImmutableDeclaration>>(m, key, {
          ...base,
          kind: "immutable",
          node: init,
          type: undefined as never,
        });
        declared.type = typeRef(m, init.arguments[0] ?? init, declared.dependencies as Uses);
        return declared;
      }
      if (is(callee, "MemberExpression") && !callee.computed && callee.property.name === "implementation") {
        const inner = unwrap(callee.object);
        const made = is(inner, "CallExpression") ? staticValue(m, unwrap(inner.callee)) : undefined;
        if (isBuiltin(made, "contract")) return error(m, key, "a lens cannot be used inside another lens");
        if (is(inner, "CallExpression") && isBuiltin(made, "library")) {
          const [nameNode, signatures] = inner.arguments.map(unwrap);
          const implementation = init.arguments[0] && unwrap(init.arguments[0]);
          if (!is(nameNode, "Literal") || typeof nameNode.value !== "string")
            return error(m, nameNode ?? inner, "the first argument of library(…) must be a string literal: its name");
          if (!is(signatures, "ObjectExpression") || !is(implementation, "ObjectExpression"))
            return error(m, init, "library(name, { … }).implementation({ … }) takes object literals written in place");
          takes(m, inner, 2, "library(…)");
          takes(m, init, 1, ".implementation(…)");
          named(m, nameNode, nameNode.value);
          const declared = register<Mutable<LibraryDeclaration>>(m, key, {
            ...base,
            name: nameNode.value,
            kind: "library",
            node: init,
            functions: [],
          });
          declared.functions = functionsOf(m, signatures, implementation, `library ${nameNode.value}`);
          for (const fn of declared.functions)
            for (const used of fn.dependencies)
              if (!declared.dependencies.includes(used)) (declared.dependencies as Uses).push(used);
          return declared;
        }
      }
    }

    busy.add(key);
    try {
      const dependencies: Uses = [];
      constantExpression(m, binding.init, dependencies);
      return register<Mutable<ConstantDeclaration>>(m, key, {
        ...base,
        kind: "constant",
        node: binding.init,
        dependencies,
        expression: binding.init,
      });
    } finally {
      busy.delete(key);
    }
  };

  // ---- the lens ------------------------------------------------------------------------------------------------------
  const entries = functionsOf(
    module,
    site.functions as unknown as AnyNode,
    site.implementation as unknown as AnyNode,
    `contract ${lens}`,
  );

  const immutables: { key: string; declaration: ImmutableDeclaration }[] = [];
  if (site.options) {
    for (const [option, value] of plainEntries(module, site.options, site.options, "the options of contract(…)")) {
      if (option !== "immutables") return error(module, value, `contract(…) has no option "${option}"`);
      for (const [key, node] of plainEntries(module, value, value, "the immutables of a contract")) {
        const named = staticValue(module, unwrap(node));
        if (named?.kind !== "declaration" || named.declaration.kind !== "immutable")
          return error(module, node, `"${key}" must name a module-level immutable(…)`);
        if (immutables.some((listed) => listed.declaration === named.declaration))
          return error(module, node, `"${key}" lists an immutable the contract already lists`);
        references.set(unwrap(node), named);
        immutables.push({ key, declaration: named.declaration });
      }
    }
  }

  // Other modules first, in the order they were first reached; then the lens's own. Source order within each.
  /* v8 ignore next -- @preserve: a module was ranked when its first declaration was reached, and the fallback after ?? is there for the type checker */
  const rank = (m: SourceModule) => (m === module ? Number.POSITIVE_INFINITY : (moduleRank.get(m) ?? 0));
  const ordered = [...declarations.values()].sort(
    (a, b) =>
      (rank(a.module) === rank(b.module) ? 0 : rank(a.module) < rank(b.module) ? -1 : 1) || a.node.start - b.node.start,
  );
  // A name is printed as it is written and is never changed, so no two declarations share one, and none
  // shares the contract's or an entry point's. A library's functions are in scope of everything the
  // library's bodies name, so a declaration shares no name with one of them either. The second is refused.
  const taken = new Map([
    [lens, `contract ${lens}`],
    ...entries.map((entry) => [entry.name, "an entry point"] as const),
  ]);
  const declared = new Map<string, string>();
  const inLibraries = new Map<string, string>();
  const shared = (m: SourceModule, at: Span, name: string, other: string | undefined): void => {
    if (other !== undefined)
      error(
        m,
        at,
        `"${name}" is also the name of ${other}. A name is printed in Solidity as it is written and is never changed, so two declarations of a lens cannot share one: rename one of them`,
      );
  };
  for (const declaration of ordered) {
    if (declaration.kind === "constant") continue;
    const { name, kind } = declaration;
    shared(declaration.module, declaration.node, name, taken.get(name) ?? declared.get(name) ?? inLibraries.get(name));
    declared.set(name, kind === "interface" || kind === "immutable" ? `an ${kind}` : `a ${kind}`);
    if (kind !== "library") continue;
    for (const fn of declaration.functions) {
      // A function of a library is reached as Library.f, so it shares a name with an entry point freely.
      shared(fn.module, fn.body, fn.name, declared.get(fn.name));
      inLibraries.set(fn.name, `a function of library ${name}`);
    }
  }

  return { site, module, name: lens, entries, immutables, declarations: ordered, references, files: [...files] };
}
