/**
 * Resolve: every name a lens uses, to a module-level declaration.
 *
 * The transform parses modules and never runs them, so everything a lens uses must be readable from
 * source. A name in a body, a signature or a constant resolves to a module-level binding: a package
 * export, a struct, an interface, a helper, an immutable, a constant, a library, or an import of one of
 * those, followed through re-exports, `export *` and namespace imports with the host's resolver.
 * Anything else stops the build with the file, line and column.
 *
 * What a declaration is, is read from the syntax of its initializer and nothing else: no initializer is
 * evaluated, and no TypeScript type is read.
 */
import type { Span } from "oxc-parser";

import { type AnyNode, children, is, keyName, patternNames, unwrap } from "./ast.js";
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
import { enclosingScopes } from "./scopes.js";
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

  /** The exports being looked up, as `file\0name`: modules may export each other's names in a circle. */
  const exporting = new Set<string>();

  /** True when two values are one binding, however each was reached. */
  const same = (a: Value, b: Value): boolean => {
    if (a.kind === "declaration") return b.kind === "declaration" && a.declaration === b.declaration;
    if (a.kind === "builtin") return b.kind === "builtin" && a.module === b.module && a.name === b.name;
    return a.kind === b.kind && a.module === b.module;
  };

  /**
   * What the `export *` declarations of `from` give as `name`, as a JavaScript engine links them: nothing
   * for `default`, one binding however many declarations lead to it, and an error for two bindings.
   */
  const starredValue = (from: SourceModule, name: string, user: SourceModule, at: Span): Value | undefined => {
    if (name === "default") return undefined;
    let found: Value | undefined;
    for (const source of from.starExports) {
      let value: Value | undefined;
      if (source === SOLIDITY_SPECIFIER) {
        if (isElementaryTypeName(name) || DECLARATION_BUILTINS.has(name) || BODY_BUILTINS.has(name))
          value = packageExport("solidity", name, user, at);
      } else if (source === YUL_SPECIFIER) {
        if (YUL_OPCODES.has(name)) value = packageExport("yul", name, user, at);
      } else {
        const next = target(from, source, user, at);
        // A module already being asked for this name is where the search came from.
        if (!exporting.has(`${next.file}\0${name}`)) value = exportedBy(next, name, user, at);
      }
      if (!value) continue;
      if (found && !same(found, value))
        return error(
          user,
          at,
          `"${name}" is exported by more than one export * of ${from.file}, from different declarations: export it by name`,
        );
      found = value;
    }
    return found;
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

  /** The names bound between the module's scope and the lens: the parameters and locals of the code around it. */
  const around = new Set(
    enclosingScopes(module.program as unknown as AnyNode, site.node).flatMap((scope) => [...scope]),
  );

  /**
   * The value of a name written in `m`. A name means its nearest binding, and inside the lens a binding of the
   * code around it is nearer than the module's: that is a capture, which a lens compiled ahead of time cannot make.
   */
  const valueOfIdentifier = (m: SourceModule, node: Span, name: string): Value => {
    if (m === module && around.has(name) && node.start >= site.node.start && node.end <= site.node.end)
      return error(
        m,
        node,
        `"${name}" is a parameter or a local of the code around this lens, which a lens cannot capture. ${NAMES}.`,
      );
    return valueOfName(m, name, m, node);
  };

  /** The value of the module-level name `name` of `m`. `user` and `at` are where it is used. */
  const valueOfName = (m: SourceModule, name: string, user: SourceModule, at: Span): Value => {
    const binding = m.bindings.get(name);
    if (!binding) {
      if (GLOBAL_BUILTINS.has(name)) return { kind: "builtin", module: "global", name };
      return error(user, at, `"${name}" is not a module-level name. ${NAMES}.`);
    }
    if (binding.kind === "import") return importedValue(m, binding.source, binding.imported, m, binding.node);
    return { kind: "declaration", declaration: declarationOf(m, binding) };
  };

  /**
   * The value an expression names without being evaluated: a name, or `namespace.name`. Undefined for any
   * other expression.
   */
  const staticValue = (m: SourceModule, node: AnyNode): Value | undefined => {
    if (is(node, "Identifier")) return valueOfIdentifier(m, node, node.name);
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

  type Scopes = readonly ReadonlySet<string>[];
  const hoisted = (statements: readonly AnyNode[]): Set<string> => {
    const scope = new Set<string>();
    for (const statement of statements) {
      if (is(statement, "VariableDeclaration"))
        for (const declarator of statement.declarations)
          for (const name of patternNames(declarator.id)) scope.add(name);
      else if ((is(statement, "FunctionDeclaration") || is(statement, "ClassDeclaration")) && statement.id)
        scope.add(statement.id.name);
    }
    return scope;
  };

  const scanFunction = (m: SourceModule, fn: AnyNode, scopes: Scopes, uses: Uses): void => {
    const scope = new Set<string>();
    for (const parameter of fn.params as Span[]) for (const name of patternNames(parameter)) scope.add(name);
    const id = fn.id as { name: string } | null | undefined;
    if (id) scope.add(id.name);
    const body = fn.body as AnyNode | null;
    /* v8 ignore next -- @preserve: a function written as an expression always has a body, and the check is there for the type checker */
    if (!body) return;
    if (is(body, "BlockStatement")) scanBlock(m, body.body as unknown as AnyNode[], [...scopes, scope], uses);
    else scan(m, body, [...scopes, scope], uses);
  };

  const scanBlock = (m: SourceModule, statements: readonly AnyNode[], scopes: Scopes, uses: Uses): void => {
    const inner = [...scopes, hoisted(statements)];
    for (const statement of statements) scan(m, statement, inner, uses);
  };

  /** Records what every free name under `node` resolves to. Locals, parameters and property names are skipped. */
  const scan = (m: SourceModule, node: AnyNode, scopes: Scopes, uses: Uses): void => {
    const isLocal = (name: string) => scopes.some((scope) => scope.has(name));
    switch (node.type) {
      case "Identifier": {
        const name = node.name as string;
        if (!isLocal(name)) use(uses, asReference(valueOfIdentifier(m, node, name), m, node), node);
        return;
      }
      case "MemberExpression": {
        const object = unwrap(node.object as Span);
        // A path through namespaces, such as `math.mulDivDown` or `lib.math.mulDivDown`, is one name.
        let root = object;
        while (is(root, "MemberExpression") && !root.computed) root = unwrap(root.object);
        if (!node.computed && is(root, "Identifier") && !isLocal(root.name)) {
          const base = valueOfIdentifier(m, root, root.name);
          const value = base.kind === "namespace" || base.kind === "package" ? staticValue(m, node) : undefined;
          if (value) {
            use(uses, asReference(value, m, node), node);
            return;
          }
        }
        scan(m, object, scopes, uses);
        if (node.computed) scan(m, node.property as AnyNode, scopes, uses);
        return;
      }
      case "Property":
        if (node.computed) scan(m, node.key as AnyNode, scopes, uses);
        scan(m, node.value as AnyNode, scopes, uses);
        return;
      case "BlockStatement":
        scanBlock(m, node.body as AnyNode[], scopes, uses);
        return;
      case "ForStatement":
      case "ForInStatement":
      case "ForOfStatement": {
        const head = (node.init ?? node.left) as AnyNode | null | undefined;
        const inner = [...scopes, hoisted(head ? [head] : [])];
        for (const child of children(node)) scan(m, child, inner, uses);
        return;
      }
      case "CatchClause":
        scanBlock(
          m,
          (node.body as AnyNode).body as AnyNode[],
          [...scopes, new Set(patternNames(node.param as Span | null))],
          uses,
        );
        return;
      case "ArrowFunctionExpression":
      case "FunctionExpression":
      case "FunctionDeclaration":
        scanFunction(m, node, scopes, uses);
        return;
      case "VariableDeclaration":
        for (const declarator of node.declarations as AnyNode[])
          if (declarator.init) scan(m, declarator.init as AnyNode, scopes, uses);
        return;
      // A type declared inside a body is only TypeScript, and names nothing a lens uses.
      case "TSTypeAliasDeclaration":
      case "TSInterfaceDeclaration":
        return;
      default:
        for (const child of children(node)) scan(m, child, scopes, uses);
    }
  };

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
      scanFunction(m, body as unknown as AnyNode, [], dependencies);
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
        scanFunction(m, helper.body as unknown as AnyNode, [], helper.dependencies as Uses);
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
  // A name is given once: a library's functions are in scope of everything the library's bodies name.
  const taken = new Set([lens, ...entries.map((entry) => entry.name)]);
  for (const declaration of ordered)
    if (declaration.kind === "library") for (const fn of declaration.functions) taken.add(fn.name);
  for (const declaration of ordered) {
    if (declaration.kind === "constant") continue;
    let name = declaration.name;
    for (let n = 2; taken.has(name); n++) name = `${declaration.name}_${n}`;
    taken.add(name);
    declaration.name = name;
  }

  return { site, module, name: lens, entries, immutables, declarations: ordered, references, files: [...files] };
}
