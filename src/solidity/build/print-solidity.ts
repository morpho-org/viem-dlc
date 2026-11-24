/**
 * Print (Solidity): a resolved lens to a Solidity source unit and its position map.
 *
 * The printer maps syntax to syntax, one construct at a time, and decides no meaning: it does no
 * arithmetic, no hashing and no operator typing, adds no temporaries and reorders nothing. Its only
 * choices are three fixed rules:
 *
 * - **The type of a local** is the type its initializer already has in the source: a lookup of
 *   declarations ({@link Printer.typeOf}). An initializer with no declared type, such as arithmetic,
 *   is a located error asking for a conversion.
 * - **Data location.** A reference-type parameter of an external function is `calldata`. Everything
 *   else is `memory`: locals, helper parameters and return values.
 * - **The spelling of a literal.** `0n` is `0`, `60_000` is `60000`, `'text'` is `"text"`, a string
 *   outside ASCII is a `unicode"…"` literal, `'0x313ce567'` is `hex"313ce567"`, and a `'0x…'` of 40
 *   digits is an address literal.
 *
 * What is only TypeScript is erased before anything is read: annotations, generics, `as`, `satisfies`,
 * `!`, a `this` parameter, and a type declared in a body. `Number(x)` and `BigInt(x)` print as `x`.
 *
 * An operator is printed as the same operator. Parentheses come from the tree and never from the source
 * text, so a module prints the same from `.ts` and from built `.js`: an operator that is an operand of
 * another operator is parenthesized, always. A module constant is printed in place, in parentheses, at
 * each use, where solc treats it as the literal expression it is.
 *
 * The source unit is laid out as: the header, the structs, the interfaces, the libraries, then the
 * contract with its immutables, its entry points in declared order, and its helpers. Each kind is in the
 * order of {@link ResolvedLens.declarations}.
 *
 * A construct no lens may contain stops the build through `fail`, at its TypeScript position. An assembly
 * block is handed to the Yul printer, which writes it into the same source unit.
 */
import type { Span } from "oxc-parser";

import { type AnyNode, children, is, keyName, unwrap } from "./ast.js";
import { isElementaryTypeName } from "./builtins.js";
import { fail } from "./errors.js";
import { Writer } from "./position-map.js";
import { printYul } from "./print-yul.js";
import type {
  ConstantDeclaration,
  InterfaceDeclaration,
  LibraryDeclaration,
  PrintedLens,
  Reference,
  ResolvedFunction,
  ResolvedLens,
  ResolvedSignature,
  SourceModule,
  StructDeclaration,
  TypeRef,
} from "./types.js";

/** What every printed lens starts with. The exact compiler is pinned by the repository's configuration file. */
export const SOLIDITY_HEADER = "// SPDX-License-Identifier: UNLICENSED\npragma solidity ^0.8.0;\n";

const BINARY: Readonly<Record<string, string>> = {
  "+": "+",
  "-": "-",
  "*": "*",
  "/": "/",
  "%": "%",
  "**": "**",
  "<<": "<<",
  ">>": ">>",
  "&": "&",
  "|": "|",
  "^": "^",
  "<": "<",
  "<=": "<=",
  ">": ">",
  ">=": ">=",
  "==": "==",
  "===": "==",
  "!=": "!=",
  "!==": "!=",
};
const COMPARISON = new Set(["<", "<=", ">", ">=", "==", "===", "!=", "!=="]);
const LOGICAL = new Set(["&&", "||"]);
const UNARY = new Set(["!", "-", "~"]);
const ASSIGNMENT = new Set(["=", "+=", "-=", "*=", "/=", "%=", "<<=", ">>=", "&=", "|=", "^="]);
/** Node types that are operators, and so are parenthesized when they are an operand. */
const OPERATORS = new Set([
  "BinaryExpression",
  "LogicalExpression",
  "ConditionalExpression",
  "UnaryExpression",
  "AssignmentExpression",
  "UpdateExpression",
]);

/** Statements that print as nothing: the empty statement, and a type declared in a body, which is only TypeScript. */
const ERASED_STATEMENTS = new Set(["EmptyStatement", "TSTypeAliasDeclaration", "TSInterfaceDeclaration"]);

const UINT256: TypeRef = { kind: "elementary", name: "uint256" };
const BOOL: TypeRef = { kind: "elementary", name: "bool" };
const STRING: TypeRef = { kind: "elementary", name: "string" };
const BYTES: TypeRef = { kind: "elementary", name: "bytes" };
const BYTES4: TypeRef = { kind: "elementary", name: "bytes4" };
const BYTES32: TypeRef = { kind: "elementary", name: "bytes32" };

/** Built-ins that are called in Solidity exactly as they are written. */
const SAME_CALL = new Set(["require", "revert", "gasleft", "keccak256"]);
/** The return types the package's built-in functions declare. */
const BUILTIN_OUTPUTS: ReadonlyMap<string, readonly TypeRef[]> = new Map([
  ["gasleft", [UINT256]],
  ["keccak256", [BYTES32]],
  ["codeSize", [UINT256]],
  ["staticcall", [BOOL, BYTES]],
]);

const THIS = "in a lens, this only calls an entry point: this.f(…)";
const INTERFACE_MEMBER =
  "an interface is used as I(address) for a handle, I.f.selector for a selector, or I.f.encode(…) for calldata";
const HANDLE =
  "a handle is written I(address). The options of a call are written where the call is made: I(address, { gas: … }).f(…)";
const TRY =
  "the block of a try holds one external call, bare or assigned: try { x = feed.f(a) } catch { … }, with no finally";
const ADDRESS: TypeRef = { kind: "elementary", name: "address" };

/** `'0x…'` with 40 hexadecimal digits is an address literal, and with any other even number a hex literal. */
const ADDRESS_LITERAL = /^0x[0-9a-fA-F]{40}$/;
const HEX_LITERAL = /^0x(?:[0-9a-fA-F]{2})*$/;

const typeName = (type: TypeRef): string =>
  type.kind === "elementary"
    ? type.name
    : type.kind === "array"
      ? `${typeName(type.element)}[]`
      : type.declaration.name;

/** True for the types Solidity gives a data location: structs, arrays, `bytes` and `string`. */
const isReferenceType = (type: TypeRef): boolean =>
  type.kind === "struct" ||
  type.kind === "array" ||
  (type.kind === "elementary" && (type.name === "bytes" || type.name === "string"));

const located = (type: TypeRef, location: "memory" | "calldata"): string =>
  isReferenceType(type) ? `${typeName(type)} ${location}` : typeName(type);

/** The `returns (…)` of a signature, or nothing when it returns nothing. A return value is `memory`. */
const returns = (signature: ResolvedSignature): string =>
  signature.outputs.length ? ` returns (${signature.outputs.map((type) => located(type, "memory")).join(", ")})` : "";

class Printer {
  readonly #lens: ResolvedLens;
  readonly #out = new Writer();
  /** The module of the body being printed, for positions. */
  #module: SourceModule;
  /** The locals in scope, innermost last, each with the type it was declared with. */
  #scopes: Map<string, TypeRef>[] = [];
  /** True while a library's functions are printed: they are outside the contract, and cannot reach into it. */
  #inLibrary = false;

  constructor(lens: ResolvedLens) {
    this.#lens = lens;
    this.#module = lens.module;
  }

  print(): PrintedLens {
    const out = this.#out;
    const { declarations } = this.#lens;
    out.write(SOLIDITY_HEADER);
    for (const declaration of declarations) if (declaration.kind === "struct") this.#struct(declaration);
    for (const declaration of declarations) if (declaration.kind === "interface") this.#interface(declaration);
    for (const declaration of declarations) if (declaration.kind === "library") this.#library(declaration);
    out.line();
    out.node({ module: this.#lens.module, node: this.#lens.site.node }, () => {
      out.write(`contract ${this.#lens.name} {`);
      out.indented(() => {
        let first = true;
        const separate = () => {
          if (!first) out.write("\n");
          first = false;
          out.line();
        };
        if (this.#lens.immutables.length) {
          separate();
          this.#immutables();
        }
        for (const entry of this.#lens.entries) {
          separate();
          this.#function(entry, "external");
        }
        for (const declaration of declarations) {
          if (declaration.kind !== "helper") continue;
          separate();
          this.#function(declaration, "internal");
        }
      });
      out.line();
      out.write("}");
    });
    out.write("\n");
    return out.finish();
  }

  // ---- declarations ------------------------------------------------------------------------------------
  #struct(struct: StructDeclaration): void {
    const out = this.#out;
    out.line();
    out.node({ module: struct.module, node: struct.node }, () => {
      out.write(`struct ${struct.name} {`);
      out.indented(() => {
        for (const field of struct.fields) {
          out.line();
          out.node(field.origin, () => out.write(`${typeName(field.type)} ${field.name};`));
        }
      });
      out.line();
      out.write("}");
    });
    out.write("\n");
  }

  /** An interface: its functions as declared, with unnamed parameters. Every one is `view` or `pure`. */
  #interface(declaration: InterfaceDeclaration): void {
    const out = this.#out;
    out.line();
    out.node({ module: declaration.module, node: declaration.node }, () => {
      out.write(`interface ${declaration.name} {`);
      out.indented(() => {
        for (const { name, signature } of declaration.functions) {
          out.line();
          out.node(signature.origin, () => {
            const inputs = signature.inputs.map((type) => located(type, "calldata")).join(", ");
            out.write(`function ${name}(${inputs}) external ${signature.mutability}${returns(signature)};`);
          });
        }
      });
      out.line();
      out.write("}");
    });
    out.write("\n");
  }

  /** A library in the same file. Its functions are internal, so a call to one is a jump and not a call. */
  #library(library: LibraryDeclaration): void {
    const out = this.#out;
    out.line();
    out.node({ module: library.module, node: library.node }, () => {
      out.write(`library ${library.name} {`);
      this.#inLibrary = true;
      out.indented(() => {
        for (const [index, fn] of library.functions.entries()) {
          if (index) out.write("\n");
          out.line();
          this.#function(fn, "internal");
        }
      });
      this.#inLibrary = false;
      out.line();
      out.write("}");
    });
    out.write("\n");
  }

  /**
   * The immutables and the constructor that sets them, in the order the contract lists them. A constructor
   * parameter is its immutable's name followed by `_`, repeated until no declaration of the lens has that name.
   */
  #immutables(): void {
    const out = this.#out;
    const taken = new Set([
      this.#lens.name,
      ...this.#lens.entries.map((entry) => entry.name),
      ...this.#lens.declarations.map((declaration) => declaration.name),
    ]);
    const immutables = this.#lens.immutables.map(({ declaration }) => {
      let parameter = `${declaration.name}_`;
      while (taken.has(parameter)) parameter += "_";
      taken.add(parameter);
      return { declaration, parameter, origin: { module: declaration.module, node: declaration.node } };
    });
    for (const [index, { declaration, origin }] of immutables.entries()) {
      if (index) out.line();
      out.node(origin, () => out.write(`${typeName(declaration.type)} immutable ${declaration.name};`));
    }
    out.write("\n");
    out.line();
    out.write("constructor(");
    for (const [index, { declaration, parameter, origin }] of immutables.entries()) {
      if (index) out.write(", ");
      out.node(origin, () => out.write(`${located(declaration.type, "memory")} ${parameter}`));
    }
    out.write(") {");
    out.indented(() => {
      for (const { declaration, parameter, origin } of immutables) {
        out.line();
        out.node(origin, () => out.write(`${declaration.name} = ${parameter};`));
      }
    });
    out.line();
    out.write("}");
  }

  #function(fn: ResolvedFunction, visibility: "external" | "internal"): void {
    const out = this.#out;
    const { signature, body } = fn;
    this.#module = fn.module;
    this.#scopes = [new Map()];
    // A `this` parameter is a TypeScript annotation, and is erased with the others.
    const params = body.params.filter((parameter) => !(is(parameter, "Identifier") && parameter.name === "this"));
    if (params.length !== signature.inputs.length)
      this.#fail(
        body,
        `"${fn.name}" declares ${signature.inputs.length} parameters and its implementation takes ${params.length}`,
      );
    out.node({ module: fn.module, node: body }, () => {
      out.write(`function ${fn.name}(`);
      for (const [index, parameter] of params.entries()) {
        const type = signature.inputs[index] as TypeRef;
        if (!is(parameter, "Identifier")) this.#fail(parameter, "a parameter must be a plain name");
        if (index) out.write(", ");
        out.node({ module: fn.module, node: parameter }, () =>
          out.write(`${located(type, visibility === "external" ? "calldata" : "memory")} ${parameter.name}`),
        );
        this.#declare(parameter.name, type);
      }
      out.write(`) ${visibility} ${signature.mutability}${returns(signature)} {`);
      out.indented(() => {
        const block = body.body as AnyNode | null;
        if (!block) this.#fail(body, "a lens function needs a body");
        if (is(block, "BlockStatement")) {
          for (const statement of block.body) this.#statement(statement as unknown as AnyNode);
          return;
        }
        // An arrow function with an expression body: `(x, y, d) => (x * y) / d`.
        out.line();
        out.node({ module: fn.module, node: block }, () => {
          if (signature.outputs.length) {
            out.write("return ");
            this.#returned(block);
          } else this.#effect(block);
          out.write(";");
        });
      });
      out.line();
      out.write("}");
    });
  }

  // ---- statements ------------------------------------------------------------------------------------------
  #statement(node: AnyNode): void {
    const out = this.#out;
    if (ERASED_STATEMENTS.has(node.type)) return;
    out.line();
    out.node({ module: this.#module, node }, () => {
      switch (node.type) {
        case "VariableDeclaration":
          this.#locals(node);
          return;
        case "ExpressionStatement": {
          const expression = this.#strip(node.expression as Span);
          if (this.#isAssembly(expression)) {
            this.#assembly(expression);
            return;
          }
          this.#effect(expression);
          out.write(";");
          return;
        }
        case "ReturnStatement":
          if (!node.argument) {
            out.write("return;");
            return;
          }
          out.write("return ");
          this.#returned(node.argument as Span);
          out.write(";");
          return;
        case "IfStatement":
          this.#if(node);
          return;
        case "BlockStatement":
          this.#block(node);
          return;
        case "WhileStatement":
          out.write("while (");
          this.#expression(node.test as Span);
          out.write(")");
          this.#nested(node.body as AnyNode);
          return;
        case "DoWhileStatement":
          out.write("do");
          this.#nested(node.body as AnyNode);
          if (is(node.body as AnyNode, "BlockStatement")) out.write(" ");
          else out.line();
          out.write("while (");
          this.#expression(node.test as Span);
          out.write(");");
          return;
        case "ForStatement":
          this.#for(node);
          return;
        case "BreakStatement":
        case "ContinueStatement":
          out.write(node.type === "BreakStatement" ? "break;" : "continue;");
          return;
        case "TryStatement":
          this.#try(node);
          return;
        default:
          this.#fail(node, `a lens body cannot contain this statement (${node.type})`);
      }
    });
  }

  /** An expression in statement position: one that assigns or calls. */
  #effect(raw: Span): void {
    const expression = this.#strip(raw);
    if (!["AssignmentExpression", "UpdateExpression", "CallExpression"].includes(expression.type))
      this.#fail(expression, "this expression is not a statement: a statement assigns or calls");
    this.#expression(expression);
  }

  /** What a `return` returns: an expression, or `[a, b]` as the tuple `(a, b)`. */
  #returned(raw: Span): void {
    const out = this.#out;
    const node = this.#strip(raw);
    if (!is(node, "ArrayExpression")) {
      this.#expression(node);
      return;
    }
    if (node.elements.length < 2)
      this.#fail(node, "several values are returned as return [a, b]; one value is returned without the brackets");
    out.node({ module: this.#module, node }, () => {
      out.write("(");
      for (const [index, element] of node.elements.entries()) {
        if (!element || is(element, "SpreadElement"))
          this.#fail(element ?? node, "every value of return [a, b] is an expression");
        if (index) out.write(", ");
        this.#expression(element);
      }
      out.write(")");
    });
  }

  /** `for (init; test; update)`, each part where it is written. The init declares one local or is a statement. */
  #for(node: AnyNode): void {
    const out = this.#out;
    const init = node.init as AnyNode | null;
    this.#scopes.push(new Map());
    out.write("for (");
    if (init)
      out.node({ module: this.#module, node: init }, () => {
        if (!is(init, "VariableDeclaration")) {
          this.#effect(init);
          out.write(";");
        } else if (init.declarations.length !== 1) this.#fail(init, "a for loop declares one local");
        else if (init.declarations[0]?.init && this.#isAssembly(this.#strip(init.declarations[0].init)))
          this.#fail(
            init.declarations[0].init,
            "an assembly block is not the initializer of a for loop's local: declare the local before the loop",
          );
        else this.#locals(init);
      });
    else out.write(";");
    if (node.test) {
      out.write(" ");
      this.#expression(node.test as Span);
    }
    out.write(";");
    if (node.update) {
      out.write(" ");
      this.#effect(node.update as Span);
    }
    out.write(")");
    this.#nested(node.body as AnyNode);
    this.#scopes.pop();
  }

  /** `if` and `else`, on the current line. An `else if` continues the line of its `else`. */
  #if(node: AnyNode): void {
    const out = this.#out;
    out.write("if (");
    this.#expression(node.test as AnyNode);
    out.write(")");
    this.#nested(node.consequent as AnyNode);
    if (!node.alternate) return;
    const alternate = node.alternate as AnyNode;
    if (is(node.consequent as AnyNode, "BlockStatement")) out.write(" else");
    else {
      out.line();
      out.write("else");
    }
    if (is(alternate, "IfStatement")) {
      out.write(" ");
      out.node({ module: this.#module, node: alternate }, () => this.#if(alternate));
    } else this.#nested(alternate);
  }

  /** The body of an `if`, an `else` or a loop: a block where one is written, and a single statement where it is not. */
  #nested(node: AnyNode): void {
    if (is(node, "BlockStatement")) {
      this.#out.write(" ");
      this.#block(node);
    } else if (ERASED_STATEMENTS.has(node.type))
      this.#fail(node, "the body of an if or a loop is a statement or a block: an empty one is written {}");
    else {
      this.#scopes.push(new Map());
      this.#out.indented(() => this.#statement(node));
      this.#scopes.pop();
    }
  }

  #block(node: AnyNode): void {
    const out = this.#out;
    out.write("{");
    this.#scopes.push(new Map());
    out.indented(() => {
      for (const statement of node.body as AnyNode[]) this.#statement(statement);
    });
    this.#scopes.pop();
    out.line();
    out.write("}");
  }

  /**
   * `try { x = feed.f(a) } catch { … }` is `try feed.f(a) returns (T r) { x = r; } catch { … }`: Solidity's
   * try takes one external call, so the block holds exactly one, bare or assigned. T is the type the called
   * function declares. The returned value is named `r`, with `_` appended while the assigned target names an
   * `r` of its own.
   */
  #try(node: AnyNode): void {
    const out = this.#out;
    const handler = node.handler as AnyNode | null;
    if (!handler || node.finalizer) this.#fail(node, TRY);
    if (handler.param) this.#fail(handler.param as Span, "catch binds nothing in a lens: write catch { … }");
    const block = node.block as AnyNode;
    const statements = (block.body as AnyNode[]).filter((statement) => !ERASED_STATEMENTS.has(statement.type));
    const [statement] = statements;
    const expression =
      statements.length === 1 && is(statement, "ExpressionStatement") ? this.#strip(statement.expression) : undefined;
    const assignment = is(expression, "AssignmentExpression") ? expression : undefined;
    const call = assignment ? this.#strip(assignment.right) : expression;
    if (!call || !this.#isExternalCall(call) || (assignment && assignment.operator !== "=")) this.#fail(block, TRY);
    out.write("try ");
    this.#expression(call);
    if (assignment) {
      const target = unwrap(assignment.left);
      const [type, ...rest] = this.#outputs(call) ?? [];
      if (!type || rest.length)
        this.#fail(
          call,
          "a try assigns the one value its call returns, so the call must be to a declared function with exactly one return value",
        );
      const names = new Set<string>();
      const collect = (part: AnyNode): void => {
        if (is(part, "Identifier")) names.add(part.name);
        else if (is(part, "MemberExpression") && !part.computed) collect(part.object as AnyNode);
        else for (const child of children(part)) collect(child);
      };
      collect(target);
      let returned = "r";
      while (names.has(returned)) returned += "_";
      out.write(` returns (${located(type, "memory")} ${returned}) {`);
      out.indented(() => {
        out.line();
        out.node({ module: this.#module, node: assignment }, () => {
          this.#expression(target, true);
          out.write(` = ${returned};`);
        });
      });
    } else out.write(" {");
    out.line();
    out.write("} catch ");
    this.#block(handler.body as AnyNode);
  }

  /** True for a call that leaves the contract: through a handle, or through `this`. */
  #isExternalCall(node: AnyNode): boolean {
    if (!is(node, "CallExpression")) return false;
    const callee = unwrap(node.callee);
    if (!is(callee, "MemberExpression") || callee.computed || this.#lens.references.has(callee)) return false;
    const object = this.#strip(callee.object);
    if (is(object, "ThisExpression")) return true;
    if (this.#interfaceFunction(object)) return false;
    const named = this.#lens.references.get(object);
    return !(named?.kind === "builtin" || (named?.kind === "declaration" && named.declaration.kind === "library"));
  }

  /** `const x = e` and `let x = e`: `T x = e;`, with T copied from the initializer. */
  #locals(node: AnyNode): void {
    const out = this.#out;
    if (node.kind !== "const" && node.kind !== "let") this.#fail(node, "a local is declared with const or let");
    for (const [index, declarator] of (node.declarations as AnyNode[]).entries()) {
      if (index) out.line();
      const id = declarator.id as AnyNode;
      if (!declarator.init)
        this.#fail(declarator, "a local needs an initializer; a local with none is written with its zero value");
      const init = this.#strip(declarator.init as Span);
      if (is(id, "ArrayPattern")) {
        this.#tuple(id, init);
        continue;
      }
      if (!is(id, "Identifier")) this.#fail(id, "a local must be a plain name");
      if (this.#isAssembly(init)) {
        this.#assembly(init, id.name);
        continue;
      }
      const type = this.typeOf(init);
      out.write(`${located(type, "memory")} ${id.name}`);
      // `new Position()` is the zero value, which a Solidity declaration has without an initializer.
      if (!(is(init, "NewExpression") && init.arguments.length === 0)) {
        out.write(" = ");
        this.#expression(init);
      }
      out.write(";");
      this.#declare(id.name, type);
    }
  }

  /** `const [a, , c] = f(x)`: `(T a, , U c) = f(x);`, each type the one `f` declares for that value. */
  #tuple(pattern: AnyNode, init: AnyNode): void {
    const out = this.#out;
    const outputs = is(init, "CallExpression") ? this.#outputs(init) : undefined;
    if (!outputs)
      this.#fail(
        init,
        "several locals are declared from one call whose return types are declared, as in const [a, b] = f(x)",
      );
    const elements = pattern.elements as (AnyNode | null)[];
    if (elements.length !== outputs.length)
      this.#fail(
        pattern,
        `this pattern has ${elements.length} places and the call returns ${outputs.length} values: a value that is not used is left as a hole, as in [a, , c]`,
      );
    const declared: [string, TypeRef][] = [];
    out.write("(");
    for (const [index, element] of elements.entries()) {
      if (index) out.write(", ");
      if (!element) continue;
      if (!is(element, "Identifier")) this.#fail(element, "a local must be a plain name");
      const type = outputs[index] as TypeRef;
      out.node({ module: this.#module, node: element }, () => out.write(`${located(type, "memory")} ${element.name}`));
      declared.push([element.name, type]);
    }
    out.write(") = ");
    this.#expression(init);
    out.write(";");
    for (const [name, type] of declared) this.#declare(name, type);
  }

  // ---- the type of a local ------------------------------------------------------------------------------------
  /**
   * The type an expression already has in the source, or a located error when it has none. This is the
   * whole local-type rule, and it is a lookup: Solidity's operator typing is not implemented, so the type
   * of arithmetic is asked of the author as a conversion.
   */
  typeOf(raw: Span): TypeRef {
    const node = this.#strip(raw);
    const known = this.#declaredType(node);
    if (known) return known;
    const outputs = is(node, "CallExpression") ? this.#outputs(node) : undefined;
    if (outputs)
      this.#fail(
        node,
        `a local takes one value and this call returns ${outputs.length}. Several values are declared together, as in const [a, b] = f(x)`,
      );
    return this.#fail(
      node,
      "this local's type cannot be copied from its initializer: name the type with a conversion, for example uint256(…)",
    );
  }

  #declaredType(node: AnyNode): TypeRef | undefined {
    if (is(node, "Literal")) {
      if (typeof node.value === "bigint" || typeof node.value === "number") return UINT256;
      if (typeof node.value === "boolean") return BOOL;
      // A hex literal has no one type in Solidity: it converts to bytes4, to bytes32 and to bytes.
      if (typeof node.value === "string")
        return ADDRESS_LITERAL.test(node.value) ? ADDRESS : HEX_LITERAL.test(node.value) ? undefined : STRING;
      return undefined;
    }
    if (is(node, "Identifier") || this.#lens.references.has(node)) {
      const local = is(node, "Identifier") ? this.#local(node.name) : undefined;
      if (local) return local;
      const reference = this.#lens.references.get(node);
      if (reference?.kind !== "declaration") return undefined;
      if (reference.declaration.kind === "constant") return UINT256;
      if (reference.declaration.kind === "immutable") return reference.declaration.type;
      return undefined;
    }
    if (is(node, "MemberExpression")) {
      const inner = this.#strip(node.object);
      if (node.computed) {
        const indexed = this.#declaredType(inner);
        return indexed?.kind === "array" ? indexed.element : undefined;
      }
      const { name } = node.property;
      if (this.#isBuiltin(inner, "block")) return name === "timestamp" || name === "number" ? UINT256 : undefined;
      if (this.#interfaceFunction(inner)) return name === "selector" ? BYTES4 : undefined;
      const object = this.#declaredType(inner);
      if (object?.kind === "array") return name === "length" ? UINT256 : undefined;
      if (object?.kind !== "struct") return undefined;
      return object.declaration.fields.find((field) => field.name === name)?.type;
    }
    if (is(node, "CallExpression")) {
      const [output, ...rest] = this.#outputs(node) ?? [];
      return rest.length ? undefined : output;
    }
    if (is(node, "NewExpression")) return { kind: "struct", declaration: this.#constructed(node) };
    if (is(node, "BinaryExpression")) return COMPARISON.has(node.operator) ? BOOL : undefined;
    if (is(node, "LogicalExpression")) return LOGICAL.has(node.operator) ? BOOL : undefined;
    if (is(node, "UnaryExpression")) return node.operator === "!" ? BOOL : undefined;
    return undefined;
  }

  /**
   * The one answer to "what types does this call return", read from the declaration of what is called:
   * a helper, a library function, an interface function, an entry point through `this`, or a built-in.
   * A conversion returns the type it names, `array(T, n)` an array of `T`, and `abi.decode` the types it
   * lists. Undefined where the source declares none.
   */
  #outputs(node: AnyNode): readonly TypeRef[] | undefined {
    const callee = unwrap(node.callee as Span);
    const reference = this.#lens.references.get(callee);
    if (reference?.kind === "declaration") {
      const { declaration } = reference;
      if (declaration.kind === "helper") return declaration.signature.outputs;
      return declaration.kind === "interface" ? [{ kind: "interface", declaration }] : undefined;
    }
    if (reference) {
      if (reference.module !== "solidity") return undefined;
      if (isElementaryTypeName(reference.name)) return [{ kind: "elementary", name: reference.name }];
      if (reference.name === "array") return [{ kind: "array", element: this.#arrayOf(node).element }];
      return BUILTIN_OUTPUTS.get(reference.name);
    }
    if (!is(callee, "MemberExpression") || callee.computed) return undefined;
    const object = this.#strip(callee.object);
    const { name } = callee.property;
    const declared = (functions: readonly { name: string; signature: ResolvedSignature }[], owner: string) =>
      functions.find((fn) => fn.name === name)?.signature.outputs ??
      this.#fail(callee, `${owner} declares no function "${name}"`);
    if (is(object, "ThisExpression")) return declared(this.#lens.entries, `contract ${this.#lens.name}`);
    const named = this.#lens.references.get(object);
    if (named?.kind === "declaration" && named.declaration.kind === "library")
      return declared(named.declaration.functions, `library ${named.declaration.name}`);
    if (this.#isBuiltin(object, "abi")) {
      if (name === "encode") return [BYTES];
      const list = name === "decode" ? (node.arguments as Span[])[1] : undefined;
      const types = list && unwrap(list);
      return is(types, "ArrayExpression") ? this.#types(types) : undefined;
    }
    if (this.#interfaceFunction(object)) return name === "encode" ? [BYTES] : undefined;
    const receiver = this.#declaredType(object);
    return receiver?.kind === "interface"
      ? declared(receiver.declaration.functions, `interface ${receiver.declaration.name}`)
      : undefined;
  }

  /** True when a node names the package's built-in `name`. */
  #isBuiltin(node: AnyNode, name: string): boolean {
    const reference = this.#lens.references.get(node);
    return reference?.kind === "builtin" && reference.module === "solidity" && reference.name === name;
  }

  /** `I.f`, where I is an interface: the function a selector or an encoded call names. */
  #interfaceFunction(node: AnyNode): { interface: InterfaceDeclaration; name: string } | undefined {
    if (!is(node, "MemberExpression") || node.computed) return undefined;
    const reference = this.#lens.references.get(this.#strip(node.object));
    return reference?.kind === "declaration" && reference.declaration.kind === "interface"
      ? { interface: reference.declaration, name: node.property.name }
      : undefined;
  }

  /** The types an array literal lists, as `abi.decode` names what it reads: `[uint128, address]`. */
  #types(list: AnyNode): TypeRef[] {
    return (list.elements as (Span | null)[]).map((element) =>
      element ? this.#namedType(element) : this.#fail(list, "a list of types has no holes"),
    );
  }

  /** The parts of `array(T, n)`: the element type it names, and its length. */
  #arrayOf(node: AnyNode): { element: TypeRef; length: AnyNode } {
    const [element, length, ...rest] = node.arguments as AnyNode[];
    if (!element || !length || rest.length || is(element, "SpreadElement") || is(length, "SpreadElement"))
      return this.#fail(node, "an array is made with array(T, n): its element type and its length");
    return { element: this.#namedType(element), length };
  }

  /** A type written as a value: an elementary type, a struct class, an interface, or `[T]`. */
  #namedType(raw: Span): TypeRef {
    const node = unwrap(raw);
    if (is(node, "ArrayExpression")) {
      const [element] = node.elements;
      if (node.elements.length !== 1 || !element || is(element, "SpreadElement"))
        return this.#fail(node, "an array type is written [T], with one element type");
      return { kind: "array", element: this.#namedType(element) };
    }
    const reference = this.#lens.references.get(node);
    if (reference?.kind === "builtin" && reference.module === "solidity" && isElementaryTypeName(reference.name))
      return { kind: "elementary", name: reference.name };
    if (reference?.kind === "declaration") {
      const { declaration } = reference;
      if (declaration.kind === "struct") return { kind: "struct", declaration };
      if (declaration.kind === "interface") return { kind: "interface", declaration };
    }
    return this.#fail(node, "this is not a type. A type is an elementary type, a struct class, an interface, or [T]");
  }

  /** The struct a `new` builds. */
  #constructed(node: AnyNode): StructDeclaration {
    const callee = this.#lens.references.get(unwrap(node.callee as Span));
    if (callee?.kind !== "declaration" || callee.declaration.kind !== "struct")
      return this.#fail(node, "new builds a struct: new T({ … }), or new T() for the zero value of a local");
    return callee.declaration;
  }

  // ---- expressions ------------------------------------------------------------------------------------------------
  /**
   * Strips what prints as nothing: parentheses, TypeScript's wrappers, and `Number(x)` and `BigInt(x)`,
   * which print as `x`. They exist because tsc wants a `number` where an index is held in a bigint.
   */
  #strip(raw: Span): AnyNode {
    let node = unwrap(raw);
    while (is(node, "CallExpression")) {
      const callee = this.#lens.references.get(unwrap(node.callee));
      if (callee?.kind !== "builtin" || callee.module !== "global") break;
      const [argument, ...rest] = node.arguments;
      if (!argument || rest.length || is(argument, "SpreadElement"))
        return this.#fail(node, `${callee.name}(x) takes one argument, and prints as x`);
      node = unwrap(argument);
    }
    return node;
  }

  /** Prints an expression. As an operand of an operator, an operator is parenthesized. */
  #expression(raw: Span, operand = false): void {
    const node = this.#strip(raw);
    const out = this.#out;
    const parenthesized = operand && OPERATORS.has(node.type);
    out.node({ module: this.#module, node }, () => {
      if (parenthesized) out.write("(");
      this.#bare(node);
      if (parenthesized) out.write(")");
    });
  }

  #bare(node: AnyNode): void {
    const out = this.#out;
    if (is(node, "Literal")) {
      out.write(this.#literal(node));
      return;
    }
    if (is(node, "Identifier") && this.#local(node.name)) {
      out.write(node.name);
      return;
    }
    const reference = this.#lens.references.get(node);
    if (reference) {
      this.#named(node, reference);
      return;
    }

    switch (node.type) {
      case "MemberExpression": {
        if (node.computed) {
          this.#expression(node.object as Span, true);
          out.write("[");
          this.#expression(node.property as Span);
          out.write("]");
          return;
        }
        const object = this.#strip(node.object as Span);
        if (is(object, "ThisExpression")) this.#fail(node, THIS);
        const property = (node.property as { name: string }).name;
        const named = this.#lens.references.get(object);
        if (named?.kind === "builtin") {
          if (!this.#isBuiltin(object, "block") || (property !== "timestamp" && property !== "number"))
            this.#fail(node, `a lens cannot read ${named.name}.${property}`);
          out.write(`block.${property}`);
          return;
        }
        // `I.f.selector`. An interface or one of its functions means nothing else as a value.
        const fn = this.#interfaceFunction(object);
        if (fn || this.#interfaceFunction(node)) {
          if (!fn || property !== "selector") this.#fail(node, INTERFACE_MEMBER);
          out.write(`${fn.interface.name}.${fn.name}.selector`);
          return;
        }
        this.#expression(object, true);
        out.write(`.${property}`);
        return;
      }
      case "UnaryExpression":
        if (!UNARY.has(node.operator as string)) this.#fail(node, `a lens has no operator ${node.operator}`);
        out.write(node.operator as string);
        this.#expression(node.argument as Span, true);
        return;
      case "BinaryExpression":
      case "LogicalExpression": {
        const operator =
          node.type === "BinaryExpression"
            ? BINARY[node.operator as string]
            : LOGICAL.has(node.operator as string)
              ? (node.operator as string)
              : undefined;
        if (operator === undefined) this.#fail(node, `a lens has no operator ${node.operator}`);
        this.#expression(node.left as Span, true);
        out.write(` ${operator} `);
        this.#expression(node.right as Span, true);
        return;
      }
      case "ConditionalExpression":
        this.#expression(node.test as Span, true);
        out.write(" ? ");
        this.#expression(node.consequent as Span, true);
        out.write(" : ");
        this.#expression(node.alternate as Span, true);
        return;
      case "AssignmentExpression": {
        if (!ASSIGNMENT.has(node.operator as string)) this.#fail(node, `a lens has no operator ${node.operator}`);
        const target = unwrap(node.left as Span);
        if (is(target, "ArrayPattern")) {
          // `[r.a, , r.b] = f(x)`: the tuple `(r.a, , r.b) = f(x)`, a hole where a value is not used.
          out.write("(");
          for (const [index, element] of target.elements.entries()) {
            if (index) out.write(", ");
            if (element) this.#expression(element);
          }
          out.write(")");
        } else this.#expression(target, true);
        out.write(` ${node.operator} `);
        this.#expression(node.right as Span);
        return;
      }
      case "UpdateExpression":
        if (node.prefix) out.write(node.operator as string);
        this.#expression(node.argument as Span, true);
        if (!node.prefix) out.write(node.operator as string);
        return;
      case "CallExpression":
        this.#call(node);
        return;
      case "NewExpression":
        this.#structLiteral(node);
        return;
      case "ThisExpression":
        this.#fail(node, THIS);
        return;
      case "Identifier":
        this.#fail(node, `"${node.name}" is used before it is declared`);
        return;
      default:
        this.#fail(node, `a lens body cannot contain this expression (${node.type})`);
    }
  }

  /** `new T({ a, b: e })`: `T({a: a, b: e})`, the fields by name and in the order written. */
  #structLiteral(node: AnyNode): void {
    const out = this.#out;
    const struct = this.#constructed(node);
    const [argument, ...rest] = node.arguments as AnyNode[];
    if (!argument)
      this.#fail(node, "new T() is the zero value of a local: write it as the initializer of a const or let");
    const fields = unwrap(argument);
    if (rest.length || !is(fields, "ObjectExpression"))
      this.#fail(node, "a struct is built from an object literal of its fields: new T({ a, b })");
    out.write(`${struct.name}({`);
    for (const [index, property] of fields.properties.entries()) {
      const name = is(property, "Property") && !property.computed ? keyName(property.key) : undefined;
      if (name === undefined || !is(property, "Property") || property.kind !== "init" || property.method)
        this.#fail(property, "a field of a struct literal is written name: value, or the name alone");
      if (index) out.write(", ");
      out.node({ module: this.#module, node: property }, () => {
        out.write(`${name}: `);
        this.#expression(property.value);
      });
    }
    out.write("})");
  }

  /** A name that resolved to a module-level declaration or a package export, used as a value. */
  #named(node: AnyNode, reference: Reference): void {
    if (reference.kind === "builtin" && reference.module === "global")
      this.#fail(node, `${reference.name} is written as a call: ${reference.name}(x), which prints as x`);
    if (reference.kind === "builtin") this.#fail(node, `"${reference.name}" cannot be used as a value here`);
    const { declaration } = reference;
    if (declaration.kind === "constant") this.#constant(declaration);
    else if (declaration.kind === "immutable") {
      if (this.#inLibrary) this.#outsideLibrary(node, declaration.name);
      if (!this.#lens.immutables.some((listed) => listed.declaration === declaration))
        this.#fail(
          node,
          `"${declaration.name}" is not listed in the immutables of ${this.#lens.name}: contract(name, functions, { immutables: { ${declaration.name} } })`,
        );
      this.#out.write(declaration.name);
    } else if (declaration.kind === "interface") this.#fail(node, INTERFACE_MEMBER);
    else this.#fail(node, `"${declaration.name}" is a ${declaration.kind} and cannot be used as a value here`);
  }

  /** A module constant: its expression printed in place, in parentheses, read in its own module. */
  #constant(constant: ConstantDeclaration): void {
    const saved = { module: this.#module, scopes: this.#scopes };
    this.#module = constant.module;
    this.#scopes = [];
    this.#out.write("(");
    this.#expression(constant.expression);
    this.#out.write(")");
    this.#module = saved.module;
    this.#scopes = saved.scopes;
  }

  #call(node: AnyNode): void {
    const out = this.#out;
    const callee = unwrap(node.callee as Span);
    const args = node.arguments as AnyNode[];
    const reference = this.#lens.references.get(callee);
    // `array(T, n)` is `new T[](n)`.
    if (this.#isBuiltin(callee, "array")) {
      const { element, length } = this.#arrayOf(node);
      out.write(`new ${typeName(element)}[](`);
      this.#expression(length);
      out.write(")");
      return;
    }
    for (const argument of args)
      if (is(argument, "SpreadElement")) this.#fail(argument, "a call cannot spread its arguments");

    if (reference?.kind === "declaration") {
      // A helper is called by its name, and an interface called on an address is the conversion to a handle.
      const { declaration } = reference;
      if (declaration.kind === "interface") {
        if (args.length !== 1) this.#fail(node, HANDLE);
      } else if (declaration.kind !== "helper")
        this.#fail(
          node,
          `"${declaration.name}" is ${declaration.kind === "immutable" ? "an" : "a"} ${declaration.kind} and cannot be called`,
        );
      else if (this.#inLibrary) this.#outsideLibrary(node, declaration.name);
      out.write(declaration.name);
      this.#arguments(args);
      return;
    }
    if (reference) {
      const { name } = reference;
      // A type name called is Solidity's conversion; require, revert, gasleft and keccak256 are spelled the same.
      if (reference.module === "solidity" && (isElementaryTypeName(name) || SAME_CALL.has(name))) {
        out.write(name);
        this.#arguments(args);
        return;
      }
      if (this.#isBuiltin(callee, "staticcall")) {
        const [target, data, options] = args;
        if (!target || !data || args.length > 3)
          this.#fail(node, "staticcall takes the target, the calldata and, optionally, { gas: … }");
        this.#expression(target, true);
        out.write(".staticcall");
        if (options) this.#options(options);
        out.write("(");
        this.#expression(data);
        out.write(")");
        return;
      }
      if (this.#isBuiltin(callee, "codeSize")) {
        const [account] = args;
        if (!account || args.length !== 1) this.#fail(node, "codeSize takes one account");
        this.#expression(account, true);
        out.write(".code.length");
        return;
      }
      if (this.#isAssembly(node))
        this.#fail(node, "an assembly block is a statement, or the initializer of a local as in const x = assembly(…)");
      if (reference.module === "yul")
        this.#fail(node, `${name}(…) is an opcode, and an opcode is called inside an assembly block`);
      this.#fail(node, `a lens body cannot call ${name}`);
    }

    if (!is(callee, "MemberExpression") || callee.computed)
      this.#fail(
        node,
        "a lens body can call helpers, interface functions, library functions and the package's built-ins",
      );
    const object = this.#strip(callee.object);
    const { name } = callee.property;
    if (is(object, "ThisExpression")) {
      out.write(`this.${name}`);
      this.#arguments(args);
      return;
    }
    const named = this.#lens.references.get(object);
    if (named?.kind === "declaration" && named.declaration.kind === "library") {
      out.write(`${named.declaration.name}.${name}`);
      this.#arguments(args);
      return;
    }
    if (named?.kind === "builtin") {
      if (!this.#isBuiltin(object, "abi") || (name !== "encode" && name !== "decode"))
        this.#fail(node, `a lens cannot call ${named.name}.${name}`);
      if (name === "encode") {
        out.write("abi.encode");
        this.#arguments(args);
        return;
      }
      // `abi.decode(data, [uint128, uint128])` is `abi.decode(data, (uint128, uint128))`.
      const [data, list] = args;
      const types = list && unwrap(list);
      if (!data || !is(types, "ArrayExpression") || args.length !== 2)
        this.#fail(
          node,
          "abi.decode takes the data and the types to read, listed in place: abi.decode(data, [uint256])",
        );
      out.write("abi.decode(");
      this.#expression(data);
      out.write(", ");
      out.node({ module: this.#module, node: types }, () =>
        out.write(`(${this.#types(types).map(typeName).join(", ")})`),
      );
      out.write(")");
      return;
    }
    const fn = this.#interfaceFunction(object);
    if (fn) {
      // `I.f.encode(a, b)` is `abi.encodeCall(I.f, (a, b))`.
      if (name !== "encode") this.#fail(node, INTERFACE_MEMBER);
      out.write(`abi.encodeCall(${fn.interface.name}.${fn.name}, `);
      this.#arguments(args);
      out.write(")");
      return;
    }
    // A call through a handle. Gas given where the handle is made is printed where Solidity takes it, after
    // the function's name: `I(a, { gas: g }).f(x)` is `I(a).f{gas: g}(x)`.
    const made = is(object, "CallExpression") ? this.#lens.references.get(unwrap(object.callee)) : undefined;
    const [address, options, ...rest] = is(object, "CallExpression") ? (object.arguments as AnyNode[]) : [];
    if (made?.kind === "declaration" && made.declaration.kind === "interface" && address && options && !rest.length) {
      out.node({ module: this.#module, node: object }, () => {
        out.write(`${made.declaration.name}(`);
        this.#expression(address);
        out.write(")");
      });
      out.write(`.${name}`);
      this.#options(options);
    } else {
      this.#expression(object, true);
      out.write(`.${name}`);
    }
    this.#arguments(args);
  }

  #arguments(args: readonly AnyNode[]): void {
    const out = this.#out;
    out.write("(");
    for (const [index, argument] of args.entries()) {
      if (index) out.write(", ");
      this.#expression(argument);
    }
    out.write(")");
  }

  /** `{ gas: g }`, the one option a call has, printed between the function and its arguments: `{gas: g}`. */
  #options(raw: Span): void {
    const out = this.#out;
    const node = unwrap(raw);
    const [property, ...rest] = is(node, "ObjectExpression") ? node.properties : [];
    if (
      !is(property, "Property") ||
      rest.length ||
      property.computed ||
      property.method ||
      property.kind !== "init" ||
      keyName(property.key) !== "gas"
    )
      this.#fail(node, "the options of a call are written { gas: … }");
    out.node({ module: this.#module, node: property }, () => {
      out.write("{gas: ");
      this.#expression(property.value);
      out.write("}");
    });
  }

  /** A library is printed outside the contract, where the contract's helpers and immutables do not exist. */
  #outsideLibrary(node: Span, name: string): never {
    return this.#fail(
      node,
      `a library function cannot use "${name}": a helper declared with internal(…) and an immutable belong to the contract. A library can use its own functions and other libraries`,
    );
  }

  /** Respells a literal. This is spelling, not arithmetic: no value is computed. */
  #literal(node: AnyNode): string {
    const { value } = node;
    if (typeof value === "boolean") return String(value);
    if (typeof value === "bigint" || typeof value === "number") {
      const text = String(node.raw ?? "")
        .replace(/_/g, "")
        .replace(/n$/, "");
      if (/^(?:0|[1-9][0-9]*)$/.test(text)) return text;
      if (/^0[xX][0-9a-fA-F]+$/.test(text)) return `0x${text.slice(2)}`;
      return this.#fail(node, "an integer literal is written in decimal or in hexadecimal");
    }
    if (typeof value === "string") {
      if (ADDRESS_LITERAL.test(value)) return value;
      if (HEX_LITERAL.test(value)) return `hex"${value.slice(2)}"`;
      return this.#string(node, value);
    }
    return this.#fail(node, "a lens has no such literal");
  }

  /**
   * A string literal. Printable ASCII is written as it is. A quote, a backslash, a control character
   * and a Unicode line break are escaped, because Solidity's scanner takes none of them raw, and a
   * literal that holds anything outside ASCII is a `unicode"…"` literal.
   */
  #string(node: AnyNode, value: string): string {
    let text = "";
    let unicode = false;
    for (const character of value) {
      const code = character.codePointAt(0) as number;
      if (code >= 0xd800 && code <= 0xdfff)
        this.#fail(node, "a string literal is well-formed Unicode: this one holds half of a surrogate pair");
      if (character === '"' || character === "\\") text += `\\${character}`;
      else if (code < 0x20 || code === 0x7f) text += `\\x${code.toString(16).padStart(2, "0")}`;
      else if (code === 0x85 || code === 0x2028 || code === 0x2029) text += `\\u${code.toString(16).padStart(4, "0")}`;
      else text += character;
      unicode ||= code > 0x7f;
    }
    return `${unicode ? "unicode" : ""}"${text}"`;
  }

  // ---- assembly -----------------------------------------------------------------------------------------------------
  #isAssembly(node: AnyNode): boolean {
    return is(node, "CallExpression") && this.#isBuiltin(unwrap(node.callee), "assembly");
  }

  /**
   * `assembly("memory-safe", () => { … })`: an assembly block, printed by the Yul printer. A block that
   * ends in `return word` is the initializer of the local `local`: the local is declared first, as a
   * uint256 or as the type named first, and the block assigns it.
   */
  #assembly(call: AnyNode, local?: string): void {
    const out = this.#out;
    const parts = (call.arguments as Span[]).map(unwrap);
    const named = parts.length === 3 ? parts[0] : undefined;
    const [flag, block] = parts.slice(named ? 1 : 0);
    if (
      parts.length < 2 ||
      parts.length > 3 ||
      !is(flag, "Literal") ||
      flag.value !== "memory-safe" ||
      (!is(block, "ArrowFunctionExpression") && !is(block, "FunctionExpression"))
    )
      this.#fail(
        call,
        'an assembly block is written assembly("memory-safe", () => { … }), or assembly(T, "memory-safe", () => { … }) to name the type of its value',
      );
    if (named && local === undefined)
      this.#fail(named, "a type is named only for an assembly block that is the initializer of a local");
    if (local !== undefined) {
      const reference = named && this.#lens.references.get(named);
      const type: TypeRef = !named
        ? UINT256
        : reference?.kind === "builtin" && reference.module === "solidity" && isElementaryTypeName(reference.name)
          ? { kind: "elementary", name: reference.name }
          : this.#fail(named, "the type of an assembly block's value is an elementary type, such as bytes32 or string");
      out.write(`${located(type, "memory")} ${local};`);
      this.#declare(local, type);
      out.line();
    }
    out.node({ module: this.#module, node: call }, () => {
      out.write('assembly ("memory-safe") ');
      printYul({
        block,
        module: this.#module,
        references: this.#lens.references,
        result: local,
        locals: new Set(this.#scopes.flatMap((scope) => [...scope.keys()])),
        writer: out,
      });
    });
  }

  // ---- scopes -------------------------------------------------------------------------------------------------------
  #declare(name: string, type: TypeRef): void {
    this.#scopes.at(-1)?.set(name, type);
  }

  #local(name: string): TypeRef | undefined {
    for (let i = this.#scopes.length - 1; i >= 0; i--) {
      const type = this.#scopes[i]?.get(name);
      if (type) return type;
    }
    return undefined;
  }

  #fail(node: Span, message: string): never {
    return fail(this.#module, node, message, this.#lens.name);
  }
}

export function printSolidity(lens: ResolvedLens): PrintedLens {
  return new Printer(lens).print();
}
