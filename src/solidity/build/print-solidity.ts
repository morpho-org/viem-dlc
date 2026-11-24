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
 * This version prints what the first vertical slice needs. A construct the design defines and this file
 * does not print yet stops the build through `notImplemented`; a construct no lens may contain stops it
 * through `fail`.
 */
import type { Span } from "oxc-parser";

import { type AnyNode, is, keyName, unwrap } from "./ast.js";
import { isElementaryTypeName } from "./builtins.js";
import { fail, notImplemented } from "./errors.js";
import { Writer } from "./position-map.js";
import type {
  ConstantDeclaration,
  PrintedLens,
  Reference,
  ResolvedFunction,
  ResolvedLens,
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

class Printer {
  readonly #lens: ResolvedLens;
  readonly #out = new Writer();
  /** The module of the body being printed, for positions. */
  #module: SourceModule;
  /** The locals in scope, innermost last, each with the type it was declared with. */
  #scopes: Map<string, TypeRef>[] = [];

  constructor(lens: ResolvedLens) {
    this.#lens = lens;
    this.#module = lens.module;
  }

  print(): PrintedLens {
    const out = this.#out;
    const { declarations } = this.#lens;
    out.write(SOLIDITY_HEADER);
    for (const declaration of declarations) {
      if (declaration.kind === "struct") this.#struct(declaration);
      else if (declaration.kind === "interface")
        notImplemented(declaration.module, declaration.node, "an interface declaration");
      else if (declaration.kind === "library")
        notImplemented(declaration.module, declaration.node, "a library declaration");
      else if (declaration.kind === "immutable") notImplemented(declaration.module, declaration.node, "an immutable");
    }
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
      out.write(`) ${visibility} ${signature.mutability}`);
      if (signature.outputs.length)
        out.write(` returns (${signature.outputs.map((type) => located(type, "memory")).join(", ")})`);
      out.write(" {");
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
        case "ExpressionStatement":
          this.#effect(node.expression as Span);
          out.write(";");
          return;
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
          notImplemented(this.#module, node, "try/catch");
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
        } else if (init.declarations.length === 1) this.#locals(init);
        else this.#fail(init, "a for loop declares one local");
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
      const object = this.#declaredType(this.#strip(node.object));
      if (node.computed) return object?.kind === "array" ? object.element : undefined;
      if (object?.kind === "array") return node.property.name === "length" ? UINT256 : undefined;
      if (object?.kind !== "struct") return undefined;
      return object.declaration.fields.find((field) => field.name === node.property.name)?.type;
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
   * The return types a call declares in the source, or undefined when it declares none there. A
   * conversion returns the type it names, and `array(T, n)` an array of `T`.
   */
  #outputs(node: AnyNode): readonly TypeRef[] | undefined {
    const callee = this.#lens.references.get(unwrap(node.callee as Span));
    if (callee?.kind === "declaration")
      return callee.declaration.kind === "helper" ? callee.declaration.signature.outputs : undefined;
    if (callee?.module !== "solidity") return undefined;
    if (isElementaryTypeName(callee.name)) return [{ kind: "elementary", name: callee.name }];
    if (callee.name === "array") return [{ kind: "array", element: this.#arrayOf(node).element }];
    return undefined;
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
        if (is(node.object as AnyNode, "ThisExpression")) notImplemented(this.#module, node, "this");
        const property = (node.property as { name: string }).name;
        const object = this.#lens.references.get(unwrap(node.object as Span));
        if (object?.kind === "builtin") notImplemented(this.#module, node, `${object.name}.${property}`);
        this.#expression(node.object as Span, true);
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
        notImplemented(this.#module, node, "this");
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
    if (reference.kind === "builtin") notImplemented(this.#module, node, `${reference.name} as a value`);
    const { declaration } = reference;
    if (declaration.kind === "constant") this.#constant(declaration);
    else if (declaration.kind === "immutable") this.#out.write(declaration.name);
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
    const reference = this.#lens.references.get(callee);
    if (reference?.kind === "builtin" && reference.module === "solidity" && reference.name === "array") {
      const { element, length } = this.#arrayOf(node);
      out.write(`new ${typeName(element)}[](`);
      this.#expression(length);
      out.write(")");
      return;
    }
    // A helper is called by its name. A type name called is Solidity's conversion, and require and
    // revert are spelled the same.
    const name =
      reference?.kind === "declaration"
        ? reference.declaration.kind === "helper"
          ? reference.declaration.name
          : notImplemented(this.#module, node, `calling a ${reference.declaration.kind}`)
        : reference?.module === "solidity" &&
            (isElementaryTypeName(reference.name) || reference.name === "require" || reference.name === "revert")
          ? reference.name
          : reference
            ? notImplemented(this.#module, node, `${reference.name}(…)`)
            : is(callee, "MemberExpression")
              ? notImplemented(this.#module, node, "a call through an interface, a library or this")
              : this.#fail(
                  node,
                  "a lens body can call helpers, interface functions, library functions and the package's built-ins",
                );
    out.write(`${name}(`);
    for (const [index, argument] of (node.arguments as AnyNode[]).entries()) {
      if (is(argument, "SpreadElement")) this.#fail(argument, "a call cannot spread its arguments");
      if (index) out.write(", ");
      this.#expression(argument);
    }
    out.write(")");
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
