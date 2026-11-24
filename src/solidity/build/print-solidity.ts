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
 *   else is `memory`.
 * - **The spelling of a literal.** `0n` is `0`, `60_000` is `60000`, `'text'` is `"text"`.
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

import { type AnyNode, is, unwrap } from "./ast.js";
import { isElementaryTypeName } from "./builtins.js";
import { fail, notImplemented } from "./errors.js";
import { Writer } from "./position-map.js";
import { printYul } from "./print-yul.js";
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

const UINT256: TypeRef = { kind: "elementary", name: "uint256" };
const BOOL: TypeRef = { kind: "elementary", name: "bool" };
const STRING: TypeRef = { kind: "elementary", name: "string" };

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
    if (body.params.length !== signature.inputs.length)
      this.#fail(
        body,
        `"${fn.name}" declares ${signature.inputs.length} parameters and its implementation takes ${body.params.length}`,
      );
    out.node({ module: fn.module, node: body }, () => {
      out.write(`function ${fn.name}(`);
      for (const [index, parameter] of body.params.entries()) {
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
          if (signature.outputs.length) out.write("return ");
          this.#expression(block);
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
    if (is(node, "EmptyStatement")) return;
    out.line();
    out.node({ module: this.#module, node }, () => {
      switch (node.type) {
        case "VariableDeclaration":
          this.#locals(node);
          return;
        case "ExpressionStatement": {
          const expression = unwrap(node.expression as Span);
          if (this.#isAssembly(expression)) {
            this.#assembly(expression);
            return;
          }
          if (!["AssignmentExpression", "UpdateExpression", "CallExpression"].includes(expression.type))
            this.#fail(expression, "this expression is not a statement: a statement assigns or calls");
          this.#expression(expression);
          out.write(";");
          return;
        }
        case "ReturnStatement":
          if (!node.argument) {
            out.write("return;");
            return;
          }
          if (is(unwrap(node.argument as Span), "ArrayExpression"))
            notImplemented(this.#module, node, "returning several values");
          out.write("return ");
          this.#expression(node.argument as AnyNode);
          out.write(";");
          return;
        case "IfStatement":
          this.#if(node);
          return;
        case "BlockStatement":
          this.#block(node);
          return;
        case "WhileStatement":
        case "DoWhileStatement":
        case "ForStatement":
          notImplemented(this.#module, node, "a loop");
          return;
        case "BreakStatement":
        case "ContinueStatement":
          notImplemented(this.#module, node, "break and continue");
          return;
        case "TryStatement":
          notImplemented(this.#module, node, "try/catch");
          return;
        default:
          this.#fail(node, `a lens body cannot contain this statement (${node.type})`);
      }
    });
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

  /** The body of an `if` or `else`: a block where one is written, and a single statement where it is not. */
  #nested(node: AnyNode): void {
    if (is(node, "BlockStatement")) {
      this.#out.write(" ");
      this.#block(node);
    } else {
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
      if (is(id, "ArrayPattern")) notImplemented(this.#module, declarator, "declaring several locals from one call");
      if (!is(id, "Identifier")) this.#fail(id, "a local must be a plain name");
      if (!declarator.init)
        this.#fail(declarator, "a local needs an initializer; a local with none is written with its zero value");
      const init = unwrap(declarator.init as Span);
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

  // ---- the type of a local ------------------------------------------------------------------------------------
  /**
   * The type an expression already has in the source, or a located error when it has none. This is the
   * whole local-type rule, and it is a lookup: Solidity's operator typing is not implemented, so the type
   * of arithmetic is asked of the author as a conversion.
   */
  typeOf(raw: Span): TypeRef {
    const node = unwrap(raw);
    const known = this.#declaredType(node);
    if (known) return known;
    return this.#fail(
      node,
      "this local's type cannot be copied from its initializer: name the type with a conversion, for example uint256(…)",
    );
  }

  #declaredType(node: AnyNode): TypeRef | undefined {
    if (is(node, "Literal")) {
      if (typeof node.value === "bigint" || typeof node.value === "number") return UINT256;
      if (typeof node.value === "boolean") return BOOL;
      if (typeof node.value === "string") return STRING;
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
      if (node.computed) return undefined;
      const object = this.#declaredType(unwrap(node.object));
      if (object?.kind !== "struct") return undefined;
      return object.declaration.fields.find((field) => field.name === node.property.name)?.type;
    }
    if (is(node, "CallExpression")) {
      const callee = this.#lens.references.get(unwrap(node.callee));
      if (callee?.kind === "builtin" && callee.module === "solidity" && isElementaryTypeName(callee.name))
        return { kind: "elementary", name: callee.name };
      if (callee?.kind === "declaration" && callee.declaration.kind === "helper") {
        const [output, ...rest] = callee.declaration.signature.outputs;
        return rest.length ? undefined : output;
      }
      return undefined;
    }
    if (is(node, "NewExpression")) {
      const callee = this.#lens.references.get(unwrap(node.callee));
      return callee?.kind === "declaration" && callee.declaration.kind === "struct"
        ? { kind: "struct", declaration: callee.declaration }
        : undefined;
    }
    if (is(node, "BinaryExpression")) return COMPARISON.has(node.operator) ? BOOL : undefined;
    if (is(node, "LogicalExpression")) return LOGICAL.has(node.operator) ? BOOL : undefined;
    if (is(node, "UnaryExpression")) return node.operator === "!" ? BOOL : undefined;
    return undefined;
  }

  // ---- expressions ------------------------------------------------------------------------------------------------
  /** Prints an expression. As an operand of an operator, an operator is parenthesized. */
  #expression(raw: Span, operand = false): void {
    const node = unwrap(raw);
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
        if (node.computed) notImplemented(this.#module, node, "indexing");
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
        if (is(target, "ArrayPattern")) notImplemented(this.#module, node, "assigning several values from one call");
        this.#expression(target, true);
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
        if ((node.arguments as unknown[]).length)
          notImplemented(this.#module, node, "building a struct from its fields");
        this.#fail(node, "new T() is the zero value of a local: write it as the initializer of a const or let");
        return;
      case "ThisExpression":
        notImplemented(this.#module, node, "this");
        return;
      default:
        this.#fail(node, `a lens body cannot contain this expression (${node.type})`);
    }
  }

  /** A name that resolved to a module-level declaration or a package export, used as a value. */
  #named(node: AnyNode, reference: Reference): void {
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
    if (this.#isAssembly(node))
      this.#fail(node, "an assembly block is a statement, or the initializer of a local as in const x = assembly(…)");
    if (reference?.kind === "builtin" && reference.module === "yul")
      this.#fail(node, `${reference.name}(…) is an opcode, and an opcode is called inside an assembly block`);
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
      if (/^0x[0-9a-fA-F]{40}$/.test(value)) return value;
      if (/^0x(?:[0-9a-fA-F]{2})*$/.test(value)) return `hex"${value.slice(2)}"`;
      if (/^[\x20-\x7e]*$/.test(value)) return JSON.stringify(value);
      return notImplemented(this.#module, node, "a string literal outside printable ASCII");
    }
    return this.#fail(node, "a lens has no such literal");
  }

  // ---- assembly -----------------------------------------------------------------------------------------------------
  #isAssembly(node: AnyNode): boolean {
    if (!is(node, "CallExpression")) return false;
    const callee = this.#lens.references.get(unwrap(node.callee));
    return callee?.kind === "builtin" && callee.module === "solidity" && callee.name === "assembly";
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
