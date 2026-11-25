/**
 * Print (Yul): one `assembly(…)` block to a Yul block.
 *
 * Opcodes are functions imported from the package's `yul` entry and every value is one 256-bit word.
 * The meaning is Yul's, wrapping and right-to-left argument evaluation included, so a handwritten
 * block is ported as a copy. The printer adds no checks of its own: solc accepts or rejects the block
 * as it would in a `.sol` file.
 *
 * | TypeScript inside the block | Yul |
 * | --- | --- |
 * | `const x = e`, `let x = e` | `let x := e` |
 * | `x = e` | `x := e`, for a Yul local or a Solidity local declared outside the block |
 * | `add(a, b)`, `mload(p)`, `staticcall(…)` | the same opcode call |
 * | `a < b`, `a > b`, `a <= b`, `a >= b`, `a == b`, `a != b` | `lt`, `gt`, `eq` and `iszero` of them |
 * | `if (c) { A }`; `if (c) { A } else { B }` | `if c { A }`; `switch c case 0 { B } default { A }` |
 * | `while (c) { … }`, `for (let i = 0n; lt(i, n); i = add(i, 1)) { … }` | `for {} c {} { … }`, `for { let i := 0 } lt(i, n) { i := add(i, 1) } { … }` |
 * | `break`, `continue` | the same |
 * | `const min = (x, y) => xor(x, mul(xor(x, y), lt(y, x)))` | `function min(x, y) -> r { r := … }` |
 * | `return word` as the block's last statement | `<result> := word`, where `result` is {@link YulBlockInput.result} |
 *
 * A function written with a block body follows the block's own rule: a final `return word` is
 * `r := word`, and a body with none is a function that returns nothing. A callback or a function
 * whose body is an expression returns that expression. `r` is the one name the printer chooses, so a
 * function that returns a word is refused where a Solidity local is already named `r`.
 *
 * A literal is an integer, respelled as in Solidity (`0n` is `0`, `0x1fn` is `0x1f`). A constant named
 * inside a block must be a single such literal, because Yul has no operators. Anything that is not in
 * the table is a located error.
 */
import type { Span } from "oxc-parser";

import { type AnyNode, is, unwrap, walk } from "./ast.js";
import { YUL_SPECIFIER } from "./builtins.js";
import { fail } from "./errors.js";
import { refusedName } from "./names.js";
import type { Reference, YulBlockInput } from "./types.js";

/**
 * The opcodes the package's `yul` entry exports, and so the only ones a block can call. None of them
 * writes state, calls with the right to write it, creates a contract or logs.
 */
export const YUL_OPCODES: ReadonlySet<string> = new Set([
  "add",
  "sub",
  "mul",
  "div",
  "sdiv",
  "mod",
  "smod",
  "exp",
  "signextend",
  "addmod",
  "mulmod",
  "lt",
  "gt",
  "slt",
  "sgt",
  "eq",
  "iszero",
  "and",
  "or",
  "xor",
  "not",
  "byte",
  "shl",
  "shr",
  "sar",
  "keccak256",
  "mload",
  "mstore",
  "mstore8",
  "mcopy",
  "calldataload",
  "calldatasize",
  "calldatacopy",
  "returndatasize",
  "returndatacopy",
  "staticcall",
  "extcodesize",
  "balance",
  "selfbalance",
  "address",
  "caller",
  "timestamp",
  "number",
  "chainid",
  "gas",
  "pop",
]);

/** A comparison is the opcode of the same arguments in the same order, inside `iszero` when negated. */
const COMPARISONS: Readonly<Record<string, readonly [opcode: string, negated: boolean]>> = {
  "<": ["lt", false],
  ">": ["gt", false],
  "<=": ["gt", true],
  ">=": ["lt", true],
  "==": ["eq", false],
  "===": ["eq", false],
  "!=": ["eq", true],
  "!==": ["eq", true],
};

/** What a function inside a block names its return value, as Yul must name it. */
const FUNCTION_RESULT = "r";

/** Respells an integer literal. This is spelling, not arithmetic. Undefined for any other node. */
const integer = (node: AnyNode): string | undefined => {
  if (!is(node, "Literal") || (typeof node.value !== "bigint" && typeof node.value !== "number")) return undefined;
  const text = String(node.raw).replace(/_/g, "").replace(/n$/, "");
  if (/^(?:0|[1-9][0-9]*)$/.test(text)) return text;
  if (/^0[xX][0-9a-fA-F]+$/.test(text)) return `0x${text.slice(2)}`;
  return undefined;
};

/** Writes the block, braces included, to `input.writer`. */
export function printYul(input: YulBlockInput): void {
  const { module, references, writer: out } = input;
  function refuse(node: Span, message: string): never {
    return fail(module, node, message);
  }

  // A name in the block is printed as it is written. What Yul itself reserves, solc refuses.
  walk(input.block as unknown as AnyNode, (node) => {
    const refused = is(node, "Identifier") && !references.has(node) ? refusedName(node.name, "yul") : undefined;
    if (refused) refuse(node, refused);
  });

  const braces = (print: () => void): void => {
    out.write("{");
    out.indented(print);
    out.line();
    out.write("}");
  };

  // ---- expressions --------------------------------------------------------------------------------------------
  /** A module-level name used as a value: only a constant that is a single integer literal has a Yul form. */
  const named = (node: AnyNode, reference: Reference): string => {
    if (reference.kind === "declaration" && reference.declaration.kind === "constant")
      return (
        integer(unwrap(reference.declaration.expression)) ??
        refuse(
          node,
          `"${reference.declaration.name}" is not a single integer literal. A constant named inside an assembly block must be one, because Yul has no operators`,
        )
      );
    const name = reference.kind === "builtin" ? reference.name : reference.declaration.name;
    return refuse(
      node,
      `"${name}" is not a value inside an assembly block. A value there is a local, an integer literal, or a module constant that is a single integer literal`,
    );
  };

  const call = (node: AnyNode): void => {
    const callee = unwrap(node.callee as Span);
    const reference = references.get(callee);
    const uncallable = (): never =>
      refuse(
        callee,
        `inside an assembly block a call is to an opcode imported from ${YUL_SPECIFIER}, or to a function declared in the block`,
      );
    const name = reference
      ? reference.kind !== "builtin" || reference.module !== "yul"
        ? uncallable()
        : YUL_OPCODES.has(reference.name)
          ? reference.name
          : refuse(callee, `"${reference.name}" is not an opcode ${YUL_SPECIFIER} exports`)
      : is(callee, "Identifier")
        ? callee.name
        : uncallable();
    out.write(`${name}(`);
    for (const [index, argument] of (node.arguments as AnyNode[]).entries()) {
      if (is(argument, "SpreadElement")) refuse(argument, "a call cannot spread its arguments");
      if (index) out.write(", ");
      expression(argument);
    }
    out.write(")");
  };

  const expression = (raw: Span): void => {
    const node = unwrap(raw);
    out.node({ module, node }, () => {
      const reference = references.get(node);
      if (reference) {
        out.write(named(node, reference));
        return;
      }
      switch (node.type) {
        case "Literal":
          out.write(
            integer(node) ??
              refuse(node, "inside an assembly block a literal is an integer, written in decimal or in hexadecimal"),
          );
          return;
        case "Identifier":
          out.write(node.name as string);
          return;
        case "CallExpression":
          call(node);
          return;
        case "BinaryExpression": {
          const comparison = COMPARISONS[node.operator as string];
          if (!comparison) break;
          const [opcode, negated] = comparison;
          out.write(negated ? `iszero(${opcode}(` : `${opcode}(`);
          expression(node.left as Span);
          out.write(", ");
          expression(node.right as Span);
          out.write(negated ? "))" : ")");
          return;
        }
        case "UnaryExpression":
        case "LogicalExpression":
        case "UpdateExpression":
          break;
        default:
          refuse(node, `an assembly block cannot contain this expression (${node.type})`);
      }
      refuse(node, `Yul has no operator ${node.operator}: call the opcode, as in add(a, b)`);
    });
  };

  // ---- statements ---------------------------------------------------------------------------------------------
  /** `x = e` and a call, the two expressions that are statements. */
  const effect = (raw: Span): void => {
    const node = unwrap(raw);
    out.node({ module, node }, () => {
      if (is(node, "CallExpression")) {
        call(node);
        return;
      }
      if (is(node, "UpdateExpression"))
        refuse(node, `Yul has no operator ${node.operator}: call the opcode, as in x = add(x, 1)`);
      if (!is(node, "AssignmentExpression"))
        refuse(node, "this expression is not a statement: inside an assembly block a statement assigns or calls");
      if (node.operator !== "=")
        refuse(node, `Yul has no operator ${node.operator}: call the opcode, as in x = add(x, y)`);
      const target = unwrap(node.left);
      if (!is(target, "Identifier") || references.has(target))
        refuse(target, "inside an assembly block only a local can be assigned");
      out.write(`${target.name} := `);
      expression(node.right);
    });
  };

  /** `const x = e` and `let x = e`; an arrow function declares a Yul function. */
  const declaration = (node: AnyNode): void => {
    if (node.kind !== "const" && node.kind !== "let") refuse(node, "a Yul local is declared with const or let");
    for (const [index, declarator] of (node.declarations as AnyNode[]).entries()) {
      if (index) out.write(" ");
      const id = declarator.id as AnyNode;
      if (!is(id, "Identifier")) refuse(id, "a Yul local must be a plain name");
      if (!declarator.init) refuse(declarator, "a Yul local needs an initializer");
      const init = unwrap(declarator.init as Span);
      if (is(init, "ArrowFunctionExpression")) {
        out.node({ module, node: init }, () => fn(id.name, init));
        continue;
      }
      out.write(`let ${id.name} := `);
      expression(init);
    }
  };

  /** A declaration or an expression statement, on the current line: a statement, or the head of a `for`. */
  const simple = (node: AnyNode): void => {
    if (is(node, "VariableDeclaration")) declaration(node);
    else effect(is(node, "ExpressionStatement") ? node.expression : node);
  };

  const statement = (node: AnyNode): void => {
    if (is(node, "EmptyStatement")) return;
    out.line();
    out.node({ module, node }, () => {
      switch (node.type) {
        case "VariableDeclaration":
        case "ExpressionStatement":
          simple(node);
          return;
        case "IfStatement":
          if (!node.alternate) {
            out.write("if ");
            expression(node.test as Span);
            out.write(" ");
            nested(node.consequent as AnyNode);
            return;
          }
          out.write("switch ");
          expression(node.test as Span);
          out.line();
          out.write("case 0 ");
          nested(node.alternate as AnyNode);
          out.line();
          out.write("default ");
          nested(node.consequent as AnyNode);
          return;
        case "WhileStatement":
          out.write("for {} ");
          expression(node.test as Span);
          out.write(" {} ");
          nested(node.body as AnyNode);
          return;
        case "ForStatement":
          if (!node.test) refuse(node, "a for loop inside an assembly block needs a condition");
          out.write("for {");
          if (node.init) {
            out.write(" ");
            simple(node.init as AnyNode);
            out.write(" ");
          }
          out.write("} ");
          expression(node.test as Span);
          out.write(" {");
          if (node.update) {
            out.write(" ");
            effect(node.update as Span);
            out.write(" ");
          }
          out.write("} ");
          nested(node.body as AnyNode);
          return;
        case "BreakStatement":
        case "ContinueStatement":
          out.write(node.type === "BreakStatement" ? "break" : "continue");
          return;
        case "ReturnStatement":
          refuse(node, "return word is the last statement of an assembly block, or of a function inside it");
          return;
        default:
          refuse(node, `an assembly block cannot contain this statement (${node.type})`);
      }
    });
  };

  /** The body of an `if`, an `else` or a loop, which Yul always writes in braces. */
  const nested = (node: AnyNode): void =>
    braces(() => {
      for (const inner of is(node, "BlockStatement") ? (node.body as unknown as AnyNode[]) : [node]) statement(inner);
    });

  // ---- bodies that give a word ----------------------------------------------------------------------------------
  /** What a callback or a function returns: its expression body, or the `return word` that ends its block. */
  const returned = (body: AnyNode): { readonly at: Span; readonly word: Span } | undefined => {
    if (!is(body, "BlockStatement")) return { at: body, word: body };
    const last = body.body.at(-1);
    if (!is(last, "ReturnStatement")) return undefined;
    return { at: last, word: last.argument ?? refuse(last, "return is followed by the word it gives") };
  };

  /** The body of a callback or of a function. What it returns is assigned to `result`. */
  const body = (node: AnyNode, result: string | undefined): void =>
    braces(() => {
      const final = returned(node);
      if (is(node, "BlockStatement"))
        for (const inner of node.body as unknown as AnyNode[]) if (inner !== final?.at) statement(inner);
      if (!final) return;
      out.line();
      out.node({ module, node: final.at }, () => {
        out.write(`${result} := `);
        expression(final.word);
      });
    });

  /** `function name(x, y) -> r { … }`, from an arrow function. */
  const fn = (name: string, arrow: AnyNode): void => {
    if (arrow.async) refuse(arrow, "a function inside an assembly block cannot be async");
    const parameters = (arrow.params as AnyNode[]).map((parameter) =>
      is(parameter, "Identifier") ? parameter.name : refuse(parameter, "a parameter must be a plain name"),
    );
    const result = returned(arrow.body as AnyNode) ? FUNCTION_RESULT : undefined;
    // solc reads the name as the Solidity local and reports the function's own assignment to it.
    if (result && input.locals.has(result))
      refuse(
        arrow,
        `a function inside an assembly block names its value "${result}" in Yul, and "${result}" is a local here: rename the local`,
      );
    out.write(`function ${name}(${parameters.join(", ")})${result ? ` -> ${result}` : ""} `);
    body(arrow.body as AnyNode, result);
  };

  // ---- the block ------------------------------------------------------------------------------------------------
  const { block } = input;
  const [parameter] = block.params;
  if (parameter) refuse(parameter, "the callback of an assembly block takes no parameters");
  if (block.async || block.generator) refuse(block, "the callback of an assembly block cannot be async or a generator");
  const blockBody = block.body as AnyNode;
  const final = returned(blockBody);
  if (final && input.result === undefined)
    refuse(
      final.at,
      "this block returns a word, so it is a value: write it as the initializer of a local, as in const x = assembly(…)",
    );
  if (!final && input.result !== undefined) refuse(blockBody, "an assembly block used as a value ends in return word");
  body(blockBody, input.result);
}
