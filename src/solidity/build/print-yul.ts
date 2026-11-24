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
 * A constant named inside a block must be a single literal, because Yul has no operators.
 */
import { notImplemented } from "./errors.js";
import type { YulBlockInput } from "./types.js";

/** Writes the block, braces included, to `input.writer`. Not implemented yet. */
export function printYul(input: YulBlockInput): void {
  notImplemented(input.module, input.block, "an assembly block (typed Yul)");
}
