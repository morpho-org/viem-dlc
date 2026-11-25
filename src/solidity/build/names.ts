/**
 * Names: what the author wrote in TypeScript that is printed into Solidity as it is.
 *
 * Such a name has to be a name in Solidity too: an identifier, and not a word Solidity reserves.
 */

const IDENTIFIER = /^[A-Za-z_$][A-Za-z0-9_$]*$/;

/** The words solc refuses as an identifier, sized type names aside. `_` is refused as a name of its own. */
export const RESERVED_WORDS: ReadonlySet<string> = new Set(
  (
    "abstract address anonymous as assembly bool break bytes calldata catch constant constructor continue " +
    "contract delete do else emit enum event external fallback false for function hex if immutable import " +
    "indexed interface internal is library mapping memory modifier new override payable pragma private " +
    "public pure receive return returns storage string struct throw true try type unchecked unicode using " +
    "view virtual while wei gwei ether seconds minutes hours days weeks years int uint byte fixed ufixed " +
    "after alias apply auto case copyof default define final implements in inline let macro match mutable " +
    "null of partial promise reference relocatable sealed sizeof static supports switch typedef typeof var _"
  ).split(" "),
);

/** `uint8` to `uint256`, `int8` to `int256`, `bytes1` to `bytes32`, and the fixed-point names. */
const isSizedType = (name: string): boolean => {
  const integer = /^u?int([1-9][0-9]*)$/.exec(name);
  if (integer) return Number(integer[1]) % 8 === 0 && Number(integer[1]) <= 256;
  const fixedBytes = /^bytes([1-9][0-9]*)$/.exec(name);
  if (fixedBytes) return Number(fixedBytes[1]) <= 32;
  return /^u?fixed[0-9]+x[0-9]+$/.test(name);
};

/**
 * Why `name` cannot be printed as a Solidity name, or undefined when it can. A `member` is a struct's
 * field or an interface's function, which may share the name of a built-in. A `yul` name is one declared
 * inside an assembly block, where the reserved words are Yul's own and solc refuses them.
 */
export const refusedName = (name: string, kind: "scoped" | "member" | "yul" = "scoped"): string | undefined => {
  if (!IDENTIFIER.test(name))
    return `"${name}" is not a Solidity identifier: a name is made of letters, digits, _ and $, and does not start with a digit`;
  if (kind === "yul") return undefined;
  if (RESERVED_WORDS.has(name) || isSizedType(name))
    return `"${name}" is a reserved word in Solidity, so it cannot be used as a name`;
  return undefined;
};
