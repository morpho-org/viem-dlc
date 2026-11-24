/**
 * The names the package exports, as the build half needs to know them. The runtime entry is not
 * imported here: the two halves share names, not code.
 */
import type { ElementaryTypeName } from "./types.js";

/** The specifier a lens module imports the DSL from. A file whose text lacks it is never parsed. */
export const SOLIDITY_SPECIFIER = "@morpho-org/viem-dlc/solidity";
/** The specifier typed-Yul opcodes are imported from. */
export const YUL_SPECIFIER = "@morpho-org/viem-dlc/solidity/yul";

const ELEMENTARY =
  /^(?:u?int(?:8|16|24|32|40|48|56|64|72|80|88|96|104|112|120|128|136|144|152|160|168|176|184|192|200|208|216|224|232|240|248|256)|address|bool|bytes32|bytes4|bytes|string)$/;

export const isElementaryTypeName = (name: string): name is ElementaryTypeName => ELEMENTARY.test(name);

/** Exports that declare something at module level. */
export const DECLARATION_BUILTINS: ReadonlySet<string> = new Set([
  "Struct",
  "iface",
  "view",
  "pure",
  "internal",
  "immutable",
  "library",
  "contract",
]);

/** Exports a body can name, besides the elementary types. */
export const BODY_BUILTINS: ReadonlySet<string> = new Set([
  "keccak256",
  "abi",
  "block",
  "gasleft",
  "require",
  "revert",
  "array",
  "staticcall",
  "codeSize",
  "assembly",
]);

/** JavaScript globals a body can name. Both print as their argument. */
export const GLOBAL_BUILTINS: ReadonlySet<string> = new Set(["Number", "BigInt"]);
