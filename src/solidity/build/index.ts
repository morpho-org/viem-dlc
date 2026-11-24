/**
 * `@morpho-org/viem-dlc/solidity/build`: the build half's core, for adapters and tests.
 *
 * An application does not import this: it adds one adapter per tool (`./solidity/vite`,
 * `./solidity/next`, `./solidity/register`, or the `viem-dlc-solidity` command). This entry and the
 * adapters are the only code that imports solc and the TypeScript parser, which are optional peer
 * dependencies.
 */
export { compile, runSolc, SolcUnavailableError } from "./compile.js";
export { CONFIG_FILE, ConfigError, findConfig, readConfig } from "./config.js";
export { formatDiagnostic, LensError } from "./errors.js";
export { find } from "./find.js";
export { parseModule } from "./module.js";
export { lookupPosition } from "./position-map.js";
export { printSolidity, SOLIDITY_HEADER } from "./print-solidity.js";
export { printYul } from "./print-yul.js";
export { resolve } from "./resolve.js";
export { rewrite } from "./rewrite.js";
export { mapSolcDiagnostics } from "./solc-errors.js";
export { build, buildSync, transform, transformSync } from "./transform.js";
export type * from "./types.js";
