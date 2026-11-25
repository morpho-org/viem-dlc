/**
 * The one configuration file of a repository: `solidity.config.json`.
 *
 * ```json
 * { "solc": { "version": "0.8.36", "settings": { "optimizer": { "enabled": true, "runs": 200 } } } }
 * ```
 *
 * Every lens under the file's directory compiles with this version and these settings, in a build and
 * in a test alike. The file is read each time it is needed, never cached in memory, so an edit to it
 * reaches the next compile.
 */
import fs from "node:fs";
import path from "node:path";

import type { SolidityConfig } from "./types.js";

export const CONFIG_FILE = "solidity.config.json";

/** Thrown when the configuration file is missing or malformed. `transform` reports it at the lens. */
export class ConfigError extends Error {
  override readonly name = "ConfigError";
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** Reads the configuration file at `file`. */
export function readConfig(file: string): SolidityConfig {
  let parsed: unknown;
  try {
    parsed = JSON.parse(fs.readFileSync(file, "utf8"));
  } catch (cause) {
    throw new ConfigError(`cannot read ${file}: ${(cause as Error).message}`);
  }
  const solc = isRecord(parsed) ? parsed.solc : undefined;
  if (!isRecord(solc) || typeof solc.version !== "string" || !/^\d+\.\d+\.\d+$/.test(solc.version))
    throw new ConfigError(`${file} must pin an exact compiler: { "solc": { "version": "0.8.36" } }`);
  if (solc.settings !== undefined && !isRecord(solc.settings))
    throw new ConfigError(`${file}: "solc.settings" must be an object of solc's standard-JSON settings`);
  const root = path.dirname(file);
  return {
    path: file,
    root,
    cacheDir: path.join(root, "node_modules", ".cache", "viem-dlc-solidity"),
    solc: { version: solc.version, settings: solc.settings ?? {} },
  };
}

/** The configuration file that applies to the lens file `file`: the nearest `solidity.config.json` above it. */
export function findConfigFile(file: string): string {
  for (let dir = path.dirname(path.resolve(file)); ; dir = path.dirname(dir)) {
    const candidate = path.join(dir, CONFIG_FILE);
    if (fs.existsSync(candidate)) return candidate;
    if (dir === path.dirname(dir))
      throw new ConfigError(
        `no ${CONFIG_FILE} found above ${file}. Add one at the repository root to pin the compiler: { "solc": { "version": "0.8.36" } }`,
      );
  }
}

/** The configuration that applies to the lens file `file`. */
export function findConfig(file: string): SolidityConfig {
  return readConfig(findConfigFile(file));
}
