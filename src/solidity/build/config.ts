/**
 * The one configuration file of a repository: `solidity.config.json`.
 *
 * ```json
 * { "solc": { "version": "0.8.36", "settings": { "optimizer": { "enabled": true, "runs": 200 } } } }
 * ```
 *
 * Every lens under the file's directory compiles with this version and these settings, in a build and
 * in a test alike. One setting is refused: an `evmVersion` older than Byzantium, which has no STATICCALL. The file is read each time it is needed, never cached in memory, so an edit to it
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

/** The EVM versions solc names that are older than Byzantium, the first with STATICCALL. */
const BEFORE_STATICCALL: ReadonlySet<unknown> = new Set(["homestead", "tangerineWhistle", "spuriousDragon"]);

/**
 * Throws unless the settings compile for an EVM that has STATICCALL. solc makes a call to a `view`
 * function a STATICCALL only where the target has one, and a CALL where it has none: below Byzantium,
 * nothing holds a lens to reading. `where` names the configuration, for the error.
 */
export function requireStaticCall(settings: Readonly<Record<string, unknown>>, where: string): void {
  if (BEFORE_STATICCALL.has(settings.evmVersion))
    throw new ConfigError(
      `${where}: "evmVersion": "${settings.evmVersion}" is older than "byzantium", the first EVM version with STATICCALL. A lens only reads because every call it makes is a STATICCALL, so "byzantium" is the oldest target it compiles for.`,
    );
}

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
  const settings = solc.settings ?? {};
  requireStaticCall(settings, file);
  const root = path.dirname(file);
  return {
    path: file,
    root,
    cacheDir: path.join(root, "node_modules", ".cache", "viem-dlc-solidity"),
    solc: { version: solc.version, settings },
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
