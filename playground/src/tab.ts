import { readLens } from "@morpho-org/viem-dlc/actions";
import { deployless, logsDivider, rateLimiter } from "@morpho-org/viem-dlc/transports";
import type { InlineContract } from "soltag";
import { type Address, type Chain, type Client, createPublicClient, type Hex, http, type Transport } from "viem";
import { base } from "viem/chains";

export type TabContext = {
  client: Client<Transport, Chain>;
  /** The lens as compiled right now — prebuilt, or from the editor's Solidity if it was changed. */
  lens: InlineContract<"MorphoPositionsLens">;
  inputs: readonly { id: Hex; user: Address }[];
  log: (message: string) => void;
};

export type Tab = (context: TabContext) => Promise<{ results: readonly unknown[]; skipped: readonly number[] }>;

/**
 * What a tab's `import` statements resolve to. Tabs are evaluated rather than bundled, so this
 * stands in for the module graph; a specifier absent here fails loudly instead of yielding
 * `undefined` at the call site.
 */
const MODULES: Record<string, Record<string, unknown>> = {
  "@morpho-org/viem-dlc/actions": { readLens },
  "@morpho-org/viem-dlc/transports": { deployless, logsDivider, rateLimiter },
  viem: { createPublicClient, http },
  "viem/chains": { base },
};

const IMPORT_BLOCK = /^(?:\s*import\b[\s\S]*?from\s*["'][^"']+["'];?\s*)+/;
const IMPORT_STATEMENT = /import\s*\{([^}]*)\}\s*from\s*["']([^"']+)["']/g;

/**
 * Turns a tab's source into the function it exports.
 *
 * The file is a real `.js` module that `tsc` typechecks through its JSDoc annotations; here its
 * imports become parameters of the same name, so the source a reader edits is the source that runs
 * without a bundler in the page.
 */
export function evaluateTab(source: string): Tab {
  const imports = source.match(IMPORT_BLOCK)?.[0] ?? "";
  const names: string[] = [];
  const values: unknown[] = [];

  for (const [, clause, specifier] of imports.matchAll(IMPORT_STATEMENT)) {
    const module = MODULES[specifier!];
    if (!module) throw new Error(`Cannot import "${specifier}" here. Available: ${Object.keys(MODULES).join(", ")}`);

    for (const raw of clause!.split(",")) {
      const name = raw.trim().replace(/^type\s+/, "");
      if (!name) continue;
      if (!(name in module)) throw new Error(`"${specifier}" does not export "${name}"`);
      names.push(name);
      values.push(module[name]);
    }
  }

  const body = source.slice(imports.length).replace(/\bexport\s+default\s+/, "return ");
  return new Function(...names, body)(...values) as Tab;
}
