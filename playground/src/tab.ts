import { getLogs2, readLens } from "@morpho-org/viem-dlc/actions";
import { HierarchicalStore } from "@morpho-org/viem-dlc/stores/hierarchical";
import { LruStore } from "@morpho-org/viem-dlc/stores/lru";
import { MemoryStore } from "@morpho-org/viem-dlc/stores/memory";
import { ThrottledStore } from "@morpho-org/viem-dlc/stores/throttled";
import { TtlStore } from "@morpho-org/viem-dlc/stores/ttl";
import {
  deployless,
  failover,
  logsDivider,
  logsEnricher,
  logsSieve,
  rateLimiter,
} from "@morpho-org/viem-dlc/transports";
import { cache, createExponentialInvalidation, createSimpleInvalidation } from "@morpho-org/viem-dlc/transports/cache";
import { createPublicClient, encodeEventTopics, http, numberToHex, parseAbiItem, rpcSchema } from "viem";
import { getBlockNumber, getLogs } from "viem/actions";
import { base } from "viem/chains";

import type { Tab } from "./examples/types.js";

/**
 * What a tab's `import` statements resolve to. Tabs are evaluated rather than bundled, so this
 * stands in for the module graph; a specifier absent here fails loudly instead of yielding
 * `undefined` at the call site.
 */
const MODULES: Record<string, Record<string, unknown>> = {
  "@morpho-org/viem-dlc/actions": { getLogs2, readLens },
  "@morpho-org/viem-dlc/transports": { deployless, failover, logsDivider, logsEnricher, logsSieve, rateLimiter },
  "@morpho-org/viem-dlc/transports/cache": { cache, createSimpleInvalidation, createExponentialInvalidation },
  // The stores barrel is bypassed at the alias level (it would drag `fs`/`path`/`crypto` in), but
  // scripts still read as they would in Node.
  "@morpho-org/viem-dlc/stores": { HierarchicalStore, LruStore, MemoryStore, ThrottledStore, TtlStore },
  viem: { createPublicClient, encodeEventTopics, http, numberToHex, parseAbiItem, rpcSchema },
  "viem/actions": { getBlockNumber, getLogs },
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
export function evaluateTab(source: string, extraModules?: Record<string, Record<string, unknown>>): Tab {
  const modules = extraModules ? { ...MODULES, ...extraModules } : MODULES;

  const imports = source.match(IMPORT_BLOCK)?.[0] ?? "";
  const names: string[] = [];
  const values: unknown[] = [];

  for (const [, clause, specifier] of imports.matchAll(IMPORT_STATEMENT)) {
    const module = modules[specifier!];
    if (!module) throw new Error(`Cannot import "${specifier}" here. Available: ${Object.keys(modules).join(", ")}`);

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
