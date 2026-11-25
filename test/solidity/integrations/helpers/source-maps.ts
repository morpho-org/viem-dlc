/**
 * Source maps of a production build: a map embeds the source of each module it maps, and the source of
 * a lens module holds the lens body, which must not ship.
 */
import fs from "node:fs";
import path from "node:path";

import { expect } from "vitest";

import { LENS_SOURCE } from "./host-suite.js";
import type { Project } from "./project.js";

interface SourceMap {
  sources: string[];
  sourcesContent?: (string | null)[];
}

const INLINE = /\/\/# sourceMappingURL=data:application\/json;(?:charset=utf-8;)?base64,([A-Za-z0-9+/=]+)/;

export interface FoundMaps {
  /** The `.map` files, by path. */
  external: string[];
  /** The scripts that hold their map inline. */
  inline: string[];
  /** The scripts that name a `.map` file. */
  linked: string[];
}

/**
 * Reads every source map under a directory of build output, in a file of its own or inline in a script,
 * and expects of each one that maps the fixture lens: the source it embeds for `src/lens.ts` is the
 * rewritten module, on the lines of the original, and no source it embeds holds a lens body.
 */
export function expectNoLensSourceInMaps(project: Project, directory: string, skip: readonly string[] = []): FoundMaps {
  const found: FoundMaps = { external: [], inline: [], linked: [] };
  const maps: SourceMap[] = [];
  for (const file of project.files([directory], skip)) {
    const text = fs.readFileSync(file, "utf8");
    const relative = path.relative(project.dir, file);
    if (file.endsWith(".map")) {
      found.external.push(relative);
      maps.push(JSON.parse(text) as SourceMap);
      continue;
    }
    const inline = INLINE.exec(text);
    if (inline) {
      found.inline.push(relative);
      maps.push(JSON.parse(Buffer.from(inline[1] as string, "base64").toString("utf8")) as SourceMap);
    } else if (/\/\/# sourceMappingURL=\S+\.map/.test(text)) found.linked.push(relative);
  }

  const source = project.read("src/lens.ts");
  // The check finds what it looks for: the module on disk holds the lens body.
  expect(source).toMatch(LENS_SOURCE);
  const embedded = maps.flatMap((map) =>
    map.sources.flatMap((name, index) => (name.endsWith("src/lens.ts") ? [map.sourcesContent?.[index]] : [])),
  );
  expect(embedded).not.toEqual([]);
  for (const content of embedded) {
    expect(content).toMatch(/CompiledLens\(\{"name":"QuoteLens".*"bytecode":"0x60[0-9a-f]+"/);
    expect(content?.split("\n")).toHaveLength(source.split("\n").length);
  }
  for (const map of maps) expect(JSON.stringify(map.sourcesContent ?? [])).not.toMatch(LENS_SOURCE);
  return found;
}
