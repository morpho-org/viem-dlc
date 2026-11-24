/**
 * The type fixture is also a lens. `types/surface.ts` holds one use of every construct a body may contain,
 * and `pnpm typecheck` proves the editor accepts it. Here the same file goes through the transform and
 * solc, so a form the types offer and the transform refuses fails a test.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import { build } from "../../src/solidity/build/index.js";

const SURFACE = path.join(path.dirname(fileURLToPath(import.meta.url)), "types", "surface.ts");

describe("the type fixture", () => {
  it("builds: the transform prints it and solc compiles it", async () => {
    const built = await build(
      fs.readFileSync(SURFACE, "utf8"),
      SURFACE,
      // The fixture imports only the package, which the transform knows by name.
      { resolve: () => undefined, addDependency: () => {} },
      { cache: false },
    );
    const [lens, ...rest] = built?.lenses ?? [];
    expect(rest).toEqual([]);
    expect(lens?.resolved.name).toBe("PriceBoard");
    const functions = lens?.artifact.abi.flatMap((item) => (item.type === "function" ? [item.name] : []));
    expect(functions?.sort()).toEqual(["board", "holdingOf", "priceOf"]);
    expect(lens?.artifact.bytecode).toMatch(/^0x[0-9a-f]{2,}$/);
    // Every kind of declaration the fixture holds reached the Solidity.
    for (const declaration of ["struct Board", "interface IPriceFeed", "library FixedPoint", "contract PriceBoard"])
      expect(lens?.printed.solidity).toContain(declaration);
    expect(lens?.printed.solidity).toContain('assembly ("memory-safe")');
  });
});
