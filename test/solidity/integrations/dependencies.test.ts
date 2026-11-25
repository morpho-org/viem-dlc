/**
 * What a build tells its host it read. A host watches those files, so a build that fails must have
 * named, by the time it fails, every file an edit to which can repair it.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { afterAll, describe, expect, it } from "vitest";

import { LensError, transform } from "../../../src/solidity/build/index.js";
import { positionIn } from "../helpers/harness.js";

const DIR = path.join(import.meta.dirname, "..", "__fixtures__");

/** Transforms the first of several modules held in memory, and returns what was reported and how it ended. */
const transformSnippet = async (files: Readonly<Record<string, string>>, dir = DIR) => {
  const modules = new Map(Object.entries(files).map(([name, code]) => [path.join(dir, name), code]));
  const [entry] = modules.keys() as unknown as [string];
  const dependencies: string[] = [];
  const outcome = await transform(
    modules.get(entry) as string,
    entry,
    {
      resolve: (specifier, importer) => path.resolve(path.dirname(importer), specifier).replace(/\.js$/, ".ts"),
      addDependency: (file) => dependencies.push(path.basename(file)),
      readFile: (file) => modules.get(file) as string,
    },
    { cache: false },
  ).then(
    () => "built" as const,
    (cause: unknown) => cause,
  );
  return { dependencies, outcome };
};

const lens = (returns: string) => `import { contract, pure, ${returns} } from "@morpho-org/viem-dlc/solidity";

import { WIDTH } from "./width.js";

export const WidthLens = contract("WidthLens", { width: pure().returns(${returns}) }).implementation({
  width: () => WIDTH,
});
`;

const temporary: string[] = [];
afterAll(() => {
  for (const dir of temporary) fs.rmSync(dir, { recursive: true, force: true });
});

describe("the files a build reports", () => {
  it("are the helper module and the configuration, when it succeeds", async () => {
    const { dependencies, outcome } = await transformSnippet({
      "main.ts": lens("uint256"),
      "width.ts": "export const WIDTH = 256n;\n",
    });
    expect(outcome).toBe("built");
    expect(dependencies).toEqual(["width.ts", "solidity.config.json"]);
  });

  it("are the same when solc refuses the lens for what the helper module holds", async () => {
    const { dependencies, outcome } = await transformSnippet({
      "main.ts": lens("uint8"),
      "width.ts": "export const WIDTH = 256n;\n",
    });
    expect(outcome).toBeInstanceOf(LensError);
    expect((outcome as LensError).message).toContain("Literal is too large to fit in uint8");
    expect(dependencies).toEqual(["width.ts", "solidity.config.json"]);
  });

  it("include a helper module that cannot be parsed", async () => {
    const { dependencies, outcome } = await transformSnippet({
      "main.ts": lens("uint256"),
      "width.ts": "export const WIDTH = ;\n",
    });
    expect(outcome).toBeInstanceOf(LensError);
    expect((outcome as LensError).diagnostics[0]).toMatchObject({ file: path.join(DIR, "width.ts"), line: 1 });
    expect(dependencies).toEqual(["width.ts"]);
  });

  it("include a configuration that is refused, reported against the lens", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "viem-dlc-dependencies-"));
    temporary.push(dir);
    fs.writeFileSync(path.join(dir, "solidity.config.json"), '{ "solc": { "version": "latest" } }\n');
    const main = lens("uint256");
    const { dependencies, outcome } = await transformSnippet(
      { "main.ts": main, "width.ts": "export const WIDTH = 256n;\n" },
      dir,
    );
    expect(outcome).toBeInstanceOf(LensError);
    expect((outcome as LensError).diagnostics[0]).toMatchObject({
      message: expect.stringContaining("solidity.config.json must pin an exact compiler"),
      file: path.join(dir, "main.ts"),
      ...positionIn(main, 'contract("WidthLens"'),
    });
    expect(dependencies).toEqual(["width.ts", "solidity.config.json"]);
  });

  it("are each reported once, for a module with two lenses", async () => {
    const two = `${lens("uint256")}
export const OtherLens = contract("OtherLens", { width: pure().returns(uint256) }).implementation({
  width: () => WIDTH + 1n,
});
`;
    const { dependencies, outcome } = await transformSnippet({
      "main.ts": two,
      "width.ts": "export const WIDTH = 256n;\n",
    });
    expect(outcome).toBe("built");
    expect(dependencies).toEqual(["width.ts", "solidity.config.json"]);
  });
});
