/**
 * Which file an import names for a lens. The host's resolver finds the file, and the build takes its
 * answer only where the import names that one file under every condition: a path, an entry of a
 * package's "exports" or "imports" with one target, or an alias that stays inside the importing
 * package. Everything else is an error at the import, and here the host is a table of answers, so each
 * case says what a host found.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { afterAll, describe, expect, it } from "vitest";

import { ImportRefused } from "../../../src/solidity/build/import-target.js";
import { build, buildSync, LensError } from "../../../src/solidity/build/index.js";
import { positionIn } from "../helpers/harness.js";

const RULE = "A lens reads a module only through an import that names one file under every condition.";

const temporary: string[] = [];
afterAll(() => {
  for (const dir of temporary) fs.rmSync(dir, { recursive: true, force: true });
});

type Files = Readonly<Record<string, string | object>>;

/** A project on disk: an application with a lens module, and whatever else a case installs. */
const projectOf = (files: Files): string => {
  // Resolved, so that a path the build reads back from the file system is the path a case names.
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "viem-dlc-import-")));
  temporary.push(dir);
  const all: Files = {
    "solidity.config.json": { solc: { version: "0.8.36" } },
    "package.json": { name: "app", type: "module" },
    ...files,
  };
  for (const [name, content] of Object.entries(all)) {
    fs.mkdirSync(path.dirname(path.join(dir, name)), { recursive: true });
    fs.writeFileSync(path.join(dir, name), typeof content === "string" ? content : JSON.stringify(content));
  }
  return dir;
};

const VALUE = "export const VALUE = 3n;\n";

const lensFor = (specifier: string) => `import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
import { VALUE } from "${specifier}";

export const Lens = contract("Lens", { read: pure().returns(uint256) }).implementation({ read: () => VALUE });
`;

/** Builds a lens that imports `specifier` in a project where the host resolves it to `answer`. */
const buildIn = async (dir: string, specifier: string, answer: string) => {
  const file = path.join(dir, "src/lens.ts");
  const code = lensFor(specifier);
  const reported: string[] = [];
  const outcome = await build(
    code,
    file,
    {
      resolve: (asked) => (asked === specifier ? path.join(dir, answer) : undefined),
      addDependency: (dependency) => reported.push(path.relative(dir, dependency)),
    },
    { cache: false },
  ).then(
    (built) => built?.dependencies.map((dependency) => path.relative(dir, dependency)),
    (cause: unknown) => cause,
  );
  return { code, file, reported, outcome };
};

const expectAccepted = async (files: Files, specifier: string, answer: string, manifests: readonly string[]) => {
  const { reported, outcome } = await buildIn(projectOf(files), specifier, answer);
  // The manifests that decided the answer are dependencies, with the module read and the configuration.
  expect(outcome).toEqual([...manifests, answer, "solidity.config.json"]);
  expect(reported).toEqual([...manifests, answer, "solidity.config.json"]);
};

const expectRefused = async (
  files: Files,
  specifier: string,
  answer: string,
  message: (dir: string) => string,
  manifests: readonly string[] = [],
) => {
  const dir = projectOf(files);
  const { code, file, reported, outcome } = await buildIn(dir, specifier, answer);
  expect(outcome).toBeInstanceOf(LensError);
  expect((outcome as LensError).diagnostics).toEqual([
    {
      file,
      ...positionIn(code, `"${specifier}"`),
      severity: "error",
      source: "transform",
      lens: "Lens",
      message: message(dir),
    },
  ]);
  // A refusal has still named the manifest an edit to which can lift it.
  expect(reported).toEqual(manifests);
};

const installed = (name: string, manifest: object, files: readonly string[] = ["index.js"]): Files => ({
  [`node_modules/${name}/package.json`]: { name, type: "module", ...manifest },
  ...Object.fromEntries(files.map((file) => [`node_modules/${name}/${file}`, VALUE])),
});

describe("a path", () => {
  it("is the file the host found, and no manifest is read for it", async () => {
    await expectAccepted({ "src/value.js": VALUE }, "./value.js", "src/value.js", []);
    await expectAccepted({ "other/value.js": VALUE }, "../other/value.js", "other/value.js", []);
  });

  it("is taken when it is absolute", async () => {
    const dir = projectOf({ "src/value.js": VALUE });
    const built = await build(
      lensFor(path.join(dir, "src/value.js")),
      path.join(dir, "src/lens.ts"),
      { resolve: (specifier) => specifier, addDependency: () => {} },
      { cache: false },
    );
    expect(built?.dependencies).toEqual([path.join(dir, "src/value.js"), path.join(dir, "solidity.config.json")]);
  });
});

describe("a package name", () => {
  it.each([
    { kind: "one file", exports: "./index.js" },
    { kind: "one file in a list", exports: ["./index.js"] },
    { kind: 'one file under "."', exports: { ".": "./index.js" } },
    {
      kind: "the same file to import and to require, and another to the type checker",
      exports: { types: "./index.d.ts", require: "./index.js", import: "./index.js" },
    },
    {
      kind: "one file to import, and none to a condition that excludes it",
      exports: { ".": { browser: null, import: { types: "./index.d.ts", default: "./index.js" } } },
    },
    {
      kind: "one file, where another subpath names the same file by condition",
      exports: { ".": "./index.js", "./other": { require: "./index.js", import: "./other.js" } },
    },
  ])("is taken where its exports name $kind", async ({ exports }) => {
    await expectAccepted(installed("units", { exports }), "units", "node_modules/units/index.js", [
      "node_modules/units/package.json",
    ]);
  });

  it("is taken for a subpath, under its own key or under the one pattern that matches", async () => {
    const files = installed(
      "@fixture/units",
      { exports: { "./rate": "./lib/rate.js", "./features/*": "./lib/features/*.js", "./*.json": "./data/*.json" } },
      ["lib/rate.js", "lib/features/deep/fee.js"],
    );
    await expectAccepted(files, "@fixture/units/rate", "node_modules/@fixture/units/lib/rate.js", [
      "node_modules/@fixture/units/package.json",
    ]);
    await expectAccepted(
      files,
      "@fixture/units/features/deep/fee",
      "node_modules/@fixture/units/lib/features/deep/fee.js",
      ["node_modules/@fixture/units/package.json"],
    );
  });

  it("is found above the importing module, through a link, as the package manager installs it", async () => {
    const dir = projectOf({
      "packages/units/package.json": { name: "units", exports: "./index.js" },
      "packages/units/index.js": VALUE,
    });
    fs.mkdirSync(path.join(dir, "node_modules"));
    fs.symlinkSync(path.join(dir, "packages/units"), path.join(dir, "node_modules/units"));
    // A host may answer with the link or with the file it leads to.
    for (const answer of ["node_modules/units/index.js", "packages/units/index.js"])
      expect((await buildIn(dir, "units", answer)).outcome).toEqual([
        "packages/units/package.json",
        answer,
        "solidity.config.json",
      ]);
  });

  it("is the importing module's own package, under its own name", async () => {
    const own = { name: "app", type: "module" };
    await expectAccepted(
      { "package.json": { ...own, exports: { "./value": "./src/value.js" } }, "src/value.js": VALUE },
      "app/value",
      "src/value.js",
      ["package.json"],
    );
    await expectRefused(
      {
        "package.json": { ...own, exports: { "./value": { import: "./src/value.js", require: "./src/other.js" } } },
        "src/value.js": VALUE,
      },
      "app/value",
      "src/value.js",
      (dir) =>
        `the entry "./value" of "exports" in ${path.join(dir, "package.json")} names "./src/value.js" and "./src/other.js", by condition. ${RULE}`,
      ["package.json"],
    );
  });

  it.each([
    {
      kind: "by condition",
      exports: { types: "./index.d.ts", "react-server": "./server.js", default: "./index.js" },
      names: '"./server.js" and "./index.js", by condition',
    },
    {
      kind: "to import and to require",
      exports: { ".": { require: "./index.cjs", import: "./index.js" } },
      names: '"./index.cjs" and "./index.js", by condition',
    },
    {
      kind: "in a list of alternatives",
      exports: ["./index.js", "./fallback.js"],
      names: '"./index.js" and "./fallback.js", by condition',
    },
    { kind: "and none for any condition but types", exports: { types: "./index.d.ts", node: null }, names: "no file" },
  ])("is refused where its exports name several files, $kind", async ({ exports, names }) => {
    await expectRefused(
      installed("units", { exports }),
      "units",
      "node_modules/units/index.js",
      (dir) =>
        `the entry "." of "exports" in ${path.join(dir, "node_modules/units/package.json")} names ${names}. ${RULE}`,
      ["node_modules/units/package.json"],
    );
  });

  it("is refused where the package has no exports, so each tool reads another field", async () => {
    await expectRefused(
      installed("units", { main: "./index.js", module: "./index.mjs" }),
      "units",
      "node_modules/units/index.js",
      (dir) =>
        `${path.join(dir, "node_modules/units/package.json")} has no "exports", so each tool chooses the file "." names. ${RULE}`,
      ["node_modules/units/package.json"],
    );
  });

  it("is refused for a subpath with no entry, or with more than one", async () => {
    const files = installed(
      "units",
      { exports: { ".": "./index.js", "./lib/*": "./lib/*.js", "./lib/internal/*": null } },
      ["index.js", "lib/rate.js", "lib/internal/fee.js"],
    );
    const manifest = (dir: string) => path.join(dir, "node_modules/units/package.json");
    await expectRefused(
      files,
      "units/rate",
      "node_modules/units/lib/rate.js",
      (dir) => `${manifest(dir)} has no entry for "./rate" in "exports". ${RULE}`,
      ["node_modules/units/package.json"],
    );
    await expectRefused(
      files,
      "units/lib/internal/fee",
      "node_modules/units/lib/internal/fee.js",
      (dir) =>
        `${manifest(dir)} has the entries "./lib/*" and "./lib/internal/*" for "./lib/internal/fee" in "exports". ${RULE}`,
      ["node_modules/units/package.json"],
    );
  });

  it("is refused where the host found another file than the entry names", async () => {
    await expectRefused(
      installed("units", { exports: "./index.js" }, ["index.js", "other.js"]),
      "units",
      "node_modules/units/other.js",
      (dir) =>
        `"units" resolves to ${path.join(dir, "node_modules/units/other.js")}, and the entry "." of "exports" in ${path.join(dir, "node_modules/units/package.json")} names ${path.join(dir, "node_modules/units/index.js")}. ${RULE}`,
      ["node_modules/units/package.json"],
    );
  });

  it("is refused where the entry names something outside the package, or the manifest cannot be read", async () => {
    await expectRefused(
      installed("units", { exports: "other-package" }),
      "units",
      "node_modules/units/index.js",
      (dir) =>
        `the entry "." of "exports" in ${path.join(dir, "node_modules/units/package.json")} names "other-package", which is neither a file of the package nor a package installed for it. ${RULE}`,
      ["node_modules/units/package.json"],
    );
    for (const text of ["{ exports: ", '"units"'])
      await expectRefused(
        { "node_modules/units/package.json": text, "node_modules/units/index.js": VALUE },
        "units",
        "node_modules/units/index.js",
        (dir) => `${path.join(dir, "node_modules/units/package.json")} cannot be read as a package manifest.`,
        ["node_modules/units/package.json"],
      );
  });
});

describe("a # import", () => {
  const own = (imports: object) => ({ "package.json": { name: "app", type: "module", imports } });

  it("is taken where the importing package's imports name one file", async () => {
    await expectAccepted({ ...own({ "#value": "./src/value.js" }), "src/value.js": VALUE }, "#value", "src/value.js", [
      "package.json",
    ]);
    await expectAccepted(
      { ...own({ "#lib/*": { types: "./src/*.d.ts", default: "./src/*.js" } }), "src/value.js": VALUE },
      "#lib/value",
      "src/value.js",
      ["package.json"],
    );
  });

  it("is taken where they name one package, which answers through its own exports", async () => {
    await expectAccepted(
      { ...own({ "#units": "units", "#rate": "units/rate" }), ...installed("units", { exports: "./index.js" }) },
      "#units",
      "node_modules/units/index.js",
      ["package.json", "node_modules/units/package.json"],
    );
  });

  it("is refused where they name one package to import and another to require", async () => {
    await expectRefused(
      {
        ...own({ "#units": { require: "units-b", import: "units-a" } }),
        ...installed("units-a", { exports: "./index.js" }),
        ...installed("units-b", { exports: "./index.js" }),
      },
      "#units",
      // What a host that is not told which of the two it is asked for can answer.
      "node_modules/units-b/index.js",
      (dir) =>
        `the entry "#units" of "imports" in ${path.join(dir, "package.json")} names "units-b" and "units-a", by condition. ${RULE}`,
      ["package.json"],
    );
  });

  it("is refused where they name a file by condition, and where the package they name does", async () => {
    await expectRefused(
      { ...own({ "#value": { development: "./src/dev.js", default: "./src/value.js" } }), "src/value.js": VALUE },
      "#value",
      "src/value.js",
      (dir) =>
        `the entry "#value" of "imports" in ${path.join(dir, "package.json")} names "./src/dev.js" and "./src/value.js", by condition. ${RULE}`,
      ["package.json"],
    );
    await expectRefused(
      {
        ...own({ "#units": "units" }),
        ...installed("units", { exports: { require: "./index.cjs", import: "./index.js" } }),
      },
      "#units",
      "node_modules/units/index.js",
      (dir) =>
        `the entry "." of "exports" in ${path.join(dir, "node_modules/units/package.json")} names "./index.cjs" and "./index.js", by condition. ${RULE}`,
      ["package.json", "node_modules/units/package.json"],
    );
  });

  it("is refused where they have no entry for it, or name a package that is not installed", async () => {
    await expectRefused(
      { "src/value.js": VALUE },
      "#value",
      "src/value.js",
      (dir) =>
        `${path.join(dir, "package.json")} has no "imports", so each tool chooses the file "#value" names. ${RULE}`,
      ["package.json"],
    );
    await expectRefused(
      { ...own({ "#units": "units", "#other": "./src/value.js" }), "src/value.js": VALUE },
      "#units",
      "src/value.js",
      (dir) =>
        `the entry "#units" of "imports" in ${path.join(dir, "package.json")} names "units", which is neither a file of the package nor a package installed for it. ${RULE}`,
      ["package.json"],
    );
  });
});

describe("a name that is no installed package, which the host answers from its aliases", () => {
  it("is taken inside the package of the importing module", async () => {
    for (const specifier of ["@/value", "~value", "@value", "src/value"])
      await expectAccepted({ "src/value.js": VALUE }, specifier, "src/value.js", []);
  });

  it("is refused where it leads into another package", async () => {
    for (const answer of ["node_modules/units/index.js", "packages/units/index.js"])
      await expectRefused(
        {
          ...installed("units", { exports: { require: "./index.cjs", import: "./index.js" } }),
          "packages/units/package.json": { name: "@fixture/units" },
          "packages/units/index.js": VALUE,
        },
        "@units",
        answer,
        (dir) =>
          `"@units" names no installed package, and resolves to ${path.join(dir, answer)}, outside the package of the module that imports it. A lens reads a module of another package only through that package's "exports": import it by the package's name, or by a relative path.`,
      );
  });
});

describe("where an import is refused", () => {
  const through = `export { VALUE } from "units";
export * from "./other.js";
`;
  const files = {
    ...installed("units", { exports: { require: "./index.cjs", import: "./index.js" } }),
    "src/through.js": through,
  };

  it("the error is in the module that names the import, at the import", async () => {
    const dir = projectOf(files);
    const answers: Record<string, string> = { "./through.js": "src/through.js", units: "node_modules/units/index.js" };
    const outcome = await build(lensFor("./through.js"), path.join(dir, "src/lens.ts"), {
      resolve: (specifier) => path.join(dir, answers[specifier] as string),
      addDependency: () => {},
    }).catch((cause: unknown) => cause);
    expect((outcome as LensError).diagnostics).toMatchObject([
      { file: path.join(dir, "src/through.js"), ...positionIn(through, '"units"'), lens: "Lens" },
    ]);
  });

  it("a host's own refusal is reported the same way, from a resolver that answers at once or later", async () => {
    const dir = projectOf({});
    const code = lensFor("./value.js");
    const refusing = {
      resolve: (specifier: string): string => {
        throw new ImportRefused(`the host cannot tell which file "${specifier}" names`);
      },
      addDependency: () => {},
    };
    const expected = [
      {
        file: path.join(dir, "src/lens.ts"),
        ...positionIn(code, '"./value.js"'),
        severity: "error",
        source: "transform",
        lens: "Lens",
        message: 'the host cannot tell which file "./value.js" names',
      },
    ];
    const later = await build(code, path.join(dir, "src/lens.ts"), refusing).catch((cause: unknown) => cause);
    expect((later as LensError).diagnostics).toEqual(expected);
    let atOnce: unknown;
    try {
      buildSync(code, path.join(dir, "src/lens.ts"), refusing);
    } catch (cause) {
      atOnce = cause;
    }
    expect((atOnce as LensError).diagnostics).toEqual(expected);
  });

  it("any other error of a host's resolver is passed on as the host threw it", async () => {
    const dir = projectOf({});
    const hostError = new Error("the host's resolver broke");
    const outcome = build(lensFor("./value.js"), path.join(dir, "src/lens.ts"), {
      resolve: () => Promise.reject(hostError),
      addDependency: () => {},
    });
    await expect(outcome).rejects.toBe(hostError);
  });
});

describe("a # import with no package.json above the importing module", () => {
  it("is refused", async () => {
    // Above the file system's temporary directory there is no package.json.
    const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "viem-dlc-import-")));
    temporary.push(dir);
    expect(fs.existsSync(path.join(dir, "..", "package.json"))).toBe(false);
    fs.writeFileSync(path.join(dir, "solidity.config.json"), JSON.stringify({ solc: { version: "0.8.36" } }));
    fs.writeFileSync(path.join(dir, "value.js"), VALUE);
    const code = lensFor("#value");
    const outcome = await build(code, path.join(dir, "lens.ts"), {
      resolve: () => path.join(dir, "value.js"),
      addDependency: () => {},
    }).catch((cause: unknown) => cause);
    expect((outcome as LensError).diagnostics).toMatchObject([
      {
        ...positionIn(code, '"#value"'),
        message: `"#value" is read from the "imports" of a package.json, and there is none above ${path.join(dir, "lens.ts")}.`,
      },
    ]);
  });
});
