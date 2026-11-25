/**
 * The two stages every integration shares below the printers: the configuration file, and the compile
 * step with its content-addressed cache.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { afterAll, beforeEach, describe, expect, it } from "vitest";

import {
  build,
  CONFIG_FILE,
  ConfigError,
  compile,
  findConfig,
  LensError,
  readConfig,
  SOLIDITY_HEADER,
  type SolidityConfig,
} from "../../../src/solidity/build/index.js";
import { positionIn } from "../helpers/harness.js";

const temporary: string[] = [];
const temporaryDirectory = (): string => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "viem-dlc-config-"));
  temporary.push(dir);
  return dir;
};
afterAll(() => {
  for (const dir of temporary) fs.rmSync(dir, { recursive: true, force: true });
});

const LENS = `import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";

export const Lens = contract("Lens", { run: pure(uint256).returns(uint256) }).implementation({
  run: (x) => x + 1n,
});
`;

describe("solidity.config.json", () => {
  const write = (dir: string, content: unknown): string => {
    const file = path.join(dir, CONFIG_FILE);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(file, typeof content === "string" ? content : JSON.stringify(content));
    return file;
  };

  it("pins the compiler and its settings, and places the cache beside it", () => {
    const dir = temporaryDirectory();
    const settings = { optimizer: { enabled: true, runs: 200 } };
    const file = write(dir, { solc: { version: "0.8.36", settings } });
    expect(readConfig(file)).toEqual({
      path: file,
      root: dir,
      cacheDir: path.join(dir, "node_modules", ".cache", "viem-dlc-solidity"),
      solc: { version: "0.8.36", settings },
    });
  });

  it("needs no settings", () => {
    expect(readConfig(write(temporaryDirectory(), { solc: { version: "0.8.36" } })).solc.settings).toEqual({});
  });

  it("is the nearest one above the lens file", () => {
    const dir = temporaryDirectory();
    write(dir, { solc: { version: "0.8.30" } });
    const inner = write(path.join(dir, "packages", "app"), { solc: { version: "0.8.36" } });
    expect(findConfig(path.join(dir, "packages", "app", "src", "deep", "lens.ts")).path).toBe(inner);
    expect(findConfig(path.join(dir, "packages", "other", "lens.ts")).solc.version).toBe("0.8.30");
  });

  it("is read again each time, so an edit reaches the next compile", () => {
    const dir = temporaryDirectory();
    const lensFile = path.join(dir, "lens.ts");
    write(dir, { solc: { version: "0.8.30" } });
    expect(findConfig(lensFile).solc.version).toBe("0.8.30");
    write(dir, { solc: { version: "0.8.36" } });
    expect(findConfig(lensFile).solc.version).toBe("0.8.36");
  });

  it.each([
    ["text that is not JSON", "{ solc: ", /^cannot read .*solidity\.config\.json: /],
    ["no compiler", {}, /must pin an exact compiler/],
    ["a file that is not an object", ["0.8.36"], /must pin an exact compiler/],
    ["a version range", { solc: { version: "^0.8.0" } }, /must pin an exact compiler/],
    ["a version that is not a string", { solc: { version: 8 } }, /must pin an exact compiler/],
    [
      "settings that are not an object",
      { solc: { version: "0.8.36", settings: [] } },
      /"solc.settings" must be an object/,
    ],
  ])("refuses %s", (_what, content, message) => {
    const file = write(temporaryDirectory(), content);
    expect(() => readConfig(file)).toThrow(ConfigError);
    expect(() => readConfig(file)).toThrow(message);
  });

  const buildIn = (dir: string) =>
    build(LENS, path.join(dir, "lens.ts"), { resolve: () => undefined, addDependency: () => {} }, { cache: false });

  it("a lens with none above it is refused, at the lens", async () => {
    const dir = temporaryDirectory();
    const error = await buildIn(dir).catch((cause: unknown) => cause);
    expect(error).toBeInstanceOf(LensError);
    expect((error as LensError).diagnostics).toEqual([
      {
        file: path.join(dir, "lens.ts"),
        ...positionIn(LENS, 'contract("Lens"'),
        severity: "error",
        source: "transform",
        lens: "Lens",
        message: expect.stringMatching(
          /^no solidity\.config\.json found above .*lens\.ts\. Add one at the repository root/,
        ),
      },
    ]);
  });

  it("a malformed one is refused, at the lens", async () => {
    const dir = temporaryDirectory();
    write(dir, { solc: { version: "latest" } });
    const error = await buildIn(dir).catch((cause: unknown) => cause);
    expect((error as LensError).diagnostics).toMatchObject([
      {
        ...positionIn(LENS, 'contract("Lens"'),
        lens: "Lens",
        message: expect.stringContaining("must pin an exact compiler"),
      },
    ]);
  });

  it("is a dependency of every lens under it", async () => {
    const dir = temporaryDirectory();
    const file = write(dir, { solc: { version: "0.8.36" } });
    expect((await buildIn(dir))?.dependencies).toEqual([file]);
  });
});

describe("compile and its cache", () => {
  const solidity = `${SOLIDITY_HEADER}
contract Counter {
    function next(uint256 x) external pure returns (uint256) {
        return x + 1;
    }
}
`;
  const input = { contractName: "Counter", solidity };
  let config: SolidityConfig;
  beforeEach(() => {
    const root = temporaryDirectory();
    config = {
      path: path.join(root, CONFIG_FILE),
      root,
      cacheDir: path.join(root, "node_modules", ".cache", "viem-dlc-solidity"),
      solc: { version: "0.8.36", settings: { optimizer: { enabled: true, runs: 200 } } },
    };
  });
  const entries = () => (fs.existsSync(config.cacheDir) ? fs.readdirSync(config.cacheDir) : []);

  it("compiles on a miss and writes one entry; a hit gives the same artifact without compiling", () => {
    const cold = compile(input, config);
    expect(cold).toMatchObject({ ok: true, cached: false, warnings: [] });
    expect(entries()).toEqual([expect.stringMatching(/^[0-9a-f]{64}\.json$/)]);

    const warm = compile(input, config);
    expect(warm).toMatchObject({ ok: true, cached: true });
    expect(warm.ok && cold.ok && warm.artifact).toEqual(cold.ok && cold.artifact);
    expect(cold.ok && cold.artifact.bytecode).toMatch(/^0x60[0-9a-f]+$/);
    expect(cold.ok && cold.artifact.abi.map((item) => "name" in item && item.name)).toEqual(["next"]);
  });

  it("keys an entry on the Solidity, the contract name, the settings and the version", () => {
    compile(input, config);
    compile({ ...input, solidity: solidity.replace("x + 1", "x + 2") }, config);
    expect(entries()).toHaveLength(2);
    compile(input, { ...config, solc: { ...config.solc, settings: { optimizer: { enabled: false } } } });
    expect(entries()).toHaveLength(3);
    // Another name misses the cache; here it also names a contract the source does not have.
    expect(() => compile({ ...input, contractName: "Other" }, config)).toThrow(/without a contract named Other/);
    // Another version misses the cache too, and then needs that version installed.
    expect(() => compile(input, { ...config, solc: { ...config.solc, version: "0.8.19" } })).toThrow(
      /pins solc 0\.8\.19/,
    );
    expect(entries()).toHaveLength(3);
  });

  it("different settings give different bytecode", () => {
    const optimized = compile(input, config);
    const plain = compile(input, { ...config, solc: { ...config.solc, settings: { optimizer: { enabled: false } } } });
    expect(optimized.ok && plain.ok && optimized.artifact.bytecode !== plain.artifact.bytecode).toBe(true);
  });

  it("with the cache off, reads nothing and writes nothing", () => {
    expect(compile(input, config, { cache: false })).toMatchObject({ ok: true, cached: false });
    expect(entries()).toEqual([]);
    compile(input, config);
    expect(compile(input, config, { cache: false })).toMatchObject({ ok: true, cached: false });
  });

  it("an entry that cannot be read is compiled again and replaced", () => {
    compile(input, config);
    const [entry] = entries();
    const file = path.join(config.cacheDir, entry as string);
    fs.writeFileSync(file, '{ "artifact": ');
    expect(compile(input, config)).toMatchObject({ ok: true, cached: false });
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toHaveProperty("artifact.bytecode");
    expect(entries()).toHaveLength(1);
  });

  it("an error is a result, with warnings apart, and is not cached", () => {
    const broken = solidity.replace("return x + 1;", "uint256 unused = x; return true;");
    const result = compile({ ...input, solidity: broken }, config);
    expect(result.ok).toBe(false);
    expect(!result.ok && result.errors.map((error) => [error.severity, error.type, error.errorCode])).toEqual([
      ["error", "TypeError", "6359"],
    ]);
    expect(!result.ok && result.errors[0]?.range).toEqual({
      start: Buffer.byteLength(broken.slice(0, broken.indexOf("true;"))),
      end: Buffer.byteLength(broken.slice(0, broken.indexOf("true;") + 4)),
    });
    expect(entries()).toEqual([]);
  });
});
