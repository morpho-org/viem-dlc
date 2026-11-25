/**
 * Fixture projects: how an integration is tested the way a consumer meets it.
 *
 * A project is a copy of a template under `../fixtures` with the packed package installed in its
 * `node_modules`, so it resolves `@morpho-org/viem-dlc` as an application does: by name, through the
 * package's `exports`, from the built files. The hosts themselves (Next, Vite, Vitest, tsdown) are this
 * repository's devDependencies, which a project finds by walking up.
 *
 * Projects live in `../.work`, which git ignores. Each is a git repository of its own, with every
 * file staged, so "what did the host change" is one `git` question.
 */
import { type ChildProcess, execFileSync, spawn } from "node:child_process";
import fs from "node:fs";
import { createServer } from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { resolveSync } from "../../../../src/solidity/build/adapters/node-resolve.js";
import { build } from "../../../../src/solidity/build/index.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(HERE, "../../../..");
const FIXTURES = path.join(HERE, "../fixtures");
const WORK = path.join(HERE, "../.work");

/** What a project's own `.gitignore` lists: build output, and `node_modules`, which holds the cache. */
export const IGNORED = [".next/", "dist/", "next-env.d.ts", "node_modules/"];

/** The cache of compiled lenses, under the directory of `solidity.config.json`. */
const COMPILE_CACHE = "node_modules/.cache/viem-dlc-solidity";

/** The entry script of a devDependency's command, to run with `node`. */
export const bin = (name: string, command = name): string => {
  const dir = path.join(ROOT, "node_modules", name);
  const manifest = JSON.parse(fs.readFileSync(path.join(dir, "package.json"), "utf8")) as {
    bin: string | Record<string, string>;
  };
  const entry = typeof manifest.bin === "string" ? manifest.bin : manifest.bin[command];
  if (!entry) throw new Error(`${name} has no command named ${command}`);
  return path.join(dir, entry);
};

let tarball: string | undefined;

/**
 * The package as `pnpm pack` makes it, once per test file. Scripts are skipped: `pnpm test` has built
 * `dist` already, and a second build would rewrite files other tests are reading.
 */
export const packedPackage = (): string => {
  if (!tarball) {
    if (!fs.existsSync(path.join(ROOT, "dist/solidity/index.js")))
      throw new Error("dist is missing: run pnpm build before these tests (pnpm test does)");
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "viem-dlc-pack-"));
    execFileSync("pnpm", ["pack", "--config.ignore-scripts=true", "--pack-destination", dir], {
      cwd: ROOT,
      stdio: "pipe",
    });
    const [name] = fs.readdirSync(dir);
    if (!name) throw new Error("pnpm pack wrote no tarball");
    tarball = path.join(dir, name);
  }
  return tarball;
};

/** Unpacks the tarball as `node_modules/@morpho-org/viem-dlc` of `dir`. */
export const installPackage = (dir: string): void => {
  const target = path.join(dir, "node_modules/@morpho-org/viem-dlc");
  fs.mkdirSync(target, { recursive: true });
  execFileSync("tar", ["-xzf", packedPackage(), "-C", target, "--strip-components=1"]);
};

/** Copies a directory as hard links to its files, or as copies where the file system cannot link them. */
const linkTree = (from: string, to: string): void => {
  fs.mkdirSync(to, { recursive: true });
  for (const entry of fs.readdirSync(from, { withFileTypes: true })) {
    const [source, target] = [path.join(from, entry.name), path.join(to, entry.name)];
    if (entry.isDirectory()) linkTree(source, target);
    else
      try {
        fs.linkSync(fs.realpathSync(source), target);
      } catch {
        fs.copyFileSync(source, target);
      }
  }
};

/**
 * Installs this repository's own copy of some packages, with everything they depend on, as real
 * directories in `node_modules` of `dir`: what a project needs when nothing above it may be read.
 */
export const installWithDependencies = (dir: string, names: readonly string[]): void => {
  /** A package by name, and the `node_modules` its own dependencies are found in. */
  const install = (name: string, from: string): void => {
    const target = path.join(dir, "node_modules", name);
    if (fs.existsSync(target) || !fs.existsSync(path.join(from, name))) return;
    const source = fs.realpathSync(path.join(from, name));
    linkTree(source, target);
    const manifest = JSON.parse(fs.readFileSync(path.join(source, "package.json"), "utf8")) as Record<
      "dependencies" | "optionalDependencies" | "peerDependencies",
      Record<string, string> | undefined
    >;
    // The package manager puts a package's dependencies beside it.
    const beside = source.slice(0, source.length - name.length - 1);
    for (const dependency of Object.keys({
      ...manifest.dependencies,
      ...manifest.optionalDependencies,
      ...manifest.peerDependencies,
    }))
      install(dependency, beside);
  };
  for (const name of names) install(name, path.join(ROOT, "node_modules"));
};

/** The environment of a host process: this one's, without what Vitest set for itself. */
const hostEnvironment = (extra: NodeJS.ProcessEnv): NodeJS.ProcessEnv => {
  const env: NodeJS.ProcessEnv = {};
  for (const [key, value] of Object.entries(process.env))
    if (!/^(VITEST|VITE_|NODE_ENV$|TEST$|MODE$|DEV$|PROD$|SSR$|BASE_URL$|FORCE_COLOR$)/.test(key)) env[key] = value;
  return { ...env, NO_COLOR: "1", NEXT_TELEMETRY_DISABLED: "1", ...extra };
};

/** One finished process. */
export interface Run {
  ok: boolean;
  /** stdout and stderr, in the order they arrived. */
  output: string;
}

/** A process that keeps running: a dev server or a watcher. */
export interface Background {
  output(): string;
  stop(): Promise<void>;
}

export interface BuiltFixtureLens {
  bytecode: string;
  solidity: string;
}

export const freePort = (): Promise<number> =>
  new Promise<number>((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address() as { port: number };
      server.close(() => resolve(port));
    });
  });

/** Polls `read` until it gives a value. */
export async function waitFor<T>(
  read: () => T | undefined | Promise<T | undefined>,
  what: string,
  context: () => string = () => "",
  timeout = 90_000,
): Promise<T> {
  const deadline = Date.now() + timeout;
  for (;;) {
    const value = await read();
    if (value !== undefined) return value;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}\n${context()}`);
    await new Promise((resolve) => setTimeout(resolve, 150));
  }
}

const BYTECODE = /"?bytecode"?:\s*"(0x[0-9a-f]+)"/g;

/** The one lens bytecode a text holds, or undefined when it holds none or several different ones. */
export const bytecodeIn = (text: string): string | undefined => {
  const found = new Set([...text.matchAll(BYTECODE)].map((match) => match[1] as string));
  return found.size === 1 ? [...found][0] : undefined;
};

/** The last line a fixture printed with its lens's bytecode. */
export const lastPrintedBytecode = (output: string): string | undefined => [...output.matchAll(BYTECODE)].at(-1)?.[1];

export class Project {
  readonly dir: string;
  /** Where the fixture's logging modules write when something imports them. Outside the project. */
  readonly importLogFile: string;

  constructor(template: "app" | "next" | "library") {
    fs.mkdirSync(WORK, { recursive: true });
    this.dir = fs.mkdtempSync(path.join(WORK, `${template}-`));
    this.importLogFile = `${this.dir}.imports.log`;
    fs.cpSync(path.join(FIXTURES, "shared"), this.dir, { recursive: true });
    fs.cpSync(path.join(FIXTURES, template), this.dir, { recursive: true });
    installPackage(this.dir);
    fs.writeFileSync(path.join(this.dir, ".gitignore"), IGNORED.map((entry) => `${entry}\n`).join(""));
    this.git("init", "--quiet");
    this.git("add", "--all");
  }

  file(relative: string): string {
    return path.join(this.dir, relative);
  }

  read(relative: string): string {
    return fs.readFileSync(this.file(relative), "utf8");
  }

  write(relative: string, content: string): void {
    fs.mkdirSync(path.dirname(this.file(relative)), { recursive: true });
    fs.writeFileSync(this.file(relative), content);
  }

  /** Replaces the one occurrence of `from` in a file. */
  edit(relative: string, from: string, to: string): void {
    const before = this.read(relative);
    if (before.split(from).length !== 2) throw new Error(`${relative} does not contain exactly one "${from}"`);
    this.write(relative, before.replace(from, to));
  }

  private git(...args: string[]): string[] {
    return execFileSync("git", args, { cwd: this.dir, encoding: "utf8" }).split("\n").filter(Boolean);
  }

  /** Tracked files that changed or went, and files that appeared and are not ignored. */
  changedFiles(): string[] {
    return [...this.git("diff", "--name-only"), ...this.git("ls-files", "--others", "--exclude-standard")];
  }

  /** The ignored entries present, as `.gitignore` names them. */
  ignoredEntries(): string[] {
    return this.git("ls-files", "--others", "--ignored", "--exclude-standard", "--directory");
  }

  /** Puts every source file back as the template has it. Build output and caches stay. */
  restore(): void {
    this.git("checkout", "--", ".");
    this.git("clean", "--force", "-d", "--quiet");
  }

  /** Removes build output and every cache, the host's own included, so the next run is cold. */
  clean(): void {
    for (const entry of [".next", "dist"]) fs.rmSync(this.file(entry), { recursive: true, force: true });
    for (const entry of fs.readdirSync(this.file("node_modules")))
      if (entry !== "@morpho-org" && entry !== "solc")
        fs.rmSync(this.file(path.join("node_modules", entry)), { recursive: true, force: true });
  }

  cachedLenses(): string[] {
    return fs.existsSync(this.file(COMPILE_CACHE)) ? fs.readdirSync(this.file(COMPILE_CACHE)) : [];
  }

  /**
   * Hides solc from the project. The project sits inside this repository, where Node would find the
   * repository's solc by walking up, so a package that fails to load exactly as a missing one does is
   * put in the way.
   */
  hideSolc(): void {
    this.write("node_modules/solc/package.json", '{ "name": "solc", "version": "0.0.0", "main": "index.js" }\n');
    this.write(
      "node_modules/solc/index.js",
      `throw Object.assign(new Error("Cannot find module 'solc'"), { code: "MODULE_NOT_FOUND" });\n`,
    );
  }

  showSolc(): void {
    fs.rmSync(this.file("node_modules/solc"), { recursive: true, force: true });
  }

  /** What the fixture's logging modules wrote: one line per import of one of them. */
  importLog(): string {
    return fs.existsSync(this.importLogFile) ? fs.readFileSync(this.importLogFile, "utf8") : "";
  }

  /** Every file under the given directories, without the directories named in `skip`. */
  files(directories: readonly string[], skip: readonly string[] = []): string[] {
    const walk = (dir: string): string[] =>
      fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
        const full = path.join(dir, entry.name);
        if (skip.some((skipped) => full === this.file(skipped))) return [];
        return entry.isDirectory() ? walk(full) : entry.isFile() ? [full] : [];
      });
    return directories.flatMap((directory) => walk(this.file(directory)));
  }

  /** The files under the given directories whose text matches. */
  search(pattern: RegExp, directories: readonly string[], skip: readonly string[] = []): string[] {
    return this.files(directories, skip)
      .filter((file) => pattern.test(fs.readFileSync(file, "latin1")))
      .map((file) => path.relative(this.dir, file));
  }

  /** The one lens bytecode the files under a directory hold. */
  bytecodeUnder(directory: string, skip: readonly string[] = []): string | undefined {
    if (!fs.existsSync(this.file(directory))) return undefined;
    try {
      return bytecodeIn(
        this.files([directory], skip)
          .map((file) => fs.readFileSync(file, "latin1"))
          .join("\n"),
      );
    } catch {
      // A watcher is rewriting the directory.
      return undefined;
    }
  }

  private spawn(args: readonly string[], env: NodeJS.ProcessEnv, detached: boolean): ChildProcess {
    return spawn(process.execPath, args, {
      cwd: this.dir,
      env: hostEnvironment({ IMPORT_LOG: this.importLogFile, ...env }),
      stdio: ["ignore", "pipe", "pipe"],
      detached,
    });
  }

  /** Runs `node …args` in the project to completion. */
  run(args: readonly string[], env: NodeJS.ProcessEnv = {}): Promise<Run> {
    return new Promise((resolve, reject) => {
      const child = this.spawn(args, env, false);
      let output = "";
      child.stdout?.on("data", (chunk: Buffer) => {
        output += chunk.toString();
      });
      child.stderr?.on("data", (chunk: Buffer) => {
        output += chunk.toString();
      });
      child.once("error", reject);
      child.once("close", (code) => resolve({ ok: code === 0, output }));
    });
  }

  /** Starts `node …args` in the project and leaves it running, in a process group of its own. */
  start(args: readonly string[], env: NodeJS.ProcessEnv = {}): Background {
    const child = this.spawn(args, env, true);
    let output = "";
    child.stdout?.on("data", (chunk: Buffer) => {
      output += chunk.toString();
    });
    child.stderr?.on("data", (chunk: Buffer) => {
      output += chunk.toString();
    });
    const closed = new Promise<void>((resolve) => child.once("close", () => resolve()));
    const signal = (name: NodeJS.Signals) => {
      try {
        // The whole group: a dev server starts workers of its own.
        if (child.pid !== undefined) process.kill(-child.pid, name);
      } catch {
        // Already gone.
      }
    };
    return {
      output: () => output,
      async stop() {
        signal("SIGTERM");
        const killer = setTimeout(() => signal("SIGKILL"), 5_000);
        await closed;
        clearTimeout(killer);
        signal("SIGKILL");
      },
    };
  }

  /**
   * The fixture lens built from the project's sources as they stand, by the core, without any cache:
   * what a host must be serving.
   */
  async expectedLens(lensFile = "src/lens.ts"): Promise<BuiltFixtureLens> {
    const file = this.file(lensFile);
    const built = await build(
      fs.readFileSync(file, "utf8"),
      file,
      { resolve: resolveSync, addDependency: () => {} },
      { cache: false },
    );
    const [lens] = built?.lenses ?? [];
    if (!lens) throw new Error(`${lensFile} holds no lens`);
    return { bytecode: lens.artifact.bytecode, solidity: lens.printed.solidity };
  }

  remove(): void {
    fs.rmSync(this.dir, { recursive: true, force: true });
    fs.rmSync(this.importLogFile, { force: true });
  }
}
