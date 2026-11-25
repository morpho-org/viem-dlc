/**
 * The Next adapter: `withSolidity` and its loader, under `next build` and `next dev` with Turbopack.
 * Runs the built package: `pnpm test` builds first.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { withSolidity } from "../../../src/solidity/build/adapters/next.js";
import { type Evm, requireAnvil, startAnvil } from "../helpers/anvil.js";

import { describeHost } from "./helpers/host-suite.js";
import { type Background, bin, freePort, Project, ROOT, waitFor } from "./helpers/project.js";

const startDev = async (project: Project, ...flags: string[]) => {
  const port = await freePort();
  const server = project.start([bin("next"), "dev", "--port", String(port), ...flags]);
  const get = async <T>(route: string): Promise<T | undefined> => {
    try {
      const response = await fetch(`http://localhost:${port}${route}`);
      return response.ok ? ((await response.json()) as T) : undefined;
    } catch {
      return undefined;
    }
  };
  return { server, get };
};

const watchDev = async (project: Project, ...flags: string[]) => {
  const { server, get } = await startDev(project, ...flags);
  return {
    served: async () => (await get<{ bytecode: string }>("/api/quote"))?.bytecode,
    output: server.output,
    stop: server.stop,
  };
};

describeHost({
  name: "Next with Turbopack",
  template: "next",
  run: (project) => project.run([bin("next"), "build"]),
  // The server bundles of the page and of the route handler both hold the lens.
  served: (project) => project.bytecodeUnder(".next/server"),
  output: { directories: [".next"], caches: [".next/cache"] },
  watch: (project) => watchDev(project),
  // With server fast refresh, Next can keep serving a module whose loader once failed, after Turbopack
  // has built it again, and it does so for a loader of ten lines that only throws. Without it the
  // server loads what Turbopack built, which is what the loader answers for.
  watchAfterFailure: (project) => watchDev(project, "--no-server-fast-refresh"),
});

describe("Next with Turbopack, beyond the shared suite", { timeout: 240_000 }, () => {
  let project: Project;
  let evm: Evm | undefined;
  let server: Background | undefined;
  beforeAll(async () => {
    project = new Project("next");
    if (requireAnvil()) evm = await startAnvil();
  }, 60_000);
  afterAll(async () => {
    await server?.stop();
    evm?.stop();
    project?.remove();
  });

  it.skipIf(!requireAnvil())("a request to next dev executes the lens", async () => {
    const dev = await startDev(project);
    server = dev.server;
    const { url: rpc } = (evm as Evm).client.transport as { url?: string };
    expect(rpc).toMatch(/^http:/);
    const response = await waitFor(
      () => dev.get<{ quote: Record<string, string> }>(`/api/quote?rpc=${rpc}`),
      "the route to answer",
      dev.server.output,
    );
    // 3 assets at a price of 2000 with a 0.3% fee, as the route asks.
    expect(response.quote).toEqual({
      gross: String(6000n * 10n ** 18n),
      fee: String(18n * 10n ** 18n),
      net: String(5982n * 10n ** 18n),
    });
  });

  it("a webpack build stops at once, naming Turbopack", async () => {
    const run = await project.run([bin("next"), "build", "--webpack"]);
    expect(run.ok, run.output).toBe(false);
    expect(run.output).toContain("compiles lenses with Turbopack only, and this build runs webpack");
  });
});

describe("a Next build, beyond the shared suite", { timeout: 240_000 }, () => {
  let project: Project;
  beforeAll(() => {
    project = new Project("next");
  }, 60_000);
  afterAll(() => project?.remove());

  it("an application's own rule for the same files still runs, on a module with a lens and on one without", async () => {
    project.restore();
    // The application's loader marks a module, and says so when a lens was still in it.
    project.write(
      "mark-loader.cjs",
      `module.exports = function (source) {
  return source.replace(":unmarked", source.includes(".implementation(") ? ":marked before its lens was compiled" : ":marked");
};
`,
    );
    project.edit(
      "next.config.mjs",
      "turbopack: { root:",
      `turbopack: {
    rules: { "*.{ts,tsx}": { loaders: [fileURLToPath(new URL("./mark-loader.cjs", import.meta.url))] } },
    root:`,
    );
    project.write(
      "src/marked/lens.ts",
      `${project.read("src/lens.ts").replace('"./lib/constants"', '"../lib/constants"')}\nexport const WITH_LENS = "with a lens:unmarked";\n`,
    );
    project.write("src/marked/plain.ts", 'export const WITHOUT_LENS = "without a lens:unmarked";\n');
    project.write(
      "src/app/api/marked/route.ts",
      `import { QuoteLens, WITH_LENS } from "@/marked/lens";
import { WITHOUT_LENS } from "@/marked/plain";

export const dynamic = "force-static";

export function GET() {
  return Response.json({ bytecode: QuoteLens.bytecode, marks: [WITH_LENS, WITHOUT_LENS] });
}
`,
    );
    const built = await project.run([bin("next"), "build"]);
    expect(built.ok, built.output).toBe(true);
    expect(JSON.parse(project.read(".next/server/app/api/marked.body"))).toEqual({
      bytecode: (await project.expectedLens()).bytecode,
      marks: ["with a lens:marked", "without a lens:marked"],
    });
  });
});

describe("withSolidity", () => {
  const loaderOf = (config: ReturnType<typeof withSolidity>) => {
    const rule = config.turbopack?.rules?.["*.{ts,tsx}"] as
      | { loaders: { loader: string; options: { cacheKey: string } }[] }
      | undefined;
    return rule?.loaders[0];
  };

  it("adds its rule and keeps the rest of the configuration", () => {
    const config = withSolidity({
      reactStrictMode: true,
      turbopack: { root: ROOT, rules: { "*.svg": { loaders: ["svg-loader"], as: "*.js" } } },
    });
    expect(config.reactStrictMode).toBe(true);
    expect(config.turbopack?.root).toBe(ROOT);
    expect(Object.keys(config.turbopack?.rules ?? {})).toEqual(["*.svg", "*.{ts,tsx}"]);
    expect(loaderOf(config)?.loader).toBe("@morpho-org/viem-dlc/solidity/next/loader");
  });

  it.each([
    {
      kind: "the rule",
      existing: { loaders: ["mark-loader"], as: "*.ts" },
      kept: [{ loaders: ["mark-loader"], as: "*.ts" }],
    },
    {
      kind: "the list of loaders",
      existing: ["mark-loader", { loader: "other-loader", options: { mark: true } }],
      kept: [{ loaders: ["mark-loader", { loader: "other-loader", options: { mark: true } }] }],
    },
    {
      kind: "the list of rules",
      existing: [{ condition: "browser", loaders: ["mark-loader"] }, { loaders: ["other-loader"] }],
      kept: [{ condition: "browser", loaders: ["mark-loader"] }, { loaders: ["other-loader"] }],
    },
  ])("keeps $kind the application already has for the same files, after its own", ({ existing, kept }) => {
    const rules = withSolidity({ turbopack: { rules: { "*.{ts,tsx}": existing } } }).turbopack?.rules?.["*.{ts,tsx}"];
    expect(rules).toEqual([
      expect.objectContaining({
        loaders: [expect.objectContaining({ loader: "@morpho-org/viem-dlc/solidity/next/loader" })],
      }),
      ...kept,
    ]);
  });

  it("keys Turbopack's cache on the versions that decide the output", async () => {
    const { default: manifest } = await import("../../../package.json", { with: { type: "json" } });
    const [own, solc, parser] = loaderOf(withSolidity())?.options.cacheKey.split("+") ?? [];
    expect(own).toBe(manifest.version);
    expect(solc).toBe(manifest.devDependencies.solc);
    expect(parser).toBe(manifest.devDependencies["oxc-parser"]);
  });
});
