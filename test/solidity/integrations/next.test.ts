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

  it("keys Turbopack's cache on the versions that decide the output", async () => {
    const { default: manifest } = await import("../../../package.json", { with: { type: "json" } });
    const [own, solc, parser] = loaderOf(withSolidity())?.options.cacheKey.split("+") ?? [];
    expect(own).toBe(manifest.version);
    expect(solc).toBe(manifest.devDependencies.solc);
    expect(parser).toBe(manifest.devDependencies["oxc-parser"]);
  });
});
