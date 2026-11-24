/**
 * The plugin in Vite: a production build, and the dev server as its watch mode.
 * Runs the built package: `pnpm test` builds first.
 */
import { describeHost } from "./helpers/host-suite.js";
import { bin, bytecodeIn, freePort } from "./helpers/project.js";

describeHost({
  name: "Vite",
  template: "app",
  run: (project) => project.run([bin("vite"), "build"]),
  served: (project) => project.bytecodeUnder("dist"),
  output: { directories: ["dist"] },
  async watch(project) {
    const port = await freePort();
    const server = project.start([bin("vite"), "--host", "127.0.0.1", "--port", String(port), "--strictPort"]);
    return {
      // The module as the dev server hands it to a browser.
      async served() {
        try {
          return bytecodeIn(await (await fetch(`http://127.0.0.1:${port}/src/lens.ts`)).text());
        } catch {
          return undefined;
        }
      },
      output: server.output,
      stop: server.stop,
    };
  },
});
