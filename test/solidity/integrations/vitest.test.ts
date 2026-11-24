/**
 * The plugin in Vitest: a test run, and watch mode.
 * Runs the built package: `pnpm test` builds first.
 */
import { describeHost } from "./helpers/host-suite.js";
import { bin, lastPrintedBytecode } from "./helpers/project.js";

describeHost({
  name: "Vitest",
  template: "app",
  run: (project) => project.run([bin("vitest"), "run"]),
  served: (_project, run) => lastPrintedBytecode(run.output),
  async watch(project) {
    const watcher = project.start([bin("vitest"), "watch"]);
    return { served: () => lastPrintedBytecode(watcher.output()), output: watcher.output, stop: watcher.stop };
  },
});
