/**
 * The plugin in tsdown, which builds with Rolldown: a library build, and watch mode.
 * Runs the built package: `pnpm test` builds first.
 */
import { describeHost } from "./helpers/host-suite.js";
import { bin } from "./helpers/project.js";

describeHost({
  name: "tsdown",
  template: "app",
  run: (project) => project.run([bin("tsdown")]),
  served: (project) => project.bytecodeUnder("dist"),
  output: { directories: ["dist"] },
  async watch(project) {
    const watcher = project.start([bin("tsdown"), "--watch"]);
    return { served: () => project.bytecodeUnder("dist"), output: watcher.output, stop: watcher.stop };
  },
});
