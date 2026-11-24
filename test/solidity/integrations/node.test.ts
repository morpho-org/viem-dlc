/**
 * The two integrations that run without a bundler: the `--import` hook, and the check command.
 * Runs the built package: `pnpm test` builds first.
 */
import { describeHost } from "./helpers/host-suite.js";
import { lastPrintedBytecode } from "./helpers/project.js";

const HOOK = ["--import", "@morpho-org/viem-dlc/solidity/register", "script.ts"];

describeHost({
  name: "the Node hook",
  template: "app",
  run: (project) => project.run(HOOK),
  served: (_project, run) => lastPrintedBytecode(run.output),
  async watch(project) {
    const watcher = project.start(["--watch", ...HOOK]);
    return { served: () => lastPrintedBytecode(watcher.output()), output: watcher.output, stop: watcher.stop };
  },
});

describeHost({
  name: "the check command",
  template: "app",
  run: (project) =>
    project.run([
      project.file("node_modules/@morpho-org/viem-dlc/dist/solidity/build/adapters/cli.js"),
      "check",
      "--print",
    ]),
  // The command serves no bytecode: it prints each lens's size and, with --print, its Solidity.
  served(_project, run) {
    const printed = /^ok {2}QuoteLens {2}src\/lens\.ts {2}(\d+) bytes\n([\s\S]*?)\n\d+ files scanned/m.exec(run.output);
    return printed ? `${printed[1]} bytes\n${printed[2]}` : undefined;
  },
  expected: (lens) => `${lens.bytecode.length / 2 - 1} bytes\n${lens.solidity}`,
});
