import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    // Pinned rather than left to the default glob, which would reach into `playground/` -- its own
    // pnpm project, whose dependencies this runner cannot resolve.
    include: ["test/**/*.test.ts"],
  },
  benchmark: {
    include: ["test/bench/**/*.bench.ts"],
  },
});
