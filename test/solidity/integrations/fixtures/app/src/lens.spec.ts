// Run by Vitest inside the fixture project. The line it prints is how the outer test reads the
// bytecode the host served.
import { expect, it } from "vitest";

import { QuoteLens } from "./lens";

it("the lens is compiled", () => {
  expect(QuoteLens.name).toBe("QuoteLens");
  console.log(JSON.stringify({ bytecode: QuoteLens.bytecode }));
});
