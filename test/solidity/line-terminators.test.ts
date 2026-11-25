/**
 * Line terminators. ECMAScript ends a line on a line feed, a carriage return, a carriage return and a
 * line feed together, U+2028 and U+2029, so every position the build reports counts all five, and the
 * rewrite keeps every line of a module on the line it had.
 */
import path from "node:path";

import { parseSync } from "oxc-parser";
import { describe, expect, it } from "vitest";

import { transform } from "../../src/solidity/build/index.js";

import { expectRejection } from "./helpers/harness.js";

const TERMINATORS = [
  ["a line feed", "\n"],
  ["a carriage return and a line feed", "\r\n"],
  ["a carriage return", "\r"],
  ["a line separator", "\u2028"],
  ["a paragraph separator", "\u2029"],
] as const;

const LINE_BREAK = /\r\n|[\n\r\u2028\u2029]/;

describe.each(TERMINATORS)("a module whose lines end in %s", (_, terminator) => {
  const module = (...lines: string[]) => lines.join(terminator);

  it("reports what solc refuses at its line and column", async () => {
    const code = module(
      'import { contract, pure, uint128, uint256 } from "@morpho-org/viem-dlc/solidity";',
      'export const Lens = contract("Lens", { read: pure(uint256).returns(uint128) }).implementation({',
      "  read(input) {",
      "    return input;",
      "  },",
      "});",
    );
    await expectRejection(code, {
      message: "Return argument type uint256 is not implicitly convertible to expected type",
      line: 4,
      column: 12,
      source: "solc",
    });
  });

  it("reports what the transform refuses at its line and column", async () => {
    const code = module(
      'import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";',
      "",
      'export const Lens = contract("Lens", { read: pure(uint256).returns(uint256) }).implementation({',
      "  read(input) {",
      "    const doubled = uint256(input * 2n);",
      "    return doubled ?? input;",
      "  },",
      "});",
    );
    await expectRejection(code, { message: "a lens has no operator ??", line: 6, column: 12, source: "transform" });
  });

  it("reports a module that does not parse at its line and column", async () => {
    const code = module(
      'import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";',
      'export const Lens = contract("Lens", { read: pure(uint256).returns(uint256) }).implementation({',
      "  read(input) {",
      "    return input +;",
      "  },",
      "});",
    );
    await expectRejection(code, { message: "cannot parse this module", line: 4, column: 19, source: "transform" });
  });

  it("is rewritten with every line on the line it had", async () => {
    const code = module(
      "import {",
      "  contract,",
      "  internal,",
      "  pure,",
      "  uint256,",
      '} from "@morpho-org/viem-dlc/solidity";',
      "",
      'export const BEFORE = "before";',
      "const double = internal(",
      "  pure(uint256).returns(uint256),",
      "  (x) => x * 2n,",
      ");",
      'export const BETWEEN = "between";',
      'export const Lens = contract("Lens", {',
      "  read: pure(uint256).returns(uint256),",
      "}).implementation({",
      "  read(input) {",
      "    return double(input);",
      "  },",
      "});",
      'export const AFTER = "after";',
      "",
    );
    const file = path.join(import.meta.dirname, "__fixtures__", "lens.ts");
    const result = await transform(code, file, { resolve: () => undefined, addDependency: () => {} }, { cache: false });
    const rewritten = result?.code ?? "";
    expect(parseSync(file, rewritten).errors).toEqual([]);
    expect(rewritten).not.toContain("x * 2n");
    expect(rewritten).not.toContain("return double(input)");

    const before = code.split(LINE_BREAK);
    const after = rewritten.split(LINE_BREAK);
    expect(after.length).toBe(before.length);
    for (const marker of ["BEFORE", "BETWEEN", "AFTER"]) {
      const line = before.findIndex((text) => text.includes(marker));
      expect(after[line], marker).toBe(before[line]);
    }
    // No terminator of another kind was added to the module.
    expect(new Set(rewritten.match(new RegExp(LINE_BREAK, "g")))).toEqual(new Set([terminator]));
  });
});

describe("a module that mixes line terminators", () => {
  it("counts each terminator once, and a carriage return with its line feed as one", async () => {
    const code = [
      'import { contract, pure, uint128, uint256 } from "@morpho-org/viem-dlc/solidity";\r\n',
      "// a comment line\r",
      "// another\u2028",
      "// another\u2029",
      'export const Lens = contract("Lens", { read: pure(uint256).returns(uint128) }).implementation({\n',
      "  read(input) {\r\n",
      "    return input;\r\n",
      "  },\r\n",
      "});\r\n",
    ].join("");
    await expectRejection(code, {
      message: "Return argument type uint256 is not implicitly convertible to expected type",
      line: 7,
      column: 12,
      source: "solc",
    });
  });
});
