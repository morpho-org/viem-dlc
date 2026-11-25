/**
 * Module constants a lens reads. A constant is the one value JavaScript and a lens both read, so it is
 * built only from what means the same in both: bigint literals, booleans, strings that do not start
 * with 0x, other such constants, and the operators where solc's literal arithmetic and BigInt agree.
 *
 * The lens below is compiled by the Vite plugin (see vitest.config.ts), so this module reads the very
 * constants the lens reads, as JavaScript.
 */
import { bool, contract, int256, pure, string, uint256 } from "@morpho-org/viem-dlc/solidity";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { type Evm, requireAnvil, startAnvil } from "./helpers/anvil.js";
import { IMPORTS } from "./helpers/core.js";
import { buildSnippet, expectRejection, positionIn } from "./helpers/harness.js";

const PERIOD = 1n << 32n;
const MASK = ((PERIOD - 1n) ^ 0xff00n) | (0x0fn & 0x3cn);
const OFFSET = (-(3n ** 4n) * 2n) % 5n;
const SHIFTED = ~PERIOD >> 4n;
const LABEL = "period";
const ENABLED = true;

const SharedLens = contract("SharedLens", {
  period: pure().returns(uint256),
  mask: pure().returns(uint256),
  offset: pure().returns(int256),
  shifted: pure().returns(int256),
  label: pure().returns(string),
  enabled: pure().returns(bool),
}).implementation({
  period: () => PERIOD,
  mask: () => MASK,
  offset: () => OFFSET,
  shifted: () => SHIFTED,
  label: () => LABEL,
  enabled: () => ENABLED,
});

describe.skipIf(!requireAnvil())("a constant means in a lens what it means in JavaScript", () => {
  let evm: Evm;
  beforeAll(async () => {
    evm = await startAnvil();
  });
  afterAll(() => evm?.stop());

  const read = (functionName: "period" | "mask" | "offset" | "shifted" | "label" | "enabled") =>
    evm.client.readContract({ abi: SharedLens.abi, code: SharedLens.bytecode, functionName });

  it("a shift, which JavaScript computes in 32 bits for Numbers and exactly for bigints", async () => {
    expect(PERIOD).toBe(4294967296n);
    expect(await read("period")).toBe(PERIOD);
  });

  it("bitwise operators, negation, a power, a remainder and a shift of a negative value", async () => {
    expect(await read("mask")).toBe(MASK);
    expect(await read("offset")).toBe(OFFSET);
    expect(await read("shifted")).toBe(SHIFTED);
    expect([OFFSET, SHIFTED]).toEqual([-2n, -268435457n]);
  });

  it("a string and a boolean", async () => {
    expect(await read("label")).toBe(LABEL);
    expect(await read("enabled")).toBe(ENABLED);
  });
});

describe("a constant a lens reads: rejections", () => {
  const NUMBER = "a Number literal cannot be part of a constant a lens reads";
  const HEX = "a string that starts with 0x cannot be a constant a lens reads";
  const lens = (constants: string, returns = "uint256", read = "LIMIT") => `${IMPORTS}
${constants}
export const Lens = contract("Lens", { f: pure().returns(${returns}) }).implementation({
  f: () => ${read},
});
`;

  it("a Number shifted, which is 1 in JavaScript and 4294967296 in Solidity", async () => {
    const code = lens("export const LIMIT = 1 << 32;");
    const diagnostic = await expectRejection(code, { message: NUMBER, ...positionIn(code, "1 << 32") });
    expect(diagnostic.message).toContain("n suffix");
  });

  it("a Number literal alone", async () => {
    const code = lens("const LIMIT = 100;");
    await expectRejection(code, { message: NUMBER, ...positionIn(code, "100;") });
  });

  it("a Number literal beside a bigint", async () => {
    const code = lens("const LIMIT = 2n ** 64n - BigInt(1);");
    await expectRejection(code, { message: "a call", ...positionIn(code, "BigInt(1)") });
    const mixed = lens("const LIMIT = (2n ** 64n) << 0x10;");
    await expectRejection(mixed, { message: NUMBER, ...positionIn(mixed, "0x10") });
  });

  it("a Number literal in a constant another constant is built from, in another module", async () => {
    const units = "export const BASE = 1e3;\nexport const DOUBLE = 2n;\n";
    const code = `import { BASE, DOUBLE } from "./units.js";\n${lens("const LIMIT = DOUBLE * BASE;")}`;
    await expectRejection(
      { "lens.ts": code, "units.ts": units },
      { message: NUMBER, file: "units.ts", ...positionIn(units, "1e3") },
    );
  });

  it("a Number constant named inside an assembly block", async () => {
    const code = `${IMPORTS}
import { assembly } from "@morpho-org/viem-dlc/solidity";
import { add } from "@morpho-org/viem-dlc/solidity/yul";
export const LIMIT = 9007199254740993;
export const Lens = contract("Lens", { f: pure(uint256).returns(uint256) }).implementation({
  f(x) {
    const sum = assembly("memory-safe", () => add(x, LIMIT));
    return sum;
  },
});
`;
    await expectRejection(code, { message: NUMBER, ...positionIn(code, "9007199254740993") });
  });

  it("a hex-looking string, which is text in JavaScript and bytes in a body", async () => {
    const code = lens('export const LIMIT = "0x6162";', "string");
    const diagnostic = await expectRejection(code, { message: HEX, ...positionIn(code, '"0x6162"') });
    expect(diagnostic.message).toContain("Write the literal in the body");
  });

  it("an address-looking string, an empty hex string and one with an odd digit", async () => {
    for (const [literal, returns] of [
      ['"0x52908400098527886E0F7030069857D2E4169EE7"', "address"],
      ['"0x"', "bytes"],
      ['"0x123"', "string"],
    ] as const) {
      const code = lens(`const LIMIT = ${literal};`, returns);
      await expectRejection(code, { message: HEX, ...positionIn(code, literal) });
    }
  });

  it("neither rule reaches a constant only JavaScript reads, or a literal written in a body", async () => {
    const code = `${IMPORTS}
export const PERIOD = 1 << 32;
export const SELECTOR = "0x313ce567";
const LIMIT = 100n;
export const Lens = contract("Lens", {
  limit: pure().returns(uint256),
  selector: pure().returns(bytes4),
  text: pure().returns(string),
}).implementation({
  limit: () => LIMIT + 1,
  selector: () => "0x313ce567",
  text: () => "0x123",
});
`;
    const built = await buildSnippet(code);
    const solidity = built.lenses[0]?.printed.solidity ?? "";
    expect(solidity).toContain("return (100) + 1;");
    expect(solidity).toContain('return hex"313ce567";');
    expect(solidity).toContain('return "0x123";');
  });
});

describe("a division in a constant a lens reads", () => {
  const DIVISION = "a constant a lens reads cannot use /";
  const lens = (expression: string) => `${IMPORTS}
const SCALE = ${expression};
export const Lens = contract("Lens", { f: pure().returns(uint256) }).implementation({
  f: () => SCALE,
});
`;

  it("is reported at the operator", async () => {
    const code = lens("7n / 2n");
    await expectRejection(code, { message: DIVISION, ...positionIn(code, "/ 2n") });
  });

  it("is reported at the operator after a block comment that holds a slash", async () => {
    const code = lens("7n /* amounts */ / 2n");
    await expectRejection(code, { message: DIVISION, ...positionIn(code, "/ 2n") });
    const several = lens("7n /* a / b */ /* c */ / /* d / e */ 2n");
    await expectRejection(several, { message: DIVISION, ...positionIn(several, "/ /* d") });
  });

  it("is reported at the operator after a line comment that holds a slash", async () => {
    const code = lens("7n // per / unit\n  / 2n");
    await expectRejection(code, { message: DIVISION, ...positionIn(code, "/ 2n") });
  });

  it("is reported at the operator between parenthesized operands", async () => {
    const code = lens("(7n /* a / b */) / (2n)");
    await expectRejection(code, { message: DIVISION, ...positionIn(code, "/ (2n)") });
  });
});
