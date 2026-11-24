/**
 * The vertical slice: a lens written inline in this ordinary test file goes find -> resolve -> print ->
 * compile -> rewrite through the Vite plugin (see vitest.config.ts), and is then executed.
 *
 * By the time this module runs, `contract(…).implementation(…)` below has been replaced by its
 * compiled lens, and the helper, the constant and the imports only the body used are gone.
 */
import { contract, internal, pure, Struct, uint256 } from "@morpho-org/viem-dlc/solidity";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { type Evm, requireAnvil, startAnvil } from "./helpers/anvil.js";

const WAD = 10n ** 18n;

class Quote extends Struct({ gross: uint256, fee: uint256, net: uint256 }) {}

const mulDivDown = internal(pure(uint256, uint256, uint256).returns(uint256), (x, y, d) => (x * y) / d);

const QuoteLens = contract("QuoteLens", {
  quote: pure(uint256, uint256, uint256).returns(Quote),
}).implementation({
  quote(assets, price, feeRate) {
    const o = new Quote();
    o.gross = mulDivDown(assets, price, WAD);
    o.fee = mulDivDown(o.gross, feeRate, WAD);
    o.net = o.gross - o.fee;
    return o;
  },
});

describe("an inline lens, through the Vite plugin", () => {
  it("is replaced by its compiled lens", () => {
    expect(QuoteLens.name).toBe("QuoteLens");
    expect(QuoteLens.bytecode).toMatch(/^0x60[0-9a-f]+$/);
    expect(QuoteLens.abi).toEqual([
      {
        type: "function",
        name: "quote",
        stateMutability: "pure",
        inputs: [
          { name: "assets", type: "uint256", internalType: "uint256" },
          { name: "price", type: "uint256", internalType: "uint256" },
          { name: "feeRate", type: "uint256", internalType: "uint256" },
        ],
        outputs: [
          {
            name: "",
            type: "tuple",
            internalType: "struct Quote",
            components: [
              { name: "gross", type: "uint256", internalType: "uint256" },
              { name: "fee", type: "uint256", internalType: "uint256" },
              { name: "net", type: "uint256", internalType: "uint256" },
            ],
          },
        ],
      },
    ]);
  });

  describe.skipIf(!requireAnvil())("executed", () => {
    let evm: Evm;
    beforeAll(async () => {
      evm = await startAnvil();
    });
    afterAll(() => evm?.stop());

    // 3 assets at a price of 2000 with a 0.3% fee.
    const args = [3n * WAD, 2000n * WAD, 3n * 10n ** 15n] as const;
    const expected = { gross: 6000n * WAD, fee: 18n * WAD, net: 5982n * WAD };

    it("through viem's deployless call, from its bytecode", async () => {
      const quote = await evm.client.readContract({
        abi: QuoteLens.abi,
        code: QuoteLens.bytecode,
        functionName: "quote",
        args,
      });
      expect(quote).toEqual(expected);
    });

    it("through viem's deployless call, from the descriptor with() gives", async () => {
      const quote = await evm.client.readContract({ ...QuoteLens.with(), functionName: "quote", args });
      // tsc knows the struct's fields from the declaration alone.
      expect(quote.net).toBe(expected.net);
      expect(quote).toEqual(expected);
    });

    it("with Solidity's arithmetic: an overflowing product reverts", async () => {
      await expect(
        evm.client.readContract({ ...QuoteLens.with(), functionName: "quote", args: [2n ** 255n, 2n, 0n] }),
      ).rejects.toThrow(/overflow|0x11/i);
    });
  });
});
