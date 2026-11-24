/**
 * Immutables: a module-level `immutable(T)` is a Solidity immutable and a constructor argument, in the order the
 * contract lists them, and `Lens.with({ … })` supplies the values.
 *
 * The lens below is written inline and goes through the Vite plugin (see vitest.config.ts), so by the time
 * this module runs it is its compiled lens, and with() is the real one.
 */
import { address, contract, iface, immutable, internal, uint8, uint256, view } from "@morpho-org/viem-dlc/solidity";
import { encodeAbiParameters, getAddress, getContractAddress, parseAbi, parseAbiParameters } from "viem";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { type Evm, requireAnvil, startAnvil } from "./helpers/anvil.js";
import { buildSnippet, compilePair, expectSameAst, expectSameExecution, type FixturePair } from "./helpers/harness.js";
import { deployMocks, type Mocks, withConstructorArgs } from "./helpers/mocks.js";

const TOKEN = "0x00000000000000000000000000000000000004d2";

const IFeed = iface({ price: view(address).returns(uint256) });
const FEED = immutable(IFeed);
const SCALE = immutable(uint256);
const SHIFT = immutable(uint8);

const scaled = internal(view(uint256).returns(uint256), (x) => x * SCALE);

const ScaledFeedLens = contract(
  "ScaledFeedLens",
  { quote: view(address).returns(uint256), feed: view().returns(address) },
  // The key is the name with() takes; the order here is the constructor's.
  { immutables: { feed: FEED, scale: SCALE, shift: SHIFT } },
).implementation({
  quote: (token) => scaled(FEED.price(token)) << SHIFT,
  feed: () => address(FEED),
});

const pairs = {
  "immutables, read by an entry point and a helper": {
    ts: `
      import { address, contract, iface, immutable, internal, uint8, uint256, view } from "@morpho-org/viem-dlc/solidity";

      const IFeed = iface({ price: view(address).returns(uint256) });
      const FEED = immutable(IFeed);
      const SCALE = immutable(uint256);
      const SHIFT = immutable(uint8);

      const scaled = internal(view(uint256).returns(uint256), (x) => x * SCALE);

      export const ScaledFeedLens = contract(
        "ScaledFeedLens",
        { quote: view(address).returns(uint256), feed: view().returns(address) },
        { immutables: { feed: FEED, scale: SCALE, shift: SHIFT } },
      ).implementation({
        quote: (token) => scaled(FEED.price(token)) << SHIFT,
        feed() {
          const feed = FEED;
          return address(feed);
        },
      });
    `,
    solidity: `
      interface IFeed {
          function price(address) external view returns (uint256);
      }

      contract ScaledFeedLens {
          IFeed immutable FEED;
          uint256 immutable SCALE;
          uint8 immutable SHIFT;

          constructor(IFeed FEED_, uint256 SCALE_, uint8 SHIFT_) {
              FEED = FEED_;
              SCALE = SCALE_;
              SHIFT = SHIFT_;
          }

          function quote(address token) external view returns (uint256) {
              return scaled(FEED.price(token)) << SHIFT;
          }

          function feed() external view returns (address) {
              IFeed feed = FEED;
              return address(feed);
          }

          function scaled(uint256 x) internal view returns (uint256) {
              return x * SCALE;
          }
      }
    `,
  },
  "a constructor parameter never takes a name the lens declares": {
    ts: `
      import { contract, immutable, uint8, uint256, view } from "@morpho-org/viem-dlc/solidity";

      const SCALE = immutable(uint256);
      const SCALE_ = immutable(uint8);

      export const ShiftLens = contract(
        "ShiftLens",
        { shifted: view(uint256).returns(uint256) },
        { immutables: { SCALE_, SCALE } },
      ).implementation({
        shifted: (x) => (x * SCALE) << SCALE_,
      });
    `,
    solidity: `
      contract ShiftLens {
          uint8 immutable SCALE_;
          uint256 immutable SCALE;

          constructor(uint8 SCALE__, uint256 SCALE___) {
              SCALE_ = SCALE__;
              SCALE = SCALE___;
          }

          function shifted(uint256 x) external view returns (uint256) {
              return (x * SCALE) << SCALE_;
          }
      }
    `,
  },
} satisfies Record<string, FixturePair>;

describe("fixture pairs: immutables", () => {
  describe("the printed Solidity is the handwritten Solidity", () => {
    for (const [name, pair] of Object.entries(pairs))
      it(name, async () => {
        expectSameAst(await compilePair(pair));
      });
  });

  describe.skipIf(!requireAnvil())("both execute to the same bytes", () => {
    let evm: Evm;
    let mocks: Mocks;
    beforeAll(async () => {
      evm = await startAnvil();
      mocks = await deployMocks(evm);
    });
    afterAll(() => evm?.stop());

    it("immutables, read by an entry point and a helper", async () => {
      const pair = await compilePair(pairs["immutables, read by an entry point and a helper"]);
      const calls = [{ functionName: "quote", args: [TOKEN] }, { functionName: "feed" }];
      const [quote, feed] = await expectSameExecution(evm, withConstructorArgs(pair, [mocks.feed, 3n, 4]), calls);
      // The mock feed prices a token at its address modulo a million, plus one.
      expect(BigInt(quote?.data ?? 0)).toBe(((BigInt(TOKEN) % 1_000_000n) + 1n) * 3n * 16n);
      expect(feed?.data).toBe(encodeAbiParameters(parseAbiParameters("address"), [mocks.feed]));
      // Other constructor arguments are another lens: the feed that reverts, and a scale that overflows.
      const [broken] = await expectSameExecution(evm, withConstructorArgs(pair, [mocks.broken, 3n, 4]), calls);
      expect(broken?.data.slice(0, 10)).toBe("0x08c379a0");
      const [overflow] = await expectSameExecution(evm, withConstructorArgs(pair, [mocks.feed, 2n ** 255n, 4]), calls);
      expect(overflow?.data.slice(0, 10)).toBe("0x4e487b71");
    });

    it("a constructor parameter never takes a name the lens declares", async () => {
      const pair = await compilePair(pairs["a constructor parameter never takes a name the lens declares"]);
      const [shifted] = await expectSameExecution(evm, withConstructorArgs(pair, [4, 3n]), [
        { functionName: "shifted", args: [5n] },
      ]);
      expect(BigInt(shifted?.data ?? 0)).toBe(5n * 3n * 16n);
    });
  });
});

describe("an inline lens with immutables, through the Vite plugin", () => {
  it("is compiled with a constructor that takes them in the order the contract lists them", () => {
    expect(ScaledFeedLens.abi).toContainEqual({
      type: "constructor",
      stateMutability: "nonpayable",
      inputs: [
        { name: "FEED_", type: "address", internalType: "contract IFeed" },
        { name: "SCALE_", type: "uint256", internalType: "uint256" },
        { name: "SHIFT_", type: "uint8", internalType: "uint8" },
      ],
    });
  });

  it("with() appends the values to the bytecode, by the names the contract lists them under", () => {
    const feed = "0x00000000000000000000000000000000000000aa";
    const call = ScaledFeedLens.with({ shift: 4n, feed, scale: 3n });
    const initcode =
      `${ScaledFeedLens.bytecode}${encodeAbiParameters(parseAbiParameters("address, uint256, uint8"), [feed, 3n, 4]).slice(2)}` as const;
    expect(call.factoryData.endsWith(initcode.slice(2))).toBe(true);
    expect(call.address).toBe(
      getContractAddress({
        opcode: "CREATE2",
        from: call.factory,
        salt: `0x${call.factoryData.slice(2, 66)}`,
        bytecode: initcode,
      }),
    );
    expect(ScaledFeedLens.with({ shift: 5n, feed, scale: 3n }).address).not.toBe(call.address);
  });

  describe.skipIf(!requireAnvil())("executed", () => {
    let evm: Evm;
    let mocks: Mocks;
    beforeAll(async () => {
      evm = await startAnvil();
      mocks = await deployMocks(evm);
    });
    afterAll(() => evm?.stop());

    it("through viem's deployless call, the immutables round-trip", async () => {
      const lens = ScaledFeedLens.with({ feed: mocks.feed, scale: 3n, shift: 4n });
      expect(await evm.client.readContract({ ...lens, functionName: "feed" })).toBe(getAddress(mocks.feed));
      const price = await evm.client.readContract({
        abi: parseAbi(["function price(address) view returns (uint256)"]),
        address: mocks.feed,
        functionName: "price",
        args: [TOKEN],
      });
      expect(await evm.client.readContract({ ...lens, functionName: "quote", args: [TOKEN] })).toBe((price * 3n) << 4n);
    });

    it("other values are another lens at another address", async () => {
      const lens = ScaledFeedLens.with({ feed: mocks.broken, scale: 1n, shift: 0n });
      expect(await evm.client.readContract({ ...lens, functionName: "feed" })).toBe(getAddress(mocks.broken));
      await expect(evm.client.readContract({ ...lens, functionName: "quote", args: [TOKEN] })).rejects.toThrow(
        /broken/,
      );
    });
  });
});

describe("what the transform prints for immutables", () => {
  it("is each immutable once, under its own name, however the contract's key spells it", async () => {
    const built = await buildSnippet(pairs["immutables, read by an entry point and a helper"].ts);
    const solidity = built.lenses[0]?.printed.solidity ?? "";
    expect(solidity).toContain("    IFeed immutable FEED;\n    uint256 immutable SCALE;\n    uint8 immutable SHIFT;\n");
    expect(solidity).toContain("constructor(IFeed FEED_, uint256 SCALE_, uint8 SHIFT_) {");
    expect(solidity).not.toMatch(/immutable (feed|scale|shift)\b/);
  });
});
