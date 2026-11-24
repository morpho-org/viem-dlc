/**
 * Fixture pairs for what a lens does with the outside world: interfaces and the handles made from them,
 * selectors and encoded calls, libraries, external calls to itself, try/catch, and the built-ins a body
 * can name. Each pair is a TypeScript snippet and the Solidity a person wrote for it; see
 * ./helpers/harness.ts for what is compared. The contracts a lens calls are in ./helpers/mocks.ts.
 */
import { encodeAbiParameters, keccak256, parseAbiParameters, toFunctionSelector, zeroAddress } from "viem";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { type Evm, requireAnvil, startAnvil } from "./helpers/anvil.js";
import {
  buildSnippet,
  compilePair,
  expectSameAst,
  expectSameExecution,
  type FixtureCall,
  type FixturePair,
} from "./helpers/harness.js";
import { deployMocks, type Mocks } from "./helpers/mocks.js";

const HOLDER = "0x00000000000000000000000000000000000004d2";
const OTHER = "0x0000000000000000000000000000000000010932";
const WAD = 10n ** 18n;

interface ExternalPair extends FixturePair {
  calls(mocks: Mocks): FixtureCall[];
}

const pairs = {
  "an interface, handles, and calls through them": {
    ts: `
      import { address, bytes, contract, iface, string, Struct, uint8, uint256, view } from "@morpho-org/viem-dlc/solidity";

      const IToken = iface({
        balanceOf: view(address).returns(uint256),
        totalSupply: view().returns(uint256),
        decimals: view().returns(uint8),
        symbol: view().returns(string),
      });
      const IFeed = iface({
        price: view(address).returns(uint256),
        describe: view(bytes).returns(bytes),
        gasLeft: view().returns(uint256),
        validate: view(address),
      });

      class Reading extends Struct({
        token: IToken,
        balance: uint256,
        supply: uint256,
        decimals: uint8,
        symbol: string,
        value: uint256,
      }) {}

      export const HandleLens = contract("HandleLens", {
        read: view(IToken, IFeed, address).returns(Reading),
        describe: view(address, bytes).returns(bytes),
        capped: view(address, uint256).returns(uint256),
      }).implementation({
        read(token, feed, holder) {
          feed.validate(holder);
          const o = new Reading();
          o.token = token;
          const balance = token.balanceOf(holder);
          o.balance = balance;
          const same = IToken(address(token));
          o.supply = same.totalSupply();
          const decimals = IToken(address(token)).decimals();
          o.decimals = decimals;
          const symbol = o.token.symbol();
          o.symbol = symbol;
          o.value = balance * feed.price(address(token));
          return o;
        },
        describe(feed, tag) {
          const text = IFeed(feed).describe(tag);
          return text;
        },
        capped: (feed, gas) => IFeed(feed, { gas: 50_000 }).gasLeft() + IFeed(feed, { gas }).gasLeft(),
      });
    `,
    solidity: `
      struct Reading {
          IToken token;
          uint256 balance;
          uint256 supply;
          uint8 decimals;
          string symbol;
          uint256 value;
      }

      interface IToken {
          function balanceOf(address) external view returns (uint256);
          function totalSupply() external view returns (uint256);
          function decimals() external view returns (uint8);
          function symbol() external view returns (string memory);
      }

      interface IFeed {
          function price(address) external view returns (uint256);
          function describe(bytes calldata) external view returns (bytes memory);
          function gasLeft() external view returns (uint256);
          function validate(address) external view;
      }

      contract HandleLens {
          function read(IToken token, IFeed feed, address holder) external view returns (Reading memory) {
              feed.validate(holder);
              Reading memory o;
              o.token = token;
              uint256 balance = token.balanceOf(holder);
              o.balance = balance;
              IToken same = IToken(address(token));
              o.supply = same.totalSupply();
              uint8 decimals = IToken(address(token)).decimals();
              o.decimals = decimals;
              string memory symbol = o.token.symbol();
              o.symbol = symbol;
              o.value = balance * feed.price(address(token));
              return o;
          }

          function describe(address feed, bytes calldata tag) external view returns (bytes memory) {
              bytes memory text = IFeed(feed).describe(tag);
              return text;
          }

          function capped(address feed, uint256 gas) external view returns (uint256) {
              return IFeed(feed).gasLeft{gas: 50000}() + IFeed(feed).gasLeft{gas: gas}();
          }
      }
    `,
    calls: ({ token, feed, broken, empty }) => [
      { functionName: "read", args: [token, feed, HOLDER] },
      { functionName: "read", args: [token, feed, OTHER] },
      { functionName: "read", args: [token, feed, zeroAddress] },
      // A call to an account whose calls revert carries the revert out, and one to an account with no code reverts empty.
      { functionName: "read", args: [broken, feed, HOLDER] },
      { functionName: "read", args: [empty, feed, HOLDER] },
      { functionName: "describe", args: [feed, "0x1234"] },
      { functionName: "capped", args: [feed, 30_000n] },
      { functionName: "capped", args: [feed, 90_000n] },
    ],
  },
  "a selector and an encoded call": {
    ts: `
      import { address, bool, bytes, bytes4, contract, iface, pure, uint256, view } from "@morpho-org/viem-dlc/solidity";

      const IToken = iface({
        balanceOf: view(address).returns(uint256),
        totalSupply: view().returns(uint256),
        allowance: view(address, address).returns(uint256),
      });

      export const CalldataLens = contract("CalldataLens", {
        balanceOfSelector: pure().returns(bytes4),
        isAllowance: pure(bytes4).returns(bool),
        balanceOfCall: pure(address).returns(bytes),
        totalSupplyCall: pure().returns(bytes),
        allowanceCall: pure(address, address).returns(bytes),
      }).implementation({
        balanceOfSelector() {
          const selector = IToken.balanceOf.selector;
          return selector;
        },
        isAllowance: (selector) => selector == IToken.allowance.selector,
        balanceOfCall(holder) {
          const data = IToken.balanceOf.encode(holder);
          return data;
        },
        totalSupplyCall: () => IToken.totalSupply.encode(),
        allowanceCall: (owner, spender) => IToken.allowance.encode(owner, spender),
      });
    `,
    solidity: `
      interface IToken {
          function balanceOf(address) external view returns (uint256);
          function totalSupply() external view returns (uint256);
          function allowance(address, address) external view returns (uint256);
      }

      contract CalldataLens {
          function balanceOfSelector() external pure returns (bytes4) {
              bytes4 selector = IToken.balanceOf.selector;
              return selector;
          }

          function isAllowance(bytes4 selector) external pure returns (bool) {
              return selector == IToken.allowance.selector;
          }

          function balanceOfCall(address holder) external pure returns (bytes memory) {
              bytes memory data = abi.encodeCall(IToken.balanceOf, (holder));
              return data;
          }

          function totalSupplyCall() external pure returns (bytes memory) {
              return abi.encodeCall(IToken.totalSupply, ());
          }

          function allowanceCall(address owner, address spender) external pure returns (bytes memory) {
              return abi.encodeCall(IToken.allowance, (owner, spender));
          }
      }
    `,
    calls: () => [
      { functionName: "balanceOfSelector" },
      { functionName: "isAllowance", args: [toFunctionSelector("allowance(address,address)")] },
      { functionName: "isAllowance", args: [toFunctionSelector("balanceOf(address)")] },
      { functionName: "balanceOfCall", args: [HOLDER] },
      { functionName: "totalSupplyCall" },
      { functionName: "allowanceCall", args: [HOLDER, OTHER] },
    ],
  },
  "libraries, called from an entry point, a helper and each other": {
    ts: `
      import { contract, internal, library, pure, require, uint256 } from "@morpho-org/viem-dlc/solidity";

      const WAD = 10n ** 18n;

      const FixedPoint = library("FixedPoint", {
        mulWad: pure(uint256, uint256).returns(uint256),
        divWad: pure(uint256, uint256).returns(uint256),
        square: pure(uint256).returns(uint256),
      }).implementation({
        mulWad: (x, y) => (x * y) / WAD,
        divWad(x, y) {
          require(y != 0n, "division by zero");
          return (x * WAD) / y;
        },
        square: (x) => FixedPoint.mulWad(x, x),
      });

      const Interest = library("Interest", {
        compound: pure(uint256, uint256).returns(uint256),
      }).implementation({
        compound(principal, rate) {
          const once = FixedPoint.mulWad(principal, WAD + rate);
          return FixedPoint.mulWad(once, WAD + rate);
        },
      });

      const half = internal(pure(uint256).returns(uint256), (x) => FixedPoint.divWad(x, 2n * WAD));

      export const LibraryLens = contract("LibraryLens", {
        compound: pure(uint256, uint256).returns(uint256),
        halfSquare: pure(uint256).returns(uint256),
        ratio: pure(uint256, uint256).returns(uint256),
      }).implementation({
        compound: (principal, rate) => Interest.compound(principal, rate),
        halfSquare(x) {
          const squared = FixedPoint.square(x);
          return half(squared);
        },
        ratio: (x, y) => FixedPoint.divWad(x, y),
      });
    `,
    solidity: `
      library FixedPoint {
          function mulWad(uint256 x, uint256 y) internal pure returns (uint256) {
              return x * y / 10 ** 18;
          }

          function divWad(uint256 x, uint256 y) internal pure returns (uint256) {
              require(y != 0, "division by zero");
              return x * 10 ** 18 / y;
          }

          function square(uint256 x) internal pure returns (uint256) {
              return FixedPoint.mulWad(x, x);
          }
      }

      library Interest {
          function compound(uint256 principal, uint256 rate) internal pure returns (uint256) {
              uint256 once = FixedPoint.mulWad(principal, 10 ** 18 + rate);
              return FixedPoint.mulWad(once, 10 ** 18 + rate);
          }
      }

      contract LibraryLens {
          function compound(uint256 principal, uint256 rate) external pure returns (uint256) {
              return Interest.compound(principal, rate);
          }

          function halfSquare(uint256 x) external pure returns (uint256) {
              uint256 squared = FixedPoint.square(x);
              return half(squared);
          }

          function ratio(uint256 x, uint256 y) external pure returns (uint256) {
              return FixedPoint.divWad(x, y);
          }

          function half(uint256 x) internal pure returns (uint256) {
              return FixedPoint.divWad(x, 2 * 10 ** 18);
          }
      }
    `,
    calls: () => [
      { functionName: "compound", args: [1000n * WAD, WAD / 10n] },
      { functionName: "halfSquare", args: [3n * WAD] },
      { functionName: "ratio", args: [WAD, 3n * WAD] },
      { functionName: "ratio", args: [WAD, 0n] },
      { functionName: "halfSquare", args: [2n ** 200n] },
    ],
  },
  "an external call to the lens itself": {
    ts: `
      import { bool, contract, require, Struct, uint256, view } from "@morpho-org/viem-dlc/solidity";

      class Quote extends Struct({ price: uint256, ok: bool }) {}

      export const SelfLens = contract("SelfLens", {
        price: view(uint256).returns(uint256),
        quote: view(uint256).returns(Quote),
        twice: view(uint256).returns(uint256),
      }).implementation({
        price(x) {
          require(x != 0n, "nothing to price");
          return x * 3n;
        },
        quote(x) {
          const o = new Quote();
          const price = this.price(x);
          o.price = price;
          o.ok = true;
          return o;
        },
        twice(x) {
          const quote = this.quote(x);
          return quote.price * 2n;
        },
      });
    `,
    solidity: `
      struct Quote {
          uint256 price;
          bool ok;
      }

      contract SelfLens {
          function price(uint256 x) external view returns (uint256) {
              require(x != 0, "nothing to price");
              return x * 3;
          }

          function quote(uint256 x) external view returns (Quote memory) {
              Quote memory o;
              uint256 price = this.price(x);
              o.price = price;
              o.ok = true;
              return o;
          }

          function twice(uint256 x) external view returns (uint256) {
              Quote memory quote = this.quote(x);
              return quote.price * 2;
          }
      }
    `,
    calls: () => [
      { functionName: "price", args: [5n] },
      { functionName: "quote", args: [5n] },
      { functionName: "twice", args: [5n] },
      // The revert of the innermost call is carried out through both external calls.
      { functionName: "twice", args: [0n] },
    ],
  },
  "try and catch, around one external call": {
    ts: `
      import { address, bool, contract, iface, require, Struct, uint256, view } from "@morpho-org/viem-dlc/solidity";

      const IFeed = iface({
        price: view(address).returns(uint256),
        spin: view().returns(uint256),
      });

      class Reading extends Struct({ price: uint256, failed: bool, r: uint256 }) {}

      export const TryLens = contract("TryLens", {
        priceOr: view(IFeed, address, uint256).returns(uint256),
        read: view(address, address).returns(Reading),
        check: view(uint256).returns(uint256),
        checked: view(uint256).returns(bool),
        bounded: view(address, uint256).returns(uint256),
        wrapped: view(address, address).returns(Reading),
      }).implementation({
        priceOr(feed, token, otherwise) {
          let price = 0n;
          try {
            price = feed.price(token);
          } catch {
            price = otherwise;
          }
          return price;
        },
        read(feed, token) {
          const o = new Reading();
          let r = 0n;
          try {
            o.price = IFeed(feed).price(token);
          } catch {
            o.failed = true;
          }
          try {
            o.r = IFeed(feed).price(token);
          } catch {}
          try {
            r = IFeed(feed).price(token);
          } catch {
            r = 1n;
          }
          o.r += r;
          return o;
        },
        check(x) {
          require(x < 10n, "too large");
          return x;
        },
        checked(x) {
          try {
            this.check(x);
          } catch {
            return false;
          }
          return true;
        },
        bounded(feed, gas) {
          let spun = 0n;
          try {
            spun = IFeed(feed, { gas }).spin();
          } catch {
            spun = 7n;
          }
          return spun;
        },
        wrapped(feed, token) {
          const r = new Reading();
          try {
            (r as Reading).price = IFeed(feed).price(token);
          } catch {}
          return r;
        },
      });
    `,
    solidity: `
      struct Reading {
          uint256 price;
          bool failed;
          uint256 r;
      }

      interface IFeed {
          function price(address) external view returns (uint256);
          function spin() external view returns (uint256);
      }

      contract TryLens {
          function priceOr(IFeed feed, address token, uint256 otherwise) external view returns (uint256) {
              uint256 price = 0;
              try feed.price(token) returns (uint256 r) {
                  price = r;
              } catch {
                  price = otherwise;
              }
              return price;
          }

          function read(address feed, address token) external view returns (Reading memory) {
              Reading memory o;
              uint256 r = 0;
              try IFeed(feed).price(token) returns (uint256 r) {
                  o.price = r;
              } catch {
                  o.failed = true;
              }
              try IFeed(feed).price(token) returns (uint256 r) {
                  o.r = r;
              } catch {}
              try IFeed(feed).price(token) returns (uint256 r_) {
                  r = r_;
              } catch {
                  r = 1;
              }
              o.r += r;
              return o;
          }

          function check(uint256 x) external view returns (uint256) {
              require(x < 10, "too large");
              return x;
          }

          function checked(uint256 x) external view returns (bool) {
              try this.check(x) {} catch {
                  return false;
              }
              return true;
          }

          function bounded(address feed, uint256 gas) external view returns (uint256) {
              uint256 spun = 0;
              try IFeed(feed).spin{gas: gas}() returns (uint256 r) {
                  spun = r;
              } catch {
                  spun = 7;
              }
              return spun;
          }

          function wrapped(address feed, address token) external view returns (Reading memory) {
              Reading memory r;
              try IFeed(feed).price(token) returns (uint256 r_) {
                  r.price = r_;
              } catch {}
              return r;
          }
      }
    `,
    calls: ({ token, feed, broken }) => [
      { functionName: "priceOr", args: [feed, token, 9n] },
      { functionName: "priceOr", args: [broken, token, 9n] },
      { functionName: "read", args: [feed, token] },
      { functionName: "read", args: [broken, token] },
      { functionName: "checked", args: [3n] },
      { functionName: "checked", args: [30n] },
      // The callee runs out of the gas it was given, and the lens goes on with what it kept.
      { functionName: "bounded", args: [feed, 30_000n] },
      { functionName: "wrapped", args: [feed, token] },
      { functionName: "wrapped", args: [broken, token] },
    ],
  },
  "the built-ins a body can name": {
    ts: `
      import {
        abi, address, block, bool, bytes, bytes32, codeSize, contract, gasleft, iface, keccak256, pure, staticcall,
        Struct, uint128, uint256, view,
      } from "@morpho-org/viem-dlc/solidity";

      const IToken = iface({ balanceOf: view(address).returns(uint256) });

      class Point extends Struct({ x: uint128, y: uint128 }) {}

      export const BuiltinLens = contract("BuiltinLens", {
        timestamp: view().returns(uint256),
        number: view().returns(uint256),
        spent: view().returns(uint256),
        hash: pure(address, uint256).returns(bytes32),
        roundTrip: pure(uint256).returns(uint256),
        point: pure(bytes).returns(Point),
        list: pure(bytes).returns([uint256]),
        pair: pure(bytes).returns(uint256, address),
        tokenOf: pure(bytes).returns(address),
        probe: view(address, address).returns(bool, bytes),
        probeCapped: view(address, address, uint256).returns(bool, bytes),
        size: view(address).returns(uint256),
        ignored: view(address).returns(bool),
      }).implementation({
        timestamp() {
          const time = block.timestamp;
          return time;
        },
        number() {
          const height = block.number;
          return height;
        },
        spent() {
          const before = gasleft();
          return before - gasleft();
        },
        hash(token, amount) {
          const data = abi.encode(token, amount);
          const id = keccak256(data);
          return id;
        },
        roundTrip(x) {
          const y = abi.decode(abi.encode(x), [uint256]);
          return y;
        },
        point(data) {
          const p = abi.decode(data, [Point]);
          return p;
        },
        list(data) {
          const xs = abi.decode(data, [[uint256]]);
          return xs;
        },
        pair: (data) => abi.decode(data, [uint256, address]),
        tokenOf(data) {
          const token = abi.decode(data, [IToken]);
          return address(token);
        },
        probe: (token, holder) => staticcall(token, IToken.balanceOf.encode(holder)),
        probeCapped: (token, holder, gas) => staticcall(token, IToken.balanceOf.encode(holder), { gas }),
        size(account) {
          const size = codeSize(account);
          return size;
        },
        ignored(token) {
          staticcall(token, "0x", { gas: 10_000 });
          return codeSize(token) > 0n;
        },
      });
    `,
    solidity: `
      struct Point {
          uint128 x;
          uint128 y;
      }

      interface IToken {
          function balanceOf(address) external view returns (uint256);
      }

      contract BuiltinLens {
          function timestamp() external view returns (uint256) {
              uint256 time = block.timestamp;
              return time;
          }

          function number() external view returns (uint256) {
              uint256 height = block.number;
              return height;
          }

          function spent() external view returns (uint256) {
              uint256 before = gasleft();
              return before - gasleft();
          }

          function hash(address token, uint256 amount) external pure returns (bytes32) {
              bytes memory data = abi.encode(token, amount);
              bytes32 id = keccak256(data);
              return id;
          }

          function roundTrip(uint256 x) external pure returns (uint256) {
              uint256 y = abi.decode(abi.encode(x), (uint256));
              return y;
          }

          function point(bytes calldata data) external pure returns (Point memory) {
              Point memory p = abi.decode(data, (Point));
              return p;
          }

          function list(bytes calldata data) external pure returns (uint256[] memory) {
              uint256[] memory xs = abi.decode(data, (uint256[]));
              return xs;
          }

          function pair(bytes calldata data) external pure returns (uint256, address) {
              return abi.decode(data, (uint256, address));
          }

          function tokenOf(bytes calldata data) external pure returns (address) {
              IToken token = abi.decode(data, (IToken));
              return address(token);
          }

          function probe(address token, address holder) external view returns (bool, bytes memory) {
              return token.staticcall(abi.encodeCall(IToken.balanceOf, (holder)));
          }

          function probeCapped(address token, address holder, uint256 gas) external view returns (bool, bytes memory) {
              return token.staticcall{gas: gas}(abi.encodeCall(IToken.balanceOf, (holder)));
          }

          function size(address account) external view returns (uint256) {
              uint256 size = account.code.length;
              return size;
          }

          function ignored(address token) external view returns (bool) {
              token.staticcall{gas: 10000}(hex"");
              return token.code.length > 0;
          }
      }
    `,
    calls: ({ token, broken, empty }) => [
      { functionName: "timestamp" },
      { functionName: "number" },
      { functionName: "spent" },
      { functionName: "hash", args: [HOLDER, 42n] },
      { functionName: "roundTrip", args: [42n] },
      { functionName: "point", args: [encodeAbiParameters(parseAbiParameters("uint128, uint128"), [3n, 4n])] },
      { functionName: "list", args: [encodeAbiParameters(parseAbiParameters("uint256[]"), [[1n, 2n, 3n]])] },
      { functionName: "pair", args: [encodeAbiParameters(parseAbiParameters("uint256, address"), [7n, HOLDER])] },
      // Data too short to decode reverts, as abi.decode does.
      { functionName: "pair", args: ["0x1234"] },
      { functionName: "tokenOf", args: [encodeAbiParameters(parseAbiParameters("address"), [token])] },
      { functionName: "probe", args: [token, HOLDER] },
      { functionName: "probe", args: [broken, HOLDER] },
      { functionName: "probe", args: [empty, HOLDER] },
      { functionName: "probeCapped", args: [token, HOLDER, 100_000n] },
      { functionName: "probeCapped", args: [token, HOLDER, 10n] },
      { functionName: "size", args: [token] },
      { functionName: "size", args: [empty] },
      { functionName: "ignored", args: [token] },
    ],
  },
} satisfies Record<string, ExternalPair>;

describe("fixture pairs: the outside world", () => {
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

    const run = async (name: keyof typeof pairs) => {
      const pair = pairs[name];
      const calls = pair.calls(mocks);
      const outcomes = await expectSameExecution(evm, await compilePair(pair), calls);
      return new Map(
        calls.map((call, index) => [`${call.functionName}(${(call.args ?? []).join(", ")})`, outcomes[index]]),
      );
    };

    it("an interface, handles, and calls through them", async () => {
      const outcomes = await run("an interface, handles, and calls through them");
      const { token, feed, broken, empty } = mocks;
      expect(outcomes.get(`read(${token}, ${feed}, ${HOLDER})`)?.status).toBe("success");
      // A function that returns nothing still reverts the lens when it reverts.
      expect(outcomes.get(`read(${token}, ${feed}, ${zeroAddress})`)?.data.slice(0, 10)).toBe("0x08c379a0");
      // Error("broken") from the callee, and nothing from an account with no code.
      expect(outcomes.get(`read(${broken}, ${feed}, ${HOLDER})`)?.data.slice(0, 10)).toBe("0x08c379a0");
      expect(outcomes.get(`read(${empty}, ${feed}, ${HOLDER})`)).toEqual({ status: "revert", data: "0x" });
      // The callee saw at most the gas the handle was made with.
      const capped = BigInt(outcomes.get(`capped(${feed}, 30000)`)?.data ?? 0);
      expect(capped).toBeGreaterThan(70_000n);
      expect(capped).toBeLessThan(80_000n);
    });

    it("a selector and an encoded call", async () => {
      const outcomes = await run("a selector and an encoded call");
      expect(outcomes.get("balanceOfSelector()")?.data.slice(0, 10)).toBe(toFunctionSelector("balanceOf(address)"));
      expect(BigInt(outcomes.get(`isAllowance(${toFunctionSelector("allowance(address,address)")})`)?.data ?? 0)).toBe(
        1n,
      );
      expect(BigInt(outcomes.get(`isAllowance(${toFunctionSelector("balanceOf(address)")})`)?.data ?? 1)).toBe(0n);
    });

    it("libraries, called from an entry point, a helper and each other", async () => {
      const outcomes = await run("libraries, called from an entry point, a helper and each other");
      expect(BigInt(outcomes.get(`compound(${1000n * WAD}, ${WAD / 10n})`)?.data ?? 0)).toBe(1210n * WAD);
      expect(outcomes.get(`ratio(${WAD}, 0)`)?.data.slice(0, 10)).toBe("0x08c379a0");
      expect(outcomes.get(`halfSquare(${2n ** 200n})`)?.data.slice(0, 10)).toBe("0x4e487b71");
    });

    it("an external call to the lens itself", async () => {
      const outcomes = await run("an external call to the lens itself");
      expect(BigInt(outcomes.get("twice(5)")?.data ?? 0)).toBe(30n);
      expect(outcomes.get("twice(0)")?.data.slice(0, 10)).toBe("0x08c379a0");
    });

    it("try and catch, around one external call", async () => {
      const outcomes = await run("try and catch, around one external call");
      const { token, feed, broken } = mocks;
      expect(outcomes.get(`priceOr(${feed}, ${token}, 9)`)?.data).not.toEqual(
        outcomes.get(`priceOr(${broken}, ${token}, 9)`)?.data,
      );
      expect(BigInt(outcomes.get(`priceOr(${broken}, ${token}, 9)`)?.data ?? 0)).toBe(9n);
      expect(BigInt(outcomes.get("checked(3)")?.data ?? 0)).toBe(1n);
      expect(BigInt(outcomes.get("checked(30)")?.data ?? 1)).toBe(0n);
      expect(BigInt(outcomes.get(`bounded(${feed}, 30000)`)?.data ?? 0)).toBe(7n);
    });

    it("the built-ins a body can name", async () => {
      const outcomes = await run("the built-ins a body can name");
      const { token, broken, empty } = mocks;
      // A call is run on the latest block.
      const block = await evm.client.getBlock();
      expect(BigInt(outcomes.get("number()")?.data ?? 0)).toBe(block.number);
      expect(BigInt(outcomes.get("timestamp()")?.data ?? 0)).toBe(block.timestamp);
      expect(BigInt(outcomes.get("spent()")?.data ?? 0)).toBeGreaterThan(0n);
      expect(outcomes.get(`hash(${HOLDER}, 42)`)?.data).toBe(
        keccak256(encodeAbiParameters(parseAbiParameters("address, uint256"), [HOLDER, 42n])),
      );
      expect(outcomes.get("pair(0x1234)")?.status).toBe("revert");
      const flag = (key: string) => BigInt((outcomes.get(key)?.data ?? "0x").slice(0, 66));
      expect(flag(`probe(${token}, ${HOLDER})`)).toBe(1n);
      expect(flag(`probe(${broken}, ${HOLDER})`)).toBe(0n);
      // A low-level call to an account with no code succeeds with no data.
      expect(flag(`probe(${empty}, ${HOLDER})`)).toBe(1n);
      expect(flag(`probeCapped(${token}, ${HOLDER}, 100000)`)).toBe(1n);
      expect(flag(`probeCapped(${token}, ${HOLDER}, 10)`)).toBe(0n);
      expect(BigInt(outcomes.get(`size(${token})`)?.data ?? 0)).toBeGreaterThan(0n);
      expect(BigInt(outcomes.get(`size(${empty})`)?.data ?? 1)).toBe(0n);
    });
  });
});

describe("what the transform prints for the outside world", () => {
  it("is the declared order: structs, interfaces, libraries, then the contract", async () => {
    const built = await buildSnippet(`
      import { address, contract, iface, library, pure, Struct, uint256, view } from "@morpho-org/viem-dlc/solidity";
      const Math = library("Math", { double: pure(uint256).returns(uint256) }).implementation({ double: (x) => x * 2n });
      const IFeed = iface({ price: view(address).returns(uint256) });
      class Quote extends Struct({ price: uint256 }) {}
      export const Lens = contract("Lens", { quote: view(IFeed, address).returns(Quote) }).implementation({
        quote(feed, token) {
          const o = new Quote();
          o.price = Math.double(feed.price(token));
          return o;
        },
      });
    `);
    const solidity = built.lenses[0]?.printed.solidity ?? "";
    const order = ["struct Quote", "interface IFeed", "library Math", "contract Lens"].map((text) =>
      solidity.indexOf(text),
    );
    expect(order.every((offset) => offset >= 0)).toBe(true);
    expect(order).toEqual([...order].sort((a, b) => a - b));
  });

  it("is the name a library and an interface were given, wherever they are imported from", async () => {
    const built = await buildSnippet({
      "lens.ts": `
        import { address, contract, uint256, view } from "@morpho-org/viem-dlc/solidity";
        import * as shared from "./shared.js";
        import { Fixed as F } from "./shared.js";

        export const Lens = contract("Lens", { f: view(address, address).returns(uint256) }).implementation({
          f: (feed, token) => F.double(shared.IFeed(feed, { gas: 50_000 }).price(token)) + shared.Fixed.double(1n),
        });
      `,
      "shared.ts": `
        import { address, iface, library, pure, uint256, view } from "@morpho-org/viem-dlc/solidity";
        export const IFeed = iface({ price: view(address).returns(uint256) });
        export const Fixed = library("FixedPoint", { double: pure(uint256).returns(uint256) }).implementation({
          double: (x) => x * 2n,
        });
      `,
    });
    const solidity = built.lenses[0]?.printed.solidity ?? "";
    expect(solidity).toContain("library FixedPoint {");
    expect(solidity).toContain(
      "return FixedPoint.double(IFeed(feed).price{gas: 50000}(token)) + FixedPoint.double(1);",
    );
  });

  it("is the built-ins' own names when the package is imported as a namespace", async () => {
    const built = await buildSnippet(`
      import * as sol from "@morpho-org/viem-dlc/solidity";
      export const Lens = sol.contract("Lens", { f: sol.view(sol.address).returns(sol.bytes32) }).implementation({
        f(account) {
          const time = sol.block.timestamp;
          const size = sol.codeSize(account);
          return sol.keccak256(sol.abi.encode(time, size, sol.gasleft()));
        },
      });
    `);
    const solidity = built.lenses[0]?.printed.solidity ?? "";
    expect(solidity).toContain("uint256 time = block.timestamp;");
    expect(solidity).toContain("uint256 size = account.code.length;");
    expect(solidity).toContain("return keccak256(abi.encode(time, size, gasleft()));");
  });
});
