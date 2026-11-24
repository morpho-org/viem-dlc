// The whole surface on made-up contracts: a price feed, an ERC-20 and some fixed-point arithmetic. Nothing
// here is run or compiled to Solidity. tsc accepting the file is the test, so it holds one use of every
// construct a lens body may contain.
import {
  abi,
  address,
  array,
  assembly,
  block,
  bool,
  bytes32,
  codeSize,
  contract,
  gasleft,
  iface,
  immutable,
  internal,
  keccak256,
  library,
  pure,
  require,
  revert,
  Struct,
  staticcall,
  uint8,
  uint64,
  uint128,
  uint256,
  view,
} from "@morpho-org/viem-dlc/solidity";
import * as yul from "@morpho-org/viem-dlc/solidity/yul";

const WAD = 10n ** 18n;
const STALE_AFTER = 3600n;
const MAX_HOLDERS = 32n;
const DECIMALS_SELECTOR = 0x313ce567n;

class Holding extends Struct({ holder: address, balance: uint256, value: uint256, share: uint128 }) {}
class Query extends Struct({ token: address, holders: [address], minBalance: uint256 }) {}
class Board extends Struct({
  id: bytes32,
  price: uint256,
  updatedAt: uint64,
  stale: bool,
  decimals: uint8,
  holdings: [Holding],
  counted: uint256,
  total: uint256,
}) {}

const IERC20 = iface({
  balanceOf: view(address).returns(uint256),
  totalSupply: view().returns(uint256),
});
const IPriceFeed = iface({
  latest: view(address).returns(uint256, uint64),
  paused: view().returns(bool),
});

const FEED = immutable(IPriceFeed);
const QUOTE_DECIMALS = immutable(uint8);

// ---- helpers: expression bodies, loops and locals -------------------------------------------------------------
const mulDiv = internal(pure(uint256, uint256, uint256).returns(uint256), (x, y, d) => (x * y) / d);
const min = internal(pure(uint256, uint256).returns(uint256), (a, b) => (a < b ? a : b));

const log2 = internal(pure(uint256).returns(uint256), (x) => {
  let n = 0n;
  while (x > 1n) {
    x >>= 1n;
    n++;
  }
  return n;
});

const sumOfSquares = internal(pure(uint256).returns(uint256), (n) => {
  let sum = 0n;
  for (let i = 1n; i <= n; i++) {
    if (i % 2n === 0n) continue;
    if (sum > WAD) break;
    sum += i * i;
  }
  do {
    sum -= 1n;
  } while (sum > WAD);
  return sum;
});

// ---- a library ------------------------------------------------------------------------------------------------
export const FixedPoint = library("FixedPoint", {
  mulWad: pure(uint256, uint256).returns(uint256),
  powWad: pure(uint256, uint256).returns(uint256),
}).implementation({
  mulWad: (x, y) => mulDiv(x, y, WAD),
  powWad(base, exponent) {
    let result = uint256(WAD);
    for (let i = 0n; i < exponent; i++) result = mulDiv(result, base, WAD);
    return result;
  },
});

// ---- typed Yul --------------------------------------------------------------------------------------------------
// A block that returns a word is a uint256, or the type named first.
const decimalsOrZero = internal(view(address).returns(uint256), (token) =>
  assembly("memory-safe", () => {
    yul.mstore(0x00, yul.shl(224, DECIMALS_SELECTOR));
    const ok = yul.and(yul.staticcall(30000, token, 0x00, 0x04, 0x00, 0x20), yul.gt(yul.returndatasize(), 0x1f));
    return yul.mul(ok, yul.mload(0x00));
  }),
);
const pairId = internal(pure(address, uint256).returns(bytes32), (token, index) =>
  assembly(bytes32, "memory-safe", () => {
    const larger = (a: bigint, b: bigint) => yul.xor(a, yul.mul(yul.xor(a, b), yul.gt(b, a)));
    yul.mstore(0x00, token);
    yul.mstore(0x20, larger(index, 1n));
    return yul.keccak256(0x00, 0x40);
  }),
);

// ---- the lens ---------------------------------------------------------------------------------------------------
export const PriceBoard = contract(
  "PriceBoard",
  {
    board: view(Query).returns(Board),
    holdingOf: view(address, address, uint256).returns(Holding),
    priceOf: view(address).returns(uint256, uint64),
  },
  { immutables: { FEED, QUOTE_DECIMALS } },
).implementation({
  board(q) {
    require(q.holders.length <= MAX_HOLDERS, "too many holders");
    const o = new Board();
    o.id = keccak256(abi.encode(q.token, q.minBalance, block.number));

    // A call that may revert, and the value used when it does.
    let paused = false;
    try {
      paused = FEED.paused();
    } catch {
      paused = true;
    }
    if (paused) revert("the feed is paused");

    // Several return values are destructured.
    const [price, updatedAt] = this.priceOf(q.token);
    o.price = price;
    o.updatedAt = updatedAt;
    o.stale = block.timestamp - updatedAt > STALE_AFTER;

    // A block that returns nothing assigns the locals declared around it.
    let decimals = 0n;
    let size = 0n;
    assembly("memory-safe", () => {
      size = yul.extcodesize(q.token);
      decimals = yul.mload(0x40);
    });
    decimals = size === 0n || codeSize(q.token) === 0n ? 0n : decimalsOrZero(q.token);
    o.decimals = decimals <= 0xffn ? uint8(decimals) : 0n;

    o.holdings = array(Holding, q.holders.length);
    for (let i = 0; i < q.holders.length; i++) {
      // A read needs `!` under noUncheckedIndexedAccess: tsc cannot know a Solidity read reverts out of range.
      const holder = q.holders[i]!;
      try {
        o.holdings[i] = this.holdingOf(q.token, holder, price);
      } catch {
        continue;
      }
      if (o.holdings[i]!.balance < q.minBalance) continue;
      o.counted++;
      o.total += o.holdings[i]!.value;
    }
    o.total = min(o.total, FixedPoint.mulWad(o.total, FixedPoint.powWad(WAD, log2(sumOfSquares(3n)))));
    return o;
  },

  holdingOf(token, holder, price) {
    const erc20 = IERC20(token);
    const balance = erc20.balanceOf(holder);
    // Gas is capped where the handle is made.
    const supply = IERC20(token, { gas: gasleft() / 2n }).totalSupply();
    // A low-level read: the success flag and the returned bytes.
    const [ok, data] = staticcall(token, abi.encode(holder), { gas: 30000n });
    if (!ok || keccak256(data) === pairId(token, 0n)) revert();
    // A struct is built from all its fields, or as its zero value and then assigned.
    return new Holding({
      holder,
      balance,
      value: mulDiv(balance, price, 10n ** uint256(QUOTE_DECIMALS)),
      share: supply === 0n ? 0n : uint128(mulDiv(balance, WAD, supply)),
    });
  },

  priceOf(token) {
    const [price, updatedAt] = FEED.latest(token);
    return [price, updatedAt];
  },
});

// What the editor refuses in a body.
export const refused = internal(view(Query).returns(uint256), (q) => {
  const board = new Board();
  // @ts-expect-error a uint256 is not a uint64 without a conversion
  board.updatedAt = block.timestamp;
  // @ts-expect-error the feed declares no such function
  FEED.write(q.token);
  // @ts-expect-error latest takes an address
  FEED.latest(1n);
  // @ts-expect-error a struct is built from its own fields
  board.holdings[0] = new Holding({ owner: q.token });
  // @ts-expect-error a Holding is not a Board
  const other: Board = new Holding();
  return other.total;
});
