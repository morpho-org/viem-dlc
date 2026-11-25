/**
 * The members a lens reads and calls. A member is a field its struct declares, the length of an array,
 * or a row of the tables: block.timestamp, block.number, I.f.selector. A call through a member is to a
 * function of a handle, a library, this, or abi. Any other member is refused before solc sees it, so a
 * native member of a Solidity type, such as the balance of an address, has no form in a lens.
 */
import { describe, it } from "vitest";

import { describePairs, type ExecutedPair } from "./helpers/core.js";
import { expectRejection, positionIn } from "./helpers/harness.js";

const HEAD = `import {
  address, array, block, bool, bytes, bytes4, contract, iface, internal, pure, string, Struct, uint256, view,
} from "@morpho-org/viem-dlc/solidity";

const IFeed = iface({ price: view().returns(uint256) });
class Pair extends Struct({ low: uint256, high: uint256 }) {}
class Book extends Struct({ pairs: [Pair], best: Pair, feed: IFeed, owner: address }) {}
`;

const OWNER = "0x52908400098527886E0F7030069857D2E4169EE7";
const FEED = "0x8617E340B3D01FA5F11F306F4090FD50E238070D";
const book = {
  pairs: [
    { low: 1n, high: 2n },
    { low: 3n, high: 4n },
  ],
  best: { low: 5n, high: 6n },
  feed: FEED,
  owner: OWNER,
};

const pairs: Record<string, ExecutedPair> = {
  "fields, lengths, block members and a selector": {
    ts: `${HEAD}
      const first = internal(pure(Book).returns(Pair), (book) => book.pairs[0]!);

      export const MemberLens = contract("MemberLens", {
        fields: pure(Book).returns(uint256, uint256, address),
        lengths: pure(Book, [uint256]).returns(uint256, uint256),
        throughCalls: pure(Book).returns(uint256, uint256),
        written: pure(Book).returns(Book),
        clock: view().returns(bool),
        selector: pure().returns(bytes4),
      }).implementation({
        fields: (book) => [book.best.low, book.pairs[1]!.high, book.owner],
        lengths: (book, xs) => [book.pairs.length, xs.length],
        // A member of what a call returns, whose type the call declares.
        throughCalls: (book) => [first(book).low, new Pair({ low: 1n, high: 2n }).high + array(uint256, 3n).length],
        written(book) {
          const out = new Book();
          out.best.low = book.best.high;
          out.owner = book.owner;
          return out;
        },
        clock: () => block.timestamp >= block.number,
        selector: () => IFeed.price.selector,
      });
    `,
    solidity: `
      struct Pair {
          uint256 low;
          uint256 high;
      }

      struct Book {
          Pair[] pairs;
          Pair best;
          IFeed feed;
          address owner;
      }

      interface IFeed {
          function price() external view returns (uint256);
      }

      contract MemberLens {
          function fields(Book calldata book) external pure returns (uint256, uint256, address) {
              return (book.best.low, book.pairs[1].high, book.owner);
          }

          function lengths(Book calldata book, uint256[] calldata xs) external pure returns (uint256, uint256) {
              return (book.pairs.length, xs.length);
          }

          function throughCalls(Book calldata book) external pure returns (uint256, uint256) {
              return (first(book).low, Pair({low: 1, high: 2}).high + new uint256[](3).length);
          }

          function written(Book calldata book) external pure returns (Book memory) {
              Book memory out;
              out.best.low = book.best.high;
              out.owner = book.owner;
              return out;
          }

          function clock() external view returns (bool) {
              return block.timestamp >= block.number;
          }

          function selector() external pure returns (bytes4) {
              return IFeed.price.selector;
          }

          function first(Book memory book) internal pure returns (Pair memory) {
              return book.pairs[0];
          }
      }
    `,
    calls: [
      { functionName: "fields", args: [book] },
      { functionName: "fields", args: [{ ...book, pairs: [] }], reverts: true },
      { functionName: "lengths", args: [book, [7n, 8n, 9n]] },
      { functionName: "throughCalls", args: [book] },
      { functionName: "written", args: [book] },
      { functionName: "clock" },
      { functionName: "selector" },
    ],
  },
};

describePairs("members", pairs);

describe("members: rejections", () => {
  /** A lens whose entry point `f(a, xs, data, book, text)` has this body. */
  const lens = (body: string) => `${HEAD}
export const Lens = contract("Lens", {
  f: view(address, [uint256], bytes, Book, string).returns(uint256),
}).implementation({
  f(a, xs, data, book, text) {
    ${body}
  },
});
`;
  const MEMBER = "is not a member a lens can read";
  const CALL = "is not a call a lens can make";
  const refuses = async (body: string, message: string, at: string) => {
    const code = lens(body);
    await expectRejection(code, { message, ...positionIn(code, at), source: "transform" });
  };

  it("the balance of an address", async () => {
    await refuses("return a.balance;", `"balance" ${MEMBER}`, "a.balance");
  });

  it("the code of an address, and a member of it", async () => {
    await refuses("return a.code;", `"code" ${MEMBER}`, "a.code");
    await refuses("return a.code.length;", `"code" ${MEMBER}`, "a.code.length");
  });

  it("a member of a conversion", async () => {
    await refuses("return address(book.feed).balance;", `"balance" ${MEMBER}`, "address(book.feed).balance");
  });

  it("the length of bytes and of a string, which are not arrays to a lens", async () => {
    await refuses("return data.length;", `"length" ${MEMBER}`, "data.length");
    await refuses("return text.length;", `"length" ${MEMBER}`, "text.length");
  });

  it("a member of an array other than its length", async () => {
    await refuses("return xs.size;", `"size" ${MEMBER}`, "xs.size");
  });

  it("a field its struct does not declare", async () => {
    await refuses("return book.missing;", 'struct Book has no field "missing"', "book.missing");
    await refuses("return book.best.middle;", 'struct Pair has no field "middle"', "book.best.middle");
  });

  it("a function of a handle that is not called", async () => {
    await refuses("return book.feed.price;", `"price" ${MEMBER}`, "book.feed.price");
  });

  it("a member of an expression whose type the source does not declare", async () => {
    await refuses("return (xs.length > 0n ? book.best : book.best).low;", `"low" ${MEMBER}`, "(xs.length");
  });

  it("a member that is assigned", async () => {
    await refuses("a.balance = 1n;\n    return 0n;", `"balance" ${MEMBER}`, "a.balance");
    await refuses("book.missing = 1n;\n    return 0n;", 'struct Book has no field "missing"', "book.missing");
  });

  it("a call of a native member of an address", async () => {
    await refuses("a.call(data);\n    return 0n;", `.call(…) ${CALL}`, "a.call(data)");
    await refuses("a.staticcall(data);\n    return 0n;", `.staticcall(…) ${CALL}`, "a.staticcall(data)");
  });

  it("a call of a member of an array, of a struct and of an integer", async () => {
    await refuses("xs.push(1n);\n    return 0n;", `.push(…) ${CALL}`, "xs.push(1n)");
    await refuses("return book.best.low.toString();", `.toString(…) ${CALL}`, "book.best.low.toString()");
    await refuses("return book.owner.price();", `.price(…) ${CALL}`, "book.owner.price()");
  });

  it("a call through an expression whose type the source does not declare", async () => {
    await refuses("return (xs.length > 0n ? book.feed : book.feed).price();", `.price(…) ${CALL}`, "(xs.length");
  });
});
