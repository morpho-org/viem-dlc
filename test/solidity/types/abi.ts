// The ABI viem consumes is computed from the declaration alone, and `with` takes the immutables by name.
import {
  address,
  bool,
  contract,
  iface,
  immutable,
  Struct,
  string,
  uint8,
  uint128,
  uint256,
  view,
} from "@morpho-org/viem-dlc/solidity";
import type { Address, ContractFunctionArgs, ContractFunctionReturnType, Hex } from "viem";

type Same<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
const same = <A, B>(_same: Same<A, B>) => {};

class Metadata extends Struct({ name: string, symbol: string, decimals: uint8 }) {}
class Balance extends Struct({ holder: address, amount: uint128, frozen: bool }) {}
class Report extends Struct({ token: Metadata, balances: [Balance], total: uint256 }) {}

const IERC20 = iface({
  name: view().returns(string),
  symbol: view().returns(string),
  decimals: view().returns(uint8),
  balanceOf: view(address).returns(uint256),
});
const TOKEN = immutable(IERC20);
const SCALE = immutable(uint256);

export const TokenLens = contract(
  "TokenLens",
  {
    report: view([address]).returns(Report),
    balanceAndDecimals: view(address).returns(uint256, uint8),
  },
  { immutables: { TOKEN, SCALE } },
).implementation({
  report(holders) {
    const o = new Report();
    o.token.name = TOKEN.name();
    o.token.symbol = TOKEN.symbol();
    o.token.decimals = TOKEN.decimals();
    // A read needs `!` under noUncheckedIndexedAccess: tsc cannot know a Solidity read reverts out of range.
    const first = holders[0]!;
    o.total = TOKEN.balanceOf(first) * SCALE;
    // @ts-expect-error the struct has no such field
    o.missing = 1n;
    return o;
  },
  balanceAndDecimals(holder) {
    return [TOKEN.balanceOf(holder), TOKEN.decimals()];
  },
});

// viem reads the return type and the arguments off the declared ABI.
type Abi = typeof TokenLens.abi;
same<ContractFunctionArgs<Abi, "view", "report">, readonly [readonly Address[]]>(true);
same<ContractFunctionArgs<Abi, "view", "balanceAndDecimals">, readonly [Address]>(true);
same<ContractFunctionReturnType<Abi, "view", "balanceAndDecimals">, readonly [bigint, number]>(true);
declare const report: ContractFunctionReturnType<Abi, "view", "report">;
same<typeof report.total, bigint>(true);
same<typeof report.token, { name: string; symbol: string; decimals: number }>(true);
same<(typeof report.balances)[number], { holder: Address; amount: bigint; frozen: boolean }>(true);
same<typeof TokenLens.name, "TokenLens">(true);
same<typeof TokenLens.bytecode, Hex>(true);

// with() takes every immutable by name, as the value viem encodes: an interface is an address.
const descriptor = TokenLens.with({ TOKEN: "0x0000000000000000000000000000000000000001", SCALE: 10n ** 18n });
same<typeof descriptor.abi, Abi>(true);
same<typeof descriptor.address, Address>(true);
same<typeof descriptor.factoryData, Hex>(true);
// @ts-expect-error SCALE is missing
TokenLens.with({ TOKEN: "0x0000000000000000000000000000000000000001" });
// @ts-expect-error the immutables are required
TokenLens.with();
// @ts-expect-error a uint256 immutable is a bigint
TokenLens.with({ TOKEN: "0x0000000000000000000000000000000000000001", SCALE: "1" });

// A lens without immutables takes none.
export const Plain = contract("Plain", { one: view().returns(uint256) }).implementation({ one: () => 1n });
Plain.with();
// @ts-expect-error there is nothing to pass
Plain.with({ SCALE: 1n });

// An implementation is checked against its declaration.
export const Checked = contract("Checked", { f: view(uint128).returns(uint128) }).implementation({
  // @ts-expect-error a bool is not a uint128
  f: (x) => x > 1n,
});
contract("Missing", { f: view().returns(uint256), g: view().returns(uint256) })
  // @ts-expect-error g is not implemented
  .implementation({ f: () => 1n });
