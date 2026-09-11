import type { Abi, AbiFunction, Hex } from "viem";

/**
 * Creation bytecode of `Factory` in ./Fixtures.sol, from
 * `solc --optimize --evm-version cancun --bin test/e2e/Fixtures.sol` (solc 0.8.36).
 */
export const FACTORY_BYTECODE: Hex =
  "0x6080604052348015600e575f5ffd5b50606780601a5f395ff3fe6080604052348015600e575f5ffd5b5036601f19018060205f375f35815f5ff59050806029575f5ffd5b805f5260205ff3fea2646970667358221220ce38666bf953fb7efddc835ee34f69d22d41cd1fe2ef824f85f8ef575ab2f1d464736f6c63430008240033";

/** Creation bytecode of `StaticLens` in ./Fixtures.sol, from the same invocation. */
export const LENS_BYTECODE: Hex =
  "0x6080604052348015600e575f5ffd5b5060db80601a5f395ff3fe6080604052348015600e575f5ffd5b50600436106026575f3560e01c8063063ad40114602a575b5f5ffd5b60396035366004606b565b604b565b60405190815260200160405180910390f35b5f8160200135600103605b575f5ffd5b6065823560026083565b92915050565b5f6040828403128015607b575f5ffd5b509092915050565b8082028115828204841417606557634e487b7160e01b5f52601160045260245ffdfea26469706673582212200a035e996db40e797f7ea169a071461cf412531198370dc2d4b1071ea8bfa3dc64736f6c63430008240033";

export const lensAbi = [
  {
    type: "function",
    name: "item",
    stateMutability: "pure",
    inputs: [
      {
        name: "x",
        type: "tuple",
        components: [
          { name: "a", type: "uint256" },
          { name: "mode", type: "uint256" },
        ],
      },
    ],
    outputs: [{ name: "", type: "uint256" }],
  },
] as const satisfies Abi;

export const itemAbi = lensAbi[0] as AbiFunction;

/** Bytes one element of `item`'s input occupies on the wire: two static words. */
export const ELEMENT_SIZE = 64;
