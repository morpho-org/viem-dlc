import type { Abi, AbiFunction, Hex } from "viem";

/**
 * Creation bytecode of `Factory` in ./Fixtures.sol, from
 * `solc --optimize --evm-version cancun --bin test/e2e/Fixtures.sol` (solc 0.8.36).
 */
export const FACTORY_BYTECODE: Hex =
  "0x6080604052348015600e575f5ffd5b50606780601a5f395ff3fe6080604052348015600e575f5ffd5b5036601f19018060205f375f35815f5ff59050806029575f5ffd5b805f5260205ff3fea2646970667358221220991bb4f29bd99c686e3f59bccfc2c697d048c45cc00db0c962420981976dea4d64736f6c63430008240033";

/** Creation bytecode of `StaticLens` in ./Fixtures.sol, from the same invocation. */
export const LENS_BYTECODE: Hex =
  "0x6080604052348015600e575f5ffd5b5060db80601a5f395ff3fe6080604052348015600e575f5ffd5b50600436106026575f3560e01c8063063ad40114602a575b5f5ffd5b60396035366004606b565b604b565b60405190815260200160405180910390f35b5f8160200135600103605b575f5ffd5b6065823560026083565b92915050565b5f6040828403128015607b575f5ffd5b509092915050565b8082028115828204841417606557634e487b7160e01b5f52601160045260245ffdfea2646970667358221220091cdcf28463a9911360d1b618fa541d57bb99499dbf803c6e9e6c381ab73ea364736f6c63430008240033";

/** Creation bytecode of `EchoLens` in ./Fixtures.sol, from the same invocation. */
export const ECHO_LENS_BYTECODE: Hex =
  "0x6080604052348015600e575f5ffd5b5061017e8061001c5f395ff3fe608060405234801561000f575f5ffd5b5060043610610029575f3560e01c8063e5a48d491461002d575b5f5ffd5b61004061003b366004610086565b610056565b60405161004d91906100f4565b60405180910390f35b60608282848460405160200161006f9493929190610129565b604051602081830303815290604052905092915050565b5f5f60208385031215610097575f5ffd5b823567ffffffffffffffff8111156100ad575f5ffd5b8301601f810185136100bd575f5ffd5b803567ffffffffffffffff8111156100d3575f5ffd5b8560208284010111156100e4575f5ffd5b6020919091019590945092505050565b602081525f82518060208401528060208501604085015e5f604082850101526040601f19601f83011684010191505092915050565b838582375f8482015f8152838582375f9301928352509094935050505056fea26469706673582212209c52bc18863db26a337f5bb1caa9210d6284916d4432201c3b357ced7b263b6c64736f6c63430008240033";

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

export const echoLensAbi = [
  {
    type: "function",
    name: "item",
    stateMutability: "pure",
    inputs: [{ name: "x", type: "bytes" }],
    outputs: [{ name: "", type: "bytes" }],
  },
] as const satisfies Abi;

export const echoItemAbi = echoLensAbi[0] as AbiFunction;
