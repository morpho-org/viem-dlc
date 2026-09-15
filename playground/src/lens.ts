import { sol } from "soltag";

export const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb" as const;

const IMorpho = `
  interface IMorpho {
    struct Position { uint256 supplyShares; uint128 borrowShares; uint128 collateral; }
    function position(bytes32 id, address user) external view returns (Position memory);
  }
`;

/** The lens from `examples/04-deployless-batching.ts`, verbatim: one view function over one item. */
export const positionsLens = sol("MorphoPositionsLens")`
  pragma solidity ^0.8.24;
  ${IMorpho}
  contract MorphoPositionsLens {
    IMorpho constant MORPHO = IMorpho(0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb);
    struct Input { bytes32 id; address user; }

    function positionOf(Input calldata x) external view returns (IMorpho.Position memory) {
      return MORPHO.position(x.id, x.user);
    }
  }
`;
