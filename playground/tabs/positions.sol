pragma solidity ^0.8.24;

interface IMorpho {
  struct Position { uint256 supplyShares; uint128 borrowShares; uint128 collateral; }
  function position(bytes32 id, address user) external view returns (Position memory);
}

// A lens is one function over one element. The transport calls it once per element in its own
// frame and paginates; nothing here knows about batching.
contract MorphoPositionsLens {
  IMorpho constant MORPHO = IMorpho(0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb);
  struct Input { bytes32 id; address user; }

  function positionOf(Input calldata x) external view returns (IMorpho.Position memory) {
    return MORPHO.position(x.id, x.user);
  }
}
