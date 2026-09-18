pragma solidity ^0.8.24;

interface IVaultV2 {
  function totalAssets() external view returns (uint256);
  function asset() external view returns (address);
}

interface IERC20Metadata {
  function decimals() external view returns (uint8);
}

/// One `view` function over one element. That is the whole contract.
contract VaultSnapshotLens {
  struct Input {
    address vault;
    /// Rounds of pointless hashing. Stands in for cost you don't control; see the tutorial.
    uint256 grief;
  }

  struct Snapshot {
    uint256 totalAssets;
    address asset;
    uint8 decimals;
    bytes32 salt;
  }

  /// Each read depends on the one before it. In TypeScript that is three round trips; here it is one.
  function snapshotOf(Input calldata x) external view returns (Snapshot memory s) {
    s.totalAssets = IVaultV2(x.vault).totalAssets();
    s.asset = IVaultV2(x.vault).asset();
    s.decimals = IERC20Metadata(s.asset).decimals();
    for (uint256 i; i < x.grief; ++i) s.salt = keccak256(abi.encode(s.salt, i));
  }
}
