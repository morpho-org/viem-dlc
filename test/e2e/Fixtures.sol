// SPDX-License-Identifier: MIT
pragma solidity ^0.8.25;

/// Salted CREATE2 factory: calldata is `salt || initcode`, return value the deployed address.
contract Factory {
    fallback() external {
        assembly {
            calldatacopy(0, 32, sub(calldatasize(), 32))
            let a := create2(0, 0, sub(calldatasize(), 32), calldataload(0))
            if iszero(a) { revert(0, 0) }
            mstore(0, a)
            return(0, 32)
        }
    }
}

/// A paginated lens whose per-item function is pure arithmetic: `mode` 0 returns `a * 2`.
contract StaticLens {
    struct In {
        uint256 a;
        uint256 mode;
    }

    function item(In calldata x) external pure returns (uint256) {
        if (x.mode == 1) revert();
        return x.a * 2;
    }
}

/// A paginated lens with a dynamic input and a dynamic output: every item is its own input, twice.
contract EchoLens {
    function item(bytes calldata x) external pure returns (bytes memory) {
        return abi.encodePacked(x, x);
    }
}
