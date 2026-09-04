// SPDX-License-Identifier: MIT
pragma solidity ^0.8.25;

import { Env, Factory, Runner, StaticLens } from "./Fixtures.sol";

/// Per-element cost of both paths, for `forge snapshot`: the sweeps catch a corpse, not a slower
/// path, so a compiler change that lengthens the loop shows up here as a diff instead of as
/// shorter pages.
contract GasTest {
    Factory factory = new Factory();
    Runner runner = new Runner();
    bytes envelope;

    function setUp() public {
        envelope = Env.build();
    }

    function page(uint256 n, bool compressed) internal returns (uint256 used) {
        StaticLens.In[] memory xs = new StaticLens.In[](n);
        for (uint256 i; i < n; i++) xs[i] = StaticLens.In(i * 7919 % 1000, 0);
        bytes memory w = Env.wire(abi.encode(xs));
        if (compressed) w = Env.compress(w);
        uint256 cfg = Env.config(StaticLens.item.selector, false, 64, false, 32, compressed);
        (used,) = runner.execMeasured(Env.wrap(envelope, address(factory), type(StaticLens).creationCode, w, cfg));
    }

    function test_gas_clear_100elements() public {
        uint256 used = page(110, false) - page(10, false);
        require(used < 100 * 3_000, string.concat("clear per element: ", Env.VM.toString(used / 100)));
    }

    function test_gas_compressed_100elements() public {
        uint256 used = page(110, true) - page(10, true);
        require(used < 100 * 6_000, string.concat("compressed per element: ", Env.VM.toString(used / 100)));
    }
}
