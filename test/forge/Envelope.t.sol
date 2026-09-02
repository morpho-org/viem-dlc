// SPDX-License-Identifier: MIT
pragma solidity ^0.8.25;

import { DynInLens, DynOutLens, Env, Factory, HungryLens, Runner, StaticLens } from "./Fixtures.sol";

bytes4 constant OK = 0x1580d19d;
bytes4 constant OOG = 0xcc0bd34c;
bytes4 constant DEPLOY_FAILED = 0x101bb98d;
bytes4 constant MALFORMED = 0xace36ecd;

contract EnvelopeTest {
    bytes envelope;
    bytes compressed;
    Factory factory = new Factory();
    Runner runner = new Runner();

    function setUp() public {
        envelope = Env.build("build:RevertEnvelope");
        compressed = Env.build("build:RevertEnvelopeCompressed");
    }

    uint256 constant STATIC_CFG = uint256(uint32(StaticLens.item.selector)) << 224 | 64 << 64 | 32;

    function staticPage(uint256[] memory a, uint256[] memory mode) internal returns (bytes memory) {
        StaticLens.In[] memory xs = new StaticLens.In[](a.length);
        for (uint256 i; i < a.length; i++) xs[i] = StaticLens.In(a[i], mode[i]);
        return runner.exec(Env.wrap(envelope, address(factory), type(StaticLens).creationCode, abi.encodeCall(StaticLens.page, (xs)), STATIC_CFG));
    }

    function page(bytes memory ret) internal pure returns (uint256[] memory r, uint256[] memory k) {
        require(bytes4(ret) == OK, "ok sentinel");
        (r, k) = abi.decode(Env.body(ret), (uint256[], uint256[]));
    }

    function test_densePage_usedPrefixOnly() public {
        uint256[] memory a = new uint256[](3); (a[0], a[1], a[2]) = (1, 2, 3);
        bytes memory ret = staticPage(a, new uint256[](3));
        (uint256[] memory r, uint256[] memory k) = page(ret);
        require(r.length == 3 && r[0] == 2 && r[1] == 4 && r[2] == 6 && k.length == 0, "page");
        require(ret.length == 4 + 0x60 + 3 * 32 + 0x20, "used prefix only");
    }

    function test_declinesThenDeathTag() public {
        uint256[] memory a = new uint256[](6);
        uint256[] memory m = new uint256[](6);
        for (uint256 i; i < 6; i++) a[i] = i + 1;
        (m[1], m[2], m[4]) = (1, 2, 3);
        (uint256[] memory r, uint256[] memory k) = page(staticPage(a, m));
        require(r.length == 2 && r[0] == 2 && r[1] == 8, "dense prefix");
        require(k.length == 3 && k[0] == 1 && k[1] == 2 && k[2] == ~uint256(4), "skips then tag");
    }

    function test_deathAtHead() public {
        uint256[] memory m = new uint256[](3); m[0] = 3;
        (uint256[] memory r, uint256[] memory k) = page(staticPage(new uint256[](3), m));
        require(r.length == 0 && k.length == 1 && k[0] == ~uint256(0), "([], [~0])");
    }

    function test_malformedStatic() public {
        uint256[] memory m = new uint256[](2); m[1] = 4;
        bytes memory ret = staticPage(new uint256[](2), m);
        (uint256 idx, uint256 size) = abi.decode(Env.body(ret), (uint256, uint256));
        require(bytes4(ret) == MALFORMED && ret.length == 0x44 && idx == 1 && size == 0x40, "MalformedResult(1, 0x40)");
    }

    function test_deployOutOfGas() public {
        bytes memory td = abi.encodeWithSelector(bytes4(0), uint256(0x20), uint256(1), uint256(0));
        uint256 cfg = Env.config(HungryLens.item.selector, false, 32, false, 32);
        bytes memory ic = Env.wrap(envelope, address(factory), type(HungryLens).creationCode, td, cfg);
        (bool called, bytes memory raw) = address(runner).call{gas: 2_000_000}(abi.encodeCall(Runner.exec, (ic)));
        bytes memory ret = abi.decode(raw, (bytes));
        require(called && ret.length == 4 && bytes4(ret) == OOG, "OOG_SENTINEL");
    }

    /// Any grant is either a prologue failure or a page; no failure once a grant has produced a page.
    function test_gasSweep_noCorpseAboveFirstPage() public {
        uint256 n = 60;
        StaticLens.In[] memory xs = new StaticLens.In[](n);
        for (uint256 i; i < n; i++) xs[i] = StaticLens.In(15_000 + i * 500, 5);
        bytes memory ic = Env.wrap(envelope, address(factory), type(StaticLens).creationCode, abi.encodeCall(StaticLens.page, (xs)), STATIC_CFG);
        bool seenPage;
        bool seenFull;
        for (uint256 g = 100_000; g < 4_000_000; g += 12_345) {
            (bool called, bytes memory raw) = address(runner).call{gas: g}(abi.encodeCall(Runner.exec, (ic)));
            if (!called) continue;
            bytes memory ret = abi.decode(raw, (bytes));
            bytes4 sel = ret.length >= 4 ? bytes4(ret) : bytes4(0);
            if (sel != OK) {
                require(!seenPage && (sel == OOG || sel == DEPLOY_FAILED || ret.length == 0), "corpse above first page");
                continue;
            }
            seenPage = true;
            (uint256[] memory r, uint256[] memory k) = abi.decode(Env.body(ret), (uint256[], uint256[]));
            require(r.length + k.length >= 1, "attempted nothing");
            for (uint256 j; j < k.length; j++) {
                if (k[j] > type(uint256).max / 2) require(j == k.length - 1 && ~k[j] == r.length + k.length - 1, "tag last");
            }
            seenFull = seenFull || r.length == n;
        }
        require(seenPage && seenFull, "sweep never completed a page");
    }

    function test_dynamicOutput() public {
        uint256[] memory xs = new uint256[](5); (xs[0], xs[1], xs[2], xs[3], xs[4]) = (0, 5, 7, 40, 256);
        uint256 cfg = Env.config(DynOutLens.item.selector, false, 32, true, 32 + 256);
        bytes memory ret = runner.exec(Env.wrap(envelope, address(factory), type(DynOutLens).creationCode, abi.encodeCall(DynOutLens.page, (xs)), cfg));
        require(bytes4(ret) == OK, "ok");
        (bytes[] memory r, uint256[] memory k) = abi.decode(Env.body(ret), (bytes[], uint256[]));
        require(r.length == 4 && k.length == 1 && k[0] == 2, "shape");
        require(r[0].length == 0 && r[1].length == 5 && r[2].length == 40 && r[3].length == 256, "tails");
        require(r[1][0] == bytes1(uint8(5)) && r[3][255] == 0, "content");
    }

    function test_dynamicOutput_shortReturnIsMalformed() public {
        uint256[] memory xs = new uint256[](2); (xs[0], xs[1]) = (3, 9);
        uint256 cfg = Env.config(DynOutLens.item.selector, false, 32, true, 32 + 256);
        bytes memory ret = runner.exec(Env.wrap(envelope, address(factory), type(DynOutLens).creationCode, abi.encodeCall(DynOutLens.page, (xs)), cfg));
        (uint256 idx, uint256 size) = abi.decode(Env.body(ret), (uint256, uint256));
        require(bytes4(ret) == MALFORMED && idx == 1 && size == 0x10, "MalformedResult(1, 0x10)");
    }

    function test_dynamicInput_oversizeDeclined() public {
        bytes[] memory xs = new bytes[](4);
        (xs[0], xs[1], xs[2], xs[3]) = (hex"01", hex"010203", new bytes(70), new bytes(64));
        uint256 cfg = Env.config(DynInLens.item.selector, true, 32 + 64, false, 32);
        bytes memory ret = runner.exec(Env.wrap(envelope, address(factory), type(DynInLens).creationCode, abi.encodeCall(DynInLens.page, (xs)), cfg));
        (uint256[] memory r, uint256[] memory k) = page(ret);
        require(r.length == 2 && k.length == 2 && k[0] == 1 && k[1] == 2, "reverted and oversize skipped");
        require(r[0] == uint256(keccak256(hex"01")) && r[1] == uint256(keccak256(new bytes(64))), "values");
    }

    function test_compressedEnvelope() public {
        // FLZ(abi.encodeCall(StaticLens.page, ([(1,0),(2,0),(3,0)])))
        bytes memory td = hex"0441cc75c200e015000020e0151e0100032003e013000001e0131ce01a000002e01a23e01300e0139f040000000000";
        (uint256[] memory r, uint256[] memory k) = page(runner.exec(Env.wrap(compressed, address(factory), type(StaticLens).creationCode, td, STATIC_CFG)));
        require(r.length == 3 && r[0] == 2 && r[1] == 4 && r[2] == 6 && k.length == 0, "page");
    }
}
