// SPDX-License-Identifier: MIT
pragma solidity ^0.8.25;

import { DynInLens, DynOutLens, EchoLens, Env, Factory, HungryLens, Runner, StaticLens, WideLens } from "./Fixtures.sol";

bytes4 constant OOG = 0xcc0bd34c;
bytes4 constant DEPLOY_FAILED = 0x101bb98d;
bytes4 constant MALFORMED = 0xace36ecd;
bytes4 constant MALFORMED_INPUT = 0xf5880484;

contract EnvelopeTest {
    bytes envelope;
    Factory factory = new Factory();
    Runner runner = new Runner();

    function setUp() public {
        envelope = Env.build();
    }

    /*//////////////////////////////////////////////////////////////
                              BUILDERS
    //////////////////////////////////////////////////////////////*/

    /// What a mode-6 element leaves itself to return with: a little over the return's own cost.
    uint256 constant DRAIN_TO = 60;

    uint256 constant STATIC_CFG = uint256(uint32(StaticLens.item.selector)) << 224 | 64 << 64 | 32;

    function staticInputs(uint256[] memory a, uint256[] memory mode) internal pure returns (bytes memory) {
        StaticLens.In[] memory xs = new StaticLens.In[](a.length);
        for (uint256 i; i < a.length; i++) xs[i] = StaticLens.In(a[i], mode[i]);
        return Env.wire(abi.encode(xs));
    }

    function staticIc(uint256[] memory a, uint256[] memory mode) internal view returns (bytes memory) {
        return Env.wrap(envelope, address(factory), type(StaticLens).creationCode, staticInputs(a, mode), STATIC_CFG);
    }

    function staticPage(uint256[] memory a, uint256[] memory mode) internal returns (bytes memory) {
        return runner.exec(staticIc(a, mode));
    }

    function dynOutIc(uint256[] memory xs) internal view returns (bytes memory) {
        uint256 cfg = Env.config(DynOutLens.item.selector, false, 32, true, 0, false);
        return Env.wrap(envelope, address(factory), type(DynOutLens).creationCode, Env.wire(abi.encode(xs)), cfg);
    }

    function dynInIc(bytes[] memory xs) internal view returns (bytes memory) {
        uint256 cfg = Env.config(DynInLens.item.selector, true, 0, false, 32, false);
        return Env.wrap(envelope, address(factory), type(DynInLens).creationCode, Env.wireDyn(xs), cfg);
    }

    function modes(uint256 n) internal pure returns (uint256[] memory m) {
        m = new uint256[](n);
    }

    function values(uint256 n) internal pure returns (uint256[] memory a) {
        a = new uint256[](n);
        for (uint256 i; i < n; i++) a[i] = i + 1;
    }

    function summaryWithGas(bytes memory ic, uint256 g) internal returns (bool called, bytes4 sel, uint256 nA) {
        bytes memory raw;
        (called, raw) = address(runner).call{gas: g}(abi.encodeCall(Runner.summary, (ic)));
        if (called) (sel, nA,) = abi.decode(raw, (bytes4, uint256, uint256));
    }

    function execWithGas(bytes memory ic, uint256 g) internal returns (bool called, bytes memory ret) {
        bytes memory raw;
        (called, raw) = address(runner).call{gas: g}(abi.encodeCall(Runner.exec, (ic)));
        if (called) ret = abi.decode(raw, (bytes));
    }

    /*//////////////////////////////////////////////////////////////
                             STATIC PAGES
    //////////////////////////////////////////////////////////////*/

    function test_densePage_usedPrefixOnly() public {
        bytes memory ret = staticPage(values(3), modes(3));
        Env.Page memory p = Env.page(ret);
        uint256[] memory r = Env.uints(p);
        require(p.nA == 3 && r.length == 3 && r[0] == 2 && r[1] == 4 && r[2] == 6 && p.skipped.length == 0 && !p.died, "page");
        require(ret.length == Env.HEADER + 3 * (32 + 32), "used prefix only");
    }

    function test_declinesThenDeathTag() public {
        uint256[] memory m = modes(6);
        (m[1], m[2], m[4]) = (1, 2, 3);
        (bool called, bytes memory ret) = execWithGas(staticIc(values(6), m), 3_000_000);
        require(called, "called");
        Env.Page memory p = Env.page(ret);
        uint256[] memory r = Env.uints(p);
        require(p.nA == 5 && r.length == 2 && r[0] == 2 && r[1] == 8, "dense prefix");
        require(p.skipped.length == 2 && p.skipped[0] == 1 && p.skipped[1] == 2, "declines");
        require(p.died && p.diedAt == 4, "death last");
    }

    /// A callee reverting with data into a static out region leaves garbage past `P`; the next success overwrites it.
    function test_revertWithDataThenSuccess() public {
        uint256[] memory m = modes(3);
        m[1] = 2;
        Env.Page memory p = Env.page(staticPage(values(3), m));
        uint256[] memory r = Env.uints(p);
        require(r.length == 2 && r[0] == 2 && r[1] == 6 && p.skipped.length == 1 && p.skipped[0] == 1, "values");
    }

    function test_deathAtHead() public {
        uint256[] memory m = modes(3);
        m[0] = 3;
        (bool called, bytes memory ret) = execWithGas(staticIc(new uint256[](3), m), 3_000_000);
        require(called, "called");
        Env.Page memory p = Env.page(ret);
        require(p.nA == 1 && p.results.length == 0 && p.skipped.length == 0 && p.died && p.diedAt == 0, "[~0]");
        require(ret.length == Env.HEADER + 32, "one record");
    }

    /*//////////////////////////////////////////////////////////////
                              TELEMETRY
    //////////////////////////////////////////////////////////////*/

    /// The relations any sum, sum of squares and maximum of `nA` samples satisfy.
    function consistent(Env.Page memory p, uint256 served) internal pure returns (bool) {
        if (served == 0) return p.sum == 0 && p.sumSq == 0 && p.gmax == 0;
        return p.sum > 0 && p.gmax <= p.sum && p.sum * p.sum <= served * p.sumSq && p.sumSq <= p.sum * p.gmax;
    }

    /// The per-attempt mean tracks the frame's own marginal cost per element.
    function test_telemetry_densePageTracksMarginalGas() public {
        (uint256 used10,) = runner.execMeasured(staticIc(values(10), modes(10)));
        (uint256 used110, bytes memory ret) = runner.execMeasured(staticIc(values(110), modes(110)));
        Env.Page memory p = Env.page(ret);
        require(consistent(p, 110) && p.budget > p.sum, "relations");
        (, bytes memory capped) = execWithGas(staticIc(values(3), modes(3)), 2_000_000);
        require(Env.page(capped).budget < 2_000_000, "budget under the grant");
        uint256 marginal = (used110 - used10) / 100;
        uint256 mean = p.sum / 110;
        require(mean * 100 > marginal * 90 && mean * 100 < marginal * 110, "mean within 10% of marginal");
        require(p.gmax < mean * 2, "flat costs");
    }

    function test_telemetry_declinesAreCharged() public {
        uint256[] memory m = modes(3);
        (m[0], m[1], m[2]) = (1, 1, 1);
        Env.Page memory p = Env.page(staticPage(values(3), m));
        require(p.nA == 3 && p.results.length == 0 && consistent(p, 3), "three charged declines");
    }

    function test_telemetry_headDeathChargesNothing() public {
        uint256[] memory m = modes(3);
        m[0] = 3;
        (, bytes memory ret) = execWithGas(staticIc(new uint256[](3), m), 3_000_000);
        Env.Page memory p = Env.page(ret);
        require(p.died && p.diedAt == 0 && p.budget > 0 && consistent(p, 0), "budget only");
    }

    /// The first page a rising grant produces is a head refusal: nothing attempted, nothing charged,
    /// and a budget that saturates at zero rather than wrapping below the reserve.
    function test_telemetry_headRefusalChargesNothing() public {
        bytes memory ic = staticIc(values(3), modes(3));
        for (uint256 g = 150_000; g < 600_000; g += 2_000) {
            (bool called, bytes memory ret) = execWithGas(ic, g);
            if (!called || bytes4(ret) != Env.OK) continue;
            Env.Page memory p = Env.page(ret);
            require(p.nA == 1 && p.died && p.diedAt == 0 && consistent(p, 0), "[~0] uncharged");
            require(p.budget < g, "budget saturates");
            return;
        }
        revert("no page under 600k");
    }

    /// A page that dies at 4 charges the same as a page over its first four elements alone.
    function test_telemetry_deathIsNotCharged() public {
        uint256[] memory m = modes(6);
        (m[1], m[2], m[4]) = (1, 2, 3);
        (, bytes memory ret) = execWithGas(staticIc(values(6), m), 3_000_000);
        Env.Page memory dying = Env.page(ret);
        uint256[] memory m4 = modes(4);
        (m4[1], m4[2]) = (1, 2);
        Env.Page memory whole = Env.page(staticPage(values(4), m4));
        require(dying.died && dying.diedAt == 4 && consistent(dying, 4), "shape");
        uint256 diff = dying.sum > whole.sum ? dying.sum - whole.sum : whole.sum - dying.sum;
        require(diff * 100 < whole.sum, "within 1%");
    }

    function test_telemetry_compressedChargesDecompression() public {
        bytes memory w = staticInputs(values(3), modes(3));
        Env.Page memory clear = Env.page(staticPage(values(3), modes(3)));
        Env.Page memory z = Env.page(runner.exec(compressedStaticIc(Env.compress(w))));
        require(consistent(z, 3) && z.sum > clear.sum, "compressed costs more");
    }

    function test_malformedStatic() public {
        uint256[] memory m = modes(2);
        m[1] = 4;
        bytes memory ret = staticPage(new uint256[](2), m);
        (uint256 idx, uint256 size) = abi.decode(Env.body(ret), (uint256, uint256));
        require(bytes4(ret) == MALFORMED && ret.length == 0x44 && idx == 1 && size == 0x40, "MalformedResult(1, 0x40)");
    }

    /// A 32-word result at the wire's element count is served in full under 10M: nothing is reserved up front.
    function test_wideStaticResult_largeN() public {
        uint256 n = 1000;
        uint256[] memory xs = new uint256[](n);
        for (uint256 i; i < n; i++) xs[i] = i;
        uint256 cfg = Env.config(WideLens.item.selector, false, 32, false, 1024, false);
        bytes memory ic = Env.wrap(envelope, address(factory), type(WideLens).creationCode, Env.wire(abi.encode(xs)), cfg);
        (uint256 used, bytes memory ret) = runner.execMeasured(ic);
        Env.Page memory p = Env.page(ret);
        require(p.nA == n && p.results.length == n && !p.died, "full page");
        require(used < 10_000_000, string.concat("over 10M: ", Env.VM.toString(used)));
        require(p.results[0].length == 1024 && Env.word(p.results[5], 31 * 32) == 36, "wide value");
        require(ret.length == Env.HEADER + n * (32 + 1024), "record length");
    }

    /*//////////////////////////////////////////////////////////////
                              PROLOGUE
    //////////////////////////////////////////////////////////////*/

    function test_occupiedTargetFails() public {
        (bool ok,) = address(factory).call(abi.encodePacked(Env.SALT, type(StaticLens).creationCode));
        require(ok, "predeploy");
        bytes memory ret = staticPage(new uint256[](1), new uint256[](1));
        require(bytes4(ret) == DEPLOY_FAILED && ret.length == 0x44, "CounterfactualDeployFailed");
    }

    function test_deployOutOfGas() public {
        uint256[] memory xs = new uint256[](1);
        uint256 cfg = Env.config(HungryLens.item.selector, false, 32, false, 32, false);
        bytes memory ic = Env.wrap(envelope, address(factory), type(HungryLens).creationCode, Env.wire(abi.encode(xs)), cfg);
        (bool called, bytes memory ret) = execWithGas(ic, 2_000_000);
        require(called && ret.length == 4 && bytes4(ret) == OOG, "OOG_SENTINEL");
    }

    function malformedInputOf(bytes memory ret) internal pure returns (uint256 idx) {
        require(bytes4(ret) == MALFORMED_INPUT && ret.length == 0x24, "MalformedInput");
        idx = abi.decode(Env.body(ret), (uint256));
    }

    function test_malformedInput_bodyLength() public {
        uint256[] memory xs = values(2);
        bytes memory w = Env.wire(abi.encode(xs));
        // bodyLen claims one more word than the wire carries
        assembly { mstore(add(w, 0x40), 0x60) }
        uint256 cfg = Env.config(DynOutLens.item.selector, false, 32, true, 0, false);
        bytes memory ret = runner.exec(Env.wrap(envelope, address(factory), type(DynOutLens).creationCode, w, cfg));
        require(malformedInputOf(ret) == 2, "MalformedInput(n)");
    }

    function test_malformedInput_staticStride() public {
        uint256[] memory xs = values(3);
        bytes memory w = Env.wire(abi.encode(xs));
        // n claims four strides in a three-stride body
        assembly { mstore(add(w, 0x20), 4) }
        uint256 cfg = Env.config(DynOutLens.item.selector, false, 32, true, 0, false);
        bytes memory ret = runner.exec(Env.wrap(envelope, address(factory), type(DynOutLens).creationCode, w, cfg));
        require(malformedInputOf(ret) == 4, "MalformedInput(n)");
    }

    function test_malformedInput_misalignedLength() public {
        bytes[] memory xs = new bytes[](2);
        (xs[0], xs[1]) = (hex"01", hex"02");
        bytes memory w = Env.wireDyn(xs);
        // second record's L := 0x21
        assembly { mstore(add(w, add(0x60, 0x60)), 0x21) }
        bytes memory ret = runner.exec(Env.wrap(envelope, address(factory), type(DynInLens).creationCode, w, Env.config(DynInLens.item.selector, true, 0, false, 32, false)));
        require(malformedInputOf(ret) == 1, "MalformedInput(1)");
    }

    function test_malformedInput_overlongLength() public {
        bytes[] memory xs = new bytes[](2);
        (xs[0], xs[1]) = (hex"01", hex"02");
        bytes memory w = Env.wireDyn(xs);
        // second record's L runs past the body by one word
        assembly { mstore(add(w, add(0x60, 0x60)), 0x60) }
        bytes memory ret = runner.exec(Env.wrap(envelope, address(factory), type(DynInLens).creationCode, w, Env.config(DynInLens.item.selector, true, 0, false, 32, false)));
        require(malformedInputOf(ret) == 1, "MalformedInput(1)");
    }

    function test_malformedInput_zeroLength() public {
        bytes memory w = abi.encodePacked(uint256(1), uint256(32), uint256(0));
        uint256 cfg = Env.config(DynInLens.item.selector, true, 0, false, 32, false);
        bytes memory ret = runner.exec(Env.wrap(envelope, address(factory), type(DynInLens).creationCode, w, cfg));
        require(malformedInputOf(ret) == 0, "MalformedInput(0)");
    }

    function test_malformedInput_zeroLength_compressed() public {
        bytes memory w = Env.wireOf(1, 32, Env.flzLiterals(new bytes(32)));
        uint256 cfg = Env.config(DynInLens.item.selector, true, 0, false, 32, true);
        bytes memory ret = runner.exec(Env.wrap(envelope, address(factory), type(DynInLens).creationCode, w, cfg));
        require(malformedInputOf(ret) == 0, "MalformedInput(0)");
    }

    function test_malformedInput_trailingRecord() public {
        bytes[] memory xs = new bytes[](2);
        (xs[0], xs[1]) = (hex"01", hex"02");
        bytes memory w = Env.wireDyn(xs);
        // n claims one element; the body carries two
        assembly { mstore(add(w, 0x20), 1) }
        bytes memory ret = runner.exec(Env.wrap(envelope, address(factory), type(DynInLens).creationCode, w, Env.config(DynInLens.item.selector, true, 0, false, 32, false)));
        require(malformedInputOf(ret) == 0, "MalformedInput(n - 1)");
    }

    function test_malformedInput_truncatedRecord() public {
        bytes[] memory xs = new bytes[](1);
        xs[0] = hex"01";
        bytes memory w = Env.wireDyn(xs);
        // n claims a second element the body does not carry
        assembly { mstore(add(w, 0x20), 2) }
        bytes memory ret = runner.exec(Env.wrap(envelope, address(factory), type(DynInLens).creationCode, w, Env.config(DynInLens.item.selector, true, 0, false, 32, false)));
        require(malformedInputOf(ret) == 1, "MalformedInput(1)");
    }

    /*//////////////////////////////////////////////////////////////
                            DYNAMIC LAYOUTS
    //////////////////////////////////////////////////////////////*/

    function test_dynamicOutput() public {
        uint256[] memory xs = new uint256[](5);
        (xs[0], xs[1], xs[2], xs[3], xs[4]) = (0, 5, 7, 40, 256);
        Env.Page memory p = Env.page(runner.exec(dynOutIc(xs)));
        require(p.nA == 5 && p.results.length == 4 && p.skipped.length == 1 && p.skipped[0] == 2 && !p.died, "shape");
        // records carry the padded ABI tail: length word plus data
        require(p.results[0].length == 32 && p.results[1].length == 64 && p.results[2].length == 96 && p.results[3].length == 288, "tails");
        require(Env.word(p.results[1], 0) == 5 && p.results[1][32] == bytes1(uint8(5)) && Env.word(p.results[3], 0) == 256, "content");
    }

    function test_dynamicOutput_revertWithDataThenSuccess() public {
        uint256[] memory xs = new uint256[](2);
        (xs[0], xs[1]) = (8, 5);
        Env.Page memory p = Env.page(runner.exec(dynOutIc(xs)));
        require(p.results.length == 1 && p.skipped.length == 1 && p.skipped[0] == 0 && Env.word(p.results[0], 0) == 5, "shape");
    }

    function test_dynamicOutput_shortReturnIsMalformed() public {
        uint256[] memory xs = new uint256[](2);
        (xs[0], xs[1]) = (3, 9);
        bytes memory ret = runner.exec(dynOutIc(xs));
        (uint256 idx, uint256 size) = abi.decode(Env.body(ret), (uint256, uint256));
        require(bytes4(ret) == MALFORMED && idx == 1 && size == 0x10, "MalformedResult(1, 0x10)");
    }

    function test_dynamicOutput_badHeadIsMalformed() public {
        uint256[] memory xs = new uint256[](1);
        xs[0] = 10;
        bytes memory ret = runner.exec(dynOutIc(xs));
        (uint256 idx, uint256 size) = abi.decode(Env.body(ret), (uint256, uint256));
        require(bytes4(ret) == MALFORMED && idx == 0 && size == 0x40, "MalformedResult(0, 0x40)");
    }

    /// A result the frame cannot afford to keep is reported as a death and succeeds alone.
    function test_dynamicOutput_unaffordableResultIsDeath() public {
        uint256[] memory xs = new uint256[](2);
        (xs[0], xs[1]) = (5, 20_000);
        bytes memory ic = dynOutIc(xs);
        uint256 g = firstPageGrant(ic);
        bool seenDeath;
        for (; g < 6_000_000 && !seenDeath; g += 7) {
            (bool called, bytes memory ret) = execWithGas(ic, g);
            require(called, "corpse above first page");
            if (bytes4(ret) != Env.OK) continue;
            Env.Page memory p = Env.page(ret);
            if (p.died && p.diedAt == 1) {
                require(p.nA == 2 && p.results.length == 1, "death at 1");
                seenDeath = true;
            }
        }
        require(seenDeath, "never converted a result to a death");
        uint256[] memory alone = new uint256[](1);
        alone[0] = 20_000;
        Env.Page memory pAlone = Env.page(runner.exec(dynOutIc(alone)));
        require(pAlone.results.length == 1 && pAlone.results[0].length == 32 + 20_000, "alone");
    }

    function test_dynamicInput() public {
        bytes[] memory xs = new bytes[](4);
        (xs[0], xs[1], xs[2], xs[3]) = (hex"01", hex"010203", new bytes(70), new bytes(64));
        Env.Page memory p = Env.page(runner.exec(dynInIc(xs)));
        uint256[] memory r = Env.uints(p);
        require(r.length == 3 && p.skipped.length == 1 && p.skipped[0] == 1, "reverted skipped");
        require(r[0] == uint256(keccak256(hex"01")) && r[1] == uint256(keccak256(new bytes(70))) && r[2] == uint256(keccak256(new bytes(64))), "values");
    }

    function test_dynamicInDynamicOut() public {
        bytes[] memory xs = new bytes[](3);
        (xs[0], xs[1], xs[2]) = (hex"01", hex"010203", hex"aabb");
        uint256 cfg = Env.config(EchoLens.item.selector, true, 0, true, 0, false);
        Env.Page memory p = Env.page(runner.exec(Env.wrap(envelope, address(factory), type(EchoLens).creationCode, Env.wireDyn(xs), cfg)));
        require(p.results.length == 2 && p.skipped.length == 1 && p.skipped[0] == 1, "shape");
        require(Env.word(p.results[0], 0) == 2 && p.results[0][32] == 0x01 && p.results[0][33] == 0x01, "echo 01");
        require(Env.word(p.results[1], 0) == 4 && p.results[1][35] == 0xbb, "echo aabb");
    }

    /*//////////////////////////////////////////////////////////////
                              COMPRESSED
    //////////////////////////////////////////////////////////////*/

    function test_compressedStatic() public {
        bytes memory w = Env.compress(staticInputs(values(3), modes(3)));
        uint256 cfg = Env.config(StaticLens.item.selector, false, 64, false, 32, true);
        Env.Page memory p = Env.page(runner.exec(Env.wrap(envelope, address(factory), type(StaticLens).creationCode, w, cfg)));
        uint256[] memory r = Env.uints(p);
        require(r.length == 3 && r[0] == 2 && r[1] == 4 && r[2] == 6 && p.skipped.length == 0, "page");
    }

    function test_compressedDynamicInput() public {
        bytes[] memory xs = new bytes[](3);
        (xs[0], xs[1], xs[2]) = (hex"01", hex"010203", new bytes(64));
        bytes memory w = Env.compress(Env.wireDyn(xs));
        uint256 cfg = Env.config(DynInLens.item.selector, true, 0, false, 32, true);
        Env.Page memory p = Env.page(runner.exec(Env.wrap(envelope, address(factory), type(DynInLens).creationCode, w, cfg)));
        uint256[] memory r = Env.uints(p);
        require(r.length == 2 && p.skipped.length == 1 && r[0] == uint256(keccak256(hex"01")) && r[1] == uint256(keccak256(new bytes(64))), "page");
    }

    function test_compressed_bodyLengthMismatch() public {
        bytes memory w = Env.compress(staticInputs(values(3), modes(3)));
        assembly { mstore(add(w, 0x40), 0xa0) }
        uint256 cfg = Env.config(StaticLens.item.selector, false, 64, false, 32, true);
        bytes memory ret = runner.exec(Env.wrap(envelope, address(factory), type(StaticLens).creationCode, w, cfg));
        require(malformedInputOf(ret) == 3, "MalformedInput(n)");
    }

    uint256 constant STATIC_CFG_Z = uint256(uint32(StaticLens.item.selector)) << 224 | 1 << 221 | 64 << 64 | 32;

    function compressedStaticIc(bytes memory w) internal view returns (bytes memory) {
        return Env.wrap(envelope, address(factory), type(StaticLens).creationCode, w, STATIC_CFG_Z);
    }

    /// 50,000 identical elements (3.2 MB of body, ~37 KiB of wire): a page under 2M, more under 10M.
    function test_compressionBomb_pagesUnderAnyGrant() public {
        bytes memory ic = compressedStaticIc(Env.wireOf(50_000, 50_000 * 64, Env.flzRepeat(bytes32(0), 100_000)));
        (bool called, bytes4 sel, uint256 small) = summaryWithGas(ic, 2_000_000);
        require(called && sel == Env.OK && small >= 1, "page under 2M");
        uint256 large;
        (called, sel, large) = summaryWithGas(ic, 10_000_000);
        require(called && sel == Env.OK && large > small, "progress scales with gas");
    }

    /// 2,000 distinct compressible elements (128 KiB through the 16 KiB history) decode to the right values.
    function test_compressed_rebase() public {
        uint256 n = 2_000;
        uint256[] memory a = new uint256[](n);
        for (uint256 i; i < n; i++) a[i] = i % 128;
        bytes memory w = Env.compress(staticInputs(a, modes(n)));
        (uint256 used, bytes memory ret) = runner.execMeasured(compressedStaticIc(w));
        Env.Page memory p = Env.page(ret);
        uint256[] memory r = Env.uints(p);
        require(p.nA == n && r.length == n, "full page");
        for (uint256 i; i < n; i++) require(r[i] == 2 * (i % 128), "value");
        require(used < 12_000_000, "gas");
    }

    /// Dynamic elements of ~12 KiB each: the length word and the record both materialize across rebases.
    function test_compressed_rebaseInsideDynamicElement() public {
        bytes[] memory xs = new bytes[](3);
        (xs[0], xs[1], xs[2]) = (new bytes(12_000), hex"010203", new bytes(9_000));
        bytes memory w = Env.compress(Env.wireDyn(xs));
        uint256 cfg = Env.config(DynInLens.item.selector, true, 0, false, 32, true);
        Env.Page memory p = Env.page(runner.exec(Env.wrap(envelope, address(factory), type(DynInLens).creationCode, w, cfg)));
        uint256[] memory r = Env.uints(p);
        require(r.length == 2 && p.skipped.length == 1 && p.skipped[0] == 1, "shape");
        require(r[0] == uint256(keccak256(new bytes(12_000))) && r[1] == uint256(keccak256(new bytes(9_000))), "values");
    }

    function test_compressed_witnessStreamsDecode() public {
        bytes memory body = staticInputs(values(3), modes(3));
        Env.Page memory lit = Env.page(runner.exec(compressedStaticIc(Env.wireOf(3, body.length - 64, Env.flzLiterals(Env.slice(body, 64, body.length - 64))))));
        uint256[] memory r = Env.uints(lit);
        require(r.length == 3 && r[0] == 2 && r[1] == 4 && r[2] == 6, "literals");
        bytes memory zeros = new bytes(3 * 64);
        Env.Page memory d1 = Env.page(runner.exec(compressedStaticIc(Env.wireOf(3, zeros.length, Env.flzDist1(zeros)))));
        require(Env.uints(d1).length == 3 && Env.uints(d1)[2] == 0, "dist1");
    }

    function test_malformedStream_tokenPastEnd() public {
        bytes memory stream = Env.flzRepeat(bytes32(0), 4);
        bytes memory ret = runner.exec(compressedStaticIc(Env.wireOf(2, 128, Env.slice(stream, 0, stream.length - 1))));
        require(malformedInputOf(ret) == 0, "MalformedInput(0)");
    }

    function test_malformedStream_exhaustedMidRecord() public {
        // Two 32-byte literals carry one element; the second element has no bytes at all.
        bytes memory zeros = new bytes(64);
        bytes memory ret = runner.exec(compressedStaticIc(Env.wireOf(2, 128, Env.flzLiterals(zeros))));
        require(malformedInputOf(ret) == 1, "MalformedInput(1)");
    }

    function test_malformedStream_backReferenceBeforeHistory() public {
        bytes memory stream = abi.encodePacked(bytes1(uint8(224)), bytes1(uint8(253)), bytes1(uint8(31)));
        bytes memory ret = runner.exec(compressedStaticIc(Env.wireOf(1, 64, stream)));
        require(malformedInputOf(ret) == 0, "MalformedInput(0)");
    }

    function test_malformedStream_trailingTokens() public {
        bytes memory stream = abi.encodePacked(Env.flzRepeat(bytes32(0), 4), bytes1(0), bytes1(0));
        bytes memory ret = runner.exec(compressedStaticIc(Env.wireOf(2, 128, stream)));
        require(malformedInputOf(ret) == 1, "MalformedInput(n - 1)");
    }

    function test_malformedStream_overshootPastBody() public {
        // The stream produces 160 bytes for a 128-byte body: the last token's surplus is left pending.
        bytes memory ret = runner.exec(compressedStaticIc(Env.wireOf(2, 128, Env.flzRepeat(bytes32(0), 5))));
        require(malformedInputOf(ret) == 1, "MalformedInput(n - 1)");
    }

    /*//////////////////////////////////////////////////////////////
                              GAS SWEEPS
    //////////////////////////////////////////////////////////////*/

    /// The smallest grant (to 1k) at which the envelope produces a page.
    function firstPageGrant(bytes memory ic) internal returns (uint256 g) {
        for (g = 50_000; g < 5_000_000; g += 1_000) {
            uint256 mark;
            assembly { mark := mload(0x40) }
            (bool called, bytes memory ret) = execWithGas(ic, g);
            bool done = called && bytes4(ret) == Env.OK;
            assembly { mstore(0x40, mark) }
            if (done) return g;
        }
        revert("no page under 5M");
    }

    struct Sweep {
        uint256 pages;
        uint256 deaths;
        uint256 malformed;
        uint256 maxAdjudicated;
        uint256 maxResults;
    }

    /// Every grant in [lo, hi) is a prologue failure (only before the first page), a well-formed
    /// page, or a protocol error — never an empty failure once a page has been seen. Iteration
    /// memory is reused: a sweep is thousands of calls, each copying the initcode.
    function sweep(bytes memory ic, uint256 lo, uint256 hi, uint256 step) internal returns (Sweep memory s) {
        bool seenPage;
        for (uint256 g = lo; g < hi; g += step) {
            uint256 mark;
            assembly { mark := mload(0x40) }
            (bool called, bytes memory ret) = execWithGas(ic, g);
            bytes4 sel = called && ret.length >= 4 ? bytes4(ret) : bytes4(0);
            if (sel == MALFORMED) {
                s.malformed++;
            } else if (sel != Env.OK) {
                if (seenPage || (called && sel != OOG && sel != DEPLOY_FAILED && ret.length != 0)) {
                    revert(string.concat("corpse above first page at ", Env.VM.toString(g), " called=", called ? "1" : "0", " len=", Env.VM.toString(ret.length)));
                }
            } else {
                seenPage = true;
                Env.Page memory p = Env.page(ret);
                s.pages++;
                if (p.died) s.deaths++;
                if (p.nA > s.maxAdjudicated) s.maxAdjudicated = p.nA;
                if (p.results.length > s.maxResults) s.maxResults = p.results.length;
            }
            assembly { mstore(0x40, mark) }
        }
    }

    function merge(Sweep memory a, Sweep memory b) internal pure {
        a.pages += b.pages;
        a.deaths += b.deaths;
        a.malformed += b.malformed;
        if (b.maxAdjudicated > a.maxAdjudicated) a.maxAdjudicated = b.maxAdjudicated;
        if (b.maxResults > a.maxResults) a.maxResults = b.maxResults;
    }

    function test_gasSweep_noCorpseAboveFirstPage() public {
        uint256 n = 60;
        uint256[] memory a = new uint256[](n);
        for (uint256 i; i < n; i++) a[i] = 15_000 + i * 500;
        uint256[] memory m = new uint256[](n);
        for (uint256 i; i < n; i++) m[i] = 5;
        Sweep memory s = sweep(staticIc(a, m), 100_000, 4_000_000, 12_345);
        require(s.pages > 0 && s.maxAdjudicated == n, "sweep never completed a page");
    }

    function test_gasSweep_noCorpseAboveFirstPage_compressed() public {
        uint256 n = 60;
        uint256[] memory a = new uint256[](n);
        for (uint256 i; i < n; i++) a[i] = 15_000 + i * 500;
        uint256[] memory m = new uint256[](n);
        for (uint256 i; i < n; i++) m[i] = 5;
        bytes memory w = Env.compress(staticInputs(a, m));
        uint256 cfg = Env.config(StaticLens.item.selector, false, 64, false, 32, true);
        bytes memory ic = Env.wrap(envelope, address(factory), type(StaticLens).creationCode, w, cfg);
        Sweep memory s = sweep(ic, 100_000, 4_000_000, 12_345);
        require(s.pages > 0 && s.maxAdjudicated == n, "sweep never completed a page");
    }

    /// The smallest grant (to 1k) at or above `lo` — itself at or above the first page — whose page
    /// adjudicates `target` elements, or which ends in a protocol error. Any corpse on the way is a
    /// failure: past the first page every grant must produce a page.
    function fullGrant(bytes memory ic, uint256 lo, uint256 target) internal returns (uint256 g) {
        for (g = lo; g < 5_000_000; g += 1_000) {
            uint256 mark;
            assembly { mark := mload(0x40) }
            (bool called, bytes memory ret) = execWithGas(ic, g);
            if (!called || ret.length < 4) revert(string.concat("corpse above first page at ", Env.VM.toString(g)));
            bool done = bytes4(ret) == MALFORMED || (bytes4(ret) == Env.OK && Env.page(ret).nA >= target);
            assembly { mstore(0x40, mark) }
            if (done) return g;
        }
        revert("never full under 5M");
    }

    /// Step-1 sweeps across a window around every transition grant — the first page, then each
    /// further element adjudicated — so every gas phase of every post-call path is exercised.
    /// These pin `apre`, `cpost` and `dwork` in the Yul.
    function boundarySweep(bytes memory ic, uint256 target) internal returns (Sweep memory s) {
        uint256 g = firstPageGrant(ic);
        s = sweep(ic, g - 1_500, g + 1_500, 1);
        for (uint256 k = 2; k <= target && s.malformed == 0; k++) {
            g = fullGrant(ic, g, k);
            merge(s, sweep(ic, g - 1_500, g + 1_500, 1));
        }
    }

    function test_boundarySweep_staticSuccess() public {
        Sweep memory s = boundarySweep(staticIc(values(4), modes(4)), 4);
        require(s.pages > 0 && s.maxAdjudicated == 4, "pages");
    }

    /// Adversaries pin constants: a lens built to be the worst case for one term, such that lowering
    /// the term fails the sweep. This one returns with ~40 gas left however much it was granted, then
    /// another element follows, so the post-call path and the next admission's refusal run on the
    /// retained 1/64 alone. It pins `cpost`; every other fixture passes with `cpost` far too low.
    function test_adversary_drainedCallee() public {
        uint256[] memory a = values(4);
        uint256[] memory m = modes(4);
        (a[2], m[2]) = (DRAIN_TO, 6);
        Sweep memory s = boundarySweep(staticIc(a, m), 3);
        require(s.pages > 0 && s.maxResults >= 3, "drained element never served");
    }

    function test_adversary_drainedCallee_compressed() public {
        uint256[] memory a = values(4);
        uint256[] memory m = modes(4);
        (a[2], m[2]) = (DRAIN_TO, 6);
        bytes memory w = Env.compress(staticInputs(a, m));
        uint256 cfg = Env.config(StaticLens.item.selector, false, 64, false, 32, true);
        Sweep memory s = boundarySweep(Env.wrap(envelope, address(factory), type(StaticLens).creationCode, w, cfg), 3);
        require(s.pages > 0 && s.maxResults >= 3, "drained element never served");
    }

    function test_adversary_drainedCallee_dynamic() public {
        uint256[] memory xs = new uint256[](4);
        (xs[0], xs[1], xs[2], xs[3]) = (5, 40, 11, 5);
        Sweep memory s = boundarySweep(dynOutIc(xs), 3);
        // A drained dynamic result cannot be kept on the retained 1/64 alone, so it is a death, not a corpse.
        require(s.pages > 0 && s.maxAdjudicated >= 3 && s.deaths > 0, "pages");
    }

    function test_boundarySweep_literalStream() public {
        bytes memory body = staticInputs(values(4), modes(4));
        bytes memory w = Env.wireOf(4, body.length - 64, Env.flzLiterals(Env.slice(body, 64, body.length - 64)));
        Sweep memory s = boundarySweep(compressedStaticIc(w), 4);
        require(s.pages > 0 && s.maxAdjudicated == 4, "pages");
    }

    /// A 6 KiB element as one-byte literals: producing it costs more than the exit reserve can
    /// absorb (a pre-split shortfall reaches the reserve at 1/64), so this adversary pins `dwork`'s
    /// per-byte term where small elements cannot.
    function test_adversary_largeLiteralElement() public {
        bytes[] memory xs = new bytes[](3);
        (xs[0], xs[1], xs[2]) = (hex"01", new bytes(6_000), hex"02");
        bytes memory w = Env.wireDyn(xs);
        bytes memory stream = Env.flzLiterals(Env.slice(w, 64, w.length - 64));
        uint256 cfg = Env.config(DynInLens.item.selector, true, 0, false, 32, true);
        bytes memory ic = Env.wrap(envelope, address(factory), type(DynInLens).creationCode, Env.wireOf(3, w.length - 64, stream), cfg);
        Sweep memory s = boundarySweep(ic, 3);
        require(s.pages > 0 && s.maxResults == 3, "pages");
    }

    function test_boundarySweep_distanceOneStream() public {
        bytes memory zeros = new bytes(4 * 64);
        Sweep memory s = boundarySweep(compressedStaticIc(Env.wireOf(4, zeros.length, Env.flzDist1(zeros))), 4);
        require(s.pages > 0 && s.maxAdjudicated == 4, "pages");
    }

    /// Step-1 sweep over the compression bomb around its first rebase (element 256), by summary:
    /// the page is too large for a capped frame to copy back.
    function test_boundarySweep_bombRebase() public {
        bytes memory ic = compressedStaticIc(Env.wireOf(600, 600 * 64, Env.flzRepeat(bytes32(0), 1_200)));
        uint256 g0;
        uint256 g1;
        for (uint256 g = 50_000; g < 5_000_000 && g1 == 0; g += 1_000) {
            uint256 mark;
            assembly { mark := mload(0x40) }
            (bool called, bytes4 sel, uint256 nA) = summaryWithGas(ic, g);
            assembly { mstore(0x40, mark) }
            if (!called || sel != Env.OK) continue;
            if (nA >= 250 && g0 == 0) g0 = g;
            if (nA >= 262) g1 = g;
        }
        require(g0 > 0 && g1 > g0, "window");
        for (uint256 g = g0; g < g1 + 1_000; g++) {
            uint256 mark;
            assembly { mark := mload(0x40) }
            (bool called, bytes4 sel, uint256 nA) = summaryWithGas(ic, g);
            assembly { mstore(0x40, mark) }
            require(called && sel == Env.OK && nA >= 250, "corpse across the rebase");
        }
    }

    function test_boundarySweep_compressedDynamic() public {
        bytes[] memory xs = new bytes[](4);
        (xs[0], xs[1], xs[2], xs[3]) = (hex"01", hex"010203", new bytes(700), new bytes(64));
        uint256 cfg = Env.config(DynInLens.item.selector, true, 0, false, 32, true);
        bytes memory ic = Env.wrap(envelope, address(factory), type(DynInLens).creationCode, Env.compress(Env.wireDyn(xs)), cfg);
        Sweep memory s = boundarySweep(ic, 4);
        require(s.pages > 0 && s.maxAdjudicated == 4, "pages");
    }

    function test_boundarySweep_emptyRevert() public {
        uint256[] memory m = modes(4);
        (m[1], m[2]) = (1, 1);
        Sweep memory s = boundarySweep(staticIc(values(4), m), 4);
        require(s.pages > 0 && s.maxAdjudicated == 4, "pages");
    }

    function test_boundarySweep_revertWithData() public {
        uint256[] memory m = modes(4);
        (m[1], m[2]) = (2, 2);
        Sweep memory s = boundarySweep(staticIc(values(4), m), 4);
        require(s.pages > 0 && s.maxAdjudicated == 4, "pages");
    }

    function test_boundarySweep_burnEverything() public {
        uint256[] memory m = modes(4);
        m[2] = 3;
        Sweep memory s = boundarySweep(staticIc(values(4), m), 3);
        require(s.pages > 0 && s.deaths > 0 && s.maxAdjudicated == 3, "pages end at the burn");
    }

    function test_boundarySweep_staticMalformed() public {
        uint256[] memory m = modes(4);
        m[2] = 4;
        Sweep memory s = boundarySweep(staticIc(values(4), m), 4);
        require(s.pages > 0 && s.malformed > 0, "pages then malformed");
    }

    function test_boundarySweep_dynamicSuccess() public {
        uint256[] memory xs = new uint256[](4);
        (xs[0], xs[1], xs[2], xs[3]) = (5, 40, 7, 300);
        Sweep memory s = boundarySweep(dynOutIc(xs), 4);
        require(s.pages > 0 && s.maxAdjudicated == 4 && s.deaths > 0, "pages incl. deposit deaths");
    }

    function test_boundarySweep_dynamicShortReturn() public {
        uint256[] memory xs = new uint256[](4);
        (xs[0], xs[1], xs[2], xs[3]) = (5, 40, 9, 5);
        Sweep memory s = boundarySweep(dynOutIc(xs), 4);
        require(s.pages > 0 && s.malformed > 0, "pages then malformed");
    }

    function test_boundarySweep_dynamicBadHead() public {
        uint256[] memory xs = new uint256[](4);
        (xs[0], xs[1], xs[2], xs[3]) = (5, 40, 10, 5);
        Sweep memory s = boundarySweep(dynOutIc(xs), 4);
        require(s.pages > 0 && s.malformed > 0, "pages then malformed");
    }

    /// A compressed singleton refused at its admission floor is a head death, not a malformed stream.
    function test_boundarySweep_compressedSingleton() public {
        bytes[] memory xs = new bytes[](1);
        xs[0] = new bytes(3_000);
        uint256 cfg = Env.config(DynInLens.item.selector, true, 0, false, 32, true);
        bytes memory ic = Env.wrap(envelope, address(factory), type(DynInLens).creationCode, Env.compress(Env.wireDyn(xs)), cfg);
        Sweep memory s = boundarySweep(ic, 1);
        require(s.pages > 0 && s.deaths > 0 && s.malformed == 0, "head death, never malformed");
        Env.Page memory p = Env.page(runner.exec(ic));
        require(p.results.length == 1 && Env.word(p.results[0], 0) == uint256(keccak256(new bytes(3_000))), "served alone");
    }

    function test_boundarySweep_dynamicInput() public {
        bytes[] memory xs = new bytes[](4);
        (xs[0], xs[1], xs[2], xs[3]) = (hex"01", hex"010203", new bytes(70), new bytes(64));
        Sweep memory s = boundarySweep(dynInIc(xs), 4);
        require(s.pages > 0 && s.maxAdjudicated == 4, "pages");
    }
}
