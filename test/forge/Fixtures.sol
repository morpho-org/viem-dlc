// SPDX-License-Identifier: MIT
pragma solidity ^0.8.25;

interface Vm {
    function ffi(string[] calldata) external returns (bytes memory);
    function readFile(string calldata) external view returns (string memory);
    function toString(bytes calldata) external pure returns (string memory);
    function toString(uint256) external pure returns (string memory);
}

library Env {
    Vm constant VM = Vm(address(uint160(uint256(keccak256("hevm cheat code")))));
    bytes32 constant SALT = bytes32(uint256(1));
    bytes4 constant OK = 0xf90a85b5;

    struct Page {
        uint256 nA;
        bytes[] results;
        uint256[] skipped;
        bool died;
        uint256 diedAt;
    }

    /// Builds the envelope from its Yul with the package's own script, and checks the pasted constant matches.
    function build() internal returns (bytes memory code) {
        string[] memory cmd = new string[](5);
        (cmd[0], cmd[1], cmd[2], cmd[3], cmd[4]) = ("pnpm", "-s", "--dir", "../..", "build:Envelope");
        code = VM.ffi(cmd);
        bytes memory ts = bytes(VM.readFile("../../src/utils/deployless/codec.envelope.ts"));
        bytes memory hex_ = bytes(VM.toString(code));
        for (uint256 i; i + hex_.length <= ts.length; i++) {
            uint256 j;
            while (j < hex_.length && ts[i + j] == hex_[j]) j++;
            if (j == hex_.length) return code;
        }
        revert("codec.envelope.ts is stale: rerun pnpm build:Envelope");
    }

    function config(bytes4 sel, bool inDyn, uint256 inSize, bool outDyn, uint256 outSize, bool compressed)
        internal pure returns (uint256)
    {
        return (uint256(uint32(sel)) << 224) | ((inDyn ? uint256(1) : 0) << 223) | ((outDyn ? uint256(1) : 0) << 222)
            | ((compressed ? uint256(1) : 0) << 221) | (inSize << 64) | outSize;
    }

    /// The wire for a static `T[]`, from `abi.encode(array)`: `n ‖ bodyLen ‖ strides`.
    function wire(bytes memory encodedArray) internal pure returns (bytes memory) {
        uint256 n = word(encodedArray, 32);
        bytes memory body = slice(encodedArray, 64, encodedArray.length - 64);
        return abi.encodePacked(n, body.length, body);
    }

    /// The wire for a dynamic `T[]`: `n ‖ bodyLen ‖ (L ‖ tail)*`, each tail the padded ABI tail of `abi.encode(x)`.
    function wireDyn(bytes[] memory xs) internal pure returns (bytes memory) {
        bytes memory body;
        for (uint256 i; i < xs.length; i++) {
            bytes memory tail = slice(abi.encode(xs[i]), 32, 32 + ((xs[i].length + 31) / 32) * 32);
            body = abi.encodePacked(body, tail.length, tail);
        }
        return abi.encodePacked(xs.length, body.length, body);
    }

    /// Replaces a clear wire's body with its FastLZ compression, using the package's own compressor.
    function compress(bytes memory clearWire) internal returns (bytes memory) {
        string[] memory cmd = new string[](8);
        (cmd[0], cmd[1], cmd[2], cmd[3]) = ("pnpm", "-s", "--dir", "../..");
        (cmd[4], cmd[5], cmd[6]) = ("exec", "tsx", "test/forge/flz-compress.ts");
        cmd[7] = VM.toString(slice(clearWire, 64, clearWire.length - 64));
        return abi.encodePacked(word(clearWire, 0), word(clearWire, 32), VM.ffi(cmd));
    }

    /// A hand-built FastLZ stream for `word` repeated `n` times: one literal, then matches at distance 32.
    function flzRepeat(bytes32 word_, uint256 n) internal pure returns (bytes memory out) {
        out = abi.encodePacked(bytes1(uint8(31)), word_);
        uint256 rem = 32 * n - 32;
        while (rem >= 262) {
            out = abi.encodePacked(out, bytes1(uint8(224)), bytes1(uint8(253)), bytes1(uint8(31)));
            rem -= 262;
        }
        if (rem >= 9) out = abi.encodePacked(out, bytes1(uint8(224)), bytes1(uint8(rem - 9)), bytes1(uint8(31)));
        else if (rem >= 3) out = abi.encodePacked(out, bytes1(uint8((rem - 2) << 5)), bytes1(uint8(31)));
        else if (rem > 0) out = abi.encodePacked(out, bytes1(uint8(rem - 1)), slice(abi.encodePacked(word_), 0, rem));
    }

    /// `body` as one-byte literal tokens: the costliest stream per output byte.
    function flzLiterals(bytes memory body) internal pure returns (bytes memory out) {
        out = new bytes(2 * body.length);
        for (uint256 i; i < body.length; i++) out[2 * i + 1] = body[i];
    }

    /// `body`, whose every 32-byte word must repeat one byte, as a literal plus a distance-one match
    /// per word: the costliest tokens.
    function flzDist1(bytes memory body) internal pure returns (bytes memory out) {
        for (uint256 i; i < body.length; i += 32) {
            out = abi.encodePacked(out, bytes1(0), body[i], bytes1(uint8(224)), bytes1(uint8(22)), bytes1(0));
        }
    }

    /// `n ‖ bodyLen ‖ stream` for a hand-built stream.
    function wireOf(uint256 n, uint256 bodyLen, bytes memory stream) internal pure returns (bytes memory) {
        return abi.encodePacked(n, bodyLen, stream);
    }

    /// `envelope || abi.encode(target, targetData, factory, factoryData, config)`, as the TS codec wraps.
    function wrap(bytes memory envelope, address factory, bytes memory initcode, bytes memory targetData, uint256 cfg)
        internal pure returns (bytes memory)
    {
        address target = address(uint160(uint256(keccak256(abi.encodePacked(bytes1(0xff), factory, SALT, keccak256(initcode))))));
        return abi.encodePacked(envelope, abi.encode(target, targetData, factory, abi.encodePacked(SALT, initcode), cfg));
    }

    /// Decodes an outcome stream, requiring every record to be bound to its ordinal.
    function page(bytes memory ret) internal pure returns (Page memory p) {
        require(bytes4(ret) == OK, "ok sentinel");
        require(ret.length >= 36, "no nA");
        p.nA = word(ret, 4);
        require(p.nA >= 1, "nA >= 1");
        p.results = new bytes[](p.nA);
        p.skipped = new uint256[](p.nA);
        uint256 nR;
        uint256 nS;
        uint256 at = 36;
        for (uint256 j; j < p.nA; j++) {
            require(at + 32 <= ret.length, "record missing");
            uint256 w = word(ret, at);
            at += 32;
            uint256 kind = w >> 254;
            if (kind == 0) {
                require(w == j, "decline ordinal");
                p.skipped[nS++] = j;
            } else if (kind == 3) {
                require(~w == j && j == p.nA - 1, "death not last or misbound");
                p.died = true;
                p.diedAt = j;
            } else if (kind == 2) {
                uint256 L = w ^ (1 << 255);
                require(at + L <= ret.length, "result overruns");
                p.results[nR++] = slice(ret, at, L);
                at += L;
            } else {
                revert("record kind 01");
            }
        }
        require(at == ret.length, "trailing bytes");
        bytes[] memory results = p.results;
        uint256[] memory skipped = p.skipped;
        assembly {
            mstore(results, nR)
            mstore(skipped, nS)
        }
    }

    function uints(Page memory p) internal pure returns (uint256[] memory out) {
        out = new uint256[](p.results.length);
        for (uint256 i; i < out.length; i++) {
            require(p.results[i].length == 32, "not a word");
            out[i] = word(p.results[i], 0);
        }
    }

    function body(bytes memory ret) internal pure returns (bytes memory) {
        return slice(ret, 4, ret.length - 4);
    }

    function word(bytes memory b, uint256 at) internal pure returns (uint256 w) {
        assembly { w := mload(add(add(b, 0x20), at)) }
    }

    function slice(bytes memory b, uint256 at, uint256 len) internal pure returns (bytes memory out) {
        out = new bytes(len);
        assembly { mcopy(add(out, 0x20), add(add(b, 0x20), at), len) }
    }
}

/// Salted CREATE2 factory: calldata is `salt || initcode`.
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

/// The deployless eth_call: CREATE the envelope and hand back its revert data, and the gas the
/// envelope's frame consumed.
contract Runner {
    function exec(bytes memory initcode) external returns (bytes memory ret) {
        (, ret) = execMeasured(initcode);
    }

    /// The revert's selector, `nA` and length, without copying the page: for pages too large to
    /// copy on the gas a capped frame has left.
    function summary(bytes memory initcode) external returns (bytes4 sel, uint256 nA, uint256 len) {
        assembly {
            if create(0, add(initcode, 0x20), mload(initcode)) { revert(0, 0) }
            len := returndatasize()
            returndatacopy(0, 0, 0x24)
            sel := mload(0)
            nA := mload(4)
            if lt(len, 0x24) { nA := 0 }
        }
    }

    function execMeasured(bytes memory initcode) public returns (uint256 used, bytes memory ret) {
        assembly {
            let g := gas()
            if create(0, add(initcode, 0x20), mload(initcode)) { revert(0, 0) }
            used := sub(g, gas())
            ret := mload(0x40)
            mstore(ret, returndatasize())
            returndatacopy(add(ret, 0x20), 0, returndatasize())
            mstore(0x40, add(add(ret, 0x20), returndatasize()))
        }
    }
}

contract StaticLens {
    struct In { uint256 a; uint256 mode; }
    // mode: 0 ok · 1 revert() · 2 revert("nope") · 3 burn everything · 4 malformed · 5 burn `a` gas then ok
    //       6 return with a little under `a` gas left, however much was granted
    function item(In calldata x) external view returns (uint256) {
        if (x.mode == 1) revert();
        if (x.mode == 2) revert("nope");
        if (x.mode == 3) { uint256 s; while (true) s++; }
        if (x.mode == 4) assembly { return(0, 0x40) }
        uint256 start = gasleft();
        uint256 n;
        if (x.mode == 5) while (start - gasleft() < x.a) n++;
        if (x.mode == 6) assembly {
            let a := calldataload(4)
            for {} gt(gas(), a) {} {}
            mstore(0, mul(a, 2))
            return(0, 0x20)
        }
        return x.a * 2;
    }
}

/// A 32-word static result, cheaply: word `i` is `x + i`.
contract WideLens {
    function item(uint256 x) external pure returns (uint256[32] memory) {
        assembly {
            for { let i := 0 } lt(i, 32) { i := add(i, 1) } { mstore(mul(i, 32), add(x, i)) }
            return(0, 1024)
        }
    }
}

contract DynOutLens {
    // x: 7 revert() · 8 revert("nope") · 9 success shorter than a head word · 10 bad head
    //    11 return 64 bytes with ~50 gas left, however much was granted · else `x` bytes of x
    function item(uint256 x) external view returns (bytes memory b) {
        if (x == 7) revert();
        if (x == 8) revert("nope");
        if (x == 9) assembly { return(0, 0x10) }
        if (x == 10) assembly { mstore(0, 0x40) return(0, 0x40) }
        if (x == 11) assembly {
            for {} gt(gas(), 80) {} {}
            mstore(0, 0x20)
            mstore(0x20, 0x40)
            mstore(0x40, 11)
            mstore(0x60, 11)
            return(0, 0x80)
        }
        b = new bytes(x);
        for (uint256 i; i < x; i++) b[i] = bytes1(uint8(x));
    }
}

contract DynInLens {
    function item(bytes calldata x) external pure returns (uint256) {
        if (x.length == 3) revert();
        return uint256(keccak256(x));
    }
}

contract EchoLens {
    function item(bytes calldata x) external pure returns (bytes memory) {
        if (x.length == 3) revert();
        return abi.encodePacked(x, x);
    }
}

contract HungryLens {
    constructor() { uint256 s; while (true) s++; }
    function item(uint256) external pure returns (uint256) { return 1; }
}
