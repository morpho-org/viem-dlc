// SPDX-License-Identifier: MIT
pragma solidity ^0.8.25;

interface Vm {
    function ffi(string[] calldata) external returns (bytes memory);
    function readFile(string calldata) external view returns (string memory);
    function toString(bytes calldata) external pure returns (string memory);
}

library Env {
    Vm constant VM = Vm(address(uint160(uint256(keccak256("hevm cheat code")))));
    bytes32 constant SALT = bytes32(uint256(1));

    /// Builds an envelope from its Yul with the package's own script, and checks the pasted constant matches.
    function build(string memory script) internal returns (bytes memory code) {
        string[] memory cmd = new string[](5);
        (cmd[0], cmd[1], cmd[2], cmd[3], cmd[4]) = ("pnpm", "-s", "--dir", "../..", script);
        code = VM.ffi(cmd);
        bytes memory ts = bytes(VM.readFile("../../src/utils/deployless/codec.envelope.ts"));
        bytes memory hex_ = bytes(VM.toString(code));
        for (uint256 i; i + hex_.length <= ts.length; i++) {
            uint256 j;
            while (j < hex_.length && ts[i + j] == hex_[j]) j++;
            if (j == hex_.length) return code;
        }
        revert(string.concat("codec.envelope.ts is stale: rerun pnpm ", script));
    }

    function config(bytes4 sel, bool inDyn, uint256 inSize, bool outDyn, uint256 outSize) internal pure returns (uint256) {
        return (uint256(uint32(sel)) << 224) | ((inDyn ? uint256(1) : 0) << 223) | ((outDyn ? uint256(1) : 0) << 222) | (inSize << 64) | outSize;
    }

    /// `envelope || abi.encode(target, targetData, factory, factoryData, config)`, as the TS codec wraps.
    function wrap(bytes memory envelope, address factory, bytes memory initcode, bytes memory targetData, uint256 cfg)
        internal pure returns (bytes memory)
    {
        address target = address(uint160(uint256(keccak256(abi.encodePacked(bytes1(0xff), factory, SALT, keccak256(initcode))))));
        return abi.encodePacked(envelope, abi.encode(target, targetData, factory, abi.encodePacked(SALT, initcode), cfg));
    }

    function body(bytes memory ret) internal pure returns (bytes memory b) {
        b = new bytes(ret.length - 4);
        for (uint256 i; i < b.length; i++) b[i] = ret[i + 4];
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

/// The deployless eth_call: CREATE the envelope and hand back its revert data.
contract Runner {
    function exec(bytes memory initcode) external returns (bytes memory ret) {
        assembly {
            if create(0, add(initcode, 0x20), mload(initcode)) { revert(0, 0) }
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
    function item(In calldata x) external view returns (uint256) {
        if (x.mode == 1) revert();
        if (x.mode == 2) revert("nope");
        if (x.mode == 3) { uint256 s; while (true) s++; }
        if (x.mode == 4) assembly { return(0, 0x40) }
        uint256 start = gasleft();
        uint256 n;
        if (x.mode == 5) while (start - gasleft() < x.a) n++;
        return x.a * 2;
    }
    function page(In[] calldata) external view returns (uint256[] memory, uint256[] memory) {}
}

contract DynOutLens {
    function item(uint256 x) external pure returns (bytes memory b) {
        if (x == 7) revert();
        if (x == 8) return new bytes(300); // past the 256-byte bound
        if (x == 9) assembly { return(0, 0x10) } // success shorter than a head word
        b = new bytes(x);
        for (uint256 i; i < x; i++) b[i] = bytes1(uint8(x));
    }
    function page(uint256[] calldata) external view returns (bytes[] memory, uint256[] memory) {}
}

contract DynInLens {
    function item(bytes calldata x) external pure returns (uint256) {
        if (x.length == 3) revert();
        return uint256(keccak256(x));
    }
    function page(bytes[] calldata) external view returns (uint256[] memory, uint256[] memory) {}
}

contract HungryLens {
    constructor() { uint256 s; while (true) s++; }
    function item(uint256) external pure returns (uint256) { return 1; }
}
