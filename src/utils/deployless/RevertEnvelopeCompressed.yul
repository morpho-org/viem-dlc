/*
 * Outer constructor wrapper for deployless eth_call with FLZ input compression (Yul source).
 *
 * Constructor args layout (identical to RevertEnvelope.yul):
 *   [0..32]:   target address
 *   [32..64]:  compressedTargetData offset (= 128 if no funny business)
 *   [64..96]:  factory address
 *   [96..128]: factoryData offset
 *   [128..]:   compressedTargetData length + bytes, factoryData length + bytes
 *
 * Wire protocol (input compressed only):
 *   JS side:        targetData = flzCompress(ABI-encoded calldata)
 *   Solidity side:  decompresses targetData, calls lens, reverts with raw returndata
 *   JS side:        extracts sentinel; no decompression needed
 *
 * On lens success:  revert(OK_SENTINEL || returndata)
 * On lens revert:   revert(returndata verbatim)
 * On deploy fail:   revert(CounterfactualDeployFailed(bytes("")))
 *
 * Memory layout (monotonically increasing, no aliasing):
 *   [0, argsLen)                  constructor args (codecopy)
 *   [argsEnd, argsEnd+decompLen)  decompressed targetData
 *   [decompEnd, ...)              OK_SENTINEL + raw returndata
 *
 * FLZ algorithm ported from Solady's LibZip.sol (MIT License, Vectorized).
 *   https://github.com/Vectorized/solady/blob/main/src/utils/LibZip.sol
 *
 * Build: `pnpm build:RevertEnvelopeCompressed`
 */
object "RevertEnvelopeCompressed" {
    code {
        // Patch bytecodeLen with PUSH3 placeholder (same technique as RevertEnvelope.yul).
        let bytecodeLen := verbatim_0i_1o(hex"62BBBBBB")
        let argsLen := sub(codesize(), bytecodeLen)
        codecopy(0x00, bytecodeLen, argsLen)

        let target  := mload(0x00)
        let tdOff   := mload(0x20)
        let factory := mload(0x40)
        let fdOff   := mload(0x60)

        // factory.call(factoryData) — deploys lens at `target`.
        // Require both that the call succeeded AND that target has code afterward.
        let fdLen := mload(fdOff)
        let deployed := call(gas(), factory, 0, add(fdOff, 0x20), fdLen, 0, 0)
        if or(iszero(deployed), iszero(extcodesize(target))) {
            // bytes4(keccak256("CounterfactualDeployFailed(bytes)")) = 0x101bb98d
            mstore(0x00, 0x101bb98d00000000000000000000000000000000000000000000000000000000)
            mstore(0x04, 0x20)
            mstore(0x24, 0)
            revert(0x00, 0x44)
        }

        // Decompress targetData into a fresh memory region at msize().
        let compTdLen := mload(tdOff)
        let compTdPtr := add(tdOff, 0x20)
        let decompPtr := msize()
        let decompLen := flzDecompress(compTdPtr, compTdLen, decompPtr)

        // Call lens with decompressed targetData.
        let ok := call(gas(), target, 0, decompPtr, decompLen, 0, 0)
        let rdLen := returndatasize()

        if ok {
            // Write OK_SENTINEL followed by raw returndata and revert.
            // OK_SENTINEL = bytes4(keccak256("ViemDlcOk()")) = 0x1580d19d
            let sentinelPtr := add(decompPtr, decompLen)
            mstore(sentinelPtr, 0x1580d19d00000000000000000000000000000000000000000000000000000000)
            returndatacopy(add(sentinelPtr, 0x04), 0x00, rdLen)
            revert(sentinelPtr, add(0x04, rdLen))
        }

        // Lens revert: pass through verbatim.
        returndatacopy(0x00, 0x00, rdLen)
        revert(0x00, rdLen)

        // ─────────────────────────────────────────────────────────────────────────
        // FLZ DECOMPRESSOR  (Solady LibZip.sol, MIT)
        //
        // flzDecompress(inPtr, inLen, outPtr) -> outLen
        //   Reads `inLen` bytes of FastLZ-compressed data from memory at `inPtr`,
        //   writes decompressed bytes to `outPtr`, and returns the decompressed length.
        //   Uses mstore for 32-byte back-ref copies (stride = min(distance, 32));
        //   bytes beyond outLen may be garbage but are never returned to caller.
        // ─────────────────────────────────────────────────────────────────────────
        function flzDecompress(inPtr, inLen, outPtr) -> outLen {
            let op  := outPtr
            let end := add(inPtr, inLen)
            for {} lt(inPtr, end) {} {
                let w    := mload(inPtr)
                let ctrl := byte(0, w)
                let t    := shr(5, ctrl)
                switch iszero(t)
                case 1 {
                    // Literal run: ctrl+1 bytes
                    mstore(op, mload(add(inPtr, 1)))
                    inPtr := add(inPtr, add(2, ctrl))
                    op    := add(op, add(1, ctrl))
                }
                default {
                    // Back-reference
                    let g := eq(t, 7)                                          // 1 if long match
                    let l := add(2, xor(t, mul(g, xor(t, add(7, byte(1, w))))))  // decoded match length
                    let s := add(add(shl(8, and(0x1f, ctrl)), byte(add(1, g), w)), 1) // back-distance (1-based)
                    let r := sub(op, s)
                    // Copy stride: min(s, 32). Uses 32-byte mstore for large distances.
                    let f := xor(s, mul(gt(s, 0x20), xor(s, 0x20)))
                    for { let j := 0 } 1 {} {
                        mstore(add(op, j), mload(add(r, j)))
                        j := add(j, f)
                        if lt(j, l) { continue }
                        inPtr := add(inPtr, add(2, g))
                        op    := add(op, l)
                        break
                    }
                }
            }
            outLen := sub(op, outPtr)
        }
    }
}
