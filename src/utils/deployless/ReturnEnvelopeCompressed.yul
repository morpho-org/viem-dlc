/*
 * Outer constructor wrapper for deployless eth_call with FLZ input compression,
 * RETURN-mode exfiltration (Yul source).
 *
 * Constructor args: see RevertEnvelope.yul — `targetData` is FLZ-compressed here.
 *
 * Wire protocol (input compressed only):
 *   caller:    targetData = flzCompress(ABI-encoded calldata)
 *   envelope:  decompresses targetData, calls lens, returns raw returndata
 *   caller:    no decompression needed
 *
 * On lens success:  return(returndata)   — no sentinel needed
 * On lens revert:   revert(returndata verbatim)
 * On lens OOG:      revert(OOG_SENTINEL)
 * On deploy fail:   revert(CounterfactualDeployFailed(bytes("")))
 *
 * Memory layout (monotonically increasing, no aliasing):
 *   [0, argsLen)                  constructor args (codecopy)
 *   [argsEnd, argsEnd+decompLen)  decompressed targetData
 *   [decompEnd, ...)              raw returndata from lens
 *
 * FLZ algorithm ported from Solady's LibZip.sol (MIT License, Vectorized).
 *   https://github.com/Vectorized/solady/blob/main/src/utils/LibZip.sol
 *
 * Build: `pnpm build:ReturnEnvelopeCompressed` — prints the hex constant to paste into
 * codec.envelope.ts.
 */
object "ReturnEnvelopeCompressed" {
    code {
        // PUSH3 placeholder patched post-compile — see RevertEnvelope.yul.
        let bytecodeLen := verbatim_0i_1o(hex"62BBBBBB")
        let argsLen := sub(codesize(), bytecodeLen)
        codecopy(0x00, bytecodeLen, argsLen)

        let target  := mload(0x00)
        let tdOff   := mload(0x20)
        let factory := mload(0x40)
        let fdOff   := mload(0x60)

        // Deploy only into an empty `target` — see RevertEnvelope.yul.
        if iszero(extcodesize(target)) {
            let fdLen := mload(fdOff)
            let deployed := call(gas(), factory, 0, add(fdOff, 0x20), fdLen, 0, 0)
            if or(iszero(deployed), iszero(extcodesize(target))) {
                // bytes4(keccak256("CounterfactualDeployFailed(bytes)")) = 0x101bb98d
                mstore(0x00, 0x101bb98d00000000000000000000000000000000000000000000000000000000)
                mstore(0x04, 0x20)
                mstore(0x24, 0)
                revert(0x00, 0x44)
            }
        }

        let compTdLen := mload(tdOff)
        let compTdPtr := add(tdOff, 0x20)
        let decompPtr := msize()
        let decompLen := flzDecompress(compTdPtr, compTdLen, decompPtr)

        let gasBefore := gas()
        let ok := call(gas(), target, 0, decompPtr, decompLen, 0, 0)
        let rdLen := returndatasize()

        if ok {
            // No compression on the output side.
            let rdPtr := add(decompPtr, decompLen)
            returndatacopy(rdPtr, 0x00, rdLen)
            return(rdPtr, rdLen)
        }

        // Lens frame out of gas — see RevertEnvelope.yul. The marker has to leave via `revert`:
        // a *returned* selector would be indistinguishable from lens payload.
        if and(iszero(rdLen), iszero(gt(gas(), div(gasBefore, 64)))) {
            // OOG_SENTINEL = bytes4(keccak256("ViemDlcOutOfGas()")) = 0xcc0bd34c
            mstore(0x00, 0xcc0bd34c00000000000000000000000000000000000000000000000000000000)
            revert(0x00, 0x04)
        }

        returndatacopy(0x00, 0x00, rdLen)
        revert(0x00, rdLen)

        /// Reads `inLen` bytes of FastLZ-compressed data at `inPtr`, writes the decompressed bytes
        /// to `outPtr`, and returns their length. Copies back-refs with 32-byte mstores
        /// (stride = min(distance, 32)), so bytes past `outLen` may be garbage — never returned.
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
