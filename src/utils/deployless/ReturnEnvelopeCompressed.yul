/*
 * Outer constructor wrapper for deployless eth_call with FLZ wire compression,
 * RETURN-mode exfiltration (Yul source).
 *
 * Constructor args layout (identical to RevertEnvelopeCompressed.yul):
 *   [0..32]:   target address
 *   [32..64]:  compressedTargetData offset (= 128 if no funny business)
 *   [64..96]:  factory address
 *   [96..128]: factoryData offset
 *   [128..]:   compressedTargetData length + bytes, factoryData length + bytes
 *
 * Wire protocol (both directions compressed):
 *   JS side:        targetData = flzCompress(ABI-encoded calldata)
 *   Solidity side:  decompresses targetData, calls lens, flzCompresses returndata
 *   JS side:        decompresses the returned bytes
 *
 * On lens success:  return(flzCompress(returndata))   — no sentinel needed
 * On lens revert:   revert(returndata verbatim)
 * On deploy fail:   revert(DeploymentFailed(target))
 *
 * Memory layout (monotonically increasing, no aliasing):
 *   [0, argsLen)                  constructor args (codecopy)
 *   [argsEnd, argsEnd+decompLen)  decompressed targetData
 *   [decompEnd, decompEnd+rdLen)  raw returndata from lens
 *   [rdEnd, rdEnd+0x8000)         32 KB hash table for compressor (calldatacopy-zeroed)
 *   [htEnd, ...)                  flzCompress(returndata) output
 *
 * FLZ algorithm ported from Solady's LibZip.sol (MIT License, Vectorized).
 *   https://github.com/Vectorized/solady/blob/main/src/utils/LibZip.sol
 *
 * Build: `pnpm build:ReturnEnvelopeCompressed`
 */
object "ReturnEnvelopeCompressed" {
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
            // bytes4(keccak256("DeploymentFailed(address)")) = 0x9deffc1b
            mstore(0x00, 0x9deffc1b00000000000000000000000000000000000000000000000000000000)
            mstore(0x04, target)
            revert(0x00, 0x24)
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
            // Copy raw returndata after the decompressed region.
            let rdPtr := add(decompPtr, decompLen)
            returndatacopy(rdPtr, 0x00, rdLen)

            // Allocate 32 KB hash table for the compressor.
            // CALLDATASIZE = 0 in CREATE context, so calldatacopy writes zeros.
            let htBase := add(rdPtr, rdLen)
            calldatacopy(htBase, 0x00, 0x8000)

            // Compress returndata and RETURN it directly (no sentinel needed for RETURN mode).
            let outDataPtr := add(htBase, 0x8000)
            let cmpLen := flzCompress(rdPtr, rdLen, htBase, outDataPtr)
            return(outDataPtr, cmpLen)
        }

        // Lens revert: pass through verbatim (no compression for failure path).
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

        // ─────────────────────────────────────────────────────────────────────────
        // FLZ COMPRESSOR  (Solady LibZip.sol, MIT)
        //
        // flzCompress(inPtr, inLen, htBase, outPtr) -> outLen
        //   Reads `inLen` bytes from `inPtr`, writes FastLZ-compressed output to
        //   `outPtr`, and returns the compressed length.
        //   `htBase` must point to a zeroed 32 KB (0x8000-byte) region used as the
        //   hash table. Caller is responsible for zeroing it before the call.
        // ─────────────────────────────────────────────────────────────────────────
        function flzCompress(inPtr, inLen, htBase, outPtr) -> outLen {
            let ipStart := inPtr
            // ipLimit: 13 bytes before end — main loop needs lookahead.
            // If inLen < 13 the subtraction wraps; lt(ip, ipLimit) is then false and
            // the main loop is skipped, falling through to the trailing literals flush.
            let ipLimit := sub(add(inPtr, inLen), 13)
            let op := outPtr
            let a  := inPtr

            for { let ip := add(2, inPtr) } lt(ip, ipLimit) {} {
                let r := 0
                let d := 0

                // Inner scan: hash-table lookup loop. Advance `ip` until a back-reference
                // match is found (s == u24(r)) or the limit is reached.
                for {} 1 {} {
                    let s := flzU24(ip)
                    let h := flzHash(s)
                    r := add(ipStart, flzGetHash(htBase, h))
                    flzSetHash(htBase, h, sub(ip, ipStart))
                    d := sub(ip, r)
                    if iszero(lt(ip, ipLimit)) { break }
                    ip := add(ip, 1)
                    if iszero(gt(d, 0x1fff)) {
                        if eq(s, flzU24(r)) { break }
                    }
                }

                if iszero(lt(ip, ipLimit)) { break }

                // Found a match at ip-1 (inner loop incremented ip past the match).
                ip := sub(ip, 1)
                if gt(ip, a) { op := flzLiterals(sub(ip, a), a, op) }

                // Count bytes matched past the implicit 3-byte hash match.
                let l := flzCmp(add(r, 3), add(ip, 3), add(ipLimit, 9))
                op := flzMt(l, d, op)

                // Advance ip by l+2 and update the hash table for both skipped positions.
                let ipL  := add(ip, l)
                flzSetHash(htBase, flzHash(flzU24(ipL)),        sub(ipL, ipStart))
                let ipL1 := add(ipL, 1)
                flzSetHash(htBase, flzHash(flzU24(ipL1)),       sub(ipL1, ipStart))
                ip := add(ipL1, 1)
                a  := ip
            }

            // Flush remaining bytes as literals.
            op := flzLiterals(sub(add(inPtr, inLen), a), a, op)
            outLen := sub(op, outPtr)
        }

        // ─── FLZ helper functions ─────────────────────────────────────────────────

        // Little-endian 3-byte read at memory address p.
        function flzU24(p) -> r {
            r := mload(p)
            r := or(shl(16, byte(2, r)), or(shl(8, byte(1, r)), byte(0, r)))
        }

        // 13-bit Knuth multiplicative hash of a 24-bit value.
        // Agrees with JS's `((2654435769 * x) >> 19) & 8191` because both extract
        // bits [31:19] of the product (the Int32 truncation in JS and the uint256
        // arithmetic in EVM yield identical bits in that range after the final mask).
        function flzHash(v) -> r {
            r := and(shr(19, mul(2654435769, v)), 0x1fff)
        }

        // Read a 32-bit hash-table entry at index i (table base = htBase, 4 bytes/entry).
        function flzGetHash(base, i) -> r {
            r := shr(224, mload(add(base, shl(2, i))))
        }

        // Write a 32-bit hash-table entry at index i (top 4 bytes of 32-byte word).
        function flzSetHash(base, i, v) {
            let p := add(base, shl(2, i))
            mstore(p, xor(mload(p), shl(224, xor(shr(224, mload(p)), v))))
        }

        // Compare bytes at p and q up to e-q bytes; returns match length (see Solady notes).
        function flzCmp(p, q, e) -> l {
            for { e := sub(e, q) } lt(l, e) { l := add(l, 1) } {
                e := mul(iszero(byte(0, xor(mload(add(p, l)), mload(add(q, l))))), e)
            }
        }

        // Emit `runs` literal bytes from src to dest; returns updated dest pointer.
        function flzLiterals(runs, src, dest) -> o {
            for { o := dest } iszero(lt(runs, 0x20)) { runs := sub(runs, 0x20) } {
                mstore8(o, 31)
                mstore(add(o, 1), mload(src))
                o   := add(o, 0x21)
                src := add(src, 0x20)
            }
            if iszero(runs) { leave }
            mstore8(o, sub(runs, 1))
            mstore(add(o, 1), mload(src))
            o := add(1, add(o, runs))
        }

        // Emit a match token for length l at distance d, writing to o; returns updated o.
        function flzMt(l, d, o) -> ro {
            d := sub(d, 1)
            for {} iszero(lt(l, 263)) { l := sub(l, 262) } {
                mstore8(o,             add(224, shr(8, d)))
                mstore8(add(o, 0x01),  253)
                mstore8(add(o, 0x02),  and(0xff, d))
                o := add(o, 3)
            }
            switch iszero(lt(l, 7))
            case 1 {
                mstore8(o,             add(224, shr(8, d)))
                mstore8(add(o, 0x01),  sub(l, 7))
                mstore8(add(o, 0x02),  and(0xff, d))
                ro := add(o, 3)
            }
            default {
                mstore8(o,             add(shl(5, l), shr(8, d)))
                mstore8(add(o, 0x01),  and(0xff, d))
                ro := add(o, 2)
            }
        }
    }
}
