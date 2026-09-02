/*
 * Outer constructor wrapper for deployless eth_call with FLZ input compression,
 * REVERT-mode exfiltration (Yul source).
 *
 * Constructor args and page loop: see RevertEnvelope.yul — `targetData` is FLZ-compressed here,
 * and the `deploy` / `paginate` / `stage` / `malformed` functions are copied from it verbatim.
 *
 * Wire protocol (input compressed only):
 *   caller:    targetData = flzCompress(ABI-encoded array-shaped calldata)
 *   envelope:  decompresses targetData, runs the page, reverts with OK_SENTINEL || (results, skipped)
 *   caller:    strips the sentinel; no decompression needed
 *
 * Memory layout (monotonically increasing, no aliasing):
 *   [base, base+argsLen)          constructor args (codecopy)
 *   [argsEnd, argsEnd+decompLen)  decompressed targetData
 *   [decompEnd, ...)              page frame, staging, slab — see RevertEnvelope.yul
 *
 * FLZ algorithm ported from Solady's LibZip.sol (MIT License, Vectorized).
 *   https://github.com/Vectorized/solady/blob/main/src/utils/LibZip.sol
 *
 * Build: `pnpm build:RevertEnvelopeCompressed` — prints the hex constant to paste into
 * codec.envelope.ts.
 */
object "RevertEnvelopeCompressed" {
    code {
        // PUSH3 placeholder patched post-compile — see RevertEnvelope.yul.
        let bytecodeLen := verbatim_0i_1o(hex"62BBBBBB")
        let base := memoryguard(0x80)
        let argsEnd := add(base, sub(codesize(), bytecodeLen))
        codecopy(base, bytecodeLen, sub(argsEnd, base))

        let lens := mload(base)
        deploy(lens, mload(add(base, 0x40)), add(base, mload(add(base, 0x60))))

        let tdOff := add(base, mload(add(base, 0x20)))
        let decompPtr := and(add(argsEnd, 31), not(31))
        let decompLen := flzDecompress(add(tdOff, 0x20), mload(tdOff), decompPtr)
        paginate(lens, decompPtr, decompLen, mload(add(base, 0x80)))

        // Unlike viem's wrapper, resident code at `target` is a failure: nothing here can check it
        // was built from `factoryData`, so the only lens trusted is the one this frame watched the
        // factory deploy (a CREATE2 address commits to that initcode). The post-check catches a
        // factory that succeeded without deploying at the precomputed address.
        function deploy(target, factory, fdOff) {
            if iszero(extcodesize(target)) {
                let gasBefore := gas()
                let deployed := call(gas(), factory, 0, add(fdOff, 0x20), mload(fdOff), 0, 0)
                // A constructor that dies of gas is two frames down: the factory keeps its own 1/64
                // and hands it back, so a drained deploy leaves at most ~2/64 of `gasBefore` here.
                if and(iszero(deployed), and(iszero(returndatasize()), iszero(gt(gas(), div(gasBefore, 32))))) {
                    // The factory (or the constructor inside it) ran out of gas: a prologue death the
                    // envelope can report. OOG_SENTINEL = bytes4(keccak256("ViemDlcOutOfGas()")) = 0xcc0bd34c
                    mstore(0x00, 0xcc0bd34c00000000000000000000000000000000000000000000000000000000)
                    revert(0x00, 0x04)
                }
                if and(deployed, iszero(iszero(extcodesize(target)))) { leave }
            }
            // bytes4(keccak256("CounterfactualDeployFailed(bytes)")) = 0x101bb98d
            mstore(0x00, 0x101bb98d00000000000000000000000000000000000000000000000000000000)
            mstore(0x04, 0x20)
            mstore(0x24, 0)
            revert(0x00, 0x44)
        }

        // Runs the page over the elements in `targetData` at `td` (length `tdLen`) and reverts.
        //
        // Memory, all at or above `td + tdLen`:
        //   F  (5 words)  frame: floor | scratch ptr | elements ptr | elements end | config
        //   staging       selector || element (static: stride; dynamic: 0x20 head || tail ≤ bound)
        //   slab          OK_SENTINEL (4 bytes) then the tuple at T = slab + 4:
        //                 [0x40][skippedAt][nR][body: static strides | n offset words + tails][nS][skips]
        //   scratch       skip words at the slab's far end, relocated behind the results cursor at exit
        // The tuple's `skippedAt` and `nR` words are patched at exit, so the body holds the used
        // prefix only and the REVERT covers exactly what was produced.
        function paginate(target, td, tdLen, config) {
            let n := mload(add(td, 0x24))
            let F := and(add(add(td, tdLen), 31), not(31))
            let staging := add(F, 0xa0)
            mstore(staging, and(config, shl(224, 0xffffffff)))
            let slab := and(add(add(staging, add(0x24, inSize(config))), 31), not(31))
            // OK_SENTINEL = bytes4(keccak256("ViemDlcOk()")) = 0x1580d19d
            mstore(slab, 0x1580d19d00000000000000000000000000000000000000000000000000000000)
            let T := add(slab, 4)
            mstore(T, 0x40)
            let body := add(T, 0x60)
            let outDyn := and(shr(222, config), 1)
            let cursor := add(body, mul(outDyn, mul(n, 0x20)))
            {
                let scratch := add(add(body, mul(n, add(outSize(config), mul(outDyn, 0x20)))), 0x20)
                mstore(add(scratch, mul(n, 0x20)), 0)
                // FLOOR = 64·C + B + M + S: C funds one post-call deposit (plus the dynamic-result
                // copy, 3 gas/word) and the exit; B the call-site cost before EIP-150's split; M
                // rounding and the loop's own opcodes; S the staging copy of one element, paid
                // before the split. Per skip to relocate, 64·3 more.
                mstore(
                    F,
                    add(
                        add(mul(64, add(500, mul(outDyn, mul(3, shr(5, add(outSize(config), 31)))))), 400),
                        mul(3, shr(5, add(inSize(config), 31)))
                    )
                )
                mstore(add(F, 0x20), scratch)
                mstore(add(F, 0x40), add(td, 0x44))
                mstore(add(F, 0x60), add(td, tdLen))
                mstore(add(F, 0x80), config)
            }

            let nR := 0
            let nS := 0
            for { let i := 0 } lt(i, n) { i := add(i, 1) } {
                // Admit an attempt only if the exit can still be afforded after it appends a skip.
                if iszero(gt(gas(), add(mload(F), mul(192, add(nS, 1))))) {
                    if iszero(i) {
                        mstore(mload(add(F, 0x20)), not(0))
                        nS := 1
                    }
                    break
                }

                let argsLen := stage(F, staging, i, n)
                if iszero(argsLen) {
                    mstore(add(mload(add(F, 0x20)), mul(nS, 0x20)), i)
                    nS := add(nS, 1)
                    continue
                }

                let g := gas()
                switch outDyn
                case 0 {
                    if staticcall(gas(), target, staging, argsLen, cursor, outSize(config)) {
                        if iszero(eq(returndatasize(), outSize(config))) { malformed(i) }
                        cursor := add(cursor, outSize(config))
                        nR := add(nR, 1)
                        continue
                    }
                }
                default {
                    if staticcall(gas(), target, staging, argsLen, 0, 0) {
                        if lt(returndatasize(), 0x40) { malformed(i) }
                        let tailLen := sub(returndatasize(), 0x20)
                        returndatacopy(0x00, 0, 0x20)
                        if or(gt(tailLen, outSize(config)), or(and(tailLen, 31), iszero(eq(mload(0x00), 0x20)))) {
                            malformed(i)
                        }
                        returndatacopy(cursor, 0x20, tailLen)
                        mstore(add(body, mul(nR, 0x20)), sub(cursor, body))
                        cursor := add(cursor, tailLen)
                        nR := add(nR, 1)
                        continue
                    }
                }

                // Whatever the callee did not burn is refunded on top of the retained 1/64, so
                // only a frame that reverted empty with ~nothing left reads as a gas death.
                if and(iszero(returndatasize()), iszero(gt(gas(), add(div(g, 64), 32)))) {
                    mstore(add(mload(add(F, 0x20)), mul(nS, 0x20)), not(i))
                    nS := add(nS, 1)
                    break
                }
                mstore(add(mload(add(F, 0x20)), mul(nS, 0x20)), i)
                nS := add(nS, 1)
            }

            mcopy(add(cursor, 0x20), mload(add(F, 0x20)), mul(nS, 0x20))
            mstore(add(T, 0x40), nR)
            mstore(add(T, 0x20), sub(cursor, T))
            mstore(cursor, nS)
            revert(slab, add(sub(add(cursor, 0x24), T), mul(nS, 0x20)))
        }

        // Writes `selector || element i` at `staging` and returns its length, or 0 for a dynamic
        // element whose tail exceeds the declared bound (a deterministic decline).
        function stage(F, staging, i, n) -> argsLen {
            let config := mload(add(F, 0x80))
            let elems := mload(add(F, 0x40))
            switch and(shr(223, config), 1)
            case 0 {
                let stride := inSize(config)
                mcopy(add(staging, 4), add(elems, mul(i, stride)), stride)
                argsLen := add(4, stride)
            }
            default {
                let tail := add(elems, mload(add(elems, mul(i, 0x20))))
                let next := mload(add(F, 0x60))
                if lt(add(i, 1), n) { next := add(elems, mload(add(elems, mul(add(i, 1), 0x20)))) }
                let tailLen := sub(next, tail)
                if gt(tailLen, inSize(config)) { leave }
                mstore(add(staging, 4), 0x20)
                mcopy(add(staging, 0x24), tail, tailLen)
                argsLen := add(0x24, tailLen)
            }
        }

        function inSize(config) -> s { s := and(shr(64, config), 0xffffffffffffffff) }
        function outSize(config) -> s { s := and(config, 0xffffffffffffffff) }

        function malformed(index) {
            // bytes4(keccak256("MalformedResult(uint256,uint256)")) = 0xace36ecd
            mstore(0x00, 0xace36ecd00000000000000000000000000000000000000000000000000000000)
            mstore(0x04, index)
            mstore(0x24, returndatasize())
            revert(0x00, 0x44)
        }

        /// Reads `inLen` bytes of FastLZ-compressed data at `inPtr`, writes the decompressed bytes
        /// to `outPtr`, and returns their length. Copies back-refs with 32-byte mstores
        /// (stride = min(distance, 32)), so bytes past `outLen` may be garbage — the page frame
        /// is placed after `outLen` rounded up, and overwrites them.
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
