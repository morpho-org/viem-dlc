/*
 * The envelope: initcode for a deployless eth_call over a paginated lens (Yul source).
 * Design: docs/000016-tib-envelope-paginated-lenses.md, docs/000016-tib-outcome-stream.md.
 *
 * Constructor args (ABI tuple; viem's wrapper's four, plus a config word):
 *   [0..32]:    target address
 *   [32..64]:   targetData offset
 *   [64..96]:   factory address
 *   [96..128]:  factoryData offset
 *   [128..160]: config — per-item selector (top 32 bits), input-dynamic bit 223, output-dynamic
 *               bit 222, compressed bit 221, input stride (bits 64..127), output stride (bits 0..63);
 *               strides are static element sizes, zero for a dynamic type
 *   [160..]:    targetData length + bytes, factoryData length + bytes
 *
 * targetData = n ‖ bodyLen ‖ body. Static T: body is n strides. Dynamic T: body is n records
 * `L ‖ E`, E the padded ABI tail. With the compressed bit set, body arrives FastLZ-compressed and
 * bodyLen is its decompressed length.
 *
 * The envelope calls the lens's per-item function once per element in its own frame, with all
 * remaining gas, and appends one record per adjudicated element to an outcome stream:
 * success `(1 << 255) | L ‖ L bytes`, decline `i`, death `~i` (always last). Nothing past the page
 * header is written before the attempt that needs it, and every memory expansion is admitted
 * against the fee schedule first.
 *
 * On page:              revert(OK_SENTINEL ‖ nA ‖ records)
 * On malformed result:  revert(MalformedResult(index, returndataSize))   — lens bug
 * On malformed input:   revert(MalformedInput(index))                    — codec bug
 * On deploy OOG:        revert(OOG_SENTINEL)                             — factory/constructor drained
 * On deploy fail:       revert(CounterfactualDeployFailed(bytes("")))    — or `target` already had code
 *
 * Build: `pnpm build:Envelope` — prints the hex constant to paste into codec.envelope.ts.
 */
object "Envelope" {
    code {
        // PUSH3 placeholder (4 bytes: opcode + 3 immediate), patched post-compile with the
        // measured init-code length by substituting the immediate in `0x62BBBBBB`.
        // `verbatim_0i_1o` keeps the PUSH fixed-width so that patch stays byte-for-byte stable.
        {
            let bytecodeLen := verbatim_0i_1o(hex"62BBBBBB")
            // Everything but [0, 0x80) scratch lives at or above `base`, which is what lets the
            // optimizer spill stack variables to memory if the loop needs it.
            let base := memoryguard(0x80)
            let argsEnd := add(base, sub(codesize(), bytecodeLen))
            codecopy(base, bytecodeLen, sub(argsEnd, base))

            let lens := mload(base)
            deploy(lens, mload(add(base, 0x40)), add(base, mload(add(base, 0x60))))

            let config := mload(add(base, 0x80))
            let tdOff := add(base, mload(add(base, 0x20)))
            let wireLen := mload(tdOff)
            let n := mload(add(tdOff, 0x20))
            let bodyLen := mload(add(tdOff, 0x40))
            let body := add(tdOff, 0x60)
            if lt(wireLen, 0x40) { malformedInput(n) }
            switch and(shr(221, config), 1)
            case 0 {
                if iszero(eq(wireLen, add(0x40, bodyLen))) { malformedInput(n) }
            }
            default {
                let dst := and(add(argsEnd, 31), not(31))
                if iszero(eq(flzDecompress(body, sub(wireLen, 0x40), dst), bodyLen)) { malformedInput(n) }
                body := dst
            }
            if iszero(and(shr(223, config), 1)) {
                let stride := inSize(config)
                if or(gt(n, div(bodyLen, stride)), iszero(eq(mul(n, stride), bodyLen))) { malformedInput(n) }
            }
            paginate(lens, n, body, bodyLen, config, argsEnd)
        }

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

        // Runs the page over the `n` elements in `body` and reverts with the outcome stream.
        //
        // The slab starts at the first word past the body: OK_SENTINEL, nA (patched at exit), then
        // records. `P` is one past the last record; attempt `i` is staged at `P + 0x20`, where its
        // record's bytes will go, so a static result lands on its own arguments and abandoned
        // bytes lie at or past `P`, outside the reverted prefix. `hw` is the highest byte this
        // frame has deliberately touched — never above the true high-water — so every admission
        // prices memory expansion exactly or conservatively.
        function paginate(target, n, body, bodyLen, config, argsEnd) {
            let slab := and(add(add(body, bodyLen), 31), not(31))
            // OK_SENTINEL = bytes4(keccak256("ViemDlcPage()")) = 0xf90a85b5
            mstore(slab, 0xf90a85b500000000000000000000000000000000000000000000000000000000)
            let P := add(slab, 0x24)
            mstore(P, 0)
            let hw := add(P, 0x20)
            if gt(argsEnd, hw) { hw := argsEnd }
            let inDyn := and(shr(223, config), 1)
            let outDyn := and(shr(222, config), 1)
            let cur := body

            let i := 0
            for {} lt(i, n) { i := add(i, 1) } {
                let L := inSize(config)
                if inDyn {
                    let remaining := sub(bodyLen, sub(cur, body))
                    if lt(remaining, 0x20) { malformedInput(i) }
                    L := mload(cur)
                    if or(and(L, 31), gt(L, sub(remaining, 0x20))) { malformedInput(i) }
                }
                let argsLen := add(add(4, mul(0x20, inDyn)), L)
                let outLen := mul(iszero(outDyn), outSize(config))
                let end := add(add(P, 0x20), argsLen)
                if gt(add(add(P, 0x20), outLen), end) { end := add(add(P, 0x20), outLen) }

                // Admit an attempt only if the frame can pay for its memory, the call's upfront
                // cost, and — through EIP-150's retained 1/64 — the longest path from the call's
                // return to a valid exit. Below the floor at element 0, report it unresolved.
                if iszero(gt(gas(), add(add(expansion(end, hw), apre(argsLen)), mul(64, cpost())))) {
                    if iszero(i) {
                        mstore(P, not(0))
                        P := add(P, 0x20)
                        i := 1
                    }
                    break
                }
                mstore(sub(end, 0x20), 0)
                if gt(end, hw) { hw := end }
                mstore(add(P, 0x20), and(config, shl(224, 0xffffffff)))
                let elem := cur
                if inDyn {
                    mstore(add(P, 0x24), 0x20)
                    elem := add(cur, 0x20)
                }
                mcopy(add(add(P, 0x24), mul(0x20, inDyn)), elem, L)
                cur := add(elem, L)

                let g := gas()
                switch staticcall(gas(), target, add(P, 0x20), argsLen, add(P, 0x20), outLen)
                case 1 {
                    let Lout := outLen
                    switch outDyn
                    case 0 {
                        if iszero(eq(returndatasize(), outLen)) { malformed(i) }
                    }
                    default {
                        if lt(returndatasize(), 0x40) { malformed(i) }
                        Lout := sub(returndatasize(), 0x20)
                        returndatacopy(0x00, 0, 0x20)
                        if or(and(Lout, 31), iszero(eq(mload(0x00), 0x20))) { malformed(i) }
                        // A dynamic result's size is only known now: admit its expansion and copy
                        // against the gas actually retained, or report the element unresolved.
                        let top := add(add(P, 0x20), Lout)
                        if iszero(gt(gas(), add(add(expansion(top, hw), add(3, mul(3, shr(5, Lout)))), cpost()))) {
                            mstore(P, not(i))
                            P := add(P, 0x20)
                            i := add(i, 1)
                            break
                        }
                        returndatacopy(add(P, 0x20), 0x20, Lout)
                        if gt(top, hw) { hw := top }
                    }
                    mstore(P, or(shl(255, 1), Lout))
                    P := add(add(P, 0x20), Lout)
                }
                default {
                    // Whatever the callee did not burn is refunded on top of the retained 1/64, so
                    // only a frame that reverted empty with ~nothing left reads as a gas death.
                    if and(iszero(returndatasize()), iszero(gt(gas(), add(div(g, 64), 32)))) {
                        mstore(P, not(i))
                        P := add(P, 0x20)
                        i := add(i, 1)
                        break
                    }
                    mstore(P, i)
                    P := add(P, 0x20)
                }
            }

            mstore(add(slab, 4), i)
            revert(slab, sub(P, slab))
        }

        // `cpost` is pinned by test/forge's `test_boundarySweep_returnsDrained*` (lowering it fails
        // them); `apre` is the measured pre-call spend, whose error reaches the retained gas at 1/64.
        function apre(argsLen) -> a { a := add(200, mul(3, shr(5, add(argsLen, 31)))) }
        function cpost() -> c { c := 800 }

        function memcost(b) -> c {
            let w := shr(5, add(b, 31))
            c := add(mul(3, w), shr(9, mul(w, w)))
        }
        function expansion(b, hw) -> e {
            if gt(b, hw) { e := sub(memcost(b), memcost(hw)) }
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

        function malformedInput(index) {
            // bytes4(keccak256("MalformedInput(uint256)")) = 0xf5880484
            mstore(0x00, 0xf588048400000000000000000000000000000000000000000000000000000000)
            mstore(0x04, index)
            revert(0x00, 0x24)
        }

        /// Reads `inLen` bytes of FastLZ-compressed data at `inPtr`, writes the decompressed bytes
        /// to `outPtr`, and returns their length. Copies back-refs with 32-byte mstores
        /// (stride = min(distance, 32)), so up to 31 bytes past `outLen` may be garbage; the slab's
        /// sentinel word overwrites them. Ported from Solady's LibZip.sol (MIT, Vectorized):
        /// https://github.com/Vectorized/solady/blob/main/src/utils/LibZip.sol
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
