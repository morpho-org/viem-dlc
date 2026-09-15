/*
 * The envelope: initcode for a deployless eth_call over a paginated lens (Yul source).
 * Design: docs/000016-tib-paginated-lenses.md.
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
 * bodyLen is its decompressed length; it is decompressed one element at a time, as attempted,
 * through a fixed-size history.
 *
 * The envelope calls the lens's per-item function once per element in its own frame, with all
 * remaining gas, and appends one record per adjudicated element to an outcome stream:
 * success `(1 << 255) | L ‖ L bytes`, decline `i`, death `~i` (always last). Ahead of the records,
 * five words of gas telemetry: the usable budget the loop started with, what the frame spent
 * before it (prologue, deploy and the reserve), then the sum, sum of squares and maximum of the
 * per-attempt gas of every record but a death. The guarantee: nothing is touched before it is
 * admitted, and nothing after the call costs more than the callee is unable to take away. Gas
 * before the call's EIP-150 split reaches the retained reserve at 1/64; gas after it reaches the
 * admission floor at 64× — see `admit`.
 *
 * Frame `F` (words), the state between the input cursor and the output cursor:
 *   0x00 target · 0x20 n · 0x40 body · 0x60 bodyLen · 0x80 config · 0xa0 ip · 0xc0 op · 0xe0 cur
 *   0x100 ipEnd · 0x120 consumed · 0x140 i · 0x160 len · 0x180 floor · 0x1a0 selector
 *   0x1c0… history (compressed only), then the slab
 *
 * On page:              revert(OK_SENTINEL ‖ nA ‖ budget ‖ fixed ‖ Σg ‖ Σg² ‖ gmax ‖ records)
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
            // The frame's gas on arrival, read back in `paginate` to cost the prologue.
            mstore(0x00, gas())
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
            if iszero(and(shr(223, config), 1)) {
                let stride := inSize(config)
                if or(gt(n, div(bodyLen, stride)), iszero(eq(mul(n, stride), bodyLen))) { malformedInput(n) }
            }
            // The frame: target | n | body | bodyLen | config | ip | op | cur | ipEnd | consumed |
            // i | len | floor | selector, then, when compressed, the decompression history; the
            // slab follows.
            let F := and(add(argsEnd, 31), not(31))
            mstore(F, lens)
            mstore(add(F, 0x20), n)
            mstore(add(F, 0x40), body)
            mstore(add(F, 0x60), bodyLen)
            mstore(add(F, 0x80), config)
            mstore(add(F, 0xe0), body)
            // The uncompressed static layout's attempt is the page's: its length and pre-split
            // floor are constants the loop reads rather than derives.
            mstore(add(F, 0x160), inSize(config))
            mstore(add(F, 0x180), pageFloor(config, add(4, inSize(config))))
            mstore(add(F, 0x1a0), and(config, shl(224, 0xffffffff)))
            switch and(shr(221, config), 1)
            case 0 {
                if iszero(eq(wireLen, add(0x40, bodyLen))) { malformedInput(n) }
                paginate(F, add(F, 0x1c0))
            }
            default {
                mstore(add(F, 0xa0), body)
                mstore(add(F, 0xc0), add(F, 0x1c0))
                mstore(add(F, 0xe0), add(F, 0x1c0))
                mstore(add(F, 0x100), add(body, sub(wireLen, 0x40)))
                paginate(F, add(add(F, 0x1c0), histSize()))
            }
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

        // Runs the page over the `n` elements described by the frame `F` and reverts with the
        // outcome stream.
        //
        // The slab starts at `slab`: OK_SENTINEL, nA (patched at exit), the five telemetry words
        // (accumulated in place), then records. `P` is one past the last record; attempt `i` is
        // staged at `P + 0x20`, where its record's bytes will go, so a static result lands on its
        // own arguments and abandoned bytes lie at or past `P`, outside the reverted prefix.
        // Scratch 0x40 holds the `memcost` of the highest byte this frame has deliberately touched
        // — never above the true high-water — so every admission prices memory expansion exactly
        // or conservatively.
        function paginate(F, slab) {
            // OK_SENTINEL = bytes4(keccak256("ViemDlcPage3()")) = 0xa55835c3
            mstore(slab, 0xa55835c300000000000000000000000000000000000000000000000000000000)
            // The budget attempts can spend, and what arriving here cost. Fresh memory zeroes the
            // three accumulators.
            mstore(0x20, gas())
            mstore(add(slab, 0x24), usable(mload(0x20)))
            mstore(add(slab, 0x44), sub(mload(0x00), mload(add(slab, 0x24))))
            let P := add(slab, 0xc4)
            mstore(P, 0)
            pop(expansion(add(P, 0x20)))
            let n := mload(add(F, 0x20))
            let config := mload(add(F, 0x80))
            let outLen := mul(iszero(outDyn(config)), outSize(config))
            let strided := iszero(or(inDyn(config), compressed(config)))

            let i := 0
            for {} lt(i, n) { i := add(i, 1) account(slab) } {
                let argsLen := 0
                switch strided
                case 1 { argsLen := admitStride(F, P, outLen) }
                default { argsLen := admit(F, config, P, outLen, i) }
                if iszero(argsLen) {
                    // A head it cannot attempt is reported unresolved, in memory the prologue
                    // touched; at i > 0 the prefix so far is the page, and `i == n` below can then
                    // only mean every element was staged.
                    if iszero(i) {
                        mstore(P, not(0))
                        mstore(add(slab, 4), 1)
                        revert(slab, sub(add(P, 0x20), slab))
                    }
                    break
                }
                let g := gas()
                switch staticcall(gas(), mload(F), add(P, 0x20), argsLen, add(P, 0x20), outLen)
                case 1 {
                    let Lout := outLen
                    switch outDyn(config)
                    case 0 {
                        if iszero(eq(returndatasize(), outLen)) { malformedResult(F, config, i) }
                    }
                    default {
                        if lt(returndatasize(), 0x40) { malformedResult(F, config, i) }
                        Lout := sub(returndatasize(), 0x20)
                        returndatacopy(0x00, 0, 0x20)
                        if or(and(Lout, 31), iszero(eq(mload(0x00), 0x20))) { malformedResult(F, config, i) }
                        // A dynamic result's size is only known now: admit its expansion and copy
                        // against the gas actually retained, or report the element unresolved.
                        let top := add(add(P, 0x20), Lout)
                        if iszero(gt(gas(), add(add(expansion(top), add(3, mul(3, shr(5, Lout)))), cpost()))) {
                            mstore(P, not(i))
                            P := add(P, 0x20)
                            i := add(i, 1)
                            break
                        }
                        returndatacopy(add(P, 0x20), 0x20, Lout)
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

            // `i == n` can only mean every element was staged: a head refusal exits above.
            if mul(n, eq(i, n)) { exhausted(F, config, sub(n, 1)) }
            mstore(add(slab, 4), i)
            revert(slab, sub(P, slab))
        }

        // The staged elements must have consumed the whole body: no logical byte, and on the
        // compressed path no token and no pending byte, left over.
        function exhausted(F, config, i) {
            let bad := iszero(eq(consumed(F, config), mload(add(F, 0x60))))
            if compressed(config) {
                bad := or(bad, or(
                    iszero(eq(mload(add(F, 0xa0)), mload(add(F, 0x100)))),
                    iszero(eq(mload(add(F, 0xc0)), mload(add(F, 0xe0))))
                ))
            }
            if bad { malformedInput(i) }
        }

        // Charges the attempt that began at the gas level in scratch 0x20 to the telemetry words
        // and leaves the level the next one begins at. Reached through the loop's post block, so a
        // `break` — a death or a refusal — is never charged.
        function account(slab) {
            let now := gas()
            let d := sub(mload(0x20), now)
            mstore(0x20, now)
            mstore(add(slab, 0x64), add(mload(add(slab, 0x64)), d))
            mstore(add(slab, 0x84), add(mload(add(slab, 0x84)), mul(d, d)))
            if gt(d, mload(add(slab, 0xa4))) { mstore(add(slab, 0xa4), d) }
        }

        // Adjudicates the next element under the uncompressed static layout: the attempt's length
        // and floor are the page's, so admission is one expansion price and one comparison, and the
        // element is staged straight out of the wire.
        function admitStride(F, P, outLen) -> argsLen {
            let L := mload(add(F, 0x160))
            let len := add(L, 4)
            let touch := len
            if gt(outLen, touch) { touch := outLen }
            if iszero(gt(gas(), add(expansion(add(add(P, 0x20), touch)), mload(add(F, 0x180))))) { leave }

            argsLen := len
            // The attempt's memory is touched now, so the call's expansion never lands after the
            // death heuristic's gas sample.
            mstore(sub(add(add(P, 0x20), touch), 0x20), 0)
            mstore(add(P, 0x20), mload(add(F, 0x1a0)))
            let cur := mload(add(F, 0xe0))
            mcopy(add(P, 0x24), cur, L)
            mstore(add(F, 0xe0), add(cur, L))
        }

        // Adjudicates element `i` (the frame's): prices the attempt against `need` — the memory
        // it touches, producing the element when compressed, the call's upfront cost, and, through
        // EIP-150's retained 1/64, the longest path from the call's return to a valid exit — and
        // either stages `selector ‖ [0x20 ‖] element` at `P + 0x20` and returns its length, or
        // returns 0 having touched nothing. A compressed dynamic element's length is in the
        // stream, so it is produced first under a fixed reserve; below that, `need` is
        // unaffordable by construction.
        function admit(F, config, P, outLen, i) -> argsLen {
            mstore(add(F, 0x140), i)
            let L := mload(add(F, 0x160))
            let len := add(L, 4)
            let floor := mload(add(F, 0x180))
            if inDyn(config) {
                let remaining := sub(mload(add(F, 0x60)), consumed(F, config))
                if lt(remaining, 0x20) { malformedInput(mload(add(F, 0x140))) }
                switch compressed(config)
                case 0 { L := mload(mload(add(F, 0xe0))) }
                default {
                    if iszero(gt(gas(), add(add(dwork(0x20), apre(0x24)), mul(64, cpost())))) { leave }
                    materialize(F, 0x20, 0x00)
                    L := mload(0x00)
                }
                if or(or(iszero(L), and(L, 31)), gt(L, sub(remaining, 0x20))) { malformedInput(mload(add(F, 0x140))) }
                len := add(L, 0x24)
                floor := pageFloor(config, len)
            }
            let touch := len
            if gt(outLen, touch) { touch := outLen }
            if iszero(gt(gas(), add(expansion(add(add(P, 0x20), touch)), floor))) { leave }

            argsLen := len
            mstore(sub(add(add(P, 0x20), touch), 0x20), 0)
            mstore(add(P, 0x20), mload(add(F, 0x1a0)))
            let dst := add(P, 0x24)
            let cur := mload(add(F, 0xe0))
            if inDyn(config) {
                mstore(dst, 0x20)
                dst := add(dst, 0x20)
                cur := add(cur, 0x20)
            }
            switch compressed(config)
            case 0 {
                mcopy(dst, cur, L)
                mstore(add(F, 0xe0), add(cur, L))
            }
            default {
                materialize(F, L, dst)
                mstore(add(F, 0x120), add(mload(add(F, 0x120)), add(mul(0x20, inDyn(config)), L)))
            }
        }

        // Logical body bytes handed out so far.
        function consumed(F, config) -> c {
            switch compressed(config)
            case 0 { c := sub(mload(add(F, 0xe0)), mload(add(F, 0x40))) }
            default { c := mload(add(F, 0x120)) }
        }

        function inDyn(config) -> b { b := and(shr(223, config), 1) }
        function outDyn(config) -> b { b := and(shr(222, config), 1) }
        function compressed(config) -> b { b := and(shr(221, config), 1) }

        // The history: a FastLZ back-reference window, a growth zone of the same size so rebases
        // amortize, and headroom for the most a token's 32-byte-stride writes can overshoot.
        function histSize() -> h { h := add(mul(2, 8192), 320) }

        // Produces whole tokens until `len` bytes are pending in the history, then copies them to
        // `dst`. Cursors live in `F`: ip (compressed read), op (history write), cur (next byte not
        // yet handed out). A token may overshoot `len`; the surplus stays pending for the next
        // call. Token decoding is Solady's LibZip (MIT, Vectorized):
        // https://github.com/Vectorized/solady/blob/main/src/utils/LibZip.sol
        function materialize(F, len, dst) {
            let ip := mload(add(F, 0xa0))
            let op := mload(add(F, 0xc0))
            let cur := mload(add(F, 0xe0))
            let ipEnd := mload(add(F, 0x100))
            // The last write position that cannot overshoot the history.
            let limit := sub(add(add(F, 0x1c0), histSize()), 296)
            for {} lt(sub(op, cur), len) {} {
                if gt(op, limit) {
                    // Rebase: hand out what is pending (all of it belongs to this element), then
                    // slide the last window to the front.
                    mcopy(dst, cur, sub(op, cur))
                    dst := add(dst, sub(op, cur))
                    len := sub(len, sub(op, cur))
                    mcopy(add(F, 0x1c0), sub(op, 8192), 8192)
                    op := add(add(F, 0x1c0), 8192)
                    cur := op
                }
                let ctrl := byte(0, mload(ip))
                switch shr(5, ctrl)
                case 0 {
                    // Literal run: ctrl+1 bytes
                    if gt(add(ip, add(2, ctrl)), ipEnd) { malformedInput(mload(add(F, 0x140))) }
                    mstore(op, mload(add(ip, 1)))
                    ip := add(ip, add(2, ctrl))
                    op := add(op, add(1, ctrl))
                }
                default {
                    // Back-reference: `g` is 1 for a long match, `l` the length, `s` the 1-based distance
                    let g := eq(shr(5, ctrl), 7)
                    if gt(add(ip, add(2, g)), ipEnd) { malformedInput(mload(add(F, 0x140))) }
                    let l := add(2, xor(shr(5, ctrl), mul(g, xor(shr(5, ctrl), add(7, byte(1, mload(ip)))))))
                    let s := add(add(shl(8, and(0x1f, ctrl)), byte(add(1, g), mload(ip))), 1)
                    if gt(s, sub(op, add(F, 0x1c0))) { malformedInput(mload(add(F, 0x140))) }
                    // An overlapping copy by doubling: each round copies the whole periodic prefix
                    // written so far, so source and destination never overlap and the copy is exact.
                    for { let j := 0 } lt(j, l) {} {
                        let c := add(s, j)
                        if gt(c, sub(l, j)) { c := sub(l, j) }
                        mcopy(add(op, j), sub(op, s), c)
                        j := add(j, c)
                    }
                    ip := add(ip, add(2, g))
                    op := add(op, l)
                }
            }
            mcopy(dst, cur, len)
            mstore(add(F, 0xa0), ip)
            mstore(add(F, 0xc0), op)
            mstore(add(F, 0xe0), add(cur, len))
        }

        // Reserve for producing and copying out `L` bytes: the worst per-byte token cost (one-byte
        // literals, ~230 gas each), then one overshooting token, one rebase and the copy-out with
        // margin. Pre-split, so an error reaches the reserve at 1/64; only a large element can pin
        // the per-byte term — test/forge's `test_adversary_largeLiteralElement`.
        function dwork(L) -> d { d := add(add(mul(300, L), mul(3, shr(5, add(L, 31)))), 9000) }

        // `cpost` is post-split: the reserve itself, 64× at the floor. Pinned by a callee that returns
        // with nothing left, test/forge's `test_adversary_drainedCallee*`; nothing else can test it.
        // `apre` is pre-split, so its error reaches the reserve at 1/64.
        function apre(argsLen) -> a { a := add(200, mul(3, shr(5, add(argsLen, 31)))) }
        function cpost() -> c { c := 1400 }

        // The pre-split floor of an attempt staging a `len`-byte call: the call's upfront cost, the
        // reserve at 64×, and, compressed, producing the element. Constant for a static layout, so
        // the frame carries it.
        function pageFloor(config, len) -> f {
            f := add(apre(len), mul(64, cpost()))
            if compressed(config) { f := add(f, dwork(sub(len, add(4, mul(0x20, inDyn(config)))))) }
        }
        function usable(g) -> b { b := mul(gt(g, mul(64, cpost())), sub(g, mul(64, cpost()))) }

        function memcost(b) -> c {
            let w := shr(5, add(b, 31))
            c := add(mul(3, w), shr(9, mul(w, w)))
        }
        // Prices the expansion to `b` against the high-water's memory cost, held in scratch 0x40,
        // and raises it: every caller either touches `b` or leaves the loop.
        function expansion(b) -> e {
            let m := memcost(b)
            if gt(m, mload(0x40)) {
                e := sub(m, mload(0x40))
                mstore(0x40, m)
            }
        }

        function inSize(config) -> s { s := and(shr(64, config), 0xffffffffffffffff) }
        function outSize(config) -> s { s := and(config, 0xffffffffffffffff) }

        // The last element's wire is judged before its result: a codec bug outranks a lens bug.
        function malformedResult(F, config, i) {
            if eq(add(i, 1), mload(add(F, 0x20))) { exhausted(F, config, i) }
            malformed(i)
        }

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
    }
}
