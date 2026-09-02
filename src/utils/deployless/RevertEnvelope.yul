/*
 * Outer constructor wrapper for deployless eth_call, REVERT-mode exfiltration (Yul source).
 *
 * Canonical envelope: the compressed variant points here for the constructor-args layout, the
 * bytecode-length patching trick, the skip-deploy rule, the OOG heuristic, and the page loop
 * (`paginate`, `stage`, `malformed` are copied verbatim there — Yul has no imports).
 *
 * Constructor args (ABI tuple; viem's wrapper's four, plus a config word):
 *   [0..32]:    target address
 *   [32..64]:   targetData offset (= 160 if no funny business)
 *   [64..96]:   factory address
 *   [96..128]:  factoryData offset
 *   [128..160]: config — per-item selector (top 32 bits), input-dynamic bit (223), output-dynamic
 *               bit (222), input element size (bits 64..127), output element size (bits 0..63);
 *               sizes are static strides, or maximum tail bytes for a dynamic type
 *   [160..]:    targetData length + bytes, factoryData length + bytes
 *
 * `targetData` is the ABI-encoded array-shaped call `f(T[])`: selector, 0x20, n, elements. The
 * envelope never uses that selector; it reads `n` and the elements and calls the lens's per-item
 * function once per element in its own frame, with all remaining gas. Results are deposited into
 * a response slab laid out as the `(U[] results, uint256[] skipped)` tuple; an element the
 * per-item call reverted on is skipped; an element whose frame died of gas is reported as `~i`
 * (last `skipped` entry) and the page stops. The slab begins with OK_SENTINEL, so success is a
 * single REVERT over memory expanded during the prologue — the retained 1/64 never funds a copy.
 *
 * On page:              revert(OK_SENTINEL || (results, skipped))
 * On malformed result:  revert(MalformedResult(index, returndataSize))      — lens bug
 * On deploy OOG:        revert(OOG_SENTINEL)                                — factory/constructor drained
 * On deploy fail:       revert(CounterfactualDeployFailed(bytes("")))
 *
 * Build: `pnpm build:RevertEnvelope` — prints the hex constant to paste into codec.envelope.ts.
 */
object "RevertEnvelope" {
    code {
        // PUSH3 placeholder (4 bytes: opcode + 3 immediate), patched post-compile with the
        // measured init-code length by substituting the immediate in `0x62BBBBBB`.
        // `verbatim_0i_1o` keeps the PUSH fixed-width so that patch stays byte-for-byte stable.
        let bytecodeLen := verbatim_0i_1o(hex"62BBBBBB")
        // Everything but [0, 0x80) scratch lives at or above `base`, which is what lets the
        // optimizer spill stack variables to memory if the loop needs it.
        let base := memoryguard(0x80)
        codecopy(base, bytecodeLen, sub(codesize(), bytecodeLen))

        let lens := mload(base)
        deploy(lens, mload(add(base, 0x40)), add(base, mload(add(base, 0x60))))

        let tdOff := add(base, mload(add(base, 0x20)))
        paginate(lens, add(tdOff, 0x20), mload(tdOff), mload(add(base, 0x80)))

        // Deploy only into an empty `target`. Matches viem, and keeps a pre-deployed lens from
        // bricking every call: a CREATE2 factory asked to redeploy reverts, which would turn
        // "someone already deployed our lens" into a permanent CounterfactualDeployFailed.
        // Trusting resident code is safe because a CREATE2 address commits to its initcode hash.
        // The post-check catches a misformed (target, factory, factoryData) triple where the
        // factory succeeded without deploying at the precomputed address; without it the
        // per-item calls would hit an EOA and succeed with empty returndata.
        function deploy(target, factory, fdOff) {
            if extcodesize(target) { leave }
            let gasBefore := gas()
            let deployed := call(gas(), factory, 0, add(fdOff, 0x20), mload(fdOff), 0, 0)
            // A constructor that dies of gas is two frames down: the factory keeps its own 1/64 and
            // hands it back, so a drained deploy leaves at most ~2/64 of `gasBefore` here.
            if and(iszero(deployed), and(iszero(returndatasize()), iszero(gt(gas(), div(gasBefore, 32))))) {
                // The factory (or the constructor inside it) ran out of gas: a prologue death the
                // envelope can report. OOG_SENTINEL = bytes4(keccak256("ViemDlcOutOfGas()")) = 0xcc0bd34c
                mstore(0x00, 0xcc0bd34c00000000000000000000000000000000000000000000000000000000)
                revert(0x00, 0x04)
            }
            if or(iszero(deployed), iszero(extcodesize(target))) {
                // bytes4(keccak256("CounterfactualDeployFailed(bytes)")) = 0x101bb98d
                mstore(0x00, 0x101bb98d00000000000000000000000000000000000000000000000000000000)
                mstore(0x04, 0x20)
                mstore(0x24, 0)
                revert(0x00, 0x44)
            }
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
    }
}
