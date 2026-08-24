/*
 * Outer constructor wrapper for deployless eth_call, REVERT-mode exfiltration (Yul source).
 *
 * Canonical envelope: the other three point here for the constructor-args layout, the
 * bytecode-length patching trick, the skip-deploy rule, and the OOG heuristic.
 *
 * Constructor args (ABI tuple, identical to viem's wrapper):
 *   [0..32]:   target address
 *   [32..64]:  targetData offset (= 128 if no funny business)
 *   [64..96]:  factory address
 *   [96..128]: factoryData offset
 *   [128..]:   targetData length + bytes, factoryData length + bytes
 *
 * On lens success:  revert(OK_SENTINEL || returndata)
 * On lens revert:   revert(returndata verbatim)
 * On lens OOG:      revert(OOG_SENTINEL)
 * On deploy fail:   revert(CounterfactualDeployFailed(bytes("")))
 *
 * Build: `pnpm build:RevertEnvelope` — prints the hex constant to paste into codec.envelope.ts.
 */
object "RevertEnvelope" {
    code {
        // PUSH3 placeholder (4 bytes: opcode + 3 immediate), patched post-compile with the
        // measured init-code length by substituting the immediate in `0x62BBBBBB`.
        // `verbatim_0i_1o` keeps the PUSH fixed-width so that patch stays byte-for-byte stable.
        let bytecodeLen := verbatim_0i_1o(hex"62BBBBBB")
        let argsLen := sub(codesize(), bytecodeLen)
        codecopy(0x00, bytecodeLen, argsLen)

        let target  := mload(0x00)
        let tdOff   := mload(0x20)
        let factory := mload(0x40)
        let fdOff   := mload(0x60)

        // Deploy only into an empty `target`. Matches viem, and keeps a pre-deployed lens from
        // bricking every call: a CREATE2 factory asked to redeploy reverts, which would turn
        // "someone already deployed our lens" into a permanent CounterfactualDeployFailed.
        // Trusting resident code is safe because a CREATE2 address commits to its initcode hash.
        // The post-check catches a misformed (target, factory, factoryData) triple where the
        // factory succeeded without deploying at the precomputed address; without it the next
        // `call(target, ...)` would hit an EOA and succeed with empty returndata.
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

        let tdLen := mload(tdOff)
        let gasBefore := gas()
        let ok := call(gas(), target, 0, add(tdOff, 0x20), tdLen, 0, 0)
        let rdLen := returndatasize()

        if ok {
            // OK_SENTINEL = bytes4(keccak256("ViemDlcOk()")) = 0x1580d19d
            mstore(0x00, 0x1580d19d00000000000000000000000000000000000000000000000000000000)
            returndatacopy(0x04, 0x00, rdLen)
            revert(0x00, add(0x04, rdLen))
        }

        // A lens frame that runs out of gas cannot say so: it reverts with empty data, which is
        // indistinguishable from a bare `revert()` and leaves the batcher no reason to bisect.
        // EIP-150 handed the callee 63/64 of `gasBefore` and kept us the remainder, so empty
        // returndata plus a frame drained to that remainder is an out-of-gas. A lens that burns
        // >98.4% of its frame and *then* reverts empty is reported as OOG too — to a batcher
        // deciding whether to split, that is the same answer.
        if and(iszero(rdLen), iszero(gt(gas(), div(gasBefore, 64)))) {
            // OOG_SENTINEL = bytes4(keccak256("ViemDlcOutOfGas()")) = 0xcc0bd34c
            mstore(0x00, 0xcc0bd34c00000000000000000000000000000000000000000000000000000000)
            revert(0x00, 0x04)
        }

        returndatacopy(0x00, 0x00, rdLen)
        revert(0x00, rdLen)
    }
}
