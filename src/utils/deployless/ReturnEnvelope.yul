/*
 * Outer constructor wrapper for deployless eth_call, RETURN-mode exfiltration (Yul source).
 *
 * Constructor args: see RevertEnvelope.yul.
 *
 * On lens success:  return(returndata)   — no sentinel needed
 * On lens revert:   revert(returndata verbatim)
 * On lens OOG:      revert(OOG_SENTINEL)
 * On deploy fail:   revert(CounterfactualDeployFailed(bytes("")))
 *
 * Exists because viem's `deploylessCallViaFactoryBytecode` is a fixed constant we cannot patch,
 * so RETURN mode could not report an out-of-gas the way the other three envelopes do. Otherwise
 * behaviorally identical to viem's, except that a failed factory call reports `bytes("")` rather
 * than forwarding the factory's revert data. viem's constant is still accepted inbound — see
 * `FACTORY_BYTECODE_RETURN_VIEM` in codec.envelope.ts.
 *
 * Build: `pnpm build:ReturnEnvelope` — prints the hex constant to paste into codec.envelope.ts.
 */
object "ReturnEnvelope" {
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

        let tdLen := mload(tdOff)
        let gasBefore := gas()
        let ok := call(gas(), target, 0, add(tdOff, 0x20), tdLen, 0, 0)
        let rdLen := returndatasize()

        if ok {
            returndatacopy(0x00, 0x00, rdLen)
            return(0x00, rdLen)
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
    }
}
