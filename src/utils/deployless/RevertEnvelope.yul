/*
 * Outer constructor wrapper for deployless eth_call (Yul source).
 *
 * Constructor args layout (ABI tuple, identical to viem's wrapper):
 *   [0..32]:   target address
 *   [32..64]:  targetData offset (= 128 if no funny business)
 *   [64..96]:  factory address
 *   [96..128]: factoryData offset
 *   [128..]:   targetData length + bytes, factoryData length + bytes
 *
 * On lens success:        revert(OK_SENTINEL || returndata)
 * On lens revert:         revert(returndata)
 * On factory deploy fail: revert(DeploymentFailed(target))   (matches viem)
 *
 * `BYTECODE_LEN` is the placeholder we patch post-compile with the actual length
 * of the emitted init code. We use `verbatim_0i_1o(...)` to inline a fixed-width
 * 3-byte PUSH so the patch is a stable byte-for-byte substitution.
 *
 * Build: `pnpm build:wrapper` — runs solc, measures the binary, substitutes the
 * placeholder, and prints the patched hex constant ready to paste into
 * `src/utils/deployless/codec.envelope.ts`.
 */
object "RevertEnvelope" {
    code {
        // Bake placeholder bytecode-len constant. PUSH3 is 4 bytes total (1 opcode + 3 imm).
        // After compile, locate `0x62BBBBBB` (PUSH3 0xBBBBBB) in the binary and replace
        // the immediate with the measured init-code length.
        let bytecodeLen := verbatim_0i_1o(hex"62BBBBBB")
        let argsLen := sub(codesize(), bytecodeLen)
        codecopy(0, bytecodeLen, argsLen)

        let target := mload(0x00)
        let tdOff := mload(0x20)
        let factory := mload(0x40)
        let fdOff := mload(0x60)

        // factory.call(factoryData) — deploys lens at `target`. We require both that the
        // call itself succeeded AND that `target` actually has code afterwards: the second
        // check catches a misformed (target, factory, factoryData) triple where the factory
        // succeeded but didn't deploy at the precomputed address. Without it, the next
        // `call(target, ...)` would hit an EOA, return success with empty returndata, and
        // we'd silently revert with `OK_SENTINEL || ""` — confusing instead of "deploy failed".
        let fdLen := mload(fdOff)
        let deployed := call(gas(), factory, 0, add(fdOff, 0x20), fdLen, 0, 0)
        if or(iszero(deployed), iszero(extcodesize(target))) {
            // bytes4(keccak256("DeploymentFailed(address)")) = 0x9deffc1b
            mstore(0x00, 0x9deffc1b00000000000000000000000000000000000000000000000000000000)
            mstore(0x04, target)
            revert(0x00, 0x24)
        }

        // target.call(targetData)
        let tdLen := mload(tdOff)
        let ok := call(gas(), target, 0, add(tdOff, 0x20), tdLen, 0, 0)
        let rdLen := returndatasize()

        if ok {
            // OK_SENTINEL = bytes4(keccak256("ViemDlcOk()")) = 0x1580d19d
            mstore(0x00, 0x1580d19d00000000000000000000000000000000000000000000000000000000)
            returndatacopy(0x04, 0x00, rdLen)
            revert(0x00, add(0x04, rdLen))
        }
        returndatacopy(0x00, 0x00, rdLen)
        revert(0x00, rdLen)
    }
}
