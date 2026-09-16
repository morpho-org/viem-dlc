import { type InlineContract, sol, solFile } from "soltag";

/**
 * Lenses compiled at build time from the same `.sol` files the editors display, so what a visitor
 * reads before touching anything is what actually ran. A step names one; editing its source swaps in
 * a browser compile of the same contract.
 */
export const PREBUILT: Record<string, InlineContract> = {
  VaultSnapshotLens: sol("VaultSnapshotLens")`${solFile("../tutorials/eth-call/vault-snapshot.sol", { raw: true })}`,
};
