import { sol, solFile } from "soltag";

export const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb" as const;

export const LENS_NAME = "MorphoPositionsLens";

/**
 * The prebuilt lens, compiled from `tabs/positions.sol` at build time — the same file the editor
 * displays, so what a visitor reads before touching anything is what actually ran.
 */
export const positionsLens = sol("MorphoPositionsLens")`${solFile("../tabs/positions.sol", { raw: true })}`;
