/**
 * Checks the claims the playground rests on, in a real browser: that the in-page compiler produces
 * exactly the bytecode the build produced, and that a tab's source survives evaluation.
 *
 * Open `/selftest.html`, or drive it headlessly and read the document title.
 */
import initcodeScript from "../tabs/initcode.js?raw";
import pristineSolidity from "../tabs/positions.sol?raw";

import { compileLens } from "./compile.js";
import { LENS_NAME, positionsLens } from "./lens.js";
import { evaluateTab } from "./tab.js";

const lines: string[] = [];
let failures = 0;

function check(name: string, ok: boolean, detail = "") {
  if (!ok) failures += 1;
  lines.push(`${ok ? "pass" : "FAIL"}  ${name}${detail ? `  — ${detail}` : ""}`);
  document.querySelector("#report")!.textContent = lines.join("\n");
}

try {
  const tab = evaluateTab(initcodeScript);
  check("evaluateTab yields a callable", typeof tab === "function");

  try {
    evaluateTab('import { nope } from "nowhere";\nexport default async () => ({ results: [], skipped: [] });');
    check("unknown import rejected", false, "no error thrown");
  } catch (error) {
    check("unknown import rejected", `${error}`.includes("Cannot import"));
  }

  const built = positionsLens.with();
  const compiled = (await compileLens(LENS_NAME, pristineSolidity)).with();

  check(
    "browser bytecode matches build",
    compiled.factoryData === built.factoryData,
    `${compiled.factoryData.length} chars`,
  );
  check("counterfactual address matches", compiled.address === built.address, compiled.address);
  check("abi matches", JSON.stringify(compiled.abi) === JSON.stringify(built.abi));

  // solc hashes the source into the trailing CBOR metadata, so even a comment moves the bytecode
  // and with it the CREATE2 address. Harmless for a counterfactual lens, surprising if unstated.
  const edited = pristineSolidity.replace("uint256 supplyShares;", "uint256 supplyShares; /* edited */");
  const afterEdit = (await compileLens(LENS_NAME, edited)).with();
  check("a comment-only edit still moves the metadata hash", afterEdit.factoryData !== built.factoryData);
  check("a comment-only edit leaves the abi alone", JSON.stringify(afterEdit.abi) === JSON.stringify(built.abi));

  try {
    await compileLens(LENS_NAME, "pragma solidity ^0.8.24; contract MorphoPositionsLens { oops }");
    check("compile errors surface", false, "no error thrown");
  } catch (error) {
    check("compile errors surface", `${error}`.length > 0, `${error}`.split("\n")[0]);
  }
} catch (error) {
  check("selftest completed", false, `${error}`);
}

document.title = failures === 0 ? "SELFTEST PASS" : `SELFTEST FAIL (${failures})`;
