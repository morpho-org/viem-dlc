import type { Hex } from "viem";
import { describe, expect, it } from "vitest";

import {
  COUNTERFACTUAL_DEPLOY_FAILED_SELECTOR,
  decodeEnvelopeRevert,
  MALFORMED_RESULT_SELECTOR,
  OK_SENTINEL,
  OOG_SENTINEL,
} from "../../../src/utils/deployless/codec.envelope.js";

/** An error carrying `data`, optionally wrapping another as its `cause`. */
function revert(data: Hex, cause?: Error): Error {
  return Object.assign(new Error("execution reverted"), { data }, cause === undefined ? {} : { cause });
}

const PAGE = `${OK_SENTINEL}${"ab".repeat(8)}` as Hex;
const MALFORMED_RESULT = `${MALFORMED_RESULT_SELECTOR}${"00".repeat(64)}` as Hex;

describe("decodeEnvelopeRevert", () => {
  it("reads a page off the error's own data", () => {
    expect(decodeEnvelopeRevert(revert(PAGE))).toEqual({ kind: "page", data: `0x${"ab".repeat(8)}` });
  });

  it("prefers a page anywhere on the cause chain to a fatal revert outside it", () => {
    expect(decodeEnvelopeRevert(revert(OOG_SENTINEL, revert(PAGE)))).toEqual({
      kind: "page",
      data: `0x${"ab".repeat(8)}`,
    });
  });

  it("ranks the fatal reverts by kind, not by depth on the chain", () => {
    expect(decodeEnvelopeRevert(revert(OOG_SENTINEL, revert(MALFORMED_RESULT)))).toEqual({ kind: "malformedResult" });
    expect(decodeEnvelopeRevert(revert(COUNTERFACTUAL_DEPLOY_FAILED_SELECTOR, revert(OOG_SENTINEL)))).toEqual({
      kind: "counterfactualDeployFailed",
    });
  });

  it("matches the out-of-gas marker exactly and the malformed reverts at their declared length", () => {
    expect(decodeEnvelopeRevert(revert(OOG_SENTINEL))).toEqual({ kind: "outOfGas" });
    expect(decodeEnvelopeRevert(revert(`${OOG_SENTINEL}${"00".repeat(32)}`))).toBeNull();
    expect(decodeEnvelopeRevert(revert(`${MALFORMED_RESULT_SELECTOR}${"00".repeat(32)}`))).toBeNull();
  });

  it("is null for a lens's own revert and for an error without data", () => {
    expect(decodeEnvelopeRevert(revert("0x08c379a0"))).toBeNull();
    expect(decodeEnvelopeRevert(new Error("boom"))).toBeNull();
  });
});
