/**
 * Checks the claims the playground rests on, in a real browser: that the in-page compiler produces
 * exactly the bytecode the build produced, that a tab's source survives evaluation, and that the
 * gzip-backed `zlib` shim satisfies the contract `compressed-lines-blob.ts` expects of it.
 *
 * Open `/selftest.html`, or drive it headlessly and read the document title.
 */
import { CompressedLinesBlob, createSlot } from "../../src/internal/compressed-lines-blob.js";
import readLensScript from "../tutorials/eth-call/02-readlens.js?raw";
import pristineSolidity from "../tutorials/eth-call/vault-snapshot.sol?raw";

import { compileLens } from "./compile.js";
import { PREBUILT } from "./lens.js";
import { installNodeGlobals } from "./shim/globals.js";
import { createZstdCompress, createZstdDecompress } from "./shim/zlib-gzip.js";
import { evaluateTab } from "./tab.js";

installNodeGlobals();

const lines: string[] = [];
let failures = 0;

function check(name: string, ok: boolean, detail = "") {
  if (!ok) failures += 1;
  lines.push(`${ok ? "pass" : "FAIL"}  ${name}${detail ? `  — ${detail}` : ""}`);
  document.querySelector("#report")!.textContent = lines.join("\n");
}

try {
  check("evaluateTab yields a callable", typeof evaluateTab(readLensScript) === "function");

  try {
    evaluateTab('import { nope } from "nowhere";\nexport default async () => ({ summary: {} });');
    check("unknown import rejected", false, "no error thrown");
  } catch (error) {
    check("unknown import rejected", `${error}`.includes("Cannot import"));
  }

  const built = PREBUILT.VaultSnapshotLens!.with();
  const compiled = (await compileLens("VaultSnapshotLens", pristineSolidity)).with();
  check(
    "browser bytecode matches build",
    compiled.factoryData === built.factoryData,
    `${compiled.factoryData.length} chars`,
  );
  check("counterfactual address matches", compiled.address === built.address, compiled.address);

  // The codec substitution: gzip over CompressionStream standing in for streaming zstd.
  {
    const compressor = createZstdCompress();
    let output = 0;
    let emittedAtChunk = -1;
    compressor.on("data", (c: Uint8Array) => {
      output += c.length;
    });

    const CHUNKS = 4_000;
    const chunk = Buffer.from(`${"payload ".repeat(32)}\n`);
    for (let i = 0; i < CHUNKS; i++) {
      if (!compressor.write(chunk)) await new Promise((r) => compressor.once("drain", r));
      if (output > 0 && emittedAtChunk === -1) emittedAtChunk = i;
    }
    compressor.end();
    await new Promise((r) => compressor.once("end", r));

    // A codec that buffered its input could not emit until every chunk had been written. This is
    // what keeps the blob pipeline's peak memory independent of blob size.
    check(
      "compressor emits before its input ends",
      emittedAtChunk !== -1 && emittedAtChunk < CHUNKS,
      `first output after chunk ${emittedAtChunk} of ${CHUNKS}`,
    );
  }

  {
    const LINES = 20_000;
    const line = (i: number) => JSON.stringify({ i, payload: "x".repeat(200) });
    const slot = createSlot();
    const blob = new CompressedLinesBlob(slot);

    await blob.rewrite(
      () => {},
      (emit) => {
        for (let i = 0; i < LINES; i++) emit(line(i));
      },
    );

    let readBack = 0;
    let intact = true;
    for await (const l of blob.lines()) {
      if (l !== line(readBack)) intact = false;
      readBack += 1;
    }

    const compressedBytes = slot.get().reduce((n, c) => n + c.length, 0);
    check(
      "blob round-trips every line through the shim",
      readBack === LINES && intact,
      `${readBack} lines, ${(compressedBytes / 1024).toFixed(0)} KiB compressed`,
    );
  }

  {
    const corrupt = new CompressedLinesBlob(createSlot(Buffer.from("not a valid codec frame at all")));
    let count = 0;
    let escaped: unknown;
    try {
      for await (const _ of corrupt.lines()) count += 1;
    } catch (error) {
      escaped = error;
    }
    check(
      "corrupt blob reads as empty rather than throwing",
      count === 0 && escaped === undefined,
      escaped ? `escaped: ${escaped} (code ${(escaped as NodeJS.ErrnoException).code})` : `${count} lines`,
    );
  }

  {
    const decompressor = createZstdDecompress();
    let code: unknown;
    decompressor.on("error", (e: NodeJS.ErrnoException) => {
      code = e.code;
    });
    decompressor.end(Buffer.from("garbage"));
    await new Promise((r) => setTimeout(r, 50));
    // `compressed-lines-blob.ts` keys on exactly this to treat a bad blob as empty.
    check("decompress failure carries ERR_ZLIB_ZSTD_FAILED", code === "ERR_ZLIB_ZSTD_FAILED", String(code));
  }
} catch (error) {
  check("selftest completed", false, `${error}\n${error instanceof Error ? error.stack : ""}`);
}

document.title = failures === 0 ? "SELFTEST PASS" : `SELFTEST FAIL (${failures})`;
