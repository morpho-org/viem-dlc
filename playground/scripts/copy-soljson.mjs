import { copyFile, mkdir, stat } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";

// The compiler the browser runs must be the one the build ran, or edited Solidity would produce
// bytecode that differs from the prebuilt lens for reasons the reader cannot see.
const source = createRequire(import.meta.url).resolve("solc/soljson.js");
const target = fileURLToPath(new URL("../public/soljson.js", import.meta.url));

await mkdir(dirname(target), { recursive: true });
await copyFile(source, target);

const { size } = await stat(target);
console.log(`soljson.js → playground/public (${(size / 1024 / 1024).toFixed(1)} MB)`);
