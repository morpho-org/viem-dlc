import { readFileSync } from "node:fs";

import { flzCompress } from "../../src/utils/deployless/flz.js";

process.stdout.write(flzCompress(readFileSync(process.argv[2]!, "utf8").trim() as `0x${string}`));
