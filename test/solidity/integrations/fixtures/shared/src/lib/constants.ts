// A constant module that logs when it is imported, like ./math.ts.
import fs from "node:fs";

fs.appendFileSync(process.env.IMPORT_LOG ?? "imported.log", "src/lib/constants.ts\n");

export const WAD = 10n ** 18n;
