// A Node script, run through the import hook. The line it prints is how the outer test reads the
// bytecode the hook served.
import { QuoteLens } from "./src/lens.ts";

console.log(JSON.stringify({ bytecode: QuoteLens.bytecode }));
