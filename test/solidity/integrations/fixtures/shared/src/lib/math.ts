// A helper module that logs when it is imported. The build reads it and never runs it, and the built
// lens no longer imports it, so the log stays empty.
import fs from "node:fs";

import { internal, pure, uint256 } from "@morpho-org/viem-dlc/solidity";

fs.appendFileSync(process.env.IMPORT_LOG ?? "imported.log", "src/lib/math.ts\n");

export const mulDivDown = internal(pure(uint256, uint256, uint256).returns(uint256), (x, y, d) => {
  const lensHelperProduct = uint256(x * y);
  return lensHelperProduct / d;
});
