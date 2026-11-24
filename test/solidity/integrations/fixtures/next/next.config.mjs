import { fileURLToPath } from "node:url";

import { withSolidity } from "@morpho-org/viem-dlc/solidity/next";

export default withSolidity({
  // Next is installed in the repository this project is copied into, five directories up, as it is for
  // an application inside a monorepo.
  turbopack: { root: fileURLToPath(new URL("../../../../..", import.meta.url)) },
});
