// soltag's bundler plugin needs the TS 5 JS compiler API, which typescript@7 (native) lacks.
// Give soltag its own TS 5 instead of resolving its `typescript` peer to the repo's typescript@7.
function readPackage(pkg) {
  if (pkg.name === "soltag") {
    delete pkg.peerDependencies?.typescript;
    delete pkg.peerDependenciesMeta?.typescript;
    pkg.dependencies = { ...pkg.dependencies, typescript: "^5.9.0" };
  }
  return pkg;
}
module.exports = { hooks: { readPackage } };
