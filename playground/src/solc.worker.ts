/**
 * Compiles Solidity off the main thread. Kept free of imports so Vite emits a classic worker,
 * which is what `importScripts` needs to pull in emscripten's `soljson.js`.
 *
 * The standard-JSON input mirrors soltag's own `buildSolcInput`, and the artifact mapping mirrors
 * its `compileToArtifacts`, so a lens compiled here is interchangeable with one the build produced.
 */
declare const Module: {
  calledRun?: boolean;
  onRuntimeInitialized?: () => void;
  cwrap: (name: string, returns: string, args: string[]) => (...params: unknown[]) => string;
};

type Request = { id: number; soljsonUrl: string; source: string; runs: number };

type Compile = (input: string, callback: number, context: number) => string;

let compile: Compile | undefined;

async function load(soljsonUrl: string) {
  if (compile) return compile;

  (self as unknown as { importScripts: (url: string) => void }).importScripts(soljsonUrl);
  if (!Module.calledRun) {
    await new Promise<void>((resolve) => {
      Module.onRuntimeInitialized = resolve;
    });
  }

  compile = Module.cwrap("solidity_compile", "string", ["string", "number", "number"]) as Compile;
  return compile;
}

self.onmessage = async ({ data }: MessageEvent<Request>) => {
  try {
    const solc = await load(data.soljsonUrl);
    const input = {
      language: "Solidity",
      sources: { "inline.sol": { content: data.source } },
      settings: {
        optimizer: { enabled: true, runs: data.runs },
        outputSelection: { "*": { "*": ["abi", "evm.bytecode.object", "evm.deployedBytecode.object"] } },
      },
    };

    const output = JSON.parse(solc(JSON.stringify(input), 0, 0));
    const fatal = (output.errors ?? []).filter((e: { severity: string }) => e.severity === "error");
    if (fatal.length) throw new Error(fatal.map((e: { message: string }) => e.message).join("\n"));

    const artifacts: Record<string, unknown> = {};
    for (const contracts of Object.values(output.contracts ?? {})) {
      for (const [name, contract] of Object.entries(contracts as Record<string, never>)) {
        const { abi, evm } = contract as {
          abi: unknown;
          evm: { bytecode: { object: string }; deployedBytecode: { object: string } };
        };
        artifacts[name] = {
          abi,
          bytecode: `0x${evm.bytecode.object}`,
          deployedBytecode: `0x${evm.deployedBytecode.object}`,
        };
      }
    }

    self.postMessage({ id: data.id, artifacts });
  } catch (error) {
    self.postMessage({ id: data.id, error: `${error instanceof Error ? error.message : error}` });
  }
};
