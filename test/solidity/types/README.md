# Type tests

These files are never run. `pnpm typecheck` compiles them with the repository's tsc (see
`../tsconfig.json`), and a file passes when every `@ts-expect-error` line has an error under it and no
other line has one. They hold what the editor must accept and what it must refuse.

`surface.ts` is also built: `../surface.test.ts` takes its lens through the transform and solc, so what the
editor accepts there is what the transform prints.

Every example here is synthetic. Do not copy a real lens into this directory.
