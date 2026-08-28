# docs

Design documents for this package. The directory is flat; the filename carries everything needed to
place a document without opening it.

## Filename

```
NNNNNN-<kind>-<slug>.md        e.g. 000016-tib-upstash-store.md
```

`NNNNNN` is the `package.json` version the document was written against, each semver component
zero-padded to two digits (`0.0.16` → `000016`, `1.12.3` → `011203`). It is a **baseline**, not a
release: it names the state of the tree the document describes, which is what makes the
`src/foo.ts:42` references inside it resolvable. Those references are therefore never refreshed as
code moves — read them against the baseline.

Sorting the directory sorts by baseline, and files sharing a baseline are unordered relative to one
another. That is intended; these are not sequentially numbered ADRs.

## Kinds

| Prefix | Kind | What it is |
|---|---|---|
| `tib` | Technical Intent Brief | States what must become true and why, ahead of the work. Follows the spine below. |
| `ref` | Reference | Describes something that already exists — a protocol, a format, an invariant. No spine. |

## TIB frontmatter

```yaml
---
kind: tib
version: 0.0.16
landed:
  - 7031079
related:
  - 000016-tib-plural-store-contract.md
---
```

`landed` holds the commit(s) that shipped the work and is **absent until it ships** — that absence
is the only status distinction these documents need. `related` is omitted when empty.

## TIB spine

Fixed order. Omit a section rather than writing an empty one.

```
# TIB — <title>

<lead: the brief in one paragraph>

## Intent          what must become true; the invariant to protect
## Context         the problem, the forcing function
## Design          the mechanism
## Scope & files   what changes, what deliberately doesn't, and in what order
## Verification    how the intent is proven
## Open risks      known-unresolved
## Notes           rationale for the intent and design as stated
## Derivation      the path taken to reach it
```

The split between the last two sections is the part of the format that does work:

- **Notes** justifies the design written above it — why a guard exists, why a constant is what it
  is, what a plausible "simplification" would break. It is what a reader of the *current* design
  needs and what code comments shouldn't carry.
- **Derivation** is everything that exists only because the path was circuitous: alternatives
  declined, work explicitly omitted or deferred, incidental findings surfaced along the way. None
  of it is needed to understand the design. It is there so the same ground isn't re-covered.

If a paragraph explains the design, it is a Note. If it explains why the design isn't something
else, it is Derivation.

## Index

| Document | Landed | |
|---|---|---|
| [Paged lenses, partial results, and dropping RETURN mode](./000012-tib-paged-lenses-partial-results.md) | `07e49df`, `0df02a9` | Lenses report how far they got instead of being bisected into; RETURN-mode exfiltration removed. |
