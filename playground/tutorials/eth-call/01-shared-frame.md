viem splits a `multicall` into batches by **calldata bytes**. `batchSize` sets the budget and
defaults to 1,024 bytes, which is about fifteen of these reads. Run the step below as it stands and
watch the request count.

Eight requests for 120 vaults, and on a public endpoint some of them come back throttled. viem
reports a throttled batch as `status: "failure"` on every element in it, which looks exactly like a
revert: you lose the results and cannot tell why. That alone is a reason to raise `batchSize`.

So raise it. With `grief` at 0, the numbers improve:

| `batchSize` | requests | result |
| --- | --- | --- |
| 1,024 | 8 | varies — throttling eats batches |
| 16,384 | 1 | 120 of 120 |
| 65,536 | 1 | 120 of 120 |

One request, every element, half a second. Ship it.

**Now put `grief` back to 2,000,000 and run the same three settings.**

| `batchSize` | requests | result |
| --- | --- | --- |
| 1,024 | 8 | 119 of 120, when throttling allows |
| 16,384 | 1 | **0 of 120** |
| 65,536 | 1 | **0 of 120** |

The setting you chose from the first table returns nothing in the second. Your code did not change.
One vault got more expensive.

The mechanism is this. `aggregate3` forwards the gas it has left to each subcall, so every element
in a batch draws on one shared frame. An expensive element takes its share first, and
[EIP-150](https://eips.ethereum.org/EIPS/eip-150) leaves the outer frame only 1/64 of what it
forwarded, which is enough for a handful of ordinary reads and not for the 119 waiting behind it.
When that runs out, the whole `eth_call` exceeds the node's cap, the node returns an error, and you
get nothing, including the 119 that were fine.

Three consequences follow.

**`allowFailure` does not cover this.** It is `true` by default, and it does catch a revert in one
element. Running out of gas is not a revert. A batch that runs out returns nothing for anyone in it,
and `status: "failure"` means only "no result". A revert, a gas-starved frame and a throttled
request all produce the same value.

**Under automatic batching, the failure is not local.** viem merges concurrent reads from anywhere
in your application into shared batches. Whether this call site works therefore depends on what the
*other* elements in its batch cost, which depends on what else was in flight and how it happened to
pack. You cannot tell whether this file is correct by reading this file.

**A bigger gas ceiling does not fix it.** Base's public endpoint grants 600,000,000 gas per
`eth_call`. All 120 vaults together use about 11,000,000 of it, and the full corpus of 600 uses
54,000,000, under a tenth of the cap. That headroom is what persuades you to raise `batchSize`, and
it protects nothing. A contract that wants to spend your budget can spend all of it in one element,
so a deliberate grief empties any ceiling at once. Under automatic batching the failure stays
non-local and unpredictable at any ceiling. And where you batch by hand, the safe `batchSize` for
each call site is something you find by guess and check, against contracts you do not control, and
then find again on whatever schedule your risk appetite allows: weekly, quarterly, or never, and
hope.

None of this is particular to viem. Multicall3 is where the shared frame lives, and any client built
on it inherits the problem. The next section removes the frame.
