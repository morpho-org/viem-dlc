viem splits a `multicall` into batches by **calldata bytes**. `batchSize` sets the budget and
defaults to 1,024 bytes — about fifteen of these reads. Run the step below as it stands and watch
the request count.

Eight requests for 120 vaults, and on a public endpoint some come back throttled. viem reports a
throttled batch as `status: "failure"` on every element in it, which looks exactly like a revert: you
lose results and can't tell why. That alone is a reason to reach for `batchSize`.

So raise it. With `grief` at 0, the numbers go the right way:

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

The setting you chose on the evidence above returns nothing on the evidence below. Your code didn't
change. One vault got more expensive.

Here's the mechanism. `aggregate3` forwards the gas it has left to each subcall, so every element in
a batch draws on one shared frame. An expensive element takes its share first, and
[EIP-150](https://eips.ethereum.org/EIPS/eip-150) leaves the outer frame only 1/64 of what it
forwarded — enough for a handful of ordinary reads, not for the 119 waiting behind it. When that runs
out, the whole `eth_call` exceeds the node's cap, the node returns an error, and you get nothing
rather than the 119 that were fine.

Three consequences worth stating precisely.

**`allowFailure` doesn't cover this.** It's `true` by default and it does catch a revert in one
element. Gas isn't a revert. A batch that runs out returns nothing for anyone in it, and
`status: "failure"` means only "no result" — a revert, a gas-starved frame, and a throttled request
are the same value.

**The failure isn't local.** Whether this call site works depends on what the *other* elements in its
batch happen to cost, which depends on how many you asked for and in what order they packed. You
can't reason about this file by reading this file.

**A bigger gas ceiling doesn't fix it.** Base's public endpoint grants 600,000,000 gas per
`eth_call`. All 120 vaults together use about 11,000,000 of it, and the full corpus of 600 uses
54,000,000 — under a tenth of the cap. That headroom is exactly what convinces you to raise
`batchSize`. A higher cap moves the element count at which the batch dies; it doesn't make the
failure local, predictable, or recoverable. The failure is the shared frame, and the ceiling is not
the frame.

So the safe `batchSize` isn't a property of this call site. It's a property of the most
gas-intensive multicall anywhere in your application, applied everywhere — and it has to hold for
next quarter's version of that call, over contracts you don't control. You can set it per call site
instead, but then viem can no longer merge concurrent multicalls from different parts of your app
into one request, because they no longer agree on the budget. You've given up the automatic batching
you raised `batchSize` to get.
