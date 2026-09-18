The **plain getLogs** tab asks the endpoint for wider and wider ranges and reports where the answers
stop. On the default public endpoint they stop at 2,000 blocks. On a commercial endpoint the limit
is often 100,000, but watch the milliseconds on the way up. The range limit is one of several things
that degrade, and a request that takes 3 seconds at 100,000 blocks does not belong in a loop either.

This is what makes the hand-rolled version unpleasant. The cap is not in any header, and it is not a
single number. It varies by provider, by plan, and by how busy the contract was in the window you
asked for. The same 100,000 blocks that answer today may return a response-size error once the
contract gets popular, so a constant that was right when you wrote it can decay without any change
to your code. Any loop you write has to discover the limit at runtime and back off when it finds one.

`logsDivider` is that loop. The **logsDivider** tab runs the same range through it, with these
settings:

- **`maxBlockRange`** is a ceiling. A chunk that is rejected for size, or that times out, is halved
  and retried. Guess high and you pay a round trip; guess low and you pay extra requests. Either way
  you get the result, so this is the one number on the page you may safely get wrong.
- **`alignTo`** snaps chunk boundaries to fixed multiples of the range instead of starting wherever
  you happened to ask. Two callers asking for overlapping windows then produce the same chunks, and
  the next section depends on that.
- **`retryCount`** handles the transient failures that a few hundred requests are bound to meet.
- **`maxBytes`** drops individual logs that are too large to be worth carrying, so one oversized
  event does not sink the chunk that holds it.
- **`onLogsResponse`** hands you each chunk as it arrives. The feed at the bottom of this page fills
  in while the request is still running, which is welcome on a long read.

None of this is specific to a provider, and none of it needs setting per query. That is why this
transport sits under nearly every `eth_getLogs` call, whoever the endpoint belongs to.
