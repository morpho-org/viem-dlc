The **plain getLogs** tab asks the endpoint for a widening range and reports where it stops. On the default
public endpoint that's 2,000 blocks. On a commercial one it's often 100,000 — but watch the
milliseconds on the way up, because the range limit isn't the only thing that degrades. A request
that takes 3 seconds at 100,000 blocks isn't a request you want in a loop either.

This is the part that makes hand-rolling it unpleasant. The cap isn't in any header, and it isn't
one number. It varies by provider, by plan, and by how busy the contract was in the window you asked
for — the same 100,000 blocks that answer fine today return a response-size error once the contract
gets popular. A constant that was right when you wrote it decays without any of your code changing.
So any loop you write has to discover the limit at runtime and back off when it finds one.

`logsDivider` is that loop. The **logsDivider** tab runs the same range through it:

- **`maxBlockRange`** is a ceiling, not a promise. A chunk rejected for size, or one that times out,
  is halved and retried. Guessing high costs a round trip; guessing low costs requests. Neither
  costs you the result, which is why this is the one number here you're allowed to be wrong about.
- **`alignTo`** snaps chunk boundaries to fixed multiples of the range rather than starting wherever
  you happened to ask. Two callers asking for overlapping windows then produce the *same* chunks,
  which is what makes the next section possible.
- **`retryCount`** covers the transient failures that a few hundred requests will find.
- **`maxBytes`** drops individual logs too large to be worth carrying, so one pathological event
  doesn't sink the chunk holding it.
- **`onLogsResponse`** hands you each chunk as it lands. The feed at the bottom of this page fills
  while the request is still in flight, because progress on a long read is worth having.

None of that is specific to your provider, and none of it needs configuring per query. It's the
reason this transport goes underneath essentially every `eth_getLogs` call, whoever you're talking
to.
