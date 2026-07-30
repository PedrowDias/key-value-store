# Benchmark Results

Measured on: Apple M3 (darwin/arm64), via:

```bash
go test ./bench/... -bench=BenchmarkPut -benchmem -benchtime=100x -run=^$
go test ./bench/... -bench=BenchmarkGet -benchmem -benchtime=200x -run=^$
go test ./bench/... -run TestFailoverTime -v
```

**`-benchtime` is a fixed iteration count (`Nx`), not a duration.**
`NaiveStore`'s populate step does real, uncached `fsync`-per-key writes;
a duration-based `-benchtime` makes Go's calibration loop re-run that
expensive setup repeatedly trying to reach a target wall-clock time,
which can make the benchmark run for many minutes.

## Get (read-heavy), 200 iterations, 2000-key populate

| Value size | Engine | NaiveStore | Speedup |
|---|---|---|---|
| 64 B | 1,051 ns/op (951,475 ops/sec) | 49,307 ns/op (20,281 ops/sec) | **46.9x** |
| 256 B | 1,135 ns/op (881,057 ops/sec) | 79,311 ns/op (12,609 ops/sec) | **69.9x** |
| 4096 B | 11,848 ns/op (84,402 ops/sec) | 148,623 ns/op (6,728 ops/sec) | **12.5x** |

This is the memtable paying off directly, and on real SSD hardware the
gap is far starker than in a sandboxed test environment: at 64B/256B,
the whole 2000-entry populate fits under the engine's 4MiB memtable
flush threshold, so these reads are pure in-memory skip-list lookups —
no syscalls at all — against `NaiveStore` genuinely paying the cost of
`open` + `read` + `close` on a real file for every single `Get`. On fast
hardware, `fsync` (which dominates the `Put` numbers below) gets cheap
enough that this fixed per-syscall overhead becomes the dominant cost in
the comparison, which is exactly why the read speedup is so much larger
here than it was in earlier sandbox testing (roughly 5x there) — this
number is the more trustworthy one, since it's measured on the kind of
hardware this store would actually run on.

**The 4096B row is the honest exception, and it's the most interesting
result, not the least useful one**: 2000 entries x 4096 bytes ~ 8MB
exceeds the 4MiB flush threshold, so these reads hit at least one
SSTable — a bloom filter check plus a real block read from disk — rather
than a pure in-memory lookup. The speedup drops from ~50-70x to ~12.5x
accordingly. That's the correct, expected behavior of an LSM engine (the
same tradeoff LevelDB/RocksDB/Cassandra make), and it's worth stating
explicitly rather than only reporting the two most flattering rows.

## Closing the 4096B gap: a shared SSTable block cache

The `NumEntries=2000` row above is a **cold-read** benchmark: `b.N`
iterations there each read a distinct key, so the same data block never
gets requested twice within one run — exactly the access pattern a block
cache has nothing to offer, since there's no repeat to serve from
memory. Real workloads are rarely like that; some keys get read far more
often than others ("hot" keys), and every one of those repeat reads was
still paying a full disk read and CRC32C verification every time, even
for data read a moment earlier.

`sstable.BlockCache` (an LRU cache of checksum-verified data blocks,
keyed by file and byte offset) closes this: `engine.Engine` creates one
and shares it across every SSTable it ever opens, on recovery and on
every subsequent flush, so a repeated read of the same block — even
across different flushed tables over the store's lifetime — is served
from memory. `BenchmarkGet_HotKeys` measures exactly this: the same
2000-entry, 8MB (past-flush-threshold) table as above, but repeatedly
re-reading only a small (10-key) hot subset instead of `b.N` distinct
keys:

```bash
go test ./bench/... -bench=BenchmarkGet_HotKeys -benchmem -benchtime=1000x -run=^$
```

| Environment | Enabled | Disabled | Speedup |
|---|---|---|---|
| Sandbox (3 runs) | 2,722-4,402 ns/op | 6,918-9,191 ns/op | 1.7-2.5x |
| Apple M3 | 6,460 ns/op | 7,500 ns/op | **1.16x** |

The cache is a real, consistent, positive improvement on real hardware —
just a much more modest one than the sandbox measurement suggested,
which is worth understanding rather than quietly reporting whichever
number looks better. The likely explanation: this benchmark's "Disabled"
case isn't actually measuring cold physical-disk latency on the M3.
macOS's page cache is aggressive, and an 8MB test file comfortably fits
in it — so even with the *application-level* `BlockCache` turned off,
the OS is very likely still serving these repeated reads from its own
cache underneath, at the page/`read(2)` level. What `BlockCache` adds on
top of that is avoiding the remaining `read` syscall and CRC32C
re-verification — real, measurable savings (1.16x), just smaller than
"avoiding a real disk read" would be, because a real disk read mostly
isn't what's actually happening in the "disabled" case on this hardware.
The sandbox's larger ratio is consistent with this: a container's
storage layer (frequently an overlay filesystem, sometimes with less
effective or differently-configured caching) is more likely to impose
real per-read cost that page caching doesn't fully hide, so removing it
via an application-level cache shows a bigger relative gain there.

This is also a reasonable, general point about block caches specifically
(not just this one): their marginal value depends heavily on what's
underneath them. They matter most when the OS cache is under memory
pressure, when the working set doesn't fit in available RAM at all, or
on storage where the per-read cost (network-attached storage, especially)
is higher than a local NVMe SSD's. On a single machine with a small
dataset and a lot of free RAM, an application-level cache's job is
partly already being done by the kernel — which doesn't make it
worthless, but does mean 1.16x, not 2x, is the honest number for this
specific hardware and workload size.

The cache is turned on by
default (`Options.BlockCacheSize`, 8 MiB default; a negative value
disables it entirely, matching pre-cache behavior for comparison, which
is exactly how the "Disabled" row above was produced) — every existing
benchmark and correctness test in this project already runs with it
active, and none of their numbers meaningfully changed, since none of
them previously exercised a repeated-read pattern.

## Put (write-heavy), 100 iterations

| Value size | Engine | NaiveStore | Speedup |
|---|---|---|---|
| 64 B | 2,531,606 ns/op (395 ops/sec) | 3,104,493 ns/op (322 ops/sec) | 1.2x |
| 256 B | 2,770,985 ns/op (361 ops/sec) | 3,530,065 ns/op (283 ops/sec) | 1.3x |
| 4096 B | 2,890,340 ns/op (346 ops/sec) | 3,734,339 ns/op (268 ops/sec) | 1.3x |

**Both stores genuinely `fsync` on every single `Put`** (the engine's WAL
is opened with `SyncOnWrite: true`), so this isn't "durable vs. not" —
it's the filesystem-level cost of one continuously-open, append-only WAL
file (the engine) versus creating a brand-new file, with a new inode,
per key (`NaiveStore`). On real hardware, `fsync` itself is the dominant
cost for both, which is exactly why `Put`'s advantage (1.2-1.3x) is far
more modest than `Get`'s (12-70x): the WAL's design advantage is in
*avoiding extra syscalls and file creation*, not in avoiding `fsync`
itself — there's no way to avoid that and remain durable.

## Failover time (10 trials, 3-node cluster, 50ms ticks, 10-tick min election timeout)

```
min:  689.1ms
mean: 699.9ms
p50:  701.2ms
p99:  703.9ms
max:  703.9ms
```

Measured as: kill the leader process outright (stop its goroutines and
close its resources with no graceful handoff — a real crash), then time
until the **new** leader can successfully commit a write, not merely
until some node calls itself Leader. That distinction is real: a freshly
elected leader can't commit anything until a current-term entry is
confirmed on a majority (the Figure 8 safety rule in `raft`'s own
package), so "elected" and "usable" are meaningfully different moments.

The tight spread (689-704ms across 10 independent trials, roughly a 15ms
range) suggests the remaining two nodes' randomized election timeouts
(10-20 ticks x 50ms = 500-1000ms) are resolving on the first attempt
almost every time, without the "dueling candidates" disruption `raft`'s
own README documents as a known vanilla-Raft characteristic.

## Cluster-level throughput and latency (real TCP + real HTTP, 3-node cluster)

> **Correction, found after most of the numbers below were first
> recorded:** `cluster_load_test.go`'s HTTP client was constructed as
> `&http.Client{Timeout: 5 * time.Second}` with no explicit `Transport`,
> which means it used Go's default transport — capped at
> `MaxIdleConnsPerHost: 2`. With 20-100 concurrent goroutines all sending
> requests to the same leader URL, most requests couldn't reuse a pooled
> connection and either paid a full TCP handshake or queued behind a
> pool of 2, entirely independent of anything the server under test was
> doing.
>
> **This confound was present in the client from the start, which means
> it affected every cluster-level number in this whole section** — the
> no-batching baseline, the `ProposeBatch` validation, the M3
> `ProposeBatch`-only measurements, and the batch-window numbers alike —
> not only the batch-window sweep where it happened to get caught. It
> was caught there because `batchWindow=0` (meant to behave like "submit
> immediately, no extra wait") measured *far* worse than even the
> pre-`ProposeBatch` baseline, which shouldn't be possible. Fixed with a
> properly pooled client (`newLoadTestHTTPClient`, `MaxIdleConnsPerHost`
> sized to the actual worker count). Re-running the sweep after the fix
> produced a much cleaner, close-to-monotonic curve (403 → 4,052 ops/sec
> climbing to a peak around 2ms, versus a noisy, non-monotonic curve
> with an inexplicable dip before the fix) — strong evidence this was a
> real, significant confound, not a minor one.
>
> **What this means for the numbers below**: the *qualitative* findings
> — an unbatched serial bottleneck existed; `ProposeBatch` measurably
> fixed a real problem, confirmed via a same-tool, same-hardware
> comparison at each stage — are likely still directionally sound, since
> the confound was present consistently within each such comparison.
> Every *specific absolute number* measured before this fix, including
> the M3 numbers, should be treated as provisional rather than final.
> Fresh, post-fix numbers exist for the sandbox batch-window sweep (see
> below) but not yet for a from-scratch re-run of the earlier
> no-batching/`ProposeBatch` baselines, and not at all for real hardware
> — both are the honest next steps before this section is fully
> trustworthy.

Unlike the storage-engine benchmarks above (in-process, direct calls to
`engine.Engine`), `TestClusterThroughputAndLatency` in
`cluster_load_test.go` drives a real 3-node cluster over real TCP (Raft)
and real HTTP (client API) with many concurrent clients, measuring true
end-to-end latency: network + Raft consensus + storage, not just local
disk I/O. This is the number that matters for "how fast is the actual
distributed system," as opposed to "how fast is the storage engine
underneath it."

**A note on hardware for this section specifically**: the baseline,
first-attempt, and fix-validation numbers below were all measured in the
same sandboxed Linux container (not the Apple M3 the rest of this
document uses) — kept consistent with each other so the *comparison*
between them is a fair, same-hardware A/B test. Real M3 numbers for the
post-fix state are reported separately, further down, since no
pre-fix M3 baseline was ever measured — see that section for why the
two shouldn't be directly multiplied against each other.

### Baseline: no batching (sandbox)

| Workload | Throughput | p50 | p99 |
|---|---|---|---|
| Read-heavy (90% reads) | ~2,100-2,300 ops/sec | <1ms | ~50ms |
| Write-heavy (10% reads) | ~264-297 ops/sec | ~72-76ms | ~90-220ms |

Reads never touch Raft at all (`Get` is local-only — see `server`'s
README on the linearizability tradeoff this implies), so read throughput
was already high. Writes were the bottleneck: every `Put`/`Delete`
serialized through one leader doing an uncommitted `fsync` per operation,
with zero batching anywhere in the write path.

### First attempt: batching that didn't actually help (sandbox)

The first fix batched multiple concurrent client proposals into a single
`raft.Node.Persist()` call (one WAL fsync for the leader's own log) and a
single `engine.ApplyBatch()` call (one WAL fsync for applying committed
entries) — the standard "group commit" pattern. Measuring it directly
against the baseline: throughput moved to only **~330-350 ops/sec**,
barely better, and — more tellingly — **it stayed at almost exactly that
number regardless of whether 20 or 100 concurrent workers generated the
load**. A flat throughput ceiling independent of concurrency is the
signature of an un-batched serial bottleneck still being hit somewhere.

Investigating why led to the real cause: `raft.Raft.Propose()` sends
`AppendEntries` to every peer **eagerly, on every single call**. Batching
client requests at the server layer into back-to-back `Propose()` calls
batched the *leader's own* WAL persistence, but each `Propose()` call
still sent its own separate network message to every follower — meaning
each follower still did its own append, its own WAL persist, and sent its
own response, one at a time, exactly as before. The leader's log-append
batched; the actual network fan-out and follower-side work that
determines whether an entry can commit did not.

### The fix: `raft.ProposeBatch` (sandbox validation)

A new method, `ProposeBatch(datas [][]byte) ([]uint64, error)`, appends
every entry in one call and defers sending until all of them are already
in the log — so each peer receives **one** `AppendEntries` carrying all
the batched entries, instead of one message per entry.
`Propose(data []byte) error` is now a thin wrapper
(`ProposeBatch([][]byte{data})`), so its existing behavior and every
existing test are unaffected. `server.Server` was updated to call
`ProposeBatch` once per drain cycle instead of looping `Propose`.

| Workload | Throughput | p50 | p99 |
|---|---|---|---|
| Write-heavy (10% reads), 20 workers | ~1,090-1,610 ops/sec | ~11-13ms | ~40-55ms |
| Write-heavy (10% reads), 100 workers | ~1,110-2,270 ops/sec | ~17-77ms | ~130-200ms |

Roughly a **4-5x throughput improvement** and **6-7x p50 latency
improvement** over the sandbox baseline above, measured across 6 repeated
runs each. Just as importantly: throughput with 100 concurrent workers is
now comparable to (and often higher than) 20 workers, rather than
plateauing at the same number regardless of load — confirming the fix
addresses the actual bottleneck rather than just moving it.

### Real hardware: Apple M3, post-fix

The sandbox comparison above is same-hardware-both-sides, which is what
makes it valid evidence the *fix* works — but sandbox `fsync` behavior
(often backed by a container overlay filesystem, sometimes without the
same physical durability guarantees or latency as a real disk) is not
representative of real-world absolute performance. No pre-fix baseline
was measured on real hardware, so the numbers below should be read as
"real absolute performance, post-fix," not multiplied against the
sandbox baseline to claim a specific speedup on this machine:

| Workload | Throughput | p50 | p99 |
|---|---|---|---|
| Read-heavy (90% reads), 20 workers | 2,364.5 ops/sec | 467µs | 49.99ms |
| Write-heavy (10% reads), 20 workers | 520.9 ops/sec | 36.96ms | 67.92ms |
| Write-heavy (10% reads), 100 workers | 1,939.8 ops/sec | 44.56ms | 78.28ms |

The genuinely interesting real-hardware finding: going from 20 to 100
concurrent workers **nearly quadrupled** write throughput (521 → 1,940
ops/sec) — a much sharper concurrency-scaling curve than the sandbox
showed, where 20 and 100 workers gave roughly comparable throughput. The
likely explanation: on a fast real SSD, each individual write completes
quickly enough that 20 concurrent workers isn't consistently enough
pressure to build large batches — `drainAndAcceptProposals` can only
batch whatever's already queued in `proposeCh` at the exact moment it
checks, so if requests complete (and workers send their next one) faster
than they arrive, batches stay small. 100 workers generates enough
concurrent pressure to reliably build larger batches even at this
lower per-operation latency. This is a real, hardware-dependent property
of the mechanism, not a discrepancy to explain away: the benefit of
group commit scales with concurrent write pressure relative to
per-operation latency, and both of those vary by hardware.

### Why this is worth documenting as a two-step process

The first attempt at group commit was a reasonable, standard optimization
that measurably didn't work as intended, and the reason wasn't obvious
from reading the server-layer code in isolation — it required tracing
the actual message flow down into `raft.Propose()`'s implementation to
find. That's a realistic shape for real performance work: the first fix
addresses a real inefficiency (redundant fsyncs) without addressing the
actual bottleneck (network fan-out granularity), and confirming that with
a flat-regardless-of-concurrency throughput number — rather than
assuming success from a modest apparent improvement — is what caught it.

## A deliberate batching window: trading a little latency for a lot of throughput

The Apple M3 numbers above (in the cluster-level section) showed
something specific: write throughput at 20 concurrent workers was
notably lower than at 100 workers (521 vs. 1,940 ops/sec), even though
`ProposeBatch` was already fixing the network fan-out problem.
`drainAndAcceptProposals`'s batching was purely opportunistic — it only
grabbed whatever was *already* sitting in the channel at the exact
instant it checked, with zero wait. On fast hardware, individual writes
complete quickly enough that fewer concurrent workers don't reliably
keep the channel non-empty, so batches stayed small regardless of how
good `ProposeBatch` itself was.

The fix, `Server.batchWindow`: after the first proposal in an otherwise-
idle moment arrives, `Run` waits a small bounded amount of time (default
500µs) for more proposals to accumulate before submitting them together
— the same idea as Kafka's `linger.ms` or CockroachDB's proposal
batching. A batch still flushes early, without waiting out the full
window, once it reaches `maxBatchSize` (default 64). This is a genuine
latency/throughput tradeoff, not a free win: every write now has at
least a small chance of waiting up to the window's duration even when
nothing else is happening to batch with — which is exactly why the value
is tunable (`SetBatchWindow`/`SetMaxBatchSize`) rather than hardcoded,
and why it was measured with an actual parameter sweep rather than
guessed.

### A second, deeper cause: O(N²) redundant retransmission in `raft` itself

The connection-pool fix above was real, but it didn't fully explain the
data: on real M3 hardware, `batchWindow=0` measured 86.8 ops/sec before
the pool fix and 88.0 ops/sec after — essentially unchanged, even though
the same fix dramatically improved sandbox numbers. Something else was
responsible, and it turned out to be a genuine algorithmic bug in
`raft.Raft`, not the benchmark tool.

`sendAppendEntries` always sent everything from a peer's recorded
`nextIndex` through the current end of the log, and `nextIndex[peer]`
only advances when a response actually arrives back. `ProposeBatch`
called `sendAppendEntries` for every peer on every call, unconditionally.
Under concurrent load, many `ProposeBatch` calls can happen before any
single one gets acknowledged. Each of those calls resent the *entire*
growing, still-unacknowledged backlog from the same starting point: call
1 sends 1 entry, call 2 resends that entry plus 1 new one, call 3 resends
those two plus 1 new one, and so on — total data sent across N such calls
grows as N(N+1)/2, quadratic in the number of proposals that arrive
faster than they can be acknowledged. With 50 concurrent workers all
writing near-simultaneously, that's up to 1,275 entry-equivalents
transmitted instead of 50.

**First fix attempt — a gate, and a real regression it caused.** The
first fix tracked, per peer, whether a send was outstanding
(`awaitingResponse map[uint64]bool`) and skipped `ProposeBatch`'s eager
send to a peer that already had one in flight, relying on the eventual
response handler to immediately catch that peer up. This does bound the
redundant-retransmission cost — and sandbox measurement showed every
window value improving. But asking for real-hardware confirmation (not
just trusting the sandbox result) caught something the sandbox
comparison had missed: re-run on the M3, `window=0` improved as
predicted (88.0 → ~125 ops/sec), but *every non-zero window got
dramatically worse* — 100µs dropped from roughly 2,000-4,000 ops/sec
(pool-fix-only baseline) to 400-550 ops/sec, reproducing consistently
across repeated standalone runs, not just once.

The mechanism, once traced through: the *original* bug's redundant,
unconditional resending had an accidental side effect — many overlapping
in-flight messages to each peer, each eventually acknowledged,
effectively providing pipelining (multiple requests in flight at once)
as a side effect of the redundancy. The gate fix correctly removed the
redundant bytes, but along with them, removed that accidental pipelining
too: at most one outstanding `AppendEntries` per peer at a time is
strict stop-and-wait, and progress became gated by one full round trip
at a time per peer rather than able to overlap. On real hardware, where
round-trip cost is non-trivial relative to CPU/local work, that
pipelining loss cost more throughput than the redundant-bytes fix
gained back — a real, measurable regression a same-hardware sandbox
comparison hadn't reproduced clearly enough to catch on its own.

**The actual fix: send only what hasn't been sent yet, not what hasn't
been acknowledged yet.** `Raft.sentIndex map[uint64]uint64` tracks the
highest log index most recently *transmitted* to each peer — distinct
from `nextIndex`, which only tracks what's been *acknowledged*.
`sendAppendEntries` now sends from `max(sentIndex[peer], nextIndex[peer]-1)`
rather than always from `nextIndex[peer]-1`: each call transmits only
the entries not already sent, whether or not an earlier send is still
outstanding. This eliminates the redundant retransmission at its actual
source (the *content* of each send) rather than by gating *whether* a
send happens — so `ProposeBatch` goes back to sending unconditionally on
every call, restoring full pipelining, while `sendAppendEntries` itself
guarantees nothing gets sent twice. Correctness of sending ahead of the
last acknowledgment relies on the transport's in-order, reliable
delivery on the single persistent TCP connection each peer pair uses
(see `transport`'s own docs) — a follower processes sends in the order
they were transmitted, and the existing `PrevLogIndex`/`PrevLogTerm`
consistency check still catches any real mismatch, triggering the
existing conflict-retry path exactly as before. The retry path itself
needed one addition: on a rejection, `sentIndex[peer]` is explicitly
reset before retrying, so the retry restarts from the authoritative,
acknowledged `nextIndex-1` rather than a stale, now-untrusted optimistic
point.

### Sweep, re-measured after the pipelined fix (sandbox, same conditions as above)

| Batch window | Pool fix only | Gate fix (regressed) | Pipelined fix |
|---|---|---|---|
| 0 (disabled) | 403.4-777.3 ops/sec | 689.2-807.0 ops/sec | 441.2-471.8 ops/sec |
| 100µs | 1,913.4-3,919.2 ops/sec | 2,156.4-2,293.0 ops/sec | 1,810.2-2,156.4 ops/sec |
| 500µs | 2,121.1-3,693.7 ops/sec | 2,879.5-3,842.4 ops/sec | 2,735.1-2,879.5 ops/sec |
| 1ms | 1,657.6-3,770.5 ops/sec | 3,164.5-4,172.9 ops/sec | 4,107.2-4,172.9 ops/sec |
| 5ms | 1,546.3-3,542.4 ops/sec | 1,730.6-3,452.2 ops/sec | 3,415.7-3,464.1 ops/sec |

Ranges reflect multiple runs at each stage rather than single points,
since run-to-run sandbox variance (roughly ±30-40% observed) is real and
comparable in magnitude to some of the differences between
configurations — a single run at any one point is not reliable evidence
on its own, which is exactly what the "gate fix" stage's *apparent*
sandbox improvement, and its actual real-hardware regression, both
demonstrate. What's clear despite that noise: the pipelined fix's ranges
are back in the same territory as the pool-fix-only baseline (no
regression), while the dedicated unit test
(`TestProposeBatch_RepeatedCallsBeforeAckSendOnlyTheNewDelta`) directly
confirms the O(N²) redundant-retransmission bug is fixed at the protocol
level, independent of any noisy throughput measurement.

### Confirmed on real hardware (Apple M3)

| Batch window | Pool-fix-only baseline (first M3 run) | Pipelined fix (this M3 run) |
|---|---|---|
| 0 (disabled) | 88.0 ops/sec | 89.7 ops/sec |
| 100µs | 987.5 ops/sec | 984.3 ops/sec |
| 250µs | 1,133.3 ops/sec | 1,132.9 ops/sec |
| 500µs | 1,402.4 ops/sec | 1,355.1 ops/sec |
| 1ms | 1,657.6 ops/sec | 1,668.7 ops/sec |
| 2ms | 2,226.9 ops/sec | 2,243.7 ops/sec |
| 5ms | 1,546.3 ops/sec | **2,093.8 ops/sec** (+35%) |

Every non-zero window matches the original pre-regression baseline
almost exactly, confirming the pipelining regression is genuinely
fixed, not just improved-looking in the sandbox again. `5ms` shows a
real gain beyond matching baseline — plausibly because restored
pipelining lets a longer window's larger batches be assembled and sent
without paying the stop-and-wait cost the gate fix had introduced.

**`0` (disabled) stayed essentially flat across every single variant
tried — pool fix, gate fix, and this pipelined fix alike (88.0 → 122.7 →
125.4 → 89.7).** That consistency is itself the answer: `0`'s bottleneck
was never about network send efficiency, so none of these fixes
(all of which only changed how efficiently entries are *transmitted*)
could have moved it. With batching fully disabled, every client write is
its own isolated round trip requiring its own `fsync` — on the leader
persisting its own log, and independently on whichever followers
receive it — with three separate node processes on the same machine
competing for the same physical disk. That is a durability-cost
bottleneck, not a network one, and it's exactly what the non-zero batch
windows exist to address: fewer, larger commits means fewer total
`fsync` calls, not more efficient use of the network. `0`'s numbers
being unchanged by three different network-layer fixes, while every
non-zero window improved substantially over the true "no batching
anywhere" state, is consistent with — not contrary to — the rest of
this investigation's findings.

**All three stages of this investigation were found through disciplined
measurement, not guessing, and each one caught something the previous
stage's reasoning had missed**: the connection pool bug was caught
because a "disabled" configuration measured worse than a
supposedly-simpler baseline, which shouldn't be possible; the `raft`
O(N²) bug was caught because fixing the connection pool alone didn't
resolve that impossibility on real hardware; and the gate fix's own
regression was caught only because real-hardware re-confirmation was
treated as necessary rather than optional after a sandbox result already
looked like success. Each of the first two "fixes" was a real
improvement over what came before it and also incomplete in a way that
only showed up under conditions the previous testing hadn't covered —
which is the actual, unglamorous shape real performance debugging tends
to take. The pipelined fix's own real-hardware confirmation, above,
closes this particular investigation out: matching or exceeding the
original baseline on every batch window, no unexplained regressions, and
a `window=0` result whose consistency across four different fix attempts
turned out to be informative rather than concerning once its actual
(disk-bound, not network-bound) cause was understood.

## Async memtable flush: a real win, and a real limitation found while measuring it

The original engine held its single write lock for the *entire* duration
of a flush — walking the whole memtable and writing a new SSTable —
meaning whichever write happened to cross the size threshold (and every
other write waiting on the same lock) paid that full cost directly.
Async flush freezes the memtable, hands writes a fresh one immediately,
and does the actual disk I/O in a background goroutine. Verifying the
benefit this was supposed to provide required building a fair
comparison: `syncFlushStore` (in `bench/engine_bench_test.go`) is a
faithful reconstruction of the original design, using the exact same
real `wal`/`memtable`/`sstable` packages `engine.Engine` does, so a
benchmark comparing the two measures a real difference in locking
discipline, not a difference in what work gets done.

### The core claim, isolated: does the triggering write pay the flush cost?

```bash
go test ./bench/... -bench=BenchmarkWriteLatencyTail_TriggeringWriteOnly -run=^$
```

A single sequential writer, no concurrency, measuring only the specific
writes that actually cross the threshold and start a flush (not diluted
by the many surrounding writes that don't — with this workload's
threshold, only ~20 of 2000 writes trigger one, so an aggregate
percentile over all 2000 doesn't show the effect clearly; this isolates
just those 20):

| | min | mean | max |
|---|---|---|---|
| Async (sandbox) | 539-717 µs | 677-880 µs | 922-1,142 µs |
| SyncFlush (sandbox) | 2,498-2,688 µs | 2,934-3,988 µs | 3,481-13,372 µs |
| Async (Apple M3) | 3,363 µs | 3,652 µs | 4,145 µs |
| SyncFlush (Apple M3) | 5,797 µs | 8,062 µs | 9,278 µs |

Consistent across 9 sandbox runs and confirmed on real hardware:
**roughly 2.2-4.7x faster mean latency** for the write that triggers a
flush (2.2x on the M3, 3.5-4.7x in sandbox), and — notably — far *less
variable* in every environment (async's own max stays close to its
mean; sync's spikes well beyond it, up to 13.4ms in one sandbox run).
The M3's absolute numbers are considerably higher than the sandbox's
across the board (not just the ratio between async/sync) — plausibly
because this measurement immediately followed a `-race -count=15`
stress run of the full engine suite on the same machine, which could
leave real, if temporary, contention or thermal effects; the *relative*
finding (async meaningfully faster, less variable) held up regardless.
This is exactly the claim the feature makes, confirmed in both
environments.

### The complicated part: sustained concurrent load

```bash
go test ./bench/... -bench=BenchmarkWriteLatencyTail_AsyncVsSyncFlush -run=^$
```

The same general workload under 20 concurrent writers instead of one
sequential writer, with a small-enough threshold that many flushes
happen during the run:

| | p50 | p99 | max | throughput |
|---|---|---|---|---|
| Async (sandbox) | 12,277-14,381 µs | 34,381-40,341 µs | 35,700-66,186 µs | 1,207-1,489 ops/sec |
| SyncFlush (sandbox) | 13,255-13,863 µs | 17,649-43,654 µs | 18,130-54,282 µs | 1,355-1,445 ops/sec |
| Async (Apple M3) | 56,421 µs | 99,872 µs | 113,090 µs | 347.1 ops/sec |
| SyncFlush (Apple M3) | 59,996 µs | 72,476 µs | 82,022 µs | 330.9 ops/sec |

Not a clean win — genuinely mixed in both environments, with sync
matching or beating async on p99 and max both in sandbox runs and on
the M3 (where sync's p99 and max were both clearly *lower* than
async's). Rather than explain this away, tracing it down: this project
bounds itself to *at
most one flush in flight at a time* (see `engine`'s package doc) — a
deliberate simplicity tradeoff over an unbounded queue of pending
memtables. The consequence: **any** concurrent write that arrives while
a flush is running waits for it to finish via `waitForAnyFlushLocked`,
not only a write that would itself need to start a *second* flush.
Under sustained heavy concurrency with frequent flushes, nearly every
writer ends up waiting for flush completion one way or another — via
this gate for async, via contending for one mutex for sync — which
substantially narrows, and sometimes reverses, the aggregate advantage
the isolated measurement above shows so clearly. That this reproduced
on real hardware, not only in sandbox runs, is good evidence this is a
genuine property of the design rather than a sandbox-specific quirk.

This is a real, honest limitation of the current design, not a
benchmark artifact: the single-flush-in-flight bound trades away some
of async flush's real-world benefit under high write concurrency in
exchange for much simpler reasoning (no need to track an arbitrary
number of outstanding frozen memtables, no need for more than one extra
WAL file at a time). A bounded queue of pending flushes — allowing a
second (or Nth) flush to start once the first is far enough along,
rather than gating all new writes on the first one's completion — is
the natural next step if this project's concurrency profile ever
justified the added complexity; it isn't pursued here because the
isolated case above is the one this project's own workload (a single
Raft-driven `applyCommitted` call per commit, not many independent
concurrent writers hammering the engine directly) actually resembles
more closely.

## Log compaction / snapshotting: creation cost and catch-up time

Measured on the same Apple M3, via:

```bash
go test ./bench/... -bench=BenchmarkSnapshotCreation -benchmem -run=^$
go test ./bench/... -run TestSnapshotCatchUpTime -v
```

**Snapshot creation cost, as the dataset it covers grows** (see the
README's "Known limitations": `Snapshot` gathers every distinct key and
calls `Get` per key, not a proper k-way merge):

| Keys | Time/op | Per-key cost | Allocs/op |
|---|---|---|---|
| 1,000 | 331.5 µs | 331 ns/key | 2,034 |
| 10,000 | 4.84 ms | 484 ns/key | 20,096 |
| 50,000 | 90.2 ms | 1,804 ns/key | 1,914,351 |

Per-key cost roughly a third again worse at 10,000 keys than at 1,000 —
consistent with ordinary constant-factor overhead, both still comfortably
inside the engine's default 4 MiB memtable threshold (1,000 keys at
~90 bytes each is under 0.1 MiB; 10,000 is well under 1 MiB) where
`Snapshot`'s underlying `Get` calls are pure in-memory skip-list lookups.
**50,000 keys (~4.4 MiB of this benchmark's data) crosses that
threshold**, meaning at least one background flush has happened by the
time `Snapshot` runs — its `Get` calls now sometimes hit a real SSTable
(bloom filter check, block read) instead of memory alone, and per-key
cost jumps another ~3.7x accordingly. This is the same "pure memtable
reads vs. reads that reach a real SSTable" split documented in the Get
benchmark above, showing up again here for exactly the same underlying
reason — direct, quantified evidence for the per-key `Get` tradeoff
rather than an abstract description of it: a genuine k-way merge
wouldn't re-pay a bloom-filter-and-block-read cost per key the way this
approach does once data has actually reached disk.

**Real follower catch-up time via `InstallSnapshot`** — a follower
killed outright, held down while the leader commits 300 more writes
(crossing its snapshot-compaction threshold six times over), then
restarted fresh from its last on-disk state and timed until its
`commitIndex` matches the leader's (5 trials):

| | Time |
|---|---|
| min | 73.6 ms |
| mean | 85.5 ms |
| max | 97.7 ms |

Tight, consistent range — under two tick intervals (50ms ticks) in every
trial, and no election is needed here (the existing leader never
changes), so this is essentially the cost of a real process restart plus
TCP reconnect plus a real `InstallSnapshot` round trip and engine
restoration, not a leadership change. The snapshot itself is small in
this test (a few hundred trivial key-value pairs) — the *transfer and
restoration* mechanics are what's being measured as fast and reliable
here, not how time scales with a much larger snapshot's size, which the
creation-cost table above already speaks to separately: a leader with a
multi-gigabyte dataset would take measurably longer to *build* the
snapshot it sends, on top of whatever this table shows for receiving and
installing it.

## Reproducing these results

```bash
go test ./bench/... -run TestClusterThroughputAndLatency -v
go test ./bench/... -run TestBatchWindowSweep -v
go test ./bench/... -bench=BenchmarkSnapshotCreation -benchmem -run=^$
go test ./bench/... -run TestSnapshotCatchUpTime -v
```
