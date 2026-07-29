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

Unlike the storage-engine benchmarks above (in-process, direct calls to
`engine.Engine`), `TestClusterThroughputAndLatency` in
`cluster_load_test.go` drives a real 3-node cluster over real TCP (Raft)
and real HTTP (client API) with many concurrent clients, measuring true
end-to-end latency: network + Raft consensus + storage, not just local
disk I/O. This is the number that matters for "how fast is the actual
distributed system," as opposed to "how fast is the storage engine
underneath it."

### Baseline: no batching

| Workload | Throughput | p50 | p99 |
|---|---|---|---|
| Read-heavy (90% reads) | ~2,100-2,300 ops/sec | <1ms | ~50ms |
| Write-heavy (10% reads) | ~264-297 ops/sec | ~72-76ms | ~90-220ms |

Reads never touch Raft at all (`Get` is local-only — see `server`'s
README on the linearizability tradeoff this implies), so read throughput
was already high. Writes were the bottleneck: every `Put`/`Delete`
serialized through one leader doing an uncommitted `fsync` per operation,
with zero batching anywhere in the write path.

### First attempt: batching that didn't actually help

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

### The fix: `raft.ProposeBatch`

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
improvement** over the original baseline, measured across 6 repeated
runs each. Just as importantly: throughput with 100 concurrent workers is
now comparable to (and often higher than) 20 workers, rather than
plateauing at the same number regardless of load — confirming the fix
addresses the actual bottleneck rather than just moving it.

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
