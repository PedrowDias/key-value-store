# Distributed Key-Value Store

A distributed, replicated key-value store built from scratch in Go: a
write-ahead log, an LSM storage engine, a Raft consensus implementation,
a custom TCP transport, and an HTTP-served cluster binary. No external
databases, no consensus libraries — every layer, including the on-disk
format and the network protocol, is implemented here.

```
client (HTTP)
     |
     v
 server.Server  --+  single event loop drives one raft.Node
     |            |  (Tick / Step / Propose / Persist)
     v            |
 raft.Node  <------+  leader election, log replication,
     |                crash-safe persistence
     v
 transport.Transport   TCP between cluster nodes
     |
     v
 engine.Engine   Put/Get/Delete
     |
     +-- wal.WAL           durability (fsync before acknowledging a write)
     +-- memtable.Memtable  in-memory skip list (recent writes)
     +-- sstable.*          flushed, sorted, immutable on-disk files
```

## Contents

- [Getting started](#getting-started)
- [Architecture](#architecture)
- [Design decisions worth reading](#design-decisions-worth-reading)
- [Real bugs found during development](#real-bugs-found-during-development)
- [Benchmark results](#benchmark-results)
- [Testing philosophy](#testing-philosophy)
- [Known limitations / possible future work](#known-limitations--possible-future-work)
- [Project layout](#project-layout)

## Getting started

```bash
go build -o kvstore ./cmd/kvstore

./kvstore -id=1 -raft-addr=127.0.0.1:7001 -http-addr=127.0.0.1:8001 \
  -peers="2=127.0.0.1:7002,3=127.0.0.1:7003" -data-dir=./data/node1 &

./kvstore -id=2 -raft-addr=127.0.0.1:7002 -http-addr=127.0.0.1:8002 \
  -peers="1=127.0.0.1:7001,3=127.0.0.1:7003" -data-dir=./data/node2 &

./kvstore -id=3 -raft-addr=127.0.0.1:7003 -http-addr=127.0.0.1:8003 \
  -peers="1=127.0.0.1:7001,2=127.0.0.1:7002" -data-dir=./data/node3 &

curl -X PUT http://127.0.0.1:8001/kv/hello --data-binary "world"
curl http://127.0.0.1:8002/kv/hello   # replicated to a follower
```

| Method | Path | Behavior |
|---|---|---|
| `PUT` | `/kv/{key}` | Body is the value. Replicates via Raft; `204` once committed. |
| `GET` | `/kv/{key}` | Reads local state (not linearizable — see below). `200` + body, or `404`. |
| `DELETE` | `/kv/{key}` | Same commit semantics as `PUT`. |
| `GET` | `/status` | JSON: `id`, `term`, `role`, `leader`, `commit_index`, `last_log_index`. |

Writing to a non-leader returns `503` with an `X-Raft-Leader-Id` header.

Run the test suite (each package also documents itself via a `godoc`
package comment — `go doc ./engine`, `go doc ./raft`, etc.):

```bash
go build ./...
go vet ./...
go test ./... -race -short
```

## Architecture

| Package | Responsibility |
|---|---|
| `wal` | Write-ahead log: per-record CRC32C framing, crash-mid-write recovery, rotation. |
| `memtable` | In-memory skip-list write buffer with snapshot-isolated iteration. |
| `sstable` | Immutable on-disk sorted tables: block index + Bloom filter for point lookups. |
| `engine` | Wires `wal` + `memtable` + `sstable` into `Put`/`Get`/`Delete`/`ApplyBatch`, with auto-flush and crash recovery. |
| `raft` | Leader election, log replication, crash-safe persistence (`Ready`/`Advance`), batched proposals. |
| `transport` | Custom binary wire protocol over TCP connecting cluster nodes. |
| `server` | The replicated state machine: drives one `raft.Node`, applies committed entries to one `engine.Engine`, serves the HTTP API. |
| `cmd/kvstore` | The runnable binary: flags, wiring, graceful shutdown. |
| `bench` | Benchmarks vs. a naive baseline, failover timing, a real cluster-level load test. |

## Design decisions worth reading

**Skip list for the memtable, not a balanced tree.** Simpler to implement
correctly under concurrent access, with the same expected O(log n), and
its layered structure gives cheap ordered iteration for flushing to an
SSTable — the actual bottleneck (disk I/O) doesn't care about the
in-memory structure's exact constant factor.

**Point lookups go bloom filter -> block index -> block scan.** A Bloom
filter rules out "definitely not present" without touching disk; a
binary search over the block index finds the right block; only then is
one block actually read and linearly scanned. This is the standard
LSM-tree read path (LevelDB, RocksDB, Cassandra all do this), and it's
directly visible in the benchmark numbers below — reads that stay in the
Bloom-filter-covered path are 50-70x faster than a naive per-key file
read.

**A shared block cache, added after the benchmarks found where it was
missing.** The Bloom-filter path above only helps a read stay *out* of
an SSTable; once data exceeds the memtable and a read genuinely needs
one, the original implementation paid a full disk read and CRC32C
verification on every single call — even for a key read a moment
earlier. `sstable.BlockCache` is a size-bounded LRU cache of
already-verified blocks, shared across every SSTable an `engine.Engine`
ever opens (not just within one file), so a repeated read of the same
block is a map lookup instead. Confirmed as a real, positive improvement
on real hardware (Apple M3: 1.16x for repeated hot-key reads) — smaller
than a sandbox measurement initially suggested (1.7-2.5x), and the gap
itself turned out to be informative: a fast machine's OS page cache is
likely already absorbing most of a "cold" read's cost for a small
dataset, so an application-level cache's marginal value depends heavily
on what's already underneath it. See
[`bench/BENCHMARKS.md`](bench/BENCHMARKS.md) for the benchmark that's
actually designed to show this (the earlier cold-read benchmarks
couldn't, since they never request the same block twice).

**Raft's `Ready`/`Advance` contract enforces a real safety invariant.**
`Ready()` returns unpersisted state; the caller MUST persist it before
sending any messages, before calling `Advance()`. Getting this ordering
wrong (sending a vote before it's durable) is exactly the kind of bug
that only shows up after a crash at the wrong moment — encoding the
ordering into the API, rather than trusting every caller to remember it,
is the point.

**Group commit took four rounds to actually get right, and the last two
found real bugs — one in the benchmark tool, one genuinely in `raft`
itself, and fixing the second one properly required a redesign, not a
patch.** Round 1 (batch a server's pending writes into one `engine` WAL
fsync) measured almost no improvement — an unbatched bottleneck existed
elsewhere. Round 2, `raft.ProposeBatch`, fixed `raft.Propose()`'s
eager, per-call `AppendEntries` send, which had been undoing the
batching benefit. Round 3 attempted a further fix — gating a peer's
eager send while one was already outstanding — which sandbox
measurement showed improving every configuration; a follow-up
real-hardware re-check (treated as necessary, not optional, precisely
because the earlier `ProposeBatch` investigation had already shown
sandbox results alone can mislead) caught something the sandbox
comparison had missed entirely: on real hardware, every non-zero batch
window got *dramatically worse*, not better. The mechanism: the
original bug's redundant, unconditional resending had accidentally
provided pipelining (many overlapping in-flight messages) as a side
effect of the redundancy; gating removed the redundant bytes but also
removed that accidental pipelining, forcing strict stop-and-wait
replication. Round 4, the actual fix: track what's been *sent* to each
peer, not just what's been *acknowledged*, and always transmit only the
delta — eliminating the redundant retransmission at its source (each
send's content) rather than by gating whether a send happens, so full
pipelining is preserved. Verified with a dedicated unit test asserting
the exact byte-level property (three proposals before any acknowledgment
transmit exactly 3 entry-equivalents, not 6), not just a throughput
number. Full account of all four rounds, including the regression that
round 3 caused and how it was caught, is in
[`bench/BENCHMARKS.md`](bench/BENCHMARKS.md) — the process, including
two rounds that were each real improvements and each incomplete in ways
that only showed up under conditions the previous testing hadn't
covered, is the more interesting and more honest part of the story than
any single number.

**Reads don't go through Raft.** `Get` reads local state directly —
fast and available even mid-election, but only eventually consistent
(a follower, or even a leader that just lost leadership, can serve a
stale value). True linearizable reads need a `ReadIndex` round
(confirming current leadership before serving), which isn't implemented.
This is what most Raft-backed stores default to for reads that don't
need strict consistency, and it's a deliberate, documented tradeoff
rather than an oversight.

## Real bugs found during development

Finding and fixing these — not just writing code that compiled — is most
of what this project actually demonstrates.

- **Bloom filter false-positive rate 2.5x the target**, caused by
  applying the same weak hash function twice instead of properly
  decorrelating two hash values. Caught by a test asserting the FP rate
  stayed within a reasonable bound; fixed with `splitmix64`-style bit
  mixing.
- **An integer-overflow-adjacent decoder vulnerability** in the wire
  protocol: an untrusted entry count from the network was used directly
  to size a slice allocation (`make([]LogEntry, count)`) before any
  bounds check against the actual bytes available — a malformed or
  malicious message could trigger an out-of-memory crash. Fixed by
  bounding the count against remaining buffer size before allocating.
- **A real concurrency bug caught on a completely different machine
  than it was written on**: `Send` wrote a frame as two unsynchronized
  `Write` calls (length prefix, then payload). Concurrent `Send` calls to
  the same peer could interleave their writes, corrupting frame
  boundaries on the wire — intermittent and machine-dependent, exactly
  the profile of a real race rather than a flaky test. Fixed with a
  per-connection mutex held across the whole frame write.
- **A data race on `Status()`**, caught immediately by `go test -race`
  on the very first multi-node integration test: a public method calling
  straight into a single-goroutine-owned `raft.Node` from a different
  goroutine. Fixed with a mutex-protected cached copy, refreshed only
  from the one goroutine that's allowed to touch the node directly.
- **A subtle Raft correctness pitfall**: if leadership changes between a
  node proposing a command and it committing, a *different* command can
  land at the log index the original proposal expected — without an
  explicit check, a client could be told its write succeeded when
  someone else's actually did. Every pending proposal now compares its
  own bytes against what actually committed at that index before
  reporting success.
- **Group commit took four rounds to actually get right**: the first
  version didn't work, a follow-up fix (a tunable batching window)
  produced numbers that didn't make sense, leading to a benchmark-tool
  connection-pool bug and a genuine O(N²) redundant-retransmission bug
  in `raft` itself. The first fix for *that* bug — gating a peer's
  eager send while one was outstanding — looked like a clean win in
  the sandbox, but a real-hardware re-check caught a regression the
  sandbox comparison had missed: the fix had accidentally traded away
  pipelining the original bug's redundancy had been providing as a
  side effect. The actual fix tracks what's been *sent* per peer, not
  just what's been *acknowledged*, eliminating the redundant bytes
  without giving up pipelining. None of this would have been caught
  without insisting on real-hardware confirmation even after a
  sandbox result already looked like success — see the design section
  above and `bench/BENCHMARKS.md` for the full account.

## Benchmark results

Full methodology and every number in
[`bench/BENCHMARKS.md`](bench/BENCHMARKS.md); summary:

| Measurement | Result |
|---|---|
| Storage engine reads vs. a naive (but genuinely durable) baseline | up to **70x** faster |
| Block cache: repeated ("hot key") reads once data exceeds the memtable | **1.16x** faster, confirmed on Apple M3 (sandbox measured 1.7-2.5x — see [`bench/BENCHMARKS.md`](bench/BENCHMARKS.md) for why real hardware showed less, and why that's not a red flag) |
| Storage engine writes vs. the same baseline | **1.2-1.3x** faster (both durably `fsync`; this isolates filesystem overhead, not durability) |
| Real cluster write throughput, sandbox: no batching -> `ProposeBatch` | ~280 -> ~1,300-1,600 ops/sec (same hardware) |
| Real cluster write throughput, `ProposeBatch` + batch window, confirmed on Apple M3 | ~90-2,240 ops/sec depending on window size, matching or exceeding the pre-regression baseline on every window tested — full four-round investigation (including a real regression found and fixed) in [`bench/BENCHMARKS.md`](bench/BENCHMARKS.md) |
| Leader failover time (kill -> new leader committing again) | **~700ms**, p99 ~704ms, across 10 trials |

The naive baseline (`bench.NaiveStore`) is deliberately *not* a strawman:
one file per key, a real `fsync` before every write returns — the same
durability guarantee the engine makes, via a different access pattern.
The comparison isolates *how* durability is achieved, not *whether* it
is.

## Testing philosophy

100% test coverage on 7 of 9 packages (98%+ on the remaining two —
`sstable` and `transport` — with the gaps being genuine OS-level
untriggerable branches, documented as such rather than hidden). This
isn't a vanity number: it's reached via small interfaces and
package-level function-variable seams (`walFile`, `sstableWriter`,
`raftNode`, `fileHandle`, and similar) that let tests inject precise
failures — a disk write that fails on the third call, a peer that never
acknowledges, a WAL that fails to fsync — deterministically and
portably, without OS-specific tricks. Every package also has real,
non-mocked integration tests: multi-node clusters over real TCP, real
crash-and-recover trials, a real HTTP client hammering a real running
cluster.

## Known limitations / possible future work

- **No pre-vote extension**: a partitioned node's inflated term can
  briefly disrupt a healthy leader on rejoin (safe, but slower to
  restabilize than necessary).
- **No log compaction / snapshotting**: the Raft log grows unboundedly;
  a new or long-partitioned node replays the whole log rather than
  installing a snapshot.
- **No `ReadIndex`**: reads are eventually consistent, not linearizable
  (see above).
- **No async memtable flush**: a flush currently blocks the writer that
  triggers it.

## Project layout

```
wal/          write-ahead log
memtable/     in-memory skip-list write buffer
sstable/      on-disk sorted tables (block index + Bloom filter)
engine/       wires the above into Put/Get/Delete/ApplyBatch
raft/         leader election, replication, persistence
transport/    TCP wire protocol between nodes
server/       the replicated state machine + HTTP API
cmd/kvstore/  the runnable binary
bench/        benchmarks, naive baseline, failover timing, cluster load test
```

Each package's `godoc` comment (`go doc ./<package>`) documents its
design and tradeoffs in more depth than fits here.
