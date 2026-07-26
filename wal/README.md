# wal — Write-Ahead Log

Append-only, crash-safe log of mutations. Every write to the engine is
recorded here durably *before* it's applied to the in-memory memtable, so a
crash between "record in the WAL" and "flushed to an SSTable" never loses
data — replaying the WAL on startup reconstructs exactly the state the
memtable had.

## On-disk format

Each record is framed independently:

```
[4B CRC32C(payload)] [4B payload length] [payload...]

payload:
  [1B type] [8B seq num] [4B key len] [key] [4B value len] [value]
```

This mirrors LevelDB/RocksDB WAL framing: a per-record length + checksum
lets a reader detect a record that was only partially flushed before a
crash, without needing block padding or a separate index.

## Durability model

- `Append`/`AppendBatch` write into a buffered writer, flush, and (if
  `SyncOnWrite: true`) `fsync` before returning. A write is only durable
  once that `fsync` completes.
- `AppendBatch` amortizes one `fsync` across many records (group commit) —
  the primitive the storage engine should use under load.
- With `SyncOnWrite: false`, call `Sync()` explicitly at whatever boundary
  you consider a durability point.

## Recovery semantics (`Replay`)

Since a WAL has exactly one writer appending to it, the *only* way a record
can be malformed is a crash (or power loss) mid-write:

- header claims a payload longer than what's actually on disk, or
- the payload's CRC doesn't match (bytes landed on disk out of order or
  incompletely).

`Replay` stops at the **first** such record, discards it and everything
after it, truncates the file on disk to match, and returns every
well-formed record before it. This means:

1. A clean shutdown replays every record.
2. A crash mid-append loses at most the one in-flight record — never an
   earlier, already-durable one.
3. After recovery, the file is truncated so a fresh `Open` + `Append`
   continues right after the last good record instead of leaving a gap.

This does *not* attempt to detect bit-rot in an already-fsynced, correctly
framed record elsewhere in the file (a different concern — e.g. periodic
full-file scrubbing) — see `wal_test.go`'s
`TestReplay_BitCorruption_DetectedViaChecksum` for what it does catch:
any checksum mismatch, wherever it occurs, is treated as "stop trusting
the file here."

## Tests

`wal_test.go` covers round-trip correctness, batching, empty/nil edge
cases, and — the cases that matter most for a WAL — simulated crash
recovery: a torn header, a torn payload, and a corrupted-but-complete
record, each verified to (a) not error, (b) recover exactly the prior good
records, and (c) leave the file truncated so appends resume cleanly.
