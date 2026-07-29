// Package memtable implements the in-memory, sorted write buffer of an
// LSM-tree storage engine. New writes land here first (after being made
// durable in the WAL); once a memtable grows past a size threshold, the
// engine freezes it and flushes it to an on-disk SSTable in sorted order,
// which a skip list's in-order traversal gives for free.
//
// Design note: this memtable keeps only the single latest value per key
// (last write wins), not a version per sequence number. That's a
// deliberate scope decision, not an oversight: real production engines
// like RocksDB keep one entry per (key, sequence number) pair instead,
// so a snapshot taken at sequence N can still read the value as of N
// even after later writes — buying point-in-time snapshot reads and
// repeatable range scans under concurrent writes, at the cost of needing
// compaction to ever reclaim old versions. This project has no
// snapshot-read feature, so that complexity isn't earning its keep here.
package memtable

import (
	"sync"
	"time"
)

// Memtable is a concurrent, sorted in-memory map from key to (value,
// deleted-flag, sequence number). It is safe for concurrent use by
// multiple goroutines.
type Memtable struct {
	mu         sync.RWMutex
	list       *skipList
	approxSize int64
}

// New creates an empty Memtable.
func New() *Memtable {
	return &Memtable{list: newSkipList(time.Now().UnixNano())}
}

// newForTest creates a Memtable with a deterministic skip-list level
// distribution, so tests that care about structural properties (e.g.
// height growth) are reproducible. Correctness tests don't need this —
// they hold regardless of the random level assignment — but it keeps
// flaky-looking failures out of CI.
func newForTest(seed int64) *Memtable {
	return &Memtable{list: newSkipList(seed)}
}

// Put inserts or overwrites the value for key, tagged with seq (the WAL
// sequence number that made this write durable, used by the engine to
// resolve ordering across the WAL/memtable/SSTable boundary).
func (m *Memtable) Put(key, value []byte, seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.approxSize += m.list.put(key, value, seq, false)
}

// Delete records a tombstone for key: Get will report it as found and
// deleted, distinguishing "was deleted" from "was never written," which
// matters once reads may fall through to older SSTables that still have
// a stale value for this key.
func (m *Memtable) Delete(key []byte, seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.approxSize += m.list.put(key, nil, seq, true)
}

// Get looks up key. found is false only if the key has never been
// written to this memtable at all. If found is true and deleted is true,
// the key was explicitly tombstoned and the engine must not fall through
// to an older SSTable's value for it.
func (m *Memtable) Get(key []byte) (value []byte, seq uint64, deleted bool, found bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n := m.list.get(key)
	if n == nil {
		return nil, 0, false, false
	}
	return n.value, n.seq, n.deleted, true
}

// ApproxSize returns an estimate, in bytes, of the memory this memtable is
// using — the number the engine compares against its flush threshold.
func (m *Memtable) ApproxSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.approxSize
}

// Len returns the number of distinct keys currently held (including
// tombstoned ones, since those still occupy a slot until compacted away).
func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.list.length
}
