// Package bench holds this project's benchmark suite: throughput/latency
// comparisons between the LSM storage engine and a deliberately naive
// baseline, and failure-injection measurements of Raft leader failover
// time.
package bench

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// NaiveStore is the simplest possible *correct* and *durable* key-value
// store: one file per key, written with a full fsync on every Put. No
// write-ahead log, no in-memory buffering, no batching, no compaction —
// every operation is exactly the syscalls it looks like it should be.
//
// This is the baseline the LSM engine (wal + memtable + sstable) is
// benchmarked against. The comparison is only meaningful if this is
// genuinely durable (a fair baseline), not a strawman: Put here really
// does fsync before returning, matching the durability guarantee the
// real engine makes. What differs is HOW that durability is achieved —
// one fsync per key here, versus the engine's batched WAL writes and
// buffered memtable — which is exactly the design difference the
// benchmark numbers are meant to demonstrate.
type NaiveStore struct {
	mu  sync.RWMutex
	dir string
}

// OpenNaiveStore creates dir if needed and returns a NaiveStore rooted
// there.
func OpenNaiveStore(dir string) (*NaiveStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("bench: creating naive store directory: %w", err)
	}
	return &NaiveStore{dir: dir}, nil
}

// keyPath maps a key to its file path. Keys are hex-encoded byte-for-byte
// into the filename so arbitrary binary keys (not just filesystem-safe
// strings) are handled correctly — a real store has to support that, and
// so does a fair baseline.
func (s *NaiveStore) keyPath(key []byte) string {
	return filepath.Join(s.dir, fmt.Sprintf("%x.kv", key))
}

// fileHandle is the subset of *os.File operations Put needs. Defined as
// an interface purely so tests can inject a fake that fails a Write,
// Sync, or Close at a precise point — the same technique used for this
// exact class of hard-to-trigger error path throughout this project
// (see e.g. wal.WAL's walFile, sstable.Writer's fileWriter). A real
// *os.File satisfies this as-is.
type fileHandle interface {
	Write(p []byte) (int, error)
	Sync() error
	Close() error
}

// openFileForWrite is a package-level indirection over creating the
// file Put writes to, so tests can substitute a fake fileHandle for the
// write/fsync/close error paths that are otherwise impractical to
// trigger against a real, already-openable file.
var openFileForWrite = func(path string) (fileHandle, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
}

// Put durably writes key=value: a full file write followed by fsync
// before returning, matching the durability guarantee a real store makes.
func (s *NaiveStore) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.keyPath(key)
	f, err := openFileForWrite(path)
	if err != nil {
		return fmt.Errorf("bench: naive put: opening file: %w", err)
	}
	if _, err := f.Write(value); err != nil {
		f.Close()
		return fmt.Errorf("bench: naive put: writing: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("bench: naive put: fsync: %w", err)
	}
	return f.Close()
}

// Get reads key's value, or reports not found.
func (s *NaiveStore) Get(key []byte) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := os.ReadFile(s.keyPath(key))
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("bench: naive get: %w", err)
	}
	return data, true, nil
}

// Delete removes key. Deleting a key that doesn't exist is not an error,
// matching engine.Engine's Delete semantics.
func (s *NaiveStore) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := os.Remove(s.keyPath(key))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("bench: naive delete: %w", err)
	}
	return nil
}

// Close is a no-op (NaiveStore holds no open file handles between
// calls); it exists so NaiveStore and engine.Engine share a similar
// enough shape for benchmark code to treat them uniformly.
func (s *NaiveStore) Close() error { return nil }
