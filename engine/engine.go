// Package engine ties the write-ahead log, memtable, and SSTable packages
// together into the actual key-value store: Put/Get/Delete, backed by the
// standard LSM-tree write path (WAL for durability, memtable for fast
// recent writes, SSTables for everything flushed to disk), plus crash
// recovery on Open.
//
// Directory layout for a store at dir:
//
//	dir/
//	  wal.log        — current write-ahead log
//	  000000.sst     — oldest flushed SSTable
//	  000001.sst     — next oldest
//	  ...            — highest number = most recently flushed = newest
//
// Write path: Put/Delete append to the WAL (durable before returning),
// then apply to the in-memory memtable. When the memtable's approximate
// size crosses MemtableSizeThreshold, it is flushed to a new SSTable and
// the WAL is rotated (truncated to empty), since every record in it is
// now durably represented in an SSTable and no longer needs replaying.
//
// Read path: check the memtable first, then consult SSTables from newest
// to oldest, returning on the first hit (a tombstone counts as a hit —
// finding one for a key means "deleted," not "keep looking further back").
// A single sstable.BlockCache (see Options.BlockCacheSize) is shared
// across every SSTable this Engine ever opens — on recovery and on every
// subsequent flush — so a repeated read of the same data block, even
// across different flushed tables over the store's lifetime, is served
// from memory rather than paying a disk read and checksum verification
// again.
//
// Concurrency: a single sync.RWMutex guards everything. Writes (and any
// flush they trigger) take the write lock; reads take the read lock. This
// means a flush — which walks the whole memtable and writes a new SSTable
// file — blocks all other engine activity for its duration. That's a
// deliberate simplicity-first tradeoff for this phase, not an oversight:
// it keeps the write path trivially easy to reason about, at the cost of
// a latency spike on whichever write happens to trigger a flush. Making
// flushes asynchronous (freeze the full memtable, keep accepting new
// writes into a fresh one, flush the frozen one in the background) is a
// natural follow-up — and a good candidate for a "before vs. after"
// benchmark once the benchmarking phase measures p99 write latency.
package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/PedrowDias/key-value-store/memtable"
	"github.com/PedrowDias/key-value-store/sstable"
	"github.com/PedrowDias/key-value-store/wal"
)

const (
	walFileName            = "wal.log"
	sstableFileNamePattern = "%06d.sst"
	defaultMemtableSizeMax = 4 * 1024 * 1024 // 4 MiB
	defaultBlockCacheSize  = 8 * 1024 * 1024 // 8 MiB
)

// walHandle, sstableWriter, and sstableReader are minimal interfaces over
// the wal and sstable packages' real types, used only so tests can inject
// a fake that fails at an exact, chosen point. Constructing a *wal.WAL or
// *sstable.Writer/*sstable.Reader that fails in a specific way from the
// outside isn't practical (they don't expose seams of their own, by
// design — that's a concern for their own packages' tests, which already
// cover it thoroughly). These three interfaces exist purely to let the
// engine's own error-propagation code (not the underlying packages'
// correctness) be exercised deterministically and portably.
type walHandle interface {
	Append(rec wal.Record) error
	AppendBatch(recs []wal.Record) error
	Close() error
}

type sstableWriter interface {
	Add(key, value []byte, seq uint64, deleted bool) error
	Finish() (*sstable.Meta, error)
}

type sstableReader interface {
	Get(key []byte) (value []byte, seq uint64, deleted bool, found bool, err error)
	Close() error
	MaxSeq() uint64
}

var (
	openWAL = func(path string, opts wal.Options) (walHandle, error) {
		return wal.Open(path, opts)
	}
	newSSTableWriter = func(path string, opts sstable.Options) (sstableWriter, error) {
		return sstable.NewWriter(path, opts)
	}
	openSSTableForRead = func(path string, cache *sstable.BlockCache) (sstableReader, error) {
		return sstable.OpenWithCache(path, cache)
	}
)

// Options configures an Engine.
type Options struct {
	// Dir is the directory the engine's files live in. Created if it
	// doesn't already exist.
	Dir string
	// MemtableSizeThreshold is the approximate memtable size, in bytes,
	// at which it's flushed to a new SSTable. Defaults to 4 MiB.
	MemtableSizeThreshold int64
	// SSTableBlockSize and SSTableBloomFPRate are passed through to each
	// SSTable written on flush; see sstable.Options for their meaning.
	// Zero values use sstable's own defaults.
	SSTableBlockSize   int
	SSTableBloomFPRate float64
	// BlockCacheSize bounds, in bytes, how much decoded SSTable block
	// data is kept in memory across every SSTable this Engine has open,
	// shared across all of them (see sstable.BlockCache's own doc for
	// why sharing matters). Defaults to 8 MiB. A negative value disables
	// caching entirely — every read goes to disk, matching this
	// project's behavior before the cache existed.
	BlockCacheSize int64
}

func (o Options) withDefaults() Options {
	if o.MemtableSizeThreshold <= 0 {
		o.MemtableSizeThreshold = defaultMemtableSizeMax
	}
	if o.BlockCacheSize == 0 {
		o.BlockCacheSize = defaultBlockCacheSize
	}
	return o
}

// sstableEntry pairs an open reader with the path it was opened from.
type sstableEntry struct {
	path   string
	reader sstableReader
}

// Engine is a single-node, durable, sorted key-value store.
type Engine struct {
	mu   sync.RWMutex
	opts Options

	w   walHandle
	mem *memtable.Memtable

	// cache is shared across every SSTable this Engine opens (on
	// recovery and on every subsequent flush) — see
	// sstable.BlockCache's own doc for why sharing across files, not
	// just within one, is what makes a block cache actually earn its
	// keep in a workload that reads across many flushed tables.
	cache *sstable.BlockCache

	// sstables is ordered newest-first (index 0 = most recently flushed),
	// matching the order reads must consult them in.
	sstables []*sstableEntry

	nextSeq      uint64 // sequence number to assign to the next write
	nextFlushSeq int    // number to use for the next flushed SSTable's filename

	closed bool
}

// Open opens (creating if necessary) a store at opts.Dir, replaying its
// WAL and discovering any existing SSTables to recover the state from a
// previous run.
func Open(opts Options) (*Engine, error) {
	opts = opts.withDefaults()
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("engine: creating directory %s: %w", opts.Dir, err)
	}

	cache := sstable.NewBlockCache(opts.BlockCacheSize)

	sstables, nextFlushSeq, maxSeqFromTables, err := discoverSSTables(opts.Dir, cache)
	if err != nil {
		return nil, err
	}

	walPath := filepath.Join(opts.Dir, walFileName)
	records, _, err := wal.Replay(walPath)
	if err != nil {
		closeAll(sstables)
		return nil, fmt.Errorf("engine: replaying wal: %w", err)
	}

	mem := memtable.New()
	maxSeqFromWAL := uint64(0)
	for _, rec := range records {
		if rec.Type == wal.RecordDelete {
			mem.Delete(rec.Key, rec.SeqNum)
		} else {
			mem.Put(rec.Key, rec.Value, rec.SeqNum)
		}
		if rec.SeqNum > maxSeqFromWAL {
			maxSeqFromWAL = rec.SeqNum
		}
	}

	nextSeq := maxSeqFromTables
	if maxSeqFromWAL > nextSeq {
		nextSeq = maxSeqFromWAL
	}
	nextSeq++ // first unused sequence number; 0 is never assigned to a real write

	w, err := openWAL(walPath, wal.Options{SyncOnWrite: true})
	if err != nil {
		closeAll(sstables)
		return nil, fmt.Errorf("engine: opening wal: %w", err)
	}

	return &Engine{
		opts:         opts,
		w:            w,
		mem:          mem,
		cache:        cache,
		sstables:     sstables,
		nextSeq:      nextSeq,
		nextFlushSeq: nextFlushSeq,
	}, nil
}

// discoverSSTables scans dir for existing "NNNNNN.sst" files, opens each
// (sharing cache across all of them), and returns them ordered
// newest-first, along with the flush-sequence number to use for the
// next new SSTable and the highest MaxSeq among them (used to resume
// sequence-number allocation after a restart).
func discoverSSTables(dir string, cache *sstable.BlockCache) (sstables []*sstableEntry, nextFlushSeq int, maxSeq uint64, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("engine: listing %s: %w", dir, err)
	}

	var nums []int
	numToName := make(map[int]string)
	for _, de := range entries {
		if de.IsDir() {
			continue
		}
		var n int
		if _, scanErr := fmt.Sscanf(de.Name(), sstableFileNamePattern, &n); scanErr != nil {
			continue
		}
		if fmt.Sprintf(sstableFileNamePattern, n) != de.Name() {
			continue // e.g. "000001.sst.bak" would otherwise Sscanf-match the prefix
		}
		nums = append(nums, n)
		numToName[n] = de.Name()
	}
	sort.Ints(nums)

	// Newest first: iterate from the highest number down.
	for i := len(nums) - 1; i >= 0; i-- {
		n := nums[i]
		path := filepath.Join(dir, numToName[n])
		r, openErr := openSSTableForRead(path, cache)
		if openErr != nil {
			closeAll(sstables)
			return nil, 0, 0, fmt.Errorf("engine: opening existing sstable %s: %w", path, openErr)
		}
		sstables = append(sstables, &sstableEntry{path: path, reader: r})
		if r.MaxSeq() > maxSeq {
			maxSeq = r.MaxSeq()
		}
	}

	if len(nums) > 0 {
		nextFlushSeq = nums[len(nums)-1] + 1
	}
	return sstables, nextFlushSeq, maxSeq, nil
}

func closeAll(entries []*sstableEntry) {
	for _, e := range entries {
		e.reader.Close()
	}
}

// Put sets key to value.
func (e *Engine) Put(key, value []byte) error {
	return e.ApplyBatch([]BatchOp{{Key: key, Value: value}})
}

// Delete removes key. A subsequent Get will report it as not found, even
// if an older, already-flushed SSTable still holds a stale value for it.
func (e *Engine) Delete(key []byte) error {
	return e.ApplyBatch([]BatchOp{{Key: key, Deleted: true}})
}

// BatchOp is one operation within a batch applied by ApplyBatch.
type BatchOp struct {
	Key     []byte
	Value   []byte // unused when Deleted is true
	Deleted bool
}

// ApplyBatch durably applies every op in ops with a single WAL fsync,
// rather than the N fsyncs that N individual Put/Delete calls would
// incur — this is group commit, the standard technique for write
// throughput under concurrent load: fsync latency, not CPU or memory
// bandwidth, is almost always the real bottleneck for a durable store,
// and batching amortizes that one unavoidable cost across every
// operation sharing the batch. Put and Delete are themselves just
// single-element-batch calls to this, so there is exactly one write
// path to reason about and test.
//
// All ops in a batch share one fsync and one auto-flush check, but each
// still gets its own sequence number and its own entry in the memtable,
// applied in order — from the memtable/SSTable's perspective, a batch is
// indistinguishable from the same N operations having arrived one at a
// time. The only difference is durability cost.
func (e *Engine) ApplyBatch(ops []BatchOp) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("engine: write on a closed engine")
	}
	if len(ops) == 0 {
		return nil
	}

	records := make([]wal.Record, len(ops))
	seqs := make([]uint64, len(ops))
	for i, op := range ops {
		seq := e.nextSeq
		e.nextSeq++
		seqs[i] = seq
		rt := wal.RecordPut
		if op.Deleted {
			rt = wal.RecordDelete
		}
		records[i] = wal.Record{SeqNum: seq, Type: rt, Key: op.Key, Value: op.Value}
	}
	if err := e.w.AppendBatch(records); err != nil {
		return fmt.Errorf("engine: wal append batch: %w", err)
	}

	for i, op := range ops {
		if op.Deleted {
			e.mem.Delete(op.Key, seqs[i])
		} else {
			e.mem.Put(op.Key, op.Value, seqs[i])
		}
	}

	if e.mem.ApproxSize() >= e.opts.MemtableSizeThreshold {
		if err := e.flushLocked(); err != nil {
			return fmt.Errorf("engine: auto-flush: %w", err)
		}
	}
	return nil
}

// Get looks up key. found is false if the key has never been written (or
// was deleted). Get consults the memtable first, then each SSTable from
// newest to oldest, stopping at the first table that has any record for
// the key at all — a tombstone there means "deleted," never "check an
// older table instead."
func (e *Engine) Get(key []byte) (value []byte, found bool, err error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil, false, fmt.Errorf("engine: read on a closed engine")
	}

	if val, _, deleted, memFound := e.mem.Get(key); memFound {
		if deleted {
			return nil, false, nil
		}
		return val, true, nil
	}

	for _, entry := range e.sstables {
		val, _, deleted, tblFound, gerr := entry.reader.Get(key)
		if gerr != nil {
			return nil, false, fmt.Errorf("engine: reading sstable %s: %w", entry.path, gerr)
		}
		if tblFound {
			if deleted {
				return nil, false, nil
			}
			return val, true, nil
		}
	}
	return nil, false, nil
}

// Flush forces the current memtable to be written out to a new SSTable
// immediately, regardless of its size. A no-op if the memtable is empty.
// Exposed mainly for tests and for an explicit "checkpoint now" operation;
// normal operation triggers this automatically via the size threshold.
func (e *Engine) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("engine: flush on a closed engine")
	}
	return e.flushLocked()
}

// flushLocked writes the current memtable to a new SSTable, rotates the
// WAL (since everything in it is now durably represented in that
// SSTable), and swaps in a fresh empty memtable. Caller must hold e.mu.
func (e *Engine) flushLocked() error {
	if e.mem.Len() == 0 {
		return nil
	}

	path := e.sstablePath(e.nextFlushSeq)
	sw, err := newSSTableWriter(path, sstable.Options{
		BlockSize:   e.opts.SSTableBlockSize,
		BloomFPRate: e.opts.SSTableBloomFPRate,
	})
	if err != nil {
		return fmt.Errorf("engine: creating sstable writer: %w", err)
	}

	it := e.mem.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if err := sw.Add(it.Key(), it.Value(), it.SeqNum(), it.Deleted()); err != nil {
			return fmt.Errorf("engine: writing sstable entry: %w", err)
		}
	}
	if _, err := sw.Finish(); err != nil {
		return fmt.Errorf("engine: finishing sstable: %w", err)
	}

	reader, err := openSSTableForRead(path, e.cache)
	if err != nil {
		return fmt.Errorf("engine: reopening flushed sstable: %w", err)
	}
	e.sstables = append([]*sstableEntry{{path: path, reader: reader}}, e.sstables...)
	e.nextFlushSeq++
	e.mem = memtable.New()

	// Rotate the WAL: everything it held is now durable in the SSTable
	// we just wrote, so it can be truncated to empty rather than growing
	// forever across the engine's lifetime.
	if err := e.w.Close(); err != nil {
		return fmt.Errorf("engine: closing wal for rotation: %w", err)
	}
	if err := removeFile(e.walPath()); err != nil {
		return fmt.Errorf("engine: removing old wal: %w", err)
	}
	newWAL, err := openWAL(e.walPath(), wal.Options{SyncOnWrite: true})
	if err != nil {
		return fmt.Errorf("engine: reopening wal after rotation: %w", err)
	}
	e.w = newWAL

	return nil
}

// removeFile is a package-level indirection over os.Remove purely so
// tests can substitute a failing stub for the WAL-rotation error path —
// a real os.Remove essentially never fails on a normal writable file you
// already control, making that branch otherwise untestable.
var removeFile = os.Remove

func (e *Engine) walPath() string {
	return filepath.Join(e.opts.Dir, walFileName)
}

func (e *Engine) sstablePath(n int) string {
	return filepath.Join(e.opts.Dir, fmt.Sprintf(sstableFileNamePattern, n))
}

// Stats reports basic observability info about the engine's current
// state, useful for the benchmarking phase and for tests.
type Stats struct {
	MemtableSize    int64
	MemtableEntries int
	NumSSTables     int
}

// Stats returns a snapshot of the engine's current state.
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return Stats{
		MemtableSize:    e.mem.ApproxSize(),
		MemtableEntries: e.mem.Len(),
		NumSSTables:     len(e.sstables),
	}
}

// Close closes the WAL and every open SSTable reader. Whatever's in the
// memtable but not yet flushed remains safely recoverable from the WAL on
// the next Open, so Close does not force a flush.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true

	var firstErr error
	if err := e.w.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("engine: closing wal: %w", err)
	}
	for _, entry := range e.sstables {
		if err := entry.reader.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("engine: closing sstable %s: %w", entry.path, err)
		}
	}
	return firstErr
}
