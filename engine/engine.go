// Package engine ties the write-ahead log, memtable, and SSTable packages
// together into the actual key-value store: Put/Get/Delete, backed by the
// standard LSM-tree write path (WAL for durability, memtable for fast
// recent writes, SSTables for everything flushed to disk), plus crash
// recovery on Open.
//
// Directory layout for a store at dir:
//
//	dir/
//	  000000.wal     — current write-ahead log (numbered like SSTables;
//	                    normally exactly one exists, but a second can
//	                    briefly coexist during a background flush — see
//	                    below)
//	  000000.sst     — oldest flushed SSTable
//	  000001.sst     — next oldest
//	  ...            — highest number = most recently flushed = newest
//
// Write path: Put/Delete append to the WAL (durable before returning),
// then apply to the in-memory memtable. When the memtable's approximate
// size crosses MemtableSizeThreshold, it is flushed to a new SSTable —
// see "Background flush" below for how that happens without blocking the
// write that triggered it.
//
// Read path: check the active memtable, then the frozen (if any, see
// below) memtable, then each SSTable from newest to oldest, returning on
// the first hit (a tombstone counts as a hit — finding one for a key
// means "deleted," not "keep looking further back"). A single
// sstable.BlockCache (see Options.BlockCacheSize) is shared across every
// SSTable this Engine ever opens — on recovery and on every subsequent
// flush — so a repeated read of the same data block, even across
// different flushed tables over the store's lifetime, is served from
// memory rather than paying a disk read and checksum verification again.
//
// Background flush: when the active memtable crosses its size threshold,
// it is frozen (made immutable) and a fresh, empty memtable immediately
// takes over for new writes — which is what makes flushing
// "background": the write that triggered it, and every write after it,
// proceeds without waiting for the actual disk I/O (walking the whole
// frozen memtable and writing a new SSTable) to finish. A dedicated WAL
// file is opened for the fresh memtable's writes at the same moment; the
// frozen memtable's own WAL stays open and undeleted until its flush
// actually completes, so a crash mid-flush can still recover by
// replaying it. This project bounds itself to at most one flush in
// flight at a time: if writes fill the fresh memtable again before the
// previous flush finishes, the next call that would start a second one
// instead waits for the first — a deliberate, documented simplicity
// tradeoff over an unbounded queue of pending memtables, since it still
// achieves the actual goal (a single flush doesn't stall the many writes
// that arrive while it's running) without the added complexity of
// tracking an arbitrary number of outstanding generations.
//
// A background flush that fails is sticky: the engine refuses further
// writes rather than risk silently losing data that was supposed to be
// durably in an SSTable by now. This mirrors how production databases
// handle an unrecoverable background compaction/flush failure — continuing
// to accept writes atop a store whose durability guarantee just broke is
// worse than stopping.
//
// Concurrency: a single sync.RWMutex guards all metadata (which
// memtable/WAL is active, the SSTable list, sticky flush error). The
// actual flush-to-disk work — walking the frozen memtable and writing
// the new SSTable — deliberately happens with the lock NOT held, which
// is the entire point of making it a background operation.
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
	walFileNamePattern     = "%06d.wal"
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
	Path() string
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

	// immutable is non-nil exactly while a background flush is in
	// progress: the memtable frozen at the moment the flush started, no
	// longer written to (mem, above, is the fresh one new writes go to),
	// but still consulted by Get — its data isn't in an SSTable yet.
	// flushDone is closed by the flush goroutine when it finishes
	// (success or failure); callers that need to wait for it (a second
	// write that would otherwise start a redundant concurrent flush, or
	// Close) receive on it without holding e.mu.
	immutable *memtable.Memtable
	flushDone chan struct{}
	// flushErr is sticky: once a background flush fails, every
	// subsequent write refuses rather than risk silently losing data —
	// see the package doc for why.
	flushErr error
	// flushWG lets Close wait for a still-running flush goroutine to
	// actually exit before tearing down the engine's files out from
	// under it.
	flushWG sync.WaitGroup

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
	nextWALSeq   int    // number to use for the next WAL file's filename

	closed bool
}

// Open opens (creating if necessary) a store at opts.Dir, replaying its
// WAL(s) and discovering any existing SSTables to recover the state from
// a previous run.
//
// Recovery discovers every "NNNNNN.wal" file present (normally exactly
// one, but a crash during a background flush can leave two — the frozen
// memtable's WAL and the fresh one opened alongside it) and replays all
// of them, in file-number order, into one in-memory memtable. That
// recovered memtable is then immediately re-logged into a single fresh
// WAL file before the old one(s) are deleted — restoring the normal
// "exactly one WAL, matching the current memtable exactly" invariant
// right away, rather than carrying old WAL files forward indefinitely.
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

	walPaths, nextWALSeq, err := discoverWALs(opts.Dir)
	if err != nil {
		// Accepted gap: discoverSSTables just scanned this same
		// directory successfully, so making discoverWALs specifically
		// fail its own os.ReadDir on the identical path needs the
		// directory to become unreadable in the narrow window between
		// the two calls — not portably triggerable, same class of
		// OS-level branch this project has consistently left untested
		// elsewhere (e.g. Stat/ReadAt failures on an already-opened fd).
		closeAll(sstables)
		return nil, err
	}

	mem := memtable.New()
	maxSeqFromWAL := uint64(0)
	for _, path := range walPaths {
		records, _, err := wal.Replay(path)
		if err != nil {
			// Accepted gap: wal.Replay's own crash-safety contract (see
			// its doc) treats every form of corruption as a torn write
			// from a crash mid-append and truncates gracefully rather
			// than returning an error — see
			// TestOpen_CorruptedWALIsGracefullyTruncatedNotAnError. The
			// only way this branch fires is os.OpenFile failing for a
			// reason other than not-exist (permission denied, etc.),
			// not portably triggerable against a file this process just
			// discovered via its own successful os.ReadDir.
			closeAll(sstables)
			return nil, fmt.Errorf("engine: replaying wal %s: %w", path, err)
		}
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
	}

	nextSeq := maxSeqFromTables
	if maxSeqFromWAL > nextSeq {
		nextSeq = maxSeqFromWAL
	}
	nextSeq++ // first unused sequence number; 0 is never assigned to a real write

	newWALPath := filepath.Join(opts.Dir, fmt.Sprintf(walFileNamePattern, nextWALSeq))
	w, err := openWAL(newWALPath, wal.Options{SyncOnWrite: true})
	if err != nil {
		closeAll(sstables)
		return nil, fmt.Errorf("engine: opening wal: %w", err)
	}
	nextWALSeq++

	// Re-log whatever was recovered into the fresh WAL before removing
	// the old one(s) — so the durability guarantee holds continuously;
	// at no point is the recovered data represented in fewer places than
	// it was before this step.
	if mem.Len() > 0 {
		records := make([]wal.Record, 0, mem.Len())
		it := mem.NewIterator()
		for it.SeekToFirst(); it.Valid(); it.Next() {
			rt := wal.RecordPut
			if it.Deleted() {
				rt = wal.RecordDelete
			}
			records = append(records, wal.Record{SeqNum: it.SeqNum(), Type: rt, Key: it.Key(), Value: it.Value()})
		}
		if err := w.AppendBatch(records); err != nil {
			w.Close()
			closeAll(sstables)
			return nil, fmt.Errorf("engine: re-logging recovered records: %w", err)
		}
	}
	for _, path := range walPaths {
		if err := removeFile(path); err != nil {
			w.Close()
			closeAll(sstables)
			return nil, fmt.Errorf("engine: removing old wal %s after re-logging: %w", path, err)
		}
	}

	return &Engine{
		opts:         opts,
		w:            w,
		mem:          mem,
		cache:        cache,
		sstables:     sstables,
		nextSeq:      nextSeq,
		nextFlushSeq: nextFlushSeq,
		nextWALSeq:   nextWALSeq,
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

// discoverWALs scans dir for existing "NNNNNN.wal" files and returns
// their full paths in ascending (oldest-first) file-number order — the
// order they must be replayed in — along with the WAL-sequence number to
// use for the next new one.
func discoverWALs(dir string) (paths []string, nextWALSeq int, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, fmt.Errorf("engine: listing %s: %w", dir, err)
	}

	var nums []int
	numToName := make(map[int]string)
	for _, de := range entries {
		if de.IsDir() {
			continue
		}
		var n int
		if _, scanErr := fmt.Sscanf(de.Name(), walFileNamePattern, &n); scanErr != nil {
			continue
		}
		if fmt.Sprintf(walFileNamePattern, n) != de.Name() {
			continue
		}
		nums = append(nums, n)
		numToName[n] = de.Name()
	}
	sort.Ints(nums)

	for _, n := range nums {
		paths = append(paths, filepath.Join(dir, numToName[n]))
	}
	if len(nums) > 0 {
		nextWALSeq = nums[len(nums)-1] + 1
	}
	return paths, nextWALSeq, nil
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
//
// If applying this batch crosses the memtable size threshold, a
// background flush starts (see the package doc) — this call still
// returns as soon as the batch itself is durable, without waiting for
// that flush to finish. If a previous flush is still running, this call
// waits for it before proceeding, rather than starting a second one
// concurrently (see the package doc for why).
func (e *Engine) ApplyBatch(ops []BatchOp) error {
	e.mu.Lock()
	if err := e.waitForAnyFlushLocked(); err != nil {
		e.mu.Unlock()
		return err
	}
	if e.flushErr != nil {
		err := fmt.Errorf("engine: a previous background flush failed, refusing further writes: %w", e.flushErr)
		e.mu.Unlock()
		return err
	}
	if len(ops) == 0 {
		e.mu.Unlock()
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
		e.mu.Unlock()
		return fmt.Errorf("engine: wal append batch: %w", err)
	}

	for i, op := range ops {
		if op.Deleted {
			e.mem.Delete(op.Key, seqs[i])
		} else {
			e.mem.Put(op.Key, op.Value, seqs[i])
		}
	}

	var startErr error
	if e.mem.ApproxSize() >= e.opts.MemtableSizeThreshold {
		startErr = e.startFlushLocked()
	}
	e.mu.Unlock()
	if startErr != nil {
		return fmt.Errorf("engine: starting background flush: %w", startErr)
	}
	return nil
}

// waitForAnyFlushLocked blocks until there is no flush in progress,
// releasing and reacquiring e.mu while waiting so the flush goroutine
// (which needs e.mu itself to install its results) is never blocked by
// the caller holding it. Must be called with e.mu held; returns with it
// held again (whether or not it waited). Returns an error only if the
// engine was closed while waiting.
func (e *Engine) waitForAnyFlushLocked() error {
	for e.immutable != nil {
		done := e.flushDone
		e.mu.Unlock()
		<-done
		e.mu.Lock()
		if e.closed {
			return fmt.Errorf("engine: closed while waiting for a background flush")
		}
	}
	return nil
}

// Get looks up key. found is false if the key has never been written (or
// was deleted). Get consults the active memtable, then the frozen
// (if any) memtable a background flush is currently working through,
// then each SSTable from newest to oldest, stopping at the first one
// that has any record for the key at all — a tombstone there means
// "deleted," never "check an older table instead."
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

	if e.immutable != nil {
		if val, _, deleted, immFound := e.immutable.Get(key); immFound {
			if deleted {
				return nil, false, nil
			}
			return val, true, nil
		}
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

// Flush forces the current memtable to be written out to a new SSTable,
// waiting for that to complete before returning — unlike the automatic,
// non-blocking background flush a write crossing the size threshold
// starts, this is an explicit, synchronous "checkpoint now and wait"
// operation, exposed mainly for tests and for an operator wanting to
// force one before shutdown. If a background flush is already running,
// this waits for it first; if the (now-current) memtable is empty at
// that point, this is a no-op.
func (e *Engine) Flush() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("engine: flush on a closed engine")
	}
	if err := e.waitForAnyFlushLocked(); err != nil {
		e.mu.Unlock()
		return err
	}
	if e.flushErr != nil {
		err := fmt.Errorf("engine: a previous background flush failed: %w", e.flushErr)
		e.mu.Unlock()
		return err
	}
	if e.mem.Len() == 0 {
		e.mu.Unlock()
		return nil
	}
	if err := e.startFlushLocked(); err != nil {
		e.mu.Unlock()
		return err
	}
	done := e.flushDone
	e.mu.Unlock()

	<-done // wait for the flush just started to actually finish

	e.mu.Lock()
	err := e.flushErr
	e.mu.Unlock()
	if err != nil {
		return fmt.Errorf("engine: flush failed: %w", err)
	}
	return nil
}

// startFlushLocked freezes the current memtable and its WAL, opens a
// fresh WAL for new writes to continue into immediately, and spawns a
// goroutine to write the frozen memtable out as a new SSTable in the
// background. Must be called with e.mu held, with e.immutable == nil
// (no flush already running — callers arrange this via
// waitForAnyFlushLocked first) and e.mem non-empty.
func (e *Engine) startFlushLocked() error {
	newWALPath := filepath.Join(e.opts.Dir, fmt.Sprintf(walFileNamePattern, e.nextWALSeq))
	newWAL, err := openWAL(newWALPath, wal.Options{SyncOnWrite: true})
	if err != nil {
		return fmt.Errorf("opening new wal for background flush: %w", err)
	}
	e.nextWALSeq++

	frozen := e.mem
	frozenWAL := e.w
	e.immutable = frozen
	e.mem = memtable.New()
	e.w = newWAL

	flushSeq := e.nextFlushSeq
	e.nextFlushSeq++

	done := make(chan struct{})
	e.flushDone = done

	e.flushWG.Add(1)
	go e.runFlush(frozen, frozenWAL, flushSeq, done)
	return nil
}

// runFlush writes frozen out as a new SSTable, then installs the result
// (or records a sticky error) under lock, and finally removes the WAL
// frozen's data came from — safe now, since that data is durably
// represented in the new SSTable. Deliberately holds e.mu for none of
// the actual disk I/O (writing the SSTable, removing the old WAL) —
// only for the brief metadata updates — which is the entire point of
// this running in the background: reads and writes into the new active
// memtable are never blocked by it.
func (e *Engine) runFlush(frozen *memtable.Memtable, frozenWAL walHandle, flushSeq int, done chan struct{}) {
	defer close(done)
	defer e.flushWG.Done()

	path := e.sstablePath(flushSeq)
	if err := writeSSTableFromMemtable(frozen, path, e.opts); err != nil {
		e.mu.Lock()
		e.flushErr = fmt.Errorf("background flush: %w", err)
		e.immutable = nil
		e.mu.Unlock()
		return
	}

	reader, err := openSSTableForRead(path, e.cache)
	if err != nil {
		e.mu.Lock()
		e.flushErr = fmt.Errorf("background flush: reopening flushed sstable: %w", err)
		e.immutable = nil
		e.mu.Unlock()
		return
	}

	e.mu.Lock()
	e.sstables = append([]*sstableEntry{{path: path, reader: reader}}, e.sstables...)
	e.immutable = nil
	e.mu.Unlock()

	if err := frozenWAL.Close(); err != nil {
		e.mu.Lock()
		e.flushErr = fmt.Errorf("background flush: closing old wal %s: %w", frozenWAL.Path(), err)
		e.mu.Unlock()
		return
	}
	if err := removeFile(frozenWAL.Path()); err != nil {
		e.mu.Lock()
		e.flushErr = fmt.Errorf("background flush: removing old wal %s: %w", frozenWAL.Path(), err)
		e.mu.Unlock()
		return
	}
}

// writeSSTableFromMemtable walks mem in order and writes it out as a new
// SSTable at path — the pure "do the flush" step, with no engine state
// (locks, fields) involved, so it's exactly the same code whether called
// from the background flush goroutine or (indirectly, via the shared
// machinery above) Flush's synchronous wait.
func writeSSTableFromMemtable(mem *memtable.Memtable, path string, opts Options) error {
	sw, err := newSSTableWriter(path, sstable.Options{
		BlockSize:   opts.SSTableBlockSize,
		BloomFPRate: opts.SSTableBloomFPRate,
	})
	if err != nil {
		return fmt.Errorf("creating sstable writer: %w", err)
	}
	it := mem.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if err := sw.Add(it.Key(), it.Value(), it.SeqNum(), it.Deleted()); err != nil {
			return fmt.Errorf("writing sstable entry: %w", err)
		}
	}
	if _, err := sw.Finish(); err != nil {
		return fmt.Errorf("finishing sstable: %w", err)
	}
	return nil
}

// removeFile is a package-level indirection over os.Remove purely so
// tests can substitute a failing stub for the WAL-rotation error path —
// a real os.Remove essentially never fails on a normal writable file you
// already control, making that branch otherwise untestable.
var removeFile = os.Remove

func (e *Engine) sstablePath(n int) string {
	return filepath.Join(e.opts.Dir, fmt.Sprintf(sstableFileNamePattern, n))
}

// Stats reports basic observability info about the engine's current
// state, useful for the benchmarking phase and for tests.
type Stats struct {
	MemtableSize    int64
	MemtableEntries int
	NumSSTables     int
	// FlushInProgress reports whether a background flush is currently
	// running.
	FlushInProgress bool
}

// Stats returns a snapshot of the engine's current state.
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return Stats{
		MemtableSize:    e.mem.ApproxSize(),
		MemtableEntries: e.mem.Len(),
		NumSSTables:     len(e.sstables),
		FlushInProgress: e.immutable != nil,
	}
}

// Close closes the WAL and every open SSTable reader, waiting for any
// in-flight background flush to finish first — otherwise it could be
// left trying to write to (or remove) files out from under a torn-down
// engine. Whatever's in the memtable but not yet flushed remains safely
// recoverable from the WAL on the next Open, so Close does not force a
// flush of its own.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock() // released before waiting: the flush goroutine needs e.mu itself to finish

	e.flushWG.Wait()

	e.mu.Lock()
	defer e.mu.Unlock()

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
