// Benchmarks comparing the LSM storage engine (wal + memtable + sstable)
// against NaiveStore (one-file-per-key, fsync-per-write) under different
// workload shapes. Run with:
//
//	go test ./bench/... -bench=. -benchmem -run=^$
//
// -run=^$ skips the correctness tests in this package so only benchmarks
// run. -benchmem reports allocations, useful for spotting unnecessary
// copies in the hot path.
package bench

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PedrowDias/key-value-store/engine"
	"github.com/PedrowDias/key-value-store/memtable"
	"github.com/PedrowDias/key-value-store/sstable"
	"github.com/PedrowDias/key-value-store/wal"
)

// store is the minimal interface both engine.Engine and NaiveStore
// satisfy, letting every benchmark below run against either
// implementation from the same code.
type store interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, bool, error)
	Delete(key []byte) error
	Close() error
}

func openEngine(b *testing.B) store {
	b.Helper()
	e, err := engine.Open(engine.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	return e
}

func openNaive(b *testing.B) store {
	b.Helper()
	s, err := OpenNaiveStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	return s
}

// randomValue returns n pseudo-random bytes. Using real random content
// (not a fixed pattern) avoids accidentally benchmarking a code path that
// gets to special-case repetitive data.
func randomValue(rng *rand.Rand, n int) []byte {
	v := make([]byte, n)
	rng.Read(v)
	return v
}

func keyFor(i int) []byte {
	return []byte(fmt.Sprintf("key-%010d", i))
}

// --- Write-heavy: pure sequential Put ---------------------------------------

func BenchmarkPut(b *testing.B) {
	for _, valSize := range []int{64, 256, 4096} {
		b.Run(fmt.Sprintf("Engine/valsize=%d", valSize), func(b *testing.B) {
			s := openEngine(b)
			defer s.Close()
			benchmarkPut(b, s, valSize)
		})
		b.Run(fmt.Sprintf("Naive/valsize=%d", valSize), func(b *testing.B) {
			s := openNaive(b)
			defer s.Close()
			benchmarkPut(b, s, valSize)
		})
	}
}

func benchmarkPut(b *testing.B, s store, valSize int) {
	rng := rand.New(rand.NewSource(1))
	value := randomValue(rng, valSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Put(keyFor(i), value); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Read-heavy: Get against a pre-populated store --------------------------

// readBenchPopulateSize is deliberately modest: NaiveStore's Put does a
// real fsync per call, and the populate step here isn't itself being
// measured — it just needs to be big enough that Get exercises a real,
// non-trivially-small store, not so big that setup dominates wall-clock
// time on a slower disk.
const readBenchPopulateSize = 2000

func BenchmarkGet(b *testing.B) {
	for _, valSize := range []int{64, 256, 4096} {
		b.Run(fmt.Sprintf("Engine/valsize=%d", valSize), func(b *testing.B) {
			s := openEngine(b)
			defer s.Close()
			benchmarkGet(b, s, valSize)
		})
		b.Run(fmt.Sprintf("Naive/valsize=%d", valSize), func(b *testing.B) {
			s := openNaive(b)
			defer s.Close()
			benchmarkGet(b, s, valSize)
		})
	}
}

func benchmarkGet(b *testing.B, s store, valSize int) {
	rng := rand.New(rand.NewSource(1))
	value := randomValue(rng, valSize)
	for i := 0; i < readBenchPopulateSize; i++ {
		if err := s.Put(keyFor(i), value); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := s.Get(keyFor(i % readBenchPopulateSize))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- Mixed workloads: 90/10 and 50/50 read/write ratios ---------------------

func BenchmarkMixedWorkload(b *testing.B) {
	for _, readPct := range []int{90, 50} {
		b.Run(fmt.Sprintf("Engine/reads=%d%%", readPct), func(b *testing.B) {
			s := openEngine(b)
			defer s.Close()
			benchmarkMixed(b, s, readPct)
		})
		b.Run(fmt.Sprintf("Naive/reads=%d%%", readPct), func(b *testing.B) {
			s := openNaive(b)
			defer s.Close()
			benchmarkMixed(b, s, readPct)
		})
	}
}

func benchmarkMixed(b *testing.B, s store, readPct int) {
	const valSize = 256
	rng := rand.New(rand.NewSource(1))
	value := randomValue(rng, valSize)

	// Pre-populate so reads have something real to find.
	for i := 0; i < readBenchPopulateSize; i++ {
		if err := s.Put(keyFor(i), value); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if rng.Intn(100) < readPct {
			if _, _, err := s.Get(keyFor(i % readBenchPopulateSize)); err != nil {
				b.Fatal(err)
			}
		} else {
			if err := s.Put(keyFor(i%readBenchPopulateSize), value); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// --- Block cache: repeated reads of the same ("hot") keys -------------------

// hotKeyPopulateSize and hotKeyValueSize are sized so the populated data
// (2000 * 4096 ≈ 8MB) exceeds the engine's default 4MiB memtable flush
// threshold, forcing every subsequent Get to fall through to an SSTable
// on disk — the case this benchmark exists to measure, since a cache
// has nothing to demonstrate against pure in-memory memtable hits.
const (
	hotKeyPopulateSize = 2000
	hotKeyValueSize    = 4096
	hotKeySetSize      = 10 // the small subset of keys actually re-read
)

// BenchmarkGet_HotKeys repeatedly re-reads the SAME small set of keys —
// unlike BenchmarkGet elsewhere in this file, which reads b.N DISTINCT
// keys and so never actually exercises a cache hit (each key is read at
// most once). A block cache has nothing to show for itself against
// always-cold, never-repeated reads; this benchmark's whole point is
// measuring what it does for the repeated/"hot" access pattern real
// workloads actually have.
func BenchmarkGet_HotKeys(b *testing.B) {
	b.Run("CacheEnabled", func(b *testing.B) {
		e, err := engine.Open(engine.Options{Dir: b.TempDir()})
		if err != nil {
			b.Fatal(err)
		}
		defer e.Close()
		benchmarkHotKeyGet(b, e)
	})
	b.Run("CacheDisabled", func(b *testing.B) {
		e, err := engine.Open(engine.Options{Dir: b.TempDir(), BlockCacheSize: -1})
		if err != nil {
			b.Fatal(err)
		}
		defer e.Close()
		benchmarkHotKeyGet(b, e)
	})
}

func benchmarkHotKeyGet(b *testing.B, e store) {
	rng := rand.New(rand.NewSource(1))
	value := randomValue(rng, hotKeyValueSize)
	for i := 0; i < hotKeyPopulateSize; i++ {
		if err := e.Put(keyFor(i), value); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Cycle through only the first hotKeySetSize keys, repeatedly —
		// every one of these lives in an already-flushed SSTable (the
		// populate step above exceeds the flush threshold), so every
		// read here is a real disk read without caching, and a cache
		// hit after the first pass through the set with it enabled.
		if _, _, err := e.Get(keyFor(i % hotKeySetSize)); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Concurrent throughput ---------------------------------------------------

func BenchmarkConcurrentPut(b *testing.B) {
	b.Run("Engine", func(b *testing.B) {
		s := openEngine(b)
		defer s.Close()
		benchmarkConcurrentPut(b, s)
	})
	b.Run("Naive", func(b *testing.B) {
		s := openNaive(b)
		defer s.Close()
		benchmarkConcurrentPut(b, s)
	})
}

func benchmarkConcurrentPut(b *testing.B, s store) {
	const valSize = 256
	value := randomValue(rand.New(rand.NewSource(1)), valSize)

	var counter int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(atomic.AddInt64(&counter, 1))
			if err := s.Put(keyFor(i), value); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// --- Async vs. synchronous flush: write-latency tail under sustained load ---

// syncFlushStore is a minimal, faithful reconstruction of this project's
// ORIGINAL flush design (before it became asynchronous): Put holds one
// mutex for its entire duration, including — when triggered — walking
// the whole memtable and writing a new SSTable, all while every other
// concurrent Put is blocked waiting on the same mutex. It uses the
// exact same real wal/memtable/sstable packages as engine.Engine, not a
// simplified stand-in, so a benchmark comparing the two measures a real
// difference in locking discipline, not a difference in what work gets
// done. This exists purely for that comparison; it is not used anywhere
// else in this project.
type syncFlushStore struct {
	mu   sync.Mutex
	dir  string
	opts engineLikeOptions

	w   *wal.WAL
	mem *memtable.Memtable

	sstables     []*sstable.Reader
	nextSeq      uint64
	nextFlushSeq int
}

type engineLikeOptions struct {
	MemtableSizeThreshold int64
}

func newSyncFlushStore(dir string, threshold int64) (*syncFlushStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	w, err := wal.Open(filepath.Join(dir, "sync.wal"), wal.Options{SyncOnWrite: true})
	if err != nil {
		return nil, err
	}
	return &syncFlushStore{
		dir:  dir,
		opts: engineLikeOptions{MemtableSizeThreshold: threshold},
		w:    w,
		mem:  memtable.New(),
	}, nil
}

func (s *syncFlushStore) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	seq := s.nextSeq
	s.nextSeq++
	if err := s.w.Append(wal.Record{SeqNum: seq, Type: wal.RecordPut, Key: key, Value: value}); err != nil {
		return err
	}
	s.mem.Put(key, value, seq)

	if s.mem.ApproxSize() >= s.opts.MemtableSizeThreshold {
		// The synchronous part this whole benchmark exists to measure
		// the cost of: every concurrent Put waiting on s.mu blocks for
		// this entire flush, not just the metadata swap.
		path := filepath.Join(s.dir, fmt.Sprintf("%06d.sst", s.nextFlushSeq))
		sw, err := sstable.NewWriter(path, sstable.Options{})
		if err != nil {
			return err
		}
		it := s.mem.NewIterator()
		for it.SeekToFirst(); it.Valid(); it.Next() {
			if err := sw.Add(it.Key(), it.Value(), it.SeqNum(), it.Deleted()); err != nil {
				return err
			}
		}
		if _, err := sw.Finish(); err != nil {
			return err
		}
		reader, err := sstable.Open(path)
		if err != nil {
			return err
		}
		s.sstables = append([]*sstable.Reader{reader}, s.sstables...)
		s.nextFlushSeq++
		s.mem = memtable.New()
	}
	return nil
}

func (s *syncFlushStore) Get(key []byte) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if val, _, deleted, found := s.mem.Get(key); found {
		return val, !deleted, nil
	}
	for _, r := range s.sstables {
		val, _, deleted, found, err := r.Get(key)
		if err != nil {
			return nil, false, err
		}
		if found {
			return val, !deleted, nil
		}
	}
	return nil, false, nil
}

func (s *syncFlushStore) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq := s.nextSeq
	s.nextSeq++
	if err := s.w.Append(wal.Record{SeqNum: seq, Type: wal.RecordDelete, Key: key}); err != nil {
		return err
	}
	s.mem.Delete(key, seq)
	return nil
}

func (s *syncFlushStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.sstables {
		r.Close()
	}
	return s.w.Close()
}

// benchmarkWriteLatencyTail runs numWorkers concurrent writers issuing a
// fixed number of Puts each against s, recording every individual Put's
// latency, and reports p50/p99/max at the end — the distribution is the
// point, not the mean: async flush's whole benefit is bounding how badly
// the UNLUCKY write that happens to arrive during a flush (or, for
// syncFlushStore, EVERY write concurrent with one) gets stalled.
func benchmarkWriteLatencyTail(b *testing.B, s store, numWorkers, putsPerWorker int, valSize int) {
	b.Helper()
	value := randomValue(rand.New(rand.NewSource(1)), valSize)

	total := numWorkers * putsPerWorker
	latencies := make([]time.Duration, total)
	var idx int64

	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < putsPerWorker; j++ {
				key := keyFor(workerID*putsPerWorker + j)
				t0 := time.Now()
				if err := s.Put(key, value); err != nil {
					b.Error(err)
					return
				}
				i := atomic.AddInt64(&idx, 1) - 1
				latencies[i] = time.Since(t0)
			}
		}(w)
	}
	wg.Wait()
	wall := time.Since(start)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[len(latencies)*50/100]
	p99 := latencies[len(latencies)*99/100]
	max := latencies[len(latencies)-1]

	b.ReportMetric(float64(p50.Microseconds()), "p50-us")
	b.ReportMetric(float64(p99.Microseconds()), "p99-us")
	b.ReportMetric(float64(max.Microseconds()), "max-us")
	b.ReportMetric(float64(total)/wall.Seconds(), "ops/sec")
}

// BenchmarkWriteLatencyTail_TriggeringWriteOnly isolates the specific
// claim async flush makes — the write whose own operation crosses the
// threshold does not itself pay the disk-I/O cost of flushing — by
// measuring ONLY the latency of writes that actually trigger a flush,
// not diluting the signal across the many surrounding writes that don't
// (a sequential run of numPuts writes with this threshold only crosses
// it a handful of times, so an aggregate p50/max over ALL writes is
// dominated by ordinary per-write cost and doesn't show the effect
// cleanly). BenchmarkWriteLatencyTail_AsyncVsSyncFlush below shows
// concurrent-contention effects are a real, separate factor under
// sustained heavy load; this benchmark isolates the simpler question —
// does MY write block on MY OWN flush — from that.
func BenchmarkWriteLatencyTail_TriggeringWriteOnly(b *testing.B) {
	const (
		numPuts   = 2000
		valSize   = 2000
		threshold = 200 * 1024
	)

	b.Run("Async", func(b *testing.B) {
		e, err := engine.Open(engine.Options{Dir: b.TempDir(), MemtableSizeThreshold: threshold})
		if err != nil {
			b.Fatal(err)
		}
		defer e.Close()
		var wasInProgress bool
		lats := benchmarkTriggeringWriteLatencies(b, e, numPuts, valSize, func() bool {
			now := e.Stats().FlushInProgress
			triggered := now && !wasInProgress // only the false->true edge: the write that STARTED this flush, not every subsequent quick write while it's still finishing
			wasInProgress = now
			return triggered
		})
		reportTriggeringLatencies(b, lats)
	})

	b.Run("SyncFlush", func(b *testing.B) {
		s, err := newSyncFlushStore(b.TempDir(), threshold)
		if err != nil {
			b.Fatal(err)
		}
		defer s.Close()
		var flushCountBefore int
		lats := benchmarkTriggeringWriteLatencies(b, s, numPuts, valSize, func() bool {
			s.mu.Lock()
			n := len(s.sstables)
			s.mu.Unlock()
			triggered := n > flushCountBefore
			flushCountBefore = n
			return triggered
		})
		reportTriggeringLatencies(b, lats)
	})
}

// benchmarkTriggeringWriteLatencies runs numPuts sequential writes,
// recording the latency of any write immediately after which
// triggered() reports true (a flush having just started/happened) and
// returning just those latencies.
func benchmarkTriggeringWriteLatencies(b *testing.B, s store, numPuts, valSize int, triggered func() bool) []time.Duration {
	b.Helper()
	value := randomValue(rand.New(rand.NewSource(1)), valSize)
	var triggering []time.Duration
	for i := 0; i < numPuts; i++ {
		t0 := time.Now()
		if err := s.Put(keyFor(i), value); err != nil {
			b.Fatal(err)
		}
		lat := time.Since(t0)
		if triggered() {
			triggering = append(triggering, lat)
		}
	}
	return triggering
}

func reportTriggeringLatencies(b *testing.B, lats []time.Duration) {
	b.Helper()
	if len(lats) == 0 {
		b.Fatal("no flush was triggered during this run — threshold/workload mismatch")
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	var sum time.Duration
	for _, l := range lats {
		sum += l
	}
	b.ReportMetric(float64(lats[0].Microseconds()), "min-us")
	b.ReportMetric(float64(sum.Microseconds())/float64(len(lats)), "mean-us")
	b.ReportMetric(float64(lats[len(lats)-1].Microseconds()), "max-us")
	b.ReportMetric(float64(len(lats)), "flush-count")
}

// BenchmarkWriteLatencyTail_AsyncVsSyncFlush measures the SAME general
// workload shape under sustained CONCURRENT load (many goroutines, many
// flushes) rather than a single sequential writer. This is a genuinely
// more complicated picture than BenchmarkWriteLatencyTail_TriggeringWriteOnly's
// clean result: because this project bounds itself to at most one flush
// in flight at a time (see Engine's package doc), ANY concurrent write
// that arrives while a flush is running — not only one that would
// itself start a SECOND flush — waits for that flush to finish, via
// waitForAnyFlushLocked. Under heavy sustained concurrency with frequent
// flushes, nearly every writer ends up waiting for flush completion one
// way or another, which measurably narrows (and in some runs reverses)
// the aggregate latency-percentile advantage over synchronous flush,
// where the same writers would instead be contending for one mutex. Run
// with:
//
//	go test ./bench/... -bench=BenchmarkWriteLatencyTail -run=^$
func BenchmarkWriteLatencyTail_AsyncVsSyncFlush(b *testing.B) {
	const (
		numWorkers    = 20
		putsPerWorker = 100
		valSize       = 2000
		threshold     = 200 * 1024 // small enough that this workload crosses it many times
	)

	b.Run("Async", func(b *testing.B) {
		e, err := engine.Open(engine.Options{Dir: b.TempDir(), MemtableSizeThreshold: threshold})
		if err != nil {
			b.Fatal(err)
		}
		defer e.Close()
		benchmarkWriteLatencyTail(b, e, numWorkers, putsPerWorker, valSize)
	})

	b.Run("SyncFlush", func(b *testing.B) {
		s, err := newSyncFlushStore(b.TempDir(), threshold)
		if err != nil {
			b.Fatal(err)
		}
		defer s.Close()
		benchmarkWriteLatencyTail(b, s, numWorkers, putsPerWorker, valSize)
	})
}
