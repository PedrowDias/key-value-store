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
	"sync/atomic"
	"testing"

	"github.com/PedrowDias/key-value-store/engine"
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
