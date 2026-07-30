// Quantifies the actual latency gap between a regular Get (reads local
// state directly, no network round trip) and LinearizableGet (confirms
// this node is still the legitimate leader via a real ReadIndex round
// trip to a majority before answering — see the README's "Reads default
// to fast and local" design note) against a real 3-node cluster: real
// TCP transport, real server.Server instances, not a simulated one.
//
// Run with:
//
//	go test ./bench/... -bench=BenchmarkGet_Vs_LinearizableGet -benchmem -run=^$
package bench

import (
	"testing"
	"time"
)

// BenchmarkGet_Vs_LinearizableGet reuses the same real-cluster wiring
// snapshot_bench_test.go already established (raft + transport + engine
// + server, real TCP) — the read path being measured here doesn't care
// that snapshotting is also configured on these nodes; nothing in this
// benchmark ever writes enough to trigger one.
func BenchmarkGet_Vs_LinearizableGet(b *testing.B) {
	nodes := startSnapBenchCluster(b, 3, 26000)
	defer func() {
		for _, n := range nodes {
			stopSnapBenchNode(n)
		}
	}()

	leader := waitForSnapBenchLeader(b, nodes, 3*time.Second)
	key := []byte("bench-key")
	if err := leader.server.Put(key, []byte("bench-value")); err != nil {
		b.Fatalf("warmup Put: %v", err)
	}
	// Give the write a moment to settle across the cluster before
	// measuring reads — not strictly required for correctness (Get and
	// LinearizableGet both only ever touch the leader here), but keeps
	// the first few iterations of each sub-benchmark from measuring
	// something other than steady-state read cost.
	time.Sleep(50 * time.Millisecond)

	b.Run("Get", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, _, err := leader.server.Get(key); err != nil {
				b.Fatalf("Get: %v", err)
			}
		}
	})

	b.Run("LinearizableGet", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, _, err := leader.server.LinearizableGet(key); err != nil {
				b.Fatalf("LinearizableGet: %v", err)
			}
		}
	})
}

// BenchmarkGet_Vs_LinearizableGet_Concurrent is the same comparison
// under concurrent load (GOMAXPROCS-scaled parallelism via b.RunParallel)
// rather than one request at a time — LinearizableGet's ReadIndex round
// trips to a majority can naturally batch/overlap across concurrent
// callers the same way normal AppendEntries replication already does,
// so the gap under concurrency is worth measuring separately from the
// single-caller case above rather than assumed to be the same ratio.
func BenchmarkGet_Vs_LinearizableGet_Concurrent(b *testing.B) {
	nodes := startSnapBenchCluster(b, 3, 26100)
	defer func() {
		for _, n := range nodes {
			stopSnapBenchNode(n)
		}
	}()

	leader := waitForSnapBenchLeader(b, nodes, 3*time.Second)
	key := []byte("bench-key")
	if err := leader.server.Put(key, []byte("bench-value")); err != nil {
		b.Fatalf("warmup Put: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	b.Run("Get", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, _, err := leader.server.Get(key); err != nil {
					b.Fatalf("Get: %v", err)
				}
			}
		})
	})

	b.Run("LinearizableGet", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, _, err := leader.server.LinearizableGet(key); err != nil {
					b.Fatalf("LinearizableGet: %v", err)
				}
			}
		})
	})
}
