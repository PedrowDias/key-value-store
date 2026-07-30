// Benchmarks for log compaction / snapshotting: how expensive is taking
// a snapshot as the dataset it covers grows (the real cost of
// engine.Snapshot's per-key Get approach — see the README's "Known
// limitations" section), and how long does a real, killed-and-restarted
// follower actually take to catch up via InstallSnapshot once healed?
//
// Run with:
//
//	go test ./bench/... -bench=BenchmarkSnapshotCreation -benchmem -run=^$
//	go test ./bench/... -run TestSnapshotCatchUpTime -v
package bench

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/PedrowDias/key-value-store/engine"
	"github.com/PedrowDias/key-value-store/raft"
	"github.com/PedrowDias/key-value-store/server"
	"github.com/PedrowDias/key-value-store/transport"
)

// --- BenchmarkSnapshotCreation: the real cost of Snapshot's per-key Get ----

// populateEngine writes n random 64-byte-value keys directly to eng,
// bypassing Raft entirely — Snapshot() only cares about the engine's own
// state, not how it got there.
func populateEngine(b *testing.B, eng *engine.Engine, n int) {
	b.Helper()
	rng := rand.New(rand.NewSource(1))
	val := make([]byte, 64)
	for i := 0; i < n; i++ {
		rng.Read(val)
		key := fmt.Sprintf("key-%08d", i)
		if err := eng.Put([]byte(key), append([]byte(nil), val...)); err != nil {
			b.Fatalf("populate Put: %v", err)
		}
	}
}

// BenchmarkSnapshotCreation measures engine.Snapshot()'s own wall-clock
// cost at increasing dataset sizes — directly quantifying the tradeoff
// documented in the README ("gathers every distinct key and calls Get
// per key" rather than a proper k-way merge): this should scale roughly
// linearly with key count, each key costing one full Get lookup, not
// sublinearly the way a genuine streaming merge would.
func BenchmarkSnapshotCreation(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 50_000} {
		b.Run(fmt.Sprintf("NumKeys=%d", n), func(b *testing.B) {
			eng, err := engine.Open(engine.Options{Dir: b.TempDir()})
			if err != nil {
				b.Fatal(err)
			}
			defer eng.Close()
			populateEngine(b, eng, n)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := eng.Snapshot(); err != nil {
					b.Fatalf("Snapshot: %v", err)
				}
			}
		})
	}
}

// --- TestSnapshotCatchUpTime: real cluster, real InstallSnapshot ------------

const (
	snapBenchTickInterval  = 50 * time.Millisecond
	snapBenchElectionTicks = 10
	snapBenchHeartbeatTick = 1
	snapBenchThreshold     = 50
)

type snapBenchNode struct {
	id      uint64
	server  *server.Server
	tr      *transport.Transport
	raft    *raft.Node
	eng     *engine.Engine
	dataDir string
}

func startSnapBenchCluster(t testing.TB, n int, basePort int) []*snapBenchNode {
	t.Helper()
	ids := make([]uint64, n)
	addrs := make(map[uint64]string, n)
	for i := 0; i < n; i++ {
		ids[i] = uint64(i + 1)
		addrs[ids[i]] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
	}

	nodes := make([]*snapBenchNode, n)
	for i, id := range ids {
		var peers []uint64
		peerAddrs := make(map[uint64]string)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
				peerAddrs[other] = addrs[other]
			}
		}

		dir := t.TempDir()
		nodes[i] = startSnapBenchNode(t, id, addrs[id], peers, peerAddrs, dir)
	}
	return nodes
}

// startSnapBenchNode opens (or reopens, if dir already has state) a full
// node — raft, transport, engine, server — matching cmd/kvstore's own
// buildComponents: a persisted snapshot found on open is restored into
// the engine and seeded into the server before Run(), exactly like a
// real restart.
func startSnapBenchNode(t testing.TB, id uint64, addr string, peers []uint64, peerAddrs map[uint64]string, dir string) *snapBenchNode {
	t.Helper()
	rn, snap, err := raft.OpenNode(raft.Config{
		ID: id, Peers: peers,
		ElectionTick: snapBenchElectionTicks, HeartbeatTick: snapBenchHeartbeatTick,
	}, filepath.Join(dir, "raft.wal"))
	if err != nil {
		t.Fatalf("OpenNode(%d): %v", id, err)
	}
	tr, err := transport.Listen(id, addr, peerAddrs)
	if err != nil {
		t.Fatalf("Listen(%d): %v", id, err)
	}
	eng, err := engine.Open(engine.Options{Dir: filepath.Join(dir, "kv")})
	if err != nil {
		t.Fatalf("engine.Open(%d): %v", id, err)
	}
	if snap != nil {
		if err := eng.RestoreSnapshot(snap.Data); err != nil {
			t.Fatalf("RestoreSnapshot(%d): %v", id, err)
		}
	}

	srv := server.New(rn, tr, eng, snapBenchTickInterval)
	srv.SetSnapshotThreshold(snapBenchThreshold)
	if snap != nil {
		srv.SeedAppliedIndex(snap.LastIncludedIndex)
	}
	go srv.Run()

	return &snapBenchNode{id: id, server: srv, tr: tr, raft: rn, eng: eng, dataDir: dir}
}

func stopSnapBenchNode(n *snapBenchNode) {
	n.server.Stop()
	n.tr.Close()
	n.raft.Close()
	n.eng.Close()
}

func waitForSnapBenchLeader(t testing.TB, nodes []*snapBenchNode, timeout time.Duration) *snapBenchNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *snapBenchNode
		count := 0
		var maxTerm uint64
		for _, n := range nodes {
			s := n.server.Status()
			if s.Role == raft.Leader {
				if s.Term > maxTerm {
					maxTerm = s.Term
					leader = n
					count = 1
				} else if s.Term == maxTerm {
					count++
				}
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timed out waiting for a single leader")
	return nil
}

// TestSnapshotCatchUpTime measures how long a real follower — killed
// outright, then restarted fresh from the same on-disk state, exactly
// like TestFailoverTime's leader kill — takes to catch back up once its
// leader has compacted its log well past what a normal AppendEntries
// replay could still cover, forcing a real InstallSnapshot exchange
// rather than ordinary replication.
func TestSnapshotCatchUpTime(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping snapshot catch-up timing in -short mode")
	}

	const trials = 5
	const clusterSize = 3
	// keysWhileDown is deliberately several multiples of
	// snapBenchThreshold, so the leader compacts past the follower's
	// last known position more than once before it's healed — a
	// single, borderline compaction wouldn't distinguish "caught up via
	// InstallSnapshot" from "got lucky and still had enough log left."
	const keysWhileDown = snapBenchThreshold * 6

	durations := make([]time.Duration, 0, trials)

	for trial := 0; trial < trials; trial++ {
		basePort := 24000 + trial*clusterSize
		nodes := startSnapBenchCluster(t, clusterSize, basePort)

		leader := waitForSnapBenchLeader(t, nodes, 3*time.Second)
		if err := leader.server.Put([]byte("warmup"), []byte("v")); err != nil {
			t.Fatalf("trial %d: warmup put failed: %v", trial, err)
		}

		var toKill *snapBenchNode
		for _, n := range nodes {
			if n.id != leader.id {
				toKill = n
				break
			}
		}
		dataDir := toKill.dataDir
		id := toKill.id
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+int(id-1))
		var peers []uint64
		peerAddrs := make(map[uint64]string)
		for _, n := range nodes {
			if n.id != id {
				peers = append(peers, n.id)
				peerAddrs[n.id] = fmt.Sprintf("127.0.0.1:%d", basePort+int(n.id-1))
			}
		}

		stopSnapBenchNode(toKill)

		for i := 0; i < keysWhileDown; i++ {
			key := fmt.Sprintf("k-%d-%d", trial, i)
			if err := leader.server.Put([]byte(key), []byte("v")); err != nil {
				t.Fatalf("trial %d: put %d while follower down failed: %v", trial, i, err)
			}
		}
		leaderStatus := leader.server.Status()

		start := time.Now()
		revived := startSnapBenchNode(t, id, addr, peers, peerAddrs, dataDir)

		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if revived.server.Status().CommitIndex >= leaderStatus.CommitIndex {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		elapsed := time.Since(start)
		got := revived.server.Status().CommitIndex
		if got < leaderStatus.CommitIndex {
			t.Fatalf("trial %d: follower only reached commitIndex %d, want at least %d (leader's) within the deadline", trial, got, leaderStatus.CommitIndex)
		}
		durations = append(durations, elapsed)
		t.Logf("trial %d: catch-up (restart -> commitIndex %d reached) took %v", trial, leaderStatus.CommitIndex, elapsed)

		stopSnapBenchNode(revived)
		for _, n := range nodes {
			if n.id != leader.id && n.id != id {
				stopSnapBenchNode(n)
			}
		}
		stopSnapBenchNode(leader)
	}

	reportSnapshotCatchUpStats(t, durations)
}

func reportSnapshotCatchUpStats(t *testing.T, durations []time.Duration) {
	sorted := append([]time.Duration(nil), durations...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	mean := sum / time.Duration(len(sorted))

	t.Logf("=== Snapshot catch-up time summary (n=%d trials) ===", len(sorted))
	t.Logf("min:  %v", sorted[0])
	t.Logf("mean: %v", mean)
	t.Logf("max:  %v", sorted[len(sorted)-1])
}
