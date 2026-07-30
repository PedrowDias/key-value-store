// Failure-injection measurement: how long does it take a real 3-node
// cluster to elect a new leader and resume accepting writes after the
// current leader is killed outright (a real crash — this test stops the
// process's goroutines and closes its resources abruptly, without any
// graceful handoff)?
//
// Run with:
//
//	go test ./bench/... -run TestFailoverTime -v
package bench

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/PedrowDias/key-value-store/engine"
	"github.com/PedrowDias/key-value-store/raft"
	"github.com/PedrowDias/key-value-store/server"
	"github.com/PedrowDias/key-value-store/transport"
)

// failoverTickInterval and failoverElectionTicks mirror realistic
// production-ish tuning (not the very fast ticks the project's own unit
// tests use for speed) so the measured failover time reflects something
// closer to what an operator would actually configure: 50ms ticks, a
// 10-20 tick (500ms-1s) election timeout.
const (
	failoverTickInterval  = 50 * time.Millisecond
	failoverElectionTicks = 10
	failoverHeartbeatTick = 1
)

type failoverNode struct {
	id     uint64
	server *server.Server
	tr     *transport.Transport
	raft   *raft.Node
	eng    *engine.Engine
}

func startFailoverCluster(t testing.TB, n int, basePort int) []*failoverNode {
	t.Helper()
	ids := make([]uint64, n)
	addrs := make(map[uint64]string, n)
	for i := 0; i < n; i++ {
		ids[i] = uint64(i + 1)
		addrs[ids[i]] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
	}

	nodes := make([]*failoverNode, n)
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
		rn, _, err := raft.OpenNode(raft.Config{
			ID: id, Peers: peers,
			ElectionTick: failoverElectionTicks, HeartbeatTick: failoverHeartbeatTick,
		}, filepath.Join(dir, "raft.wal"))
		if err != nil {
			t.Fatalf("OpenNode(%d): %v", id, err)
		}
		tr, err := transport.Listen(id, addrs[id], peerAddrs)
		if err != nil {
			t.Fatalf("Listen(%d): %v", id, err)
		}
		eng, err := engine.Open(engine.Options{Dir: filepath.Join(dir, "kv")})
		if err != nil {
			t.Fatalf("engine.Open(%d): %v", id, err)
		}

		srv := server.New(rn, tr, eng, failoverTickInterval)
		nodes[i] = &failoverNode{id: id, server: srv, tr: tr, raft: rn, eng: eng}
		go srv.Run()
	}
	return nodes
}

func stopFailoverNode(n *failoverNode) {
	n.server.Stop()
	n.tr.Close()
	n.raft.Close()
	n.eng.Close()
}

func waitForFailoverLeader(t testing.TB, nodes []*failoverNode, timeout time.Duration) *failoverNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *failoverNode
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

// TestFailoverTime runs many independent trials of "kill the leader, time
// how long until the cluster can commit a write again," and reports
// summary statistics. It's a Test (not a Benchmark) because the metric
// of interest is a single measured duration per trial with a fixed trial
// count, not a throughput rate — closer in spirit to a load-test report
// than a microbenchmark.
func TestFailoverTime(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping failover timing in -short mode")
	}

	const trials = 10
	const clusterSize = 3
	durations := make([]time.Duration, 0, trials)

	for trial := 0; trial < trials; trial++ {
		basePort := 23000 + trial*clusterSize
		nodes := startFailoverCluster(t, clusterSize, basePort)

		leader := waitForFailoverLeader(t, nodes, 3*time.Second)

		// Confirm the cluster is actually serving before measuring
		// anything — a cold start's own election shouldn't be counted
		// as "failover time."
		if err := leader.server.Put([]byte("warmup"), []byte("v")); err != nil {
			t.Fatalf("trial %d: warmup put failed: %v", trial, err)
		}

		var remaining []*failoverNode
		for _, n := range nodes {
			if n.id != leader.id {
				remaining = append(remaining, n)
			}
		}

		start := time.Now()
		stopFailoverNode(leader) // the abrupt "crash"

		newLeader := waitForFailoverLeader(t, remaining, 5*time.Second)
		// "Failover complete" means the NEW leader can actually commit a
		// write again, not just that some node calls itself Leader — a
		// freshly elected leader briefly can't commit until it confirms
		// a current-term entry on a majority (the Figure 8 rule), so this
		// is the more meaningful endpoint.
		if err := newLeader.server.Put([]byte("post-failover"), []byte("v")); err != nil {
			t.Fatalf("trial %d: post-failover put failed: %v", trial, err)
		}
		elapsed := time.Since(start)
		durations = append(durations, elapsed)

		for _, n := range remaining {
			stopFailoverNode(n)
		}
		t.Logf("trial %d: failover + first successful write took %v", trial, elapsed)
	}

	reportFailoverStats(t, durations)
}

func reportFailoverStats(t *testing.T, durations []time.Duration) {
	sorted := append([]time.Duration(nil), durations...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	mean := sum / time.Duration(len(sorted))
	p50 := percentile(sorted, 50)
	p99 := percentile(sorted, 99)

	t.Logf("=== Failover time summary (n=%d trials) ===", len(sorted))
	t.Logf("min:  %v", sorted[0])
	t.Logf("mean: %v", mean)
	t.Logf("p50:  %v", p50)
	t.Logf("p99:  %v", p99)
	t.Logf("max:  %v", sorted[len(sorted)-1])
}

// percentile returns the p-th percentile (0-100) of an already-sorted
// slice, using nearest-rank (simple and adequate for a small trial count
// like this — no need for interpolation).
func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
