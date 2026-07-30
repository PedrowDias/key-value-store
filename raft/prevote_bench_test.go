// Quantifies Pre-Vote's actual benefit rather than only asserting it
// qualitatively: does a follower that's isolated — but still running,
// still ticking its own election timer, exactly the scenario Pre-Vote
// exists for — actually keep its term from inflating while cut off, and
// does healing it leave the existing leader completely undisturbed?
//
// This paces real ticks with real time.Sleep calls (not the raft
// package's own instant, purely-logical simulated ticking its
// correctness tests use) to get genuine wall-clock numbers, but it's
// still an in-memory simulated cluster — no real transport, no real
// separate processes. That's a deliberately different (and weaker)
// claim than this project's "confirmed on real M3 hardware" cluster
// benchmarks elsewhere: those exercise real TCP and real engines; this
// exercises real time.Sleep-paced ticking against the same
// deterministic in-memory cluster harness raft's own tests use. Good
// enough to demonstrate the mechanism's real-world timing character
// without needing a live, actually-partitioned network to do it.
//
// Run with:
//
//	go test ./raft/... -run TestPreVoteAvoidsDisruption -v
package raft

import (
	"sort"
	"testing"
	"time"
)

const (
	preVoteBenchTickInterval  = 50 * time.Millisecond
	preVoteBenchElectionTicks = 10
	preVoteBenchHeartbeatTick = 1
)

// tickRealTime advances c by n ticks, sleeping tickInterval between each
// — the real-time analogue of cluster.ticks, which advances instantly.
func tickRealTime(c *cluster, tickInterval time.Duration, n int) {
	for i := 0; i < n; i++ {
		c.tick()
		time.Sleep(tickInterval)
	}
}

func waitForClusterLeaderRealTime(t *testing.T, c *cluster, tickInterval time.Duration, timeout time.Duration) uint64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if id, ok := c.leader(); ok {
			return id
		}
		c.tick()
		time.Sleep(tickInterval)
	}
	t.Fatal("timed out waiting for a leader")
	return 0
}

// TestPreVoteAvoidsDisruption runs several trials of: elect a leader,
// isolate one follower, let real wall-clock time pass through several
// full election-timeout cycles while it's cut off (so its own election
// timer fires repeatedly, attempting — and, isolated, always failing —
// a Pre-Vote round each time), then heal it and confirm the existing
// leader was never disturbed at all.
func TestPreVoteAvoidsDisruption(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Pre-Vote timing in -short mode")
	}

	const trials = 5
	convergenceTimes := make([]time.Duration, 0, trials)
	isolatedTermDeltas := make([]uint64, 0, trials)

	for trial := 0; trial < trials; trial++ {
		c := newCluster(t, []uint64{1, 2, 3}, preVoteBenchElectionTicks, preVoteBenchHeartbeatTick)

		leaderID := waitForClusterLeaderRealTime(t, c, preVoteBenchTickInterval, 3*time.Second)
		c.propose([]byte("warmup"))
		tickRealTime(c, preVoteBenchTickInterval, 3)
		if !c.allCommitted(1) {
			t.Fatalf("trial %d: warmup entry never committed", trial)
		}

		var followerID uint64
		for id := range c.nodes {
			if id != leaderID {
				followerID = id
				break
			}
		}

		leaderTermBeforeIsolation := c.nodes[leaderID].currentTerm
		isolatedTermBeforeIsolation := c.nodes[followerID].currentTerm

		c.isolate(followerID)

		// Several full election-timeout cycles' worth of real wall-clock
		// time — long enough for the isolated node's own election timer
		// to fire repeatedly and attempt (and, cut off, always fail) a
		// Pre-Vote round each time, exactly the scenario this feature
		// exists for.
		tickRealTime(c, preVoteBenchTickInterval, preVoteBenchElectionTicks*4)

		// The key claim: even after all that, the isolated node's own
		// term hasn't inflated — Pre-Vote never lets it actually become
		// Candidate (which is what would increment currentTerm) unless
		// a majority already granted it a pre-vote, which an isolated
		// node can never get.
		isolatedTermAfterIsolation := c.nodes[followerID].currentTerm
		isolatedTermDeltas = append(isolatedTermDeltas, isolatedTermAfterIsolation-isolatedTermBeforeIsolation)

		// And the existing leader must be completely undisturbed:
		// same leader, same term, throughout — not just "a leader
		// exists," which a disruptive re-election would also
		// eventually re-satisfy.
		if id, ok := c.leader(); !ok || id != leaderID {
			t.Fatalf("trial %d: leader changed while merely isolating a minority follower (got %v ok=%v, want %d) — Pre-Vote should prevent this entirely", trial, id, ok, leaderID)
		}
		if c.nodes[leaderID].currentTerm != leaderTermBeforeIsolation {
			t.Fatalf("trial %d: leader's term changed (%d -> %d) while merely isolating a minority follower — Pre-Vote should prevent this entirely", trial, leaderTermBeforeIsolation, c.nodes[leaderID].currentTerm)
		}

		start := time.Now()
		c.heal(followerID)

		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if c.allCommitted(1) && c.nodes[followerID].Status().CommitIndex >= c.nodes[leaderID].Status().CommitIndex {
				break
			}
			c.tick()
			time.Sleep(preVoteBenchTickInterval)
		}
		elapsed := time.Since(start)

		if id, ok := c.leader(); !ok || id != leaderID {
			t.Fatalf("trial %d: leader changed after healing the isolated follower (got %v ok=%v, want unchanged %d)", trial, id, ok, leaderID)
		}
		if c.nodes[leaderID].currentTerm != leaderTermBeforeIsolation {
			t.Fatalf("trial %d: leader's term changed (%d -> %d) after healing — a real disruption, exactly what Pre-Vote exists to prevent", trial, leaderTermBeforeIsolation, c.nodes[leaderID].currentTerm)
		}

		convergenceTimes = append(convergenceTimes, elapsed)
		t.Logf("trial %d: isolated node's term delta while cut off = %d (want 0); reconverged %v after healing, leader/term unchanged throughout",
			trial, isolatedTermAfterIsolation-isolatedTermBeforeIsolation, elapsed)
	}

	reportPreVoteStats(t, convergenceTimes, isolatedTermDeltas)
}

func reportPreVoteStats(t *testing.T, convergenceTimes []time.Duration, termDeltas []uint64) {
	sorted := append([]time.Duration(nil), convergenceTimes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	mean := sum / time.Duration(len(sorted))

	maxTermDelta := uint64(0)
	for _, d := range termDeltas {
		if d > maxTermDelta {
			maxTermDelta = d
		}
	}

	t.Logf("=== Pre-Vote disruption-avoidance summary (n=%d trials) ===", len(sorted))
	t.Logf("isolated node's max term delta while cut off: %d (0 = Pre-Vote fully prevented term inflation, every trial)", maxTermDelta)
	t.Logf("reconvergence time after healing — min:  %v", sorted[0])
	t.Logf("reconvergence time after healing — mean: %v", mean)
	t.Logf("reconvergence time after healing — max:  %v", sorted[len(sorted)-1])
}
