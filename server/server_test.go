package server

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PedrowDias/key-value-store/engine"
	"github.com/PedrowDias/key-value-store/raft"
	"github.com/PedrowDias/key-value-store/transport"
)

// Tick tuning for tests: fast enough that elections and replication
// resolve in well under a second, generous enough not to be flaky under
// test-machine scheduling jitter.
const (
	testTickInterval  = 10 * time.Millisecond
	testElectionTicks = 10 // randomized to [10, 20) ticks => 100-200ms
	testHeartbeatTick = 1  // every tick => 10ms heartbeats
)

// testPortBase hands out non-overlapping port ranges to each test that
// needs one, avoiding both the Listen-before-you-know-your-peers'-
// addresses chicken-and-egg problem (fixed, known-upfront addresses
// sidestep it entirely) and cross-test port collisions.
var testPortBase int32 = 21000

func nextPortRange(n int) []int {
	base := atomic.AddInt32(&testPortBase, int32(n))
	ports := make([]int, n)
	for i := 0; i < n; i++ {
		ports[i] = int(base) + i
	}
	return ports
}

type testNode struct {
	id     uint64
	server *Server
}

// newTestCluster builds and starts n fully wired nodes (real TCP
// transport, real persistent raft.Node, real storage engine, each in its
// own temp directory) and returns them already running (Run in its own
// goroutine). Cleanup (Stop + Close everything) is registered via
// t.Cleanup.
func newTestCluster(t *testing.T, n int) []*testNode {
	t.Helper()
	ports := nextPortRange(n)
	ids := make([]uint64, n)
	addrs := make(map[uint64]string, n)
	for i := 0; i < n; i++ {
		ids[i] = uint64(i + 1)
		addrs[ids[i]] = fmt.Sprintf("127.0.0.1:%d", ports[i])
	}

	nodes := make([]*testNode, n)
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
		rn, err := raft.OpenNode(raft.Config{
			ID: id, Peers: peers,
			ElectionTick: testElectionTicks, HeartbeatTick: testHeartbeatTick,
		}, filepath.Join(dir, "raft.wal"))
		if err != nil {
			t.Fatalf("OpenNode(%d): %v", id, err)
		}
		tr, err := transport.Listen(id, addrs[id], peerAddrs)
		if err != nil {
			t.Fatalf("Listen(%d): %v", id, err)
		}
		eng, err := engine.Open(engine.Options{Dir: filepath.Join(dir, "data")})
		if err != nil {
			t.Fatalf("engine.Open(%d): %v", id, err)
		}

		srv := New(rn, tr, eng, testTickInterval)
		nodes[i] = &testNode{id: id, server: srv}
		go srv.Run()

		t.Cleanup(func() {
			srv.Stop()
			tr.Close()
			rn.Close()
			eng.Close()
		})
	}
	return nodes
}

// waitForLeader polls until exactly one node reports itself Leader (at
// the highest term seen), or fails the test after timeout.
func waitForLeader(t *testing.T, nodes []*testNode, timeout time.Duration) *testNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *testNode
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
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for a single leader to emerge")
	return nil
}

// waitFor polls cond until it returns true or timeout elapses, failing
// the test (with msg) if it never does.
func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

// --- Basic single-node operation -----------------------------------------

func TestServer_SingleNode_PutGet(t *testing.T) {
	nodes := newTestCluster(t, 1)
	leader := waitForLeader(t, nodes, 2*time.Second)

	if err := leader.server.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	val, found, err := leader.server.Get([]byte("k"))
	if err != nil || !found || string(val) != "v" {
		t.Fatalf("Get(k) = %q found=%v err=%v, want v true nil", val, found, err)
	}
}

func TestServer_SingleNode_Delete(t *testing.T) {
	nodes := newTestCluster(t, 1)
	leader := waitForLeader(t, nodes, 2*time.Second)

	leader.server.Put([]byte("k"), []byte("v"))
	if err := leader.server.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, found, err := leader.server.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected not found after delete")
	}
}

// --- Multi-node replication -----------------------------------------------

func TestServer_ThreeNodeCluster_PutReplicatesToAllNodes(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader := waitForLeader(t, nodes, 2*time.Second)

	if err := leader.server.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	for _, n := range nodes {
		node := n
		waitFor(t, 2*time.Second, fmt.Sprintf("node %d to have key k", node.id), func() bool {
			val, found, err := node.server.Get([]byte("k"))
			return err == nil && found && string(val) == "v"
		})
	}
}

func TestServer_ThreeNodeCluster_DeleteReplicates(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader := waitForLeader(t, nodes, 2*time.Second)

	if err := leader.server.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		node := n
		waitFor(t, 2*time.Second, "initial put to replicate", func() bool {
			_, found, _ := node.server.Get([]byte("k"))
			return found
		})
	}

	if err := leader.server.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		node := n
		waitFor(t, 2*time.Second, fmt.Sprintf("node %d to reflect delete", node.id), func() bool {
			_, found, _ := node.server.Get([]byte("k"))
			return !found
		})
	}
}

func TestServer_MultipleSequentialPutsAllReplicate(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader := waitForLeader(t, nodes, 2*time.Second)

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("k%d", i))
		val := []byte(fmt.Sprintf("v%d", i))
		if err := leader.server.Put(key, val); err != nil {
			t.Fatalf("Put(%s): %v", key, err)
		}
	}

	for _, n := range nodes {
		node := n
		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("k%d", i))
			want := fmt.Sprintf("v%d", i)
			waitFor(t, 2*time.Second, fmt.Sprintf("node %d key %s", node.id, key), func() bool {
				val, found, _ := node.server.Get(key)
				return found && string(val) == want
			})
		}
	}
}

// --- Errors -----------------------------------------------------------------

func TestServer_Put_OnFollowerReturnsError(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader := waitForLeader(t, nodes, 2*time.Second)

	var follower *testNode
	for _, n := range nodes {
		if n.id != leader.id {
			follower = n
			break
		}
	}

	err := follower.server.Put([]byte("k"), []byte("v"))
	if err == nil {
		t.Fatal("expected an error putting through a follower")
	}
}

// --- Linearizable reads (ReadIndex) ------------------------------------------

func TestServer_SingleNode_LinearizableGet(t *testing.T) {
	nodes := newTestCluster(t, 1)
	leader := waitForLeader(t, nodes, 2*time.Second)

	if err := leader.server.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	val, found, err := leader.server.LinearizableGet([]byte("k"))
	if err != nil || !found || string(val) != "v" {
		t.Fatalf("LinearizableGet(k) = %q found=%v err=%v, want v true nil", val, found, err)
	}
}

func TestServer_LinearizableGet_NotFoundKey(t *testing.T) {
	nodes := newTestCluster(t, 1)
	leader := waitForLeader(t, nodes, 2*time.Second)

	_, found, err := leader.server.LinearizableGet([]byte("never-written"))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected not found for a key that was never written")
	}
}

func TestServer_ThreeNodeCluster_LinearizableGetOnLeaderReflectsCommittedWrite(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader := waitForLeader(t, nodes, 2*time.Second)

	if err := leader.server.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Unlike Get (which might need a wait-and-retry against a follower
	// that hasn't caught up yet), LinearizableGet against the LEADER
	// should see an already-committed write immediately, with no
	// polling needed — that's the whole point of the guarantee.
	val, found, err := leader.server.LinearizableGet([]byte("k"))
	if err != nil || !found || string(val) != "v" {
		t.Fatalf("LinearizableGet(k) on the leader = %q found=%v err=%v, want v true nil", val, found, err)
	}
}

func TestServer_LinearizableGet_OnFollowerReturnsError(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader := waitForLeader(t, nodes, 2*time.Second)

	var follower *testNode
	for _, n := range nodes {
		if n.id != leader.id {
			follower = n
			break
		}
	}

	_, _, err := follower.server.LinearizableGet([]byte("k"))
	if err == nil {
		t.Fatal("expected an error calling LinearizableGet through a follower")
	}
}

func TestServer_LinearizableGet_TimesOutWhenMajorityUnreachable(t *testing.T) {
	orig := proposeTimeout
	proposeTimeout = 300 * time.Millisecond
	defer func() { proposeTimeout = orig }()

	nodes := newTestCluster(t, 3)
	leader := waitForLeader(t, nodes, 2*time.Second)

	// Stop the two followers so the leader can never get a majority of
	// acks confirming its continued leadership — the read can never be
	// confirmed.
	for _, n := range nodes {
		if n.id != leader.id {
			n.server.Stop()
		}
	}

	_, _, err := leader.server.LinearizableGet([]byte("k"))
	if err == nil {
		t.Fatal("expected LinearizableGet to fail (timeout) when a majority is unreachable")
	}
}

// --- Leader failover ---------------------------------------------------------

func TestServer_LeaderFailover_ClusterContinuesServing(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader := waitForLeader(t, nodes, 2*time.Second)

	if err := leader.server.Put([]byte("before"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		node := n
		waitFor(t, 2*time.Second, "initial put to replicate before failover", func() bool {
			_, found, _ := node.server.Get([]byte("before"))
			return found
		})
	}

	// Stop the leader entirely (as if it crashed).
	leader.server.Stop()

	var remaining []*testNode
	for _, n := range nodes {
		if n.id != leader.id {
			remaining = append(remaining, n)
		}
	}

	newLeader := waitForLeader(t, remaining, 3*time.Second)
	if newLeader.id == leader.id {
		t.Fatal("expected a different node to become the new leader")
	}

	if err := newLeader.server.Put([]byte("after"), []byte("v2")); err != nil {
		t.Fatalf("Put through new leader: %v", err)
	}
	for _, n := range remaining {
		node := n
		waitFor(t, 2*time.Second, fmt.Sprintf("node %d to have post-failover key", node.id), func() bool {
			val, found, _ := node.server.Get([]byte("after"))
			return found && string(val) == "v2"
		})
		// The pre-failover write must also still be there — failover
		// must not lose already-committed data.
		val, found, _ := node.server.Get([]byte("before"))
		if !found || string(val) != "v1" {
			t.Fatalf("node %d lost pre-failover data: found=%v val=%q", node.id, found, val)
		}
	}
}

// --- Propose timeout ----------------------------------------------------------

func TestServer_ProposeTimesOutWhenMajorityUnreachable(t *testing.T) {
	orig := proposeTimeout
	proposeTimeout = 300 * time.Millisecond
	defer func() { proposeTimeout = orig }()

	nodes := newTestCluster(t, 3)
	leader := waitForLeader(t, nodes, 2*time.Second)

	// Stop the two followers entirely so the leader can never get an
	// AppendEntriesResponse acknowledging the new entry — it can never
	// reach a majority, so the proposal can never commit.
	for _, n := range nodes {
		if n.id != leader.id {
			n.server.Stop()
		}
	}

	err := leader.server.Put([]byte("k"), []byte("v"))
	if err == nil {
		t.Fatal("expected Put to fail (timeout) when a majority is unreachable")
	}
}
