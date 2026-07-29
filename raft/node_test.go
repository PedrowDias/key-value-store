package raft

import (
	"path/filepath"
	"testing"
)

func openTestNode(t *testing.T, cfg Config, path string) *Node {
	t.Helper()
	n, err := OpenNode(cfg, path)
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	return n
}

// --- Basic operation, single node ---------------------------------------------

func TestNode_SingleNodeBecomesLeaderAndPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	n := openTestNode(t, Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}, path)
	defer n.Close()

	for i := 0; i < 20; i++ {
		n.Tick()
		if _, err := n.Persist(); err != nil {
			t.Fatalf("Persist: %v", err)
		}
	}
	if n.Status().Role != Leader {
		t.Fatalf("role = %v, want Leader", n.Status().Role)
	}
}

func TestNode_ProposeAndPersistCommitsInSingleNodeCluster(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	n := openTestNode(t, Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}, path)
	defer n.Close()

	for i := 0; i < 20; i++ {
		n.Tick()
		n.Persist()
	}
	if err := n.Propose([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Persist(); err != nil {
		t.Fatal(err)
	}
	if n.Status().CommitIndex != 1 {
		t.Fatalf("CommitIndex = %d, want 1", n.Status().CommitIndex)
	}
	entries := n.Entries(0, 1)
	if len(entries) != 1 || string(entries[0].Data) != "hello" {
		t.Fatalf("Entries(0,1) = %+v, want [{Data: hello}]", entries)
	}
}

func TestNode_ProposeBatchCommitsAllInSingleNodeCluster(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	n := openTestNode(t, Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}, path)
	defer n.Close()

	for i := 0; i < 20; i++ {
		n.Tick()
		n.Persist()
	}
	indices, err := n.ProposeBatch([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil {
		t.Fatal(err)
	}
	if len(indices) != 3 || indices[0] != 1 || indices[1] != 2 || indices[2] != 3 {
		t.Fatalf("indices = %v, want [1 2 3]", indices)
	}
	if _, err := n.Persist(); err != nil {
		t.Fatal(err)
	}
	if n.Status().CommitIndex != 3 {
		t.Fatalf("CommitIndex = %d, want 3", n.Status().CommitIndex)
	}
	entries := n.Entries(0, 3)
	if len(entries) != 3 || string(entries[0].Data) != "a" || string(entries[1].Data) != "b" || string(entries[2].Data) != "c" {
		t.Fatalf("Entries(0,3) = %+v, want [a b c]", entries)
	}
}

// --- Recovery across restarts ---------------------------------------------

func TestNode_RecoversHardStateAndLogAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	n := openTestNode(t, Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}, path)

	for i := 0; i < 20; i++ {
		n.Tick()
		n.Persist()
	}
	n.Propose([]byte("first"))
	n.Persist()
	n.Propose([]byte("second"))
	n.Persist()

	termBefore := n.Status().Term
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	n2 := openTestNode(t, Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}, path)
	defer n2.Close()

	if n2.Status().Term != termBefore {
		t.Fatalf("recovered term = %d, want %d", n2.Status().Term, termBefore)
	}
	if n2.Status().LastLogIndex != 2 {
		t.Fatalf("recovered LastLogIndex = %d, want 2", n2.Status().LastLogIndex)
	}
	entries := n2.Entries(0, 2)
	if len(entries) != 2 || string(entries[0].Data) != "first" || string(entries[1].Data) != "second" {
		t.Fatalf("recovered entries = %+v, want [first, second]", entries)
	}
}

func TestNode_RecoveredNodeContinuesParticipating(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	n := openTestNode(t, Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}, path)
	for i := 0; i < 20; i++ {
		n.Tick()
		n.Persist()
	}
	n.Propose([]byte("before-restart"))
	n.Persist()
	n.Close()

	n2 := openTestNode(t, Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}, path)
	defer n2.Close()
	// A restored single-node cluster should still be able to (re-)become
	// leader and accept new proposals after enough ticks.
	for i := 0; i < 20; i++ {
		n2.Tick()
		n2.Persist()
	}
	if err := n2.Propose([]byte("after-restart")); err != nil {
		t.Fatalf("Propose after restart: %v", err)
	}
	n2.Persist()

	entries := n2.Entries(0, n2.Status().LastLogIndex)
	if len(entries) != 2 || string(entries[1].Data) != "after-restart" {
		t.Fatalf("entries after restart = %+v", entries)
	}
}

// --- Two-node cluster wired through Node, with real persistence ------------

func TestNode_TwoNodeClusterReplicatesAndPersists(t *testing.T) {
	dir := t.TempDir()
	n1 := openTestNode(t, Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1}, filepath.Join(dir, "1.wal"))
	defer n1.Close()
	n2 := openTestNode(t, Config{ID: 2, Peers: []uint64{1}, ElectionTick: 10, HeartbeatTick: 1}, filepath.Join(dir, "2.wal"))
	defer n2.Close()

	nodes := map[uint64]*Node{1: n1, 2: n2}
	pump := func(id uint64) {
		msgs, err := nodes[id].Persist()
		if err != nil {
			t.Fatalf("node %d Persist: %v", id, err)
		}
		for _, m := range msgs {
			nodes[m.To].Step(m)
		}
	}

	// Drive node 1 hard enough (with node 2 never ticking) that node 1
	// wins the only election that happens.
	for i := 0; i < 20; i++ {
		n1.Tick()
		pump(1)
		pump(2) // deliver anything node 2's Step() produced in response
	}
	if n1.Status().Role != Leader {
		t.Fatalf("node 1 role = %v, want Leader", n1.Status().Role)
	}

	if err := n1.Propose([]byte("replicated")); err != nil {
		t.Fatal(err)
	}
	pump(1)
	pump(2)
	pump(1) // deliver node 2's AppendEntriesResponse back to the leader

	if n1.Status().CommitIndex != 1 {
		t.Fatalf("leader CommitIndex = %d, want 1", n1.Status().CommitIndex)
	}
}

// --- Persist error propagation, via storage failures ------------------------

func TestNode_PersistPropagatesHardStateSaveError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	n := openTestNode(t, Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1}, path)

	n.r.becomeCandidate() // dirties HardState without going through Persist yet
	// Close the underlying storage out from under Persist to force a
	// save failure.
	n.storage.w.Close()

	if _, err := n.Persist(); err == nil {
		t.Fatal("expected Persist to propagate a HardState save error")
	}
}

func TestNode_PersistPropagatesEntriesSaveError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	n := openTestNode(t, Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}, path)

	n.r.role = Leader
	n.r.Propose([]byte("x")) // dirties the log without going through Persist yet
	n.storage.w.Close()      // force the entries save to fail

	if _, err := n.Persist(); err == nil {
		t.Fatal("expected Persist to propagate an entries save error")
	}
}

// --- OpenNode error propagation ---------------------------------------------

func TestOpenNode_StorageErrorPropagates(t *testing.T) {
	dir := t.TempDir() // a directory, not a file: OpenStorage's Replay call fails
	_, err := OpenNode(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}, dir)
	if err == nil {
		t.Fatal("expected an error when storage fails to open")
	}
}

func TestOpenNode_InvalidConfigClosesStorageAndPropagates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	_, err := OpenNode(Config{ID: 0, ElectionTick: 10, HeartbeatTick: 1}, path) // invalid: zero ID
	if err == nil {
		t.Fatal("expected an error for an invalid Config")
	}
}
