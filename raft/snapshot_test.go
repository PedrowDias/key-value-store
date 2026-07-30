package raft

import "testing"

// --- CreateSnapshot: local log compaction -----------------------------------

func TestCreateSnapshot_RejectsIndexBeyondCommitIndex(t *testing.T) {
	r, _ := New(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1})
	r.log = append(r.log, LogEntry{Term: 1, Index: 1, Data: []byte("a")})
	r.commitIndex = 0 // deliberately behind lastLogIndex

	err := r.CreateSnapshot(1, []byte("snap"))
	if err == nil {
		t.Fatal("expected CreateSnapshot to reject an index beyond commitIndex")
	}
}

func TestCreateSnapshot_RejectsIndexNotNewerThanExistingBoundary(t *testing.T) {
	r, _ := New(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1})
	r.log = append(r.log, LogEntry{Term: 1, Index: 1, Data: []byte("a")})
	r.commitIndex = 1

	if err := r.CreateSnapshot(1, []byte("snap")); err != nil {
		t.Fatal(err)
	}
	// Same index again: no longer newer than the boundary just set.
	if err := r.CreateSnapshot(1, []byte("snap2")); err == nil {
		t.Fatal("expected CreateSnapshot to reject an index equal to the existing boundary")
	}
	// A genuinely EARLIER index: also rejected.
	if err := r.CreateSnapshot(0, []byte("snap3")); err == nil {
		t.Fatal("expected CreateSnapshot to reject an index behind the existing boundary")
	}
}

func TestCreateSnapshot_TruncatesLogAndPreservesLaterEntries(t *testing.T) {
	r, _ := New(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1})
	r.log = append(r.log,
		LogEntry{Term: 1, Index: 1, Data: []byte("a")},
		LogEntry{Term: 1, Index: 2, Data: []byte("b")},
		LogEntry{Term: 2, Index: 3, Data: []byte("c")},
	)
	r.commitIndex = 3

	if err := r.CreateSnapshot(2, []byte("snap-through-2")); err != nil {
		t.Fatal(err)
	}

	if r.log[0].Index != 2 || r.log[0].Term != 1 {
		t.Fatalf("log[0] = %+v, want the new sentinel at (Index:2, Term:1)", r.log[0])
	}
	if len(r.log) != 2 {
		t.Fatalf("len(log) = %d, want 2 (sentinel + entry 3)", len(r.log))
	}
	if r.log[1].Index != 3 || string(r.log[1].Data) != "c" {
		t.Fatalf("log[1] = %+v, want entry 3 (Data: c) preserved", r.log[1])
	}
	if r.lastLogIndex() != 3 {
		t.Fatalf("lastLogIndex() = %d, want 3 (unaffected by the snapshot)", r.lastLogIndex())
	}
	if string(r.snapshot.Data) != "snap-through-2" || r.snapshot.LastIncludedIndex != 2 || r.snapshot.LastIncludedTerm != 1 {
		t.Fatalf("snapshot = %+v, want {Index:2 Term:1 Data:snap-through-2}", r.snapshot)
	}
}

func TestCreateSnapshot_TermAtAndEntriesRespectTheNewBoundary(t *testing.T) {
	r, _ := New(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1})
	r.log = append(r.log,
		LogEntry{Term: 1, Index: 1, Data: []byte("a")},
		LogEntry{Term: 1, Index: 2, Data: []byte("b")},
		LogEntry{Term: 2, Index: 3, Data: []byte("c")},
	)
	r.commitIndex = 3
	if err := r.CreateSnapshot(2, []byte("snap")); err != nil {
		t.Fatal(err)
	}

	// Exactly at the boundary: still available, real term.
	if got := r.termAt(2); got != 1 {
		t.Fatalf("termAt(2) = %d, want 1 (the boundary entry's real term)", got)
	}
	// Before the boundary: compacted away.
	if got := r.termAt(1); got != 0 {
		t.Fatalf("termAt(1) = %d, want 0 (compacted away)", got)
	}
	// After the boundary: unaffected.
	if got := r.termAt(3); got != 2 {
		t.Fatalf("termAt(3) = %d, want 2", got)
	}

	// Entries asked for starting before the boundary must clamp up
	// rather than panic or return something wrong.
	entries := r.Entries(0, 3)
	if len(entries) != 1 || string(entries[0].Data) != "c" {
		t.Fatalf("Entries(0,3) after compacting through 2 = %+v, want just entry 3 (Data: c)", entries)
	}
}

func TestCreateSnapshot_ReportedViaReadyThenClearedAfterAdvance(t *testing.T) {
	r, _ := New(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1})
	r.log = append(r.log, LogEntry{Term: 1, Index: 1, Data: []byte("a")})
	r.commitIndex = 1

	if err := r.CreateSnapshot(1, []byte("snap")); err != nil {
		t.Fatal(err)
	}
	rd := r.Ready()
	if rd.Snapshot == nil || rd.Snapshot.LastIncludedIndex != 1 {
		t.Fatalf("Ready().Snapshot = %+v, want a snapshot at index 1", rd.Snapshot)
	}
	r.Advance()
	if rd := r.Ready(); rd.Snapshot != nil {
		t.Fatalf("Ready().Snapshot = %+v after Advance, want nil (already reported)", rd.Snapshot)
	}
}

func TestCreateSnapshot_ReplicationContinuesForAnAlreadyCaughtUpPeer(t *testing.T) {
	// The common, expected case: a peer that's already caught up (or
	// gets caught up via normal replication after the snapshot) is
	// completely unaffected by the leader having compacted its own log
	// — snapshotting locally must not disrupt ongoing replication for
	// anyone who doesn't actually need the discarded entries.
	c := newCluster(t, []uint64{1, 2, 3}, 10, 1)
	c.ticks(25)
	leaderID, ok := c.leader()
	if !ok {
		t.Fatal("expected a leader")
	}
	leader := c.nodes[leaderID]

	c.propose([]byte("first"))
	c.ticks(3)
	if !c.allCommitted(1) {
		t.Fatal("expected the first entry to commit and replicate before snapshotting")
	}

	if err := leader.CreateSnapshot(1, []byte("snap-through-1")); err != nil {
		t.Fatal(err)
	}

	// More proposals after the snapshot must still replicate normally to
	// every (already caught-up) node.
	c.propose([]byte("second"))
	c.ticks(3)
	if !c.allCommitted(2) {
		t.Fatal("expected the second entry to commit and replicate to all nodes after the leader's local snapshot")
	}
	for id, r := range c.nodes {
		entries := r.Entries(1, 2)
		if len(entries) != 1 || string(entries[0].Data) != "second" {
			t.Fatalf("node %d: Entries(1,2) = %+v, want [{Data: second}]", id, entries)
		}
	}
}
