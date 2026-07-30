package raft

import "testing"

// --- InstallSnapshot: catching up a peer that's fallen too far behind ------

func TestSendAppendEntries_SwitchesToInstallSnapshotWhenPeerTooFarBehind(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.log = append(r.log,
		LogEntry{Term: 1, Index: 1, Data: []byte("a")},
		LogEntry{Term: 1, Index: 2, Data: []byte("b")},
	)
	r.commitIndex = 2
	r.becomeCandidate()
	r.becomeLeader()
	readyMessages(r) // drain the become-leader heartbeats

	if err := r.CreateSnapshot(2, []byte("snap-through-2")); err != nil {
		t.Fatal(err)
	}
	// becomeLeader() optimistically initializes nextIndex to
	// lastLogIndex+1 for every peer; simulate this peer never actually
	// having acked anything (e.g. it was unreachable this whole time) by
	// resetting it below the new snapshot boundary directly.
	r.nextIndex[2] = 1
	r.sendAppendEntries(2, 0)
	msgs := readyMessages(r)
	if len(msgs) != 1 || msgs[0].Type != MsgInstallSnapshot {
		t.Fatalf("messages = %+v, want exactly one InstallSnapshot", msgs)
	}
	if msgs[0].Snapshot.LastIncludedIndex != 2 || string(msgs[0].Snapshot.Data) != "snap-through-2" {
		t.Fatalf("Snapshot = %+v, want {Index:2 Data:snap-through-2}", msgs[0].Snapshot)
	}
}

func TestHandleInstallSnapshot_ReplacesLogAndSurfacesForApplication(t *testing.T) {
	r, _ := New(Config{ID: 2, Peers: []uint64{1}, ElectionTick: 10, HeartbeatTick: 1})
	// This follower has some of its own (now-stale, to-be-discarded) log.
	r.log = append(r.log, LogEntry{Term: 1, Index: 1, Data: []byte("stale")})

	snap := Snapshot{LastIncludedIndex: 5, LastIncludedTerm: 2, Data: []byte("real-snapshot-data")}
	r.Step(Message{Type: MsgInstallSnapshot, To: 2, From: 1, Term: 2, Snapshot: snap})

	if r.log[0].Index != 5 || r.log[0].Term != 2 {
		t.Fatalf("log[0] = %+v, want the new sentinel at (Index:5, Term:2)", r.log[0])
	}
	if len(r.log) != 1 {
		t.Fatalf("len(log) = %d, want 1 (just the new sentinel — the stale entry must be gone)", len(r.log))
	}
	if r.commitIndex != 5 {
		t.Fatalf("commitIndex = %d, want 5 (a snapshot's boundary is always at least committed)", r.commitIndex)
	}
	if r.role != Follower || r.leaderID != 1 {
		t.Fatalf("role=%v leaderID=%d, want Follower with leaderID 1", r.role, r.leaderID)
	}

	rd := r.Ready()
	if rd.SnapshotToApply == nil || string(rd.SnapshotToApply.Data) != "real-snapshot-data" {
		t.Fatalf("Ready().SnapshotToApply = %+v, want the received snapshot data", rd.SnapshotToApply)
	}
	if rd.Snapshot == nil || rd.Snapshot.LastIncludedIndex != 5 {
		t.Fatalf("Ready().Snapshot = %+v, want the storage layer to also be told to persist this boundary", rd.Snapshot)
	}

	msgs := readyMessages(r)
	var resp *Message
	for i := range msgs {
		if msgs[i].Type == MsgInstallSnapshotResponse {
			resp = &msgs[i]
		}
	}
	if resp == nil || resp.MatchIndex != 5 {
		t.Fatalf("expected an InstallSnapshotResponse with MatchIndex 5, got %+v", msgs)
	}
}

func TestHandleInstallSnapshot_StaleOrDuplicateAcksWithoutRegressing(t *testing.T) {
	r, _ := New(Config{ID: 2, Peers: []uint64{1}, ElectionTick: 10, HeartbeatTick: 1})
	r.log = append(r.log, LogEntry{Term: 1, Index: 1, Data: []byte("a")})
	r.commitIndex = 1
	if err := r.CreateSnapshot(1, []byte("already-have-this")); err != nil {
		t.Fatal(err)
	}
	readyMessages(r) // drain, doesn't matter for this test

	// An InstallSnapshot for an OLDER (or equal) boundary than what we
	// already have locally must not regress our state.
	stale := Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, Data: []byte("stale-attempt")}
	r.Step(Message{Type: MsgInstallSnapshot, To: 2, From: 1, Term: 1, Snapshot: stale})

	if string(r.snapshot.Data) != "already-have-this" {
		t.Fatalf("snapshot.Data = %q, want unchanged %q (a stale InstallSnapshot must not overwrite it)", r.snapshot.Data, "already-have-this")
	}
	rd := r.Ready()
	if rd.SnapshotToApply != nil {
		t.Fatalf("Ready().SnapshotToApply = %+v, want nil (nothing new to apply for a stale/duplicate snapshot)", rd.SnapshotToApply)
	}

	msgs := readyMessages(r)
	var resp *Message
	for i := range msgs {
		if msgs[i].Type == MsgInstallSnapshotResponse {
			resp = &msgs[i]
		}
	}
	if resp == nil || resp.MatchIndex != 1 {
		t.Fatalf("expected an ack reflecting our own current boundary (1), got %+v", msgs)
	}
}

func TestHandleInstallSnapshot_LowerTermRejected(t *testing.T) {
	r, _ := New(Config{ID: 2, Peers: []uint64{1}, ElectionTick: 10, HeartbeatTick: 1})
	r.currentTerm = 5
	r.log = append(r.log, LogEntry{Term: 1, Index: 1, Data: []byte("a")})

	snap := Snapshot{LastIncludedIndex: 10, LastIncludedTerm: 3, Data: []byte("data")}
	r.Step(Message{Type: MsgInstallSnapshot, To: 2, From: 1, Term: 3, Snapshot: snap})

	if len(r.log) != 2 {
		t.Fatalf("len(log) = %d, want unchanged 2 — a lower-term InstallSnapshot must be rejected outright", len(r.log))
	}
	if rd := r.Ready(); rd.SnapshotToApply != nil {
		t.Fatalf("Ready().SnapshotToApply = %+v, want nil", rd.SnapshotToApply)
	}
}

func TestHandleInstallSnapshotResponse_UpdatesNextAndMatchIndex(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	r.becomeLeader()
	readyMessages(r)

	r.Step(Message{Type: MsgInstallSnapshotResponse, To: 1, From: 2, Term: r.currentTerm, MatchIndex: 7})
	if r.matchIndex[2] != 7 {
		t.Fatalf("matchIndex[2] = %d, want 7", r.matchIndex[2])
	}
	if r.nextIndex[2] != 8 {
		t.Fatalf("nextIndex[2] = %d, want 8", r.nextIndex[2])
	}
}

func TestHandleInstallSnapshotResponse_HigherTermCausesStepDown(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	r.becomeLeader()
	term := r.currentTerm

	r.Step(Message{Type: MsgInstallSnapshotResponse, To: 1, From: 2, Term: term + 5, MatchIndex: 0})
	if r.role != Follower || r.currentTerm != term+5 {
		t.Fatalf("role=%v term=%d, want Follower at term %d", r.role, r.currentTerm, term+5)
	}
}

func TestHandleInstallSnapshotResponse_IgnoredIfNotLeader(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.Step(Message{Type: MsgInstallSnapshotResponse, To: 1, From: 2, Term: 0, MatchIndex: 5})
	if r.role != Follower {
		t.Fatalf("role = %v, want unchanged Follower", r.role)
	}
}

func TestSnapshot_LaggingPeerCatchesUpViaInstallSnapshotThenResumesNormalReplication(t *testing.T) {
	// The full, real scenario end to end: a follower isolated long
	// enough that the leader compacts past what it needs, healed, and
	// confirmed to catch up via a real InstallSnapshot exchange — then
	// continues via ordinary AppendEntries afterward, exactly as if it
	// had never fallen behind.
	c := newCluster(t, []uint64{1, 2, 3}, 10, 1)
	c.ticks(25)
	leaderID, ok := c.leader()
	if !ok {
		t.Fatal("expected a leader")
	}
	leader := c.nodes[leaderID]

	var laggingID uint64
	for id := range c.nodes {
		if id != leaderID {
			laggingID = id
			break
		}
	}
	c.isolate(laggingID)

	c.propose([]byte("first"))
	c.ticks(3)
	c.propose([]byte("second"))
	c.ticks(3)
	if !c.allCommitted(2) {
		t.Fatal("expected both entries to commit on the majority side while the lagging node is isolated")
	}

	if err := leader.CreateSnapshot(2, []byte("snapshot-through-2")); err != nil {
		t.Fatal(err)
	}

	c.heal(laggingID)
	c.ticks(30) // room for the InstallSnapshot round trip and catch-up to actually happen

	lagging := c.nodes[laggingID]
	if lagging.commitIndex < 2 {
		t.Fatalf("lagging node's commitIndex = %d, want at least 2 after catching up via InstallSnapshot", lagging.commitIndex)
	}
	if lagging.log[0].Index != leader.log[0].Index {
		t.Fatalf("lagging node's log[0] = %+v, leader's log[0] = %+v, want them to match (both reflect the same snapshot boundary)", lagging.log[0], leader.log[0])
	}

	// Normal replication must continue working afterward, exactly as if
	// this node had never fallen behind.
	c.propose([]byte("third"))
	c.ticks(5)
	if !c.allCommitted(3) {
		t.Fatal("expected normal replication to continue working for all nodes (including the just-caught-up one) after the snapshot exchange")
	}
	entries := lagging.Entries(2, 3)
	if len(entries) != 1 || string(entries[0].Data) != "third" {
		t.Fatalf("lagging node's Entries(2,3) = %+v, want [{Data: third}]", entries)
	}
}
