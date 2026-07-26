package raft

import (
	"testing"
)

// --- Config validation --------------------------------------------------------

func TestNew_RejectsZeroID(t *testing.T) {
	_, err := New(Config{ID: 0, ElectionTick: 10, HeartbeatTick: 1})
	if err == nil {
		t.Fatal("expected an error for a zero ID")
	}
}

func TestNew_RejectsZeroPeerID(t *testing.T) {
	_, err := New(Config{ID: 1, Peers: []uint64{0}, ElectionTick: 10, HeartbeatTick: 1})
	if err == nil {
		t.Fatal("expected an error for a zero peer ID")
	}
}

func TestNew_RejectsSelfInPeers(t *testing.T) {
	_, err := New(Config{ID: 1, Peers: []uint64{1, 2}, ElectionTick: 10, HeartbeatTick: 1})
	if err == nil {
		t.Fatal("expected an error when Peers includes the node's own ID")
	}
}

func TestNew_RejectsNonPositiveTicks(t *testing.T) {
	if _, err := New(Config{ID: 1, ElectionTick: 0, HeartbeatTick: 1}); err == nil {
		t.Fatal("expected an error for ElectionTick <= 0")
	}
	if _, err := New(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 0}); err == nil {
		t.Fatal("expected an error for HeartbeatTick <= 0")
	}
}

func TestNew_RejectsElectionTickTooCloseToHeartbeat(t *testing.T) {
	_, err := New(Config{ID: 1, ElectionTick: 5, HeartbeatTick: 3})
	if err == nil {
		t.Fatal("expected an error when ElectionTick is less than 2x HeartbeatTick")
	}
}

func TestNew_ValidConfigSucceeds(t *testing.T) {
	r, err := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	s := r.Status()
	if s.Role != Follower || s.Term != 0 || s.ID != 1 {
		t.Fatalf("initial status = %+v, want Follower/term 0/id 1", s)
	}
}

// --- Single-node election ------------------------------------------------------

func TestElection_SingleNodeBecomesLeaderOnTimeout(t *testing.T) {
	c := newCluster(t, []uint64{1}, 10, 1)
	if _, ok := c.leader(); ok {
		t.Fatal("expected no leader before any ticks")
	}
	c.ticks(20) // past the max possible randomized timeout (up to 2*10)
	id, ok := c.leader()
	if !ok || id != 1 {
		t.Fatalf("leader() = %d, %v; want 1, true", id, ok)
	}
}

// --- Multi-node election --------------------------------------------------------

func TestElection_ThreeNodeClusterElectsExactlyOneLeader(t *testing.T) {
	c := newCluster(t, []uint64{1, 2, 3}, 10, 1)
	c.ticks(25)
	id, ok := c.leader()
	if !ok {
		t.Fatal("expected exactly one leader to emerge")
	}
	if _, exists := c.nodes[id]; !exists {
		t.Fatalf("leader id %d isn't a known node", id)
	}
	// Every other node should be a Follower at the same term.
	leaderTerm := c.nodes[id].Status().Term
	for nid, r := range c.nodes {
		if nid == id {
			continue
		}
		s := r.Status()
		if s.Role != Follower {
			t.Fatalf("node %d has role %v, want Follower", nid, s.Role)
		}
		if s.Term != leaderTerm {
			t.Fatalf("node %d term = %d, want %d (leader's term)", nid, s.Term, leaderTerm)
		}
	}
}

func TestElection_FiveNodeClusterElectsExactlyOneLeader(t *testing.T) {
	c := newCluster(t, []uint64{1, 2, 3, 4, 5}, 10, 1)
	c.ticks(30)
	if _, ok := c.leader(); !ok {
		t.Fatal("expected exactly one leader to emerge in a 5-node cluster")
	}
}

func TestElection_LeaderSendsHeartbeats_FollowersDontReelect(t *testing.T) {
	c := newCluster(t, []uint64{1, 2, 3}, 10, 1)
	c.ticks(25)
	leaderID, ok := c.leader()
	if !ok {
		t.Fatal("expected a leader")
	}
	// Run far past what would be an election timeout if heartbeats
	// weren't resetting followers' clocks.
	c.ticks(100)
	newLeaderID, ok := c.leader()
	if !ok || newLeaderID != leaderID {
		t.Fatalf("leader changed from %d to (%d, %v); heartbeats should have prevented re-election", leaderID, newLeaderID, ok)
	}
}

// --- Term and vote rules --------------------------------------------------------

func TestVote_HigherTermCausesStepDown(t *testing.T) {
	c := newCluster(t, []uint64{1, 2, 3}, 10, 1)
	c.ticks(25)
	leaderID, ok := c.leader()
	if !ok {
		t.Fatal("expected a leader")
	}
	leaderTerm := c.nodes[leaderID].Status().Term

	// Directly inject a RequestVote at a much higher term, simulating a
	// node that's been through several elections elsewhere (e.g. was
	// partitioned and kept timing out and incrementing its own term).
	c.nodes[leaderID].Step(Message{
		Type: MsgRequestVote, From: 99, To: leaderID, Term: leaderTerm + 5,
		LastLogIndex: 0, LastLogTerm: 0,
	})
	s := c.nodes[leaderID].Status()
	if s.Role != Follower {
		t.Fatalf("former leader's role = %v, want Follower after seeing a higher term", s.Role)
	}
	if s.Term != leaderTerm+5 {
		t.Fatalf("former leader's term = %d, want %d", s.Term, leaderTerm+5)
	}
}

func TestVote_WontVoteTwiceInSameTerm(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	r.Step(Message{Type: MsgRequestVote, From: 2, To: 1, Term: 1, LastLogIndex: 0, LastLogTerm: 0})
	msgs := r.ReadMessages()
	if len(msgs) != 1 || !msgs[0].VoteGranted {
		t.Fatalf("expected the first vote request to be granted, got %+v", msgs)
	}

	// A second candidate asks for a vote in the SAME term: must be denied
	// since we already voted for node 2 this term.
	r.Step(Message{Type: MsgRequestVote, From: 3, To: 1, Term: 1, LastLogIndex: 0, LastLogTerm: 0})
	msgs = r.ReadMessages()
	if len(msgs) != 1 || msgs[0].VoteGranted {
		t.Fatalf("expected the second vote request (same term) to be denied, got %+v", msgs)
	}
}

func TestVote_WillVoteAgainInNewerTerm(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	r.Step(Message{Type: MsgRequestVote, From: 2, To: 1, Term: 1, LastLogIndex: 0, LastLogTerm: 0})
	r.ReadMessages()

	// A different candidate, but at a strictly higher term: must be
	// allowed to vote again.
	r.Step(Message{Type: MsgRequestVote, From: 3, To: 1, Term: 2, LastLogIndex: 0, LastLogTerm: 0})
	msgs := r.ReadMessages()
	if len(msgs) != 1 || !msgs[0].VoteGranted {
		t.Fatalf("expected a vote in a newer term to be granted, got %+v", msgs)
	}
}

func TestVote_DeniedIfCandidateLogIsLessUpToDate(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	// Give node 1 a log entry at term 5 (simulating it has replicated
	// data a stale candidate hasn't seen).
	r.log = append(r.log, LogEntry{Term: 5, Index: 1})

	r.Step(Message{Type: MsgRequestVote, From: 2, To: 1, Term: 6, LastLogIndex: 0, LastLogTerm: 0})
	msgs := r.ReadMessages()
	if len(msgs) != 1 || msgs[0].VoteGranted {
		t.Fatalf("expected the vote to be denied for a less-up-to-date candidate log, got %+v", msgs)
	}
}

func TestVote_GrantedIfCandidateLogIsAtLeastAsUpToDate(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.log = append(r.log, LogEntry{Term: 5, Index: 1})

	r.Step(Message{Type: MsgRequestVote, From: 2, To: 1, Term: 6, LastLogIndex: 1, LastLogTerm: 5})
	msgs := r.ReadMessages()
	if len(msgs) != 1 || !msgs[0].VoteGranted {
		t.Fatalf("expected the vote to be granted for an equally up-to-date candidate log, got %+v", msgs)
	}
}

func TestVote_RequestAtLowerTermIsRejected(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.currentTerm = 5
	r.Step(Message{Type: MsgRequestVote, From: 2, To: 1, Term: 3, LastLogIndex: 0, LastLogTerm: 0})
	msgs := r.ReadMessages()
	if len(msgs) != 1 || msgs[0].VoteGranted || msgs[0].Term != 5 {
		t.Fatalf("expected denial at our own (higher) term, got %+v", msgs)
	}
}

func TestRequestVoteResponse_HigherTermCausesStepDown(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	term := r.Status().Term

	r.Step(Message{Type: MsgRequestVoteResponse, From: 2, To: 1, Term: term + 5, VoteGranted: false})
	s := r.Status()
	if s.Role != Follower {
		t.Fatalf("role = %v, want Follower after seeing a higher term in a vote response", s.Role)
	}
	if s.Term != term+5 {
		t.Fatalf("term = %d, want %d", s.Term, term+5)
	}
}

func TestRole_String(t *testing.T) {
	cases := map[Role]string{Follower: "Follower", Candidate: "Candidate", Leader: "Leader", Role(99): "Unknown"}
	for role, want := range cases {
		if got := role.String(); got != want {
			t.Errorf("Role(%d).String() = %q, want %q", role, got, want)
		}
	}
}

func TestMessageType_String(t *testing.T) {
	cases := map[MessageType]string{
		MsgRequestVote:           "RequestVote",
		MsgRequestVoteResponse:   "RequestVoteResponse",
		MsgAppendEntries:         "AppendEntries",
		MsgAppendEntriesResponse: "AppendEntriesResponse",
		MessageType(99):          "Unknown",
	}
	for typ, want := range cases {
		if got := typ.String(); got != want {
			t.Errorf("MessageType(%d).String() = %q, want %q", typ, got, want)
		}
	}
}

func TestEntries_ClampsEndToLastLogIndex(t *testing.T) {
	r, _ := New(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1})
	r.log = append(r.log, LogEntry{Term: 1, Index: 1, Data: []byte("a")})
	// Ask for far more than exists; must clamp rather than panic/OOB.
	entries := r.Entries(0, 1000)
	if len(entries) != 1 || string(entries[0].Data) != "a" {
		t.Fatalf("Entries(0, 1000) = %+v, want [{Data: a}]", entries)
	}
}

func TestTermAt_OutOfRangeReturnsZero(t *testing.T) {
	r, _ := New(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1})
	if got := r.termAt(999); got != 0 {
		t.Fatalf("termAt(999) = %d, want 0", got)
	}
}

func TestMaybeAdvanceCommitIndex_SkipsEarlierTermEntries(t *testing.T) {
	// Directly exercises the Figure 8 safety rule: an entry from an
	// earlier term that's already on a majority must NOT be committed
	// merely because of that — only once a current-term entry also
	// reaches a majority.
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	r.currentTerm = 2
	r.role = Leader
	r.log = append(r.log,
		LogEntry{Term: 1, Index: 1}, // earlier-term entry, replicated everywhere
	)
	r.matchIndex = map[uint64]uint64{2: 1, 3: 1} // full majority already has index 1
	r.maybeAdvanceCommitIndex()
	if r.commitIndex != 0 {
		t.Fatalf("commitIndex = %d, want 0 (an old-term entry alone must never be committed)", r.commitIndex)
	}

	// Now a current-term entry also reaches a majority: index 1 (old
	// term) becomes committable too, as a side effect of the current-term
	// entry at index 2 committing (Raft commits are prefix-closed).
	r.log = append(r.log, LogEntry{Term: 2, Index: 2})
	r.matchIndex[2] = 2
	r.matchIndex[3] = 2
	r.maybeAdvanceCommitIndex()
	if r.commitIndex != 2 {
		t.Fatalf("commitIndex = %d, want 2 once a current-term entry reaches majority", r.commitIndex)
	}
}

func TestPropose_SingleNodeClusterCommitsImmediately(t *testing.T) {
	c := newCluster(t, []uint64{1}, 10, 1)
	c.ticks(20)
	id, ok := c.leader()
	if !ok || id != 1 {
		t.Fatal("expected node 1 to be leader")
	}
	if err := c.nodes[1].Propose([]byte("solo")); err != nil {
		t.Fatal(err)
	}
	if c.nodes[1].Status().CommitIndex != 1 {
		t.Fatalf("commitIndex = %d, want 1 (a single-node cluster is its own majority)", c.nodes[1].Status().CommitIndex)
	}
}

func TestStep_IgnoresMisaddressedMessage(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.Step(Message{Type: MsgRequestVote, From: 2, To: 99, Term: 1}) // To != our ID
	if msgs := r.ReadMessages(); len(msgs) != 0 {
		t.Fatalf("expected a misaddressed message to be ignored, got %+v", msgs)
	}
}

func TestRequestVoteResponse_IgnoredIfNotCandidate(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	// Still a Follower (never started an election): a stray vote
	// response must be a no-op, not a crash or a spurious leadership.
	r.Step(Message{Type: MsgRequestVoteResponse, From: 2, To: 1, Term: 0, VoteGranted: true})
	if r.Status().Role != Follower {
		t.Fatalf("role = %v, want Follower", r.Status().Role)
	}
}

func TestRequestVoteResponse_StaleTermIgnored(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	r.Tick() // not enough to trigger election by itself; force one directly instead
	r.becomeCandidate()
	term := r.Status().Term

	r.Step(Message{Type: MsgRequestVoteResponse, From: 2, To: 1, Term: term - 1, VoteGranted: true})
	if r.Status().Role != Candidate {
		t.Fatalf("a stale-term vote response changed role to %v", r.Status().Role)
	}
	if len(r.votesReceived) != 1 { // just self
		t.Fatalf("stale vote response was incorrectly counted: votesReceived=%v", r.votesReceived)
	}
}

func TestRequestVoteResponse_DeniedVoteNotCounted(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	term := r.Status().Term
	r.Step(Message{Type: MsgRequestVoteResponse, From: 2, To: 1, Term: term, VoteGranted: false})
	if r.Status().Role != Candidate {
		t.Fatalf("role = %v, want still Candidate (only 1/3 votes)", r.Status().Role)
	}
}

func TestAppendEntriesResponse_IgnoredIfNotLeader(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.Step(Message{Type: MsgAppendEntriesResponse, From: 2, To: 1, Term: 0, Success: true, MatchIndex: 5})
	// Should be a complete no-op: not leader, nothing to update, no panic.
	if r.Status().Role != Follower {
		t.Fatalf("role = %v, want Follower", r.Status().Role)
	}
}

func TestAppendEntriesResponse_StaleTermIgnored(t *testing.T) {
	c := newCluster(t, []uint64{1, 2, 3}, 10, 1)
	c.ticks(25)
	leaderID, ok := c.leader()
	if !ok {
		t.Fatal("expected a leader")
	}
	leader := c.nodes[leaderID]
	term := leader.Status().Term
	before := leader.Status().CommitIndex

	leader.Step(Message{Type: MsgAppendEntriesResponse, From: 2, To: leaderID, Term: term - 1, Success: true, MatchIndex: 999})
	if leader.Status().CommitIndex != before {
		t.Fatalf("a stale-term response should not affect commit index: before=%d after=%d", before, leader.Status().CommitIndex)
	}
}

func TestAppendEntries_LowerTermRejected(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.currentTerm = 5
	r.Step(Message{Type: MsgAppendEntries, From: 2, To: 1, Term: 3})
	msgs := r.ReadMessages()
	if len(msgs) != 1 || msgs[0].Success {
		t.Fatalf("expected AppendEntries at a lower term to be rejected, got %+v", msgs)
	}
	if r.Status().Term != 5 {
		t.Fatalf("term changed to %d, want unchanged at 5", r.Status().Term)
	}
}

// --- Log replication -----------------------------------------------------------

func TestReplication_ProposedEntryReachesAllNodesAndCommits(t *testing.T) {
	c := newCluster(t, []uint64{1, 2, 3}, 10, 1)
	c.ticks(25)
	if _, ok := c.leader(); !ok {
		t.Fatal("expected a leader")
	}

	c.propose([]byte("hello"))
	c.ticks(3) // one round to replicate + commit on the leader, one more for followers to learn the new commit index via the next heartbeat

	if !c.allCommitted(1) {
		for id, r := range c.nodes {
			t.Logf("node %d: %+v", id, r.Status())
		}
		t.Fatal("expected all nodes to have committed index 1")
	}

	leaderID, _ := c.leader()
	entries := c.nodes[leaderID].Entries(0, 1)
	if len(entries) != 1 || string(entries[0].Data) != "hello" {
		t.Fatalf("Entries(0,1) = %+v, want [{Data: hello}]", entries)
	}
}

func TestReplication_MultipleEntriesInOrder(t *testing.T) {
	c := newCluster(t, []uint64{1, 2, 3}, 10, 1)
	c.ticks(25)
	c.propose([]byte("a"))
	c.ticks(3)
	c.propose([]byte("b"))
	c.ticks(3)
	c.propose([]byte("c"))
	c.ticks(3)

	if !c.allCommitted(3) {
		t.Fatal("expected all 3 entries committed on all nodes")
	}
	leaderID, _ := c.leader()
	entries := c.nodes[leaderID].Entries(0, 3)
	want := []string{"a", "b", "c"}
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}
	for i, e := range entries {
		if string(e.Data) != want[i] {
			t.Fatalf("entry %d = %q, want %q", i, e.Data, want[i])
		}
	}
}

func TestPropose_FailsIfNotLeader(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	if err := r.Propose([]byte("x")); err != ErrNotLeader {
		t.Fatalf("Propose on a non-leader = %v, want ErrNotLeader", err)
	}
}

func TestReplication_LaggingFollowerCatchesUpAfterPartitionHeals(t *testing.T) {
	c := newCluster(t, []uint64{1, 2, 3}, 10, 1)
	c.ticks(25)
	leaderID, ok := c.leader()
	if !ok {
		t.Fatal("expected a leader")
	}

	var laggingFollower uint64
	for id := range c.nodes {
		if id != leaderID {
			laggingFollower = id
			break
		}
	}
	c.isolate(laggingFollower)

	c.propose([]byte("while-partitioned"))
	c.ticks(3)
	c.propose([]byte("still-partitioned"))
	c.ticks(3)

	// The two non-isolated nodes (leader + the other follower) should
	// have committed both entries; the isolated one has neither.
	for id, r := range c.nodes {
		if id == laggingFollower {
			continue
		}
		if r.Status().CommitIndex < 2 {
			t.Fatalf("node %d commit index = %d, want >= 2", id, r.Status().CommitIndex)
		}
	}
	if c.nodes[laggingFollower].Status().LastLogIndex >= 2 {
		t.Fatalf("isolated node shouldn't have received entries, but LastLogIndex = %d", c.nodes[laggingFollower].Status().LastLogIndex)
	}

	c.heal(laggingFollower)
	c.ticks(5) // give heartbeats a chance to bring it up to date

	if c.nodes[laggingFollower].Status().CommitIndex < 2 {
		t.Fatalf("healed node's commit index = %d, want caught up to >= 2", c.nodes[laggingFollower].Status().CommitIndex)
	}
	entries := c.nodes[laggingFollower].Entries(0, 2)
	if len(entries) != 2 || string(entries[0].Data) != "while-partitioned" || string(entries[1].Data) != "still-partitioned" {
		t.Fatalf("healed node's entries = %+v, want [while-partitioned, still-partitioned]", entries)
	}
}

func TestReplication_ConflictingEntriesAreTruncatedAndOverwritten(t *testing.T) {
	// Directly exercise the AppendEntries conflict-resolution path: a
	// follower has a stale/wrong entry at some index (as if it came from
	// an old, since-superseded leader) and must discard it plus anything
	// after it, replacing with the real leader's version.
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.log = append(r.log,
		LogEntry{Term: 1, Index: 1, Data: []byte("old-leader-entry-1")},
		LogEntry{Term: 1, Index: 2, Data: []byte("old-leader-entry-2")},
	)

	// The real leader (term 2) says: after index 1 (term 1, matches),
	// here's what should really be at index 2 onward.
	r.Step(Message{
		Type: MsgAppendEntries, From: 2, To: 1, Term: 2,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{
			{Term: 2, Index: 2, Data: []byte("real-entry-2")},
			{Term: 2, Index: 3, Data: []byte("real-entry-3")},
		},
		LeaderCommit: 3,
	})

	msgs := r.ReadMessages()
	if len(msgs) != 1 || !msgs[0].Success {
		t.Fatalf("expected AppendEntries to succeed, got %+v", msgs)
	}
	entries := r.Entries(1, 3)
	if len(entries) != 2 || string(entries[0].Data) != "real-entry-2" || string(entries[1].Data) != "real-entry-3" {
		t.Fatalf("entries after conflict resolution = %+v, want [real-entry-2, real-entry-3]", entries)
	}
}

func TestAppendEntries_PrevLogIndexBeyondOurLog(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.Step(Message{Type: MsgAppendEntries, From: 2, To: 1, Term: 1, PrevLogIndex: 5, PrevLogTerm: 1})
	msgs := r.ReadMessages()
	if len(msgs) != 1 || msgs[0].Success {
		t.Fatalf("expected failure when PrevLogIndex is beyond our log, got %+v", msgs)
	}
}

func TestAppendEntries_PrevLogTermMismatch(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.log = append(r.log, LogEntry{Term: 1, Index: 1})
	r.Step(Message{Type: MsgAppendEntries, From: 2, To: 1, Term: 2, PrevLogIndex: 1, PrevLogTerm: 99})
	msgs := r.ReadMessages()
	if len(msgs) != 1 || msgs[0].Success {
		t.Fatalf("expected failure on PrevLogTerm mismatch, got %+v", msgs)
	}
}

// --- Partition tolerance ---------------------------------------------------

func TestPartition_MinorityCannotElectLeader(t *testing.T) {
	c := newCluster(t, []uint64{1, 2, 3, 4, 5}, 10, 1)
	c.ticks(25)
	if _, ok := c.leader(); !ok {
		t.Fatal("expected an initial leader")
	}

	// Isolate a minority (2 of 5) — they can't reach a majority alone.
	c.isolate(4)
	c.isolate(5)
	c.ticks(30) // plenty of time for the isolated pair to keep timing out

	// The majority side should still have (or still elect/retain) a
	// leader throughout.
	majorityLeader, ok := c.leader()
	if !ok {
		t.Fatal("expected the majority partition to retain/elect a leader")
	}
	if majorityLeader == 4 || majorityLeader == 5 {
		t.Fatalf("leader() = %d, which is in the isolated minority", majorityLeader)
	}

	// The isolated nodes should never have become (and stayed) leader —
	// they may cycle through Candidate repeatedly, but never Leader,
	// since they can never gather a majority of votes alone.
	for _, id := range []uint64{4, 5} {
		if c.nodes[id].Status().Role == Leader {
			t.Fatalf("isolated node %d became leader with no majority available", id)
		}
	}
}

func TestPartition_HealedMinorityRejoinsWithoutSplitBrain(t *testing.T) {
	c := newCluster(t, []uint64{1, 2, 3, 4, 5}, 10, 1)
	c.ticks(25)
	initialLeader, ok := c.leader()
	if !ok {
		t.Fatal("expected an initial leader")
	}

	c.isolate(4)
	c.isolate(5)
	c.ticks(30)
	c.propose([]byte("committed-during-partition"))
	c.ticks(3)

	c.heal(4)
	c.heal(5)
	// A node that kept timing out while isolated has an inflated term
	// and can force the current leader to step down on rejoining, even
	// though it can't actually win an election (its log is behind) —
	// this can take a few disruptive rounds to settle without a
	// pre-vote extension (a known, documented vanilla-Raft
	// characteristic, not a bug: the system stays safe throughout,
	// just slower to restabilize). Give it generous room to converge.
	c.ticks(200)

	// Exactly one leader across the whole (now-healed) cluster.
	finalLeader, ok := c.leader()
	if !ok {
		t.Fatal("expected exactly one leader after healing")
	}
	_ = initialLeader // the leader may or may not have changed; what matters is there's exactly one now

	if !c.allCommitted(1) {
		t.Fatal("expected the entry committed during the partition to propagate to all nodes after healing")
	}
	// The rejoined nodes must reflect the same committed entry, not some
	// divergent history of their own.
	entries := c.nodes[finalLeader].Entries(0, 1)
	if len(entries) != 1 || string(entries[0].Data) != "committed-during-partition" {
		t.Fatalf("final leader's entries = %+v", entries)
	}
}
