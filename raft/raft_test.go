package raft

import (
	"testing"
)

// --- Ready/Advance contract ---------------------------------------------------

func TestReady_HardStateNilWhenUnchanged(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.Advance() // establish a baseline stable state
	rd := r.Ready()
	if rd.HardState != nil {
		t.Fatalf("HardState = %+v, want nil (nothing changed since Advance)", rd.HardState)
	}
}

func TestReady_HardStateReportedOnTermChange(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.Advance()
	r.becomeCandidate() // bumps currentTerm and sets votedFor

	rd := r.Ready()
	if rd.HardState == nil {
		t.Fatal("expected a non-nil HardState after a term/vote change")
	}
	if rd.HardState.Term != r.currentTerm || rd.HardState.Vote != r.votedFor {
		t.Fatalf("HardState = %+v, want Term=%d Vote=%d", rd.HardState, r.currentTerm, r.votedFor)
	}
}

func TestReady_HardStateClearedAfterAdvance(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	r.Ready()
	r.Advance()
	rd := r.Ready()
	if rd.HardState != nil {
		t.Fatalf("HardState = %+v, want nil after Advance", rd.HardState)
	}
}

func TestReady_UnstableEntriesReportedAndClearedAfterAdvance(t *testing.T) {
	r, _ := New(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1})
	r.role = Leader
	r.Advance()

	r.Propose([]byte("a"))
	rd := r.Ready()
	if len(rd.UnstableEntries) != 1 || string(rd.UnstableEntries[0].Data) != "a" {
		t.Fatalf("UnstableEntries = %+v, want one entry with Data=a", rd.UnstableEntries)
	}
	if rd.FirstUnstableIndex != 1 {
		t.Fatalf("FirstUnstableIndex = %d, want 1", rd.FirstUnstableIndex)
	}
	r.Advance()

	rd = r.Ready()
	if len(rd.UnstableEntries) != 0 {
		t.Fatalf("UnstableEntries after Advance = %+v, want empty", rd.UnstableEntries)
	}
}

func TestReady_NoUnstableEntriesWhenLogUnchanged(t *testing.T) {
	r, _ := New(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1})
	r.Advance()
	rd := r.Ready()
	if len(rd.UnstableEntries) != 0 || rd.FirstUnstableIndex != 0 {
		t.Fatalf("Ready() on an untouched log = %+v, want empty", rd)
	}
}

func TestReady_TruncationLowersFirstUnstableIndex(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	// Simulate having already reported (and the caller having
	// "persisted") two entries from an old, since-superseded leader.
	r.log = append(r.log,
		LogEntry{Term: 1, Index: 1, Data: []byte("stale-1")},
		LogEntry{Term: 1, Index: 2, Data: []byte("stale-2")},
	)
	r.Advance() // marks both as stable, as if a caller had persisted them

	// The real leader (higher term) now overwrites starting at index 2.
	r.Step(Message{
		Type: MsgAppendEntries, From: 2, To: 1, Term: 2,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 2, Index: 2, Data: []byte("real-2")}},
	})

	rd := r.Ready()
	if rd.FirstUnstableIndex != 2 {
		t.Fatalf("FirstUnstableIndex = %d, want 2 (the truncation point, even though it was previously marked stable)", rd.FirstUnstableIndex)
	}
	if len(rd.UnstableEntries) != 1 || string(rd.UnstableEntries[0].Data) != "real-2" {
		t.Fatalf("UnstableEntries = %+v, want [{Data: real-2}]", rd.UnstableEntries)
	}
}

func TestReady_MessagesAvailableUntilAdvance(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.Advance()
	r.becomeCandidate()

	rd1 := r.Ready()
	if len(rd1.Messages) == 0 {
		t.Fatal("expected outbound RequestVote messages")
	}
	// Calling Ready() again without Advance() must return the same
	// pending messages, not lose them.
	rd2 := r.Ready()
	if len(rd2.Messages) != len(rd1.Messages) {
		t.Fatalf("second Ready() before Advance returned %d messages, want %d", len(rd2.Messages), len(rd1.Messages))
	}
	r.Advance()
	rd3 := r.Ready()
	if len(rd3.Messages) != 0 {
		t.Fatalf("Ready() after Advance returned %d messages, want 0", len(rd3.Messages))
	}
}

func TestRestoreState_SeedsRecoveredStateBeforeParticipating(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	recoveredLog := []LogEntry{
		{Term: 0, Index: 0}, // dummy sentinel, as New() itself would have
		{Term: 3, Index: 1, Data: []byte("recovered")},
	}
	r.restoreState(HardState{Term: 5, Vote: 2}, recoveredLog)

	s := r.Status()
	if s.Term != 5 {
		t.Fatalf("Term = %d, want 5", s.Term)
	}
	if r.votedFor != 2 {
		t.Fatalf("votedFor = %d, want 2", r.votedFor)
	}
	if s.LastLogIndex != 1 {
		t.Fatalf("LastLogIndex = %d, want 1", s.LastLogIndex)
	}
	// Recovered state must be considered already-stable: a fresh Ready()
	// must not try to re-report it as needing persistence.
	rd := r.Ready()
	if rd.HardState != nil {
		t.Fatalf("HardState = %+v, want nil (recovered state is already stable)", rd.HardState)
	}
	if len(rd.UnstableEntries) != 0 {
		t.Fatalf("UnstableEntries = %+v, want empty (recovered entries are already stable)", rd.UnstableEntries)
	}
}

func TestRestoreState_EmptyEntriesKeepsDefaultLog(t *testing.T) {
	r, _ := New(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1})
	r.restoreState(HardState{Term: 2, Vote: 0}, nil)
	if r.Status().LastLogIndex != 0 {
		t.Fatalf("LastLogIndex = %d, want 0 (default dummy-sentinel-only log preserved)", r.Status().LastLogIndex)
	}
}

// --- Config validation --------------------------------------------------------

// readyMessages is a test-only convenience wrapping the real Ready/Advance
// contract: call Ready(), grab its Messages, then Advance() to mark
// everything in that Ready as durable — equivalent to the old
// ReadMessages()'s drain-and-clear behavior, but going through the actual
// API real callers use rather than a testing-only shortcut.
func readyMessages(r *Raft) []Message {
	rd := r.Ready()
	r.Advance()
	return rd.Messages
}

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
	msgs := readyMessages(r)
	if len(msgs) != 1 || !msgs[0].VoteGranted {
		t.Fatalf("expected the first vote request to be granted, got %+v", msgs)
	}

	// A second candidate asks for a vote in the SAME term: must be denied
	// since we already voted for node 2 this term.
	r.Step(Message{Type: MsgRequestVote, From: 3, To: 1, Term: 1, LastLogIndex: 0, LastLogTerm: 0})
	msgs = readyMessages(r)
	if len(msgs) != 1 || msgs[0].VoteGranted {
		t.Fatalf("expected the second vote request (same term) to be denied, got %+v", msgs)
	}
}

func TestVote_WillVoteAgainInNewerTerm(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	r.Step(Message{Type: MsgRequestVote, From: 2, To: 1, Term: 1, LastLogIndex: 0, LastLogTerm: 0})
	readyMessages(r)

	// A different candidate, but at a strictly higher term: must be
	// allowed to vote again.
	r.Step(Message{Type: MsgRequestVote, From: 3, To: 1, Term: 2, LastLogIndex: 0, LastLogTerm: 0})
	msgs := readyMessages(r)
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
	msgs := readyMessages(r)
	if len(msgs) != 1 || msgs[0].VoteGranted {
		t.Fatalf("expected the vote to be denied for a less-up-to-date candidate log, got %+v", msgs)
	}
}

func TestVote_GrantedIfCandidateLogIsAtLeastAsUpToDate(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.log = append(r.log, LogEntry{Term: 5, Index: 1})

	r.Step(Message{Type: MsgRequestVote, From: 2, To: 1, Term: 6, LastLogIndex: 1, LastLogTerm: 5})
	msgs := readyMessages(r)
	if len(msgs) != 1 || !msgs[0].VoteGranted {
		t.Fatalf("expected the vote to be granted for an equally up-to-date candidate log, got %+v", msgs)
	}
}

func TestVote_RequestAtLowerTermIsRejected(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.currentTerm = 5
	r.Step(Message{Type: MsgRequestVote, From: 2, To: 1, Term: 3, LastLogIndex: 0, LastLogTerm: 0})
	msgs := readyMessages(r)
	if len(msgs) != 1 || msgs[0].VoteGranted || msgs[0].Term != 5 {
		t.Fatalf("expected denial at our own (higher) term, got %+v", msgs)
	}
}

// --- Pre-Vote: the core anti-disruption property ----------------------------

// TestPreVote_IsolatedNodeNeverInflatesItsTermWhileCutOff is the direct,
// deterministic version of the core claim
// TestPartition_HealedMinorityRejoinsWithoutSplitBrain confirms at the
// cluster-integration level: repeated election-timeout firings on a node
// that can't reach anyone must never increment currentTerm, since that's
// specifically what would otherwise let it disrupt a healthy leader the
// moment it reconnects (see becomePreCandidate's doc).
func TestPreVote_IsolatedNodeNeverInflatesItsTermWhileCutOff(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	startTerm := r.currentTerm

	// Fire the election timeout repeatedly with no responses ever
	// arriving (simulating total isolation) — each one should produce a
	// PreVote round, never a real, term-incrementing candidacy.
	for i := 0; i < 5; i++ {
		for j := 0; j < 20; j++ { // comfortably past any randomized timeout
			r.Tick()
		}
		readyMessages(r) // drain and discard; nothing ever responds
	}

	if r.currentTerm != startTerm {
		t.Fatalf("currentTerm = %d after repeated isolated timeouts, want unchanged %d", r.currentTerm, startTerm)
	}
	if r.role != PreCandidate {
		t.Fatalf("role = %v, want PreCandidate (never able to gather enough pre-votes to proceed)", r.role)
	}
}

func TestPreVote_TimeoutSendsPreVoteNotRequestVote(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	for i := 0; i < 20; i++ {
		r.Tick()
	}
	msgs := readyMessages(r)
	if len(msgs) != 2 {
		t.Fatalf("got %d messages, want 2 (one PreVote per peer)", len(msgs))
	}
	for _, m := range msgs {
		if m.Type != MsgPreVote {
			t.Fatalf("message type = %v, want MsgPreVote", m.Type)
		}
	}
	if r.role != PreCandidate {
		t.Fatalf("role = %v, want PreCandidate", r.role)
	}
	if r.currentTerm != 0 {
		t.Fatalf("currentTerm = %d, want unchanged at 0 (a pre-vote round must not increment it)", r.currentTerm)
	}
}

func TestPreVote_MajorityGrantsProceedsToRealCandidacy(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	startTerm := r.currentTerm
	for i := 0; i < 20; i++ {
		r.Tick()
	}
	readyMessages(r)

	r.Step(Message{Type: MsgPreVoteResponse, To: 1, From: 2, VoteGranted: true})
	if r.role != Candidate {
		t.Fatalf("role = %v after a majority of pre-vote grants, want Candidate", r.role)
	}
	if r.currentTerm != startTerm+1 {
		t.Fatalf("currentTerm = %d, want %d (incremented for the real election)", r.currentTerm, startTerm+1)
	}
	msgs := readyMessages(r)
	var sawRealVoteRequest bool
	for _, m := range msgs {
		if m.Type == MsgRequestVote {
			sawRealVoteRequest = true
		}
	}
	if !sawRealVoteRequest {
		t.Fatalf("expected real RequestVote messages after the pre-vote round succeeded, got %+v", msgs)
	}
}

func TestPreVote_DeniedResponseNotCounted(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	for i := 0; i < 20; i++ {
		r.Tick()
	}
	readyMessages(r)

	r.Step(Message{Type: MsgPreVoteResponse, To: 1, From: 2, VoteGranted: false})
	if r.role != PreCandidate {
		t.Fatalf("role = %v after a DENIED pre-vote, want still PreCandidate", r.role)
	}
	if len(r.preVotesReceived) != 1 { // just our own
		t.Fatalf("preVotesReceived = %v, want only self", r.preVotesReceived)
	}
}

func TestPreVote_ResponseIgnoredIfNotPreCandidate(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	// Never entered a pre-vote round at all (still a fresh Follower).
	r.Step(Message{Type: MsgPreVoteResponse, To: 1, From: 2, VoteGranted: true})
	if r.role != Follower {
		t.Fatalf("role = %v, want unchanged Follower for a stale/irrelevant pre-vote response", r.role)
	}
}

func TestPreVote_AppendEntriesDuringPreVoteRoundStepsDownToFollower(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	for i := 0; i < 20; i++ {
		r.Tick()
	}
	readyMessages(r)
	if r.role != PreCandidate {
		t.Fatalf("precondition failed: role = %v, want PreCandidate", r.role)
	}

	// A legitimate leader turns out to exist after all.
	r.Step(Message{Type: MsgAppendEntries, To: 1, From: 2, Term: r.currentTerm + 1, PrevLogIndex: 0, PrevLogTerm: 0})
	if r.role != Follower {
		t.Fatalf("role = %v after a valid AppendEntries mid-pre-vote, want Follower", r.role)
	}
	if r.leaderID != 2 {
		t.Fatalf("leaderID = %d, want 2", r.leaderID)
	}
}

// --- Pre-Vote: grant/deny logic on the recipient side -----------------------

func TestPreVote_GrantedIfLogUpToDateAndHaventVotedThisTerm(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	// m.Term (the sender's actual current term, per send()'s auto-fill
	// convention) is 0, so the prospective term this pre-vote is really
	// asking about is 1.
	r.Step(Message{Type: MsgPreVote, To: 1, From: 2, Term: 0, LastLogIndex: 0, LastLogTerm: 0})
	msgs := readyMessages(r)
	if len(msgs) != 1 || msgs[0].Type != MsgPreVoteResponse || !msgs[0].VoteGranted {
		t.Fatalf("expected a granted PreVoteResponse, got %+v", msgs)
	}
	// Critically, granting a pre-vote must NOT mutate any real state.
	if r.votedFor != 0 {
		t.Fatalf("votedFor = %d after a pre-vote grant, want unchanged 0 — pre-vote must never record a real vote", r.votedFor)
	}
	if r.currentTerm != 0 {
		t.Fatalf("currentTerm = %d after a pre-vote grant, want unchanged 0", r.currentTerm)
	}
}

func TestPreVote_DeniedIfProspectiveTermBehindOurCurrentTerm(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.currentTerm = 10
	// Sender's actual term is 5, so it's asking about prospective term 6
	// — behind our current term of 10; even a real election there
	// couldn't beat where we already are.
	r.Step(Message{Type: MsgPreVote, To: 1, From: 2, Term: 5, LastLogIndex: 0, LastLogTerm: 0})
	msgs := readyMessages(r)
	if len(msgs) != 1 || msgs[0].VoteGranted {
		t.Fatalf("expected a denied PreVoteResponse for a prospective term behind ours, got %+v", msgs)
	}
}

func TestPreVote_DeniedIfCandidateLogIsLessUpToDate(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.log = append(r.log, LogEntry{Term: 5, Index: 1})

	r.Step(Message{Type: MsgPreVote, To: 1, From: 2, Term: 5, LastLogIndex: 0, LastLogTerm: 0})
	msgs := readyMessages(r)
	if len(msgs) != 1 || msgs[0].VoteGranted {
		t.Fatalf("expected a denied PreVoteResponse for a less-up-to-date candidate log, got %+v", msgs)
	}
}

func TestPreVote_GrantedDespiteAlreadyVotedIfProspectiveTermIsHigher(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	// Already voted for node 3 this term — a REAL vote request from
	// node 2 at the SAME term would be denied (see TestVote_WontVoteTwiceInSameTerm).
	r.currentTerm = 1
	r.votedFor = 3

	// But a pre-vote asking about a STRICTLY HIGHER prospective term
	// (sender's actual term 1, prospective term 2) can honestly be
	// granted: a real request at term 2 would first advance us there
	// and clear our stale vote for 3, so we'd genuinely be free to vote
	// for node 2 at that point.
	r.Step(Message{Type: MsgPreVote, To: 1, From: 2, Term: 1, LastLogIndex: 0, LastLogTerm: 0})
	msgs := readyMessages(r)
	if len(msgs) != 1 || !msgs[0].VoteGranted {
		t.Fatalf("expected a granted PreVoteResponse despite an existing same-term vote, since the prospective term is strictly higher, got %+v", msgs)
	}
	// Still must not have mutated our actual vote record.
	if r.votedFor != 3 {
		t.Fatalf("votedFor = %d after a pre-vote grant, want unchanged 3", r.votedFor)
	}
}

func TestPreVote_DeniedIfAlreadyVotedAndProspectiveTermNotHigher(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	r.currentTerm = 2
	r.votedFor = 3

	// Sender's actual term is 1, so prospective term is 2 — equal to,
	// not higher than, our current term. We've already committed our
	// real vote to node 3 this exact term, so a real request at term 2
	// from node 2 would genuinely be denied — the pre-vote must say so
	// truthfully too.
	r.Step(Message{Type: MsgPreVote, To: 1, From: 2, Term: 1, LastLogIndex: 0, LastLogTerm: 0})
	msgs := readyMessages(r)
	if len(msgs) != 1 || msgs[0].VoteGranted {
		t.Fatalf("expected a denied PreVoteResponse (already voted this term, prospective term not higher), got %+v", msgs)
	}
}

// --- ReadIndex: the linearizable-read confirmation protocol -----------------

func TestRequestReadIndex_NotLeaderReturnsError(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	if err := r.RequestReadIndex(1); err != ErrNotLeader {
		t.Fatalf("RequestReadIndex on a non-leader = %v, want ErrNotLeader", err)
	}
}

func TestRequestReadIndex_SingleNodeClusterConfirmsImmediately(t *testing.T) {
	r, _ := New(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate() // no peers: becomes leader immediately
	if err := r.RequestReadIndex(42); err != nil {
		t.Fatal(err)
	}
	rd := r.Ready()
	if len(rd.ReadStates) != 1 || rd.ReadStates[0].Context != 42 {
		t.Fatalf("ReadStates = %+v, want exactly one confirmed for context 42", rd.ReadStates)
	}
}

func TestRequestReadIndex_SendsAppendEntriesWithReadContextToEveryPeer(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	r.becomeLeader()
	readyMessages(r) // drain the become-leader heartbeats

	if err := r.RequestReadIndex(7); err != nil {
		t.Fatal(err)
	}
	msgs := readyMessages(r)
	if len(msgs) != 2 {
		t.Fatalf("got %d messages, want 2 (one probe per peer)", len(msgs))
	}
	for _, m := range msgs {
		if m.Type != MsgAppendEntries || m.ReadContext != 7 {
			t.Fatalf("message = %+v, want an AppendEntries carrying ReadContext=7", m)
		}
	}
}

func TestRequestReadIndex_MajorityAcksConfirmsReadState(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3, 4, 5}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	r.becomeLeader()
	readyMessages(r)

	if err := r.RequestReadIndex(7); err != nil {
		t.Fatal(err)
	}
	readyMessages(r)

	// Self-ack (1 of 5) plus two peers = 3 of 5, a majority.
	r.Step(Message{Type: MsgAppendEntriesResponse, To: 1, From: 2, Term: r.currentTerm, Success: true, ReadContext: 7})
	if rd := r.Ready(); len(rd.ReadStates) != 0 {
		t.Fatalf("ReadStates = %+v after only 2 of 5 acks, want none yet", rd.ReadStates)
	}
	r.Step(Message{Type: MsgAppendEntriesResponse, To: 1, From: 3, Term: r.currentTerm, Success: true, ReadContext: 7})

	rd := r.Ready()
	if len(rd.ReadStates) != 1 || rd.ReadStates[0].Context != 7 {
		t.Fatalf("ReadStates = %+v after a majority of acks, want exactly one confirmed for context 7", rd.ReadStates)
	}
}

func TestRequestReadIndex_ResponseWithoutReadContextDoesNotConfirmAnything(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	r.becomeLeader()
	readyMessages(r)

	if err := r.RequestReadIndex(7); err != nil {
		t.Fatal(err)
	}
	readyMessages(r)

	// An ordinary response, unrelated to any read, must not accidentally
	// count toward a pending read's majority.
	r.Step(Message{Type: MsgAppendEntriesResponse, To: 1, From: 2, Term: r.currentTerm, Success: true})
	if rd := r.Ready(); len(rd.ReadStates) != 0 {
		t.Fatalf("ReadStates = %+v after a response with no ReadContext, want none", rd.ReadStates)
	}
}

func TestRequestReadIndex_StaleOrUnknownContextIgnored(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	r.becomeLeader()
	readyMessages(r)

	// No RequestReadIndex(999) was ever made; an ack referencing it must
	// be a harmless no-op, not a panic (e.g. from a nil map lookup).
	r.Step(Message{Type: MsgAppendEntriesResponse, To: 1, From: 2, Term: r.currentTerm, Success: true, ReadContext: 999})
	if rd := r.Ready(); len(rd.ReadStates) != 0 {
		t.Fatalf("ReadStates = %+v for an unknown context, want none", rd.ReadStates)
	}
}

func TestRequestReadIndex_MultipleConcurrentRequestsTrackedIndependently(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	r.becomeLeader()
	readyMessages(r)

	if err := r.RequestReadIndex(100); err != nil {
		t.Fatal(err)
	}
	if err := r.RequestReadIndex(200); err != nil {
		t.Fatal(err)
	}
	readyMessages(r)

	// Confirm only context 100 (2 of 3, a majority).
	r.Step(Message{Type: MsgAppendEntriesResponse, To: 1, From: 2, Term: r.currentTerm, Success: true, ReadContext: 100})
	rd := r.Ready()
	if len(rd.ReadStates) != 1 || rd.ReadStates[0].Context != 100 {
		t.Fatalf("ReadStates = %+v, want exactly context 100 confirmed, not 200", rd.ReadStates)
	}
	r.Advance()

	// Now confirm 200 too.
	r.Step(Message{Type: MsgAppendEntriesResponse, To: 1, From: 3, Term: r.currentTerm, Success: true, ReadContext: 200})
	rd = r.Ready()
	if len(rd.ReadStates) != 1 || rd.ReadStates[0].Context != 200 {
		t.Fatalf("ReadStates = %+v, want exactly context 200 confirmed", rd.ReadStates)
	}
}

func TestRequestReadIndex_IndexReflectsCommitIndexAtRequestTimeNotLater(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	r.becomeLeader()
	readyMessages(r) // drain the become-leader heartbeats

	// Commit one entry before requesting the read: propose it, then
	// manually ack it from both peers so it actually commits.
	if _, err := r.ProposeBatch([][]byte{[]byte("committed-before-the-read")}); err != nil {
		t.Fatal(err)
	}
	readyMessages(r)
	r.Step(Message{Type: MsgAppendEntriesResponse, To: 1, From: 2, Term: r.currentTerm, Success: true, MatchIndex: 1})
	r.Step(Message{Type: MsgAppendEntriesResponse, To: 1, From: 3, Term: r.currentTerm, Success: true, MatchIndex: 1})
	indexAtReadTime := r.commitIndex
	if indexAtReadTime != 1 {
		t.Fatalf("precondition failed: commitIndex = %d, want 1", indexAtReadTime)
	}

	if err := r.RequestReadIndex(55); err != nil {
		t.Fatal(err)
	}
	probes := readyMessages(r)
	if len(probes) != 2 {
		t.Fatalf("got %d read-index probe messages, want 2", len(probes))
	}

	// Commit a SECOND entry — advancing commitIndex further — before
	// delivering the read-index probe responses below. The eventually
	// confirmed ReadState must still reflect indexAtReadTime (1), not
	// this newer, higher commitIndex.
	if _, err := r.ProposeBatch([][]byte{[]byte("committed-after-the-read-was-requested")}); err != nil {
		t.Fatal(err)
	}
	readyMessages(r)
	r.Step(Message{Type: MsgAppendEntriesResponse, To: 1, From: 2, Term: r.currentTerm, Success: true, MatchIndex: 2})
	r.Step(Message{Type: MsgAppendEntriesResponse, To: 1, From: 3, Term: r.currentTerm, Success: true, MatchIndex: 2})
	if r.commitIndex != 2 {
		t.Fatalf("commitIndex after the second commit = %d, want 2 (otherwise this test isn't exercising the scenario)", r.commitIndex)
	}

	// NOW deliver the (still-outstanding) read-index probe responses —
	// echoing ReadContext=55, matching what a real follower's
	// AppendEntriesResponse to those earlier probes would carry.
	r.Step(Message{Type: MsgAppendEntriesResponse, To: 1, From: 2, Term: r.currentTerm, Success: true, MatchIndex: 2, ReadContext: 55})
	rd := r.Ready()
	var found *ReadState
	for i := range rd.ReadStates {
		if rd.ReadStates[i].Context == 55 {
			found = &rd.ReadStates[i]
		}
	}
	if found == nil {
		// Only 1 of 2 peers acked so far — not yet a majority (2 of 3
		// including self); confirm it's still correctly pending.
		r.Step(Message{Type: MsgAppendEntriesResponse, To: 1, From: 3, Term: r.currentTerm, Success: true, MatchIndex: 2, ReadContext: 55})
		rd = r.Ready()
		for i := range rd.ReadStates {
			if rd.ReadStates[i].Context == 55 {
				found = &rd.ReadStates[i]
			}
		}
	}
	if found == nil {
		t.Fatalf("expected a confirmed ReadState for context 55, got %+v", rd.ReadStates)
	}
	if found.Index != indexAtReadTime {
		t.Fatalf("ReadState.Index = %d, want %d (commitIndex as of the request, not the later, higher one)", found.Index, indexAtReadTime)
	}
}

func TestRequestReadIndex_PendingReadsResetOnNewLeadership(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	r.becomeLeader()
	readyMessages(r)
	if err := r.RequestReadIndex(9); err != nil {
		t.Fatal(err)
	}
	readyMessages(r)

	// Lose leadership before this read is ever confirmed.
	r.becomeFollower(r.currentTerm + 1)
	// Regain it later (a fresh term).
	r.becomeCandidate()
	r.becomeLeader()
	readyMessages(r)

	// A late-arriving ack for the OLD, now-abandoned context must not
	// resurrect it or do anything at all.
	r.Step(Message{Type: MsgAppendEntriesResponse, To: 1, From: 2, Term: r.currentTerm, Success: true, ReadContext: 9})
	if rd := r.Ready(); len(rd.ReadStates) != 0 {
		t.Fatalf("ReadStates = %+v for a context from a previous, since-abandoned leadership term, want none", rd.ReadStates)
	}
}

func TestReady_ReadStatesClearedAfterAdvance(t *testing.T) {
	r, _ := New(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate() // single-node: leader immediately
	if err := r.RequestReadIndex(1); err != nil {
		t.Fatal(err)
	}
	if len(r.Ready().ReadStates) != 1 {
		t.Fatal("expected one ReadState before Advance")
	}
	r.Advance()
	if len(r.Ready().ReadStates) != 0 {
		t.Fatal("expected ReadStates to be cleared after Advance")
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
	cases := map[Role]string{Follower: "Follower", PreCandidate: "PreCandidate", Candidate: "Candidate", Leader: "Leader", Role(99): "Unknown"}
	for role, want := range cases {
		if got := role.String(); got != want {
			t.Errorf("Role(%d).String() = %q, want %q", role, got, want)
		}
	}
}

func TestMessageType_String(t *testing.T) {
	cases := map[MessageType]string{
		MsgPreVote:                 "PreVote",
		MsgPreVoteResponse:         "PreVoteResponse",
		MsgRequestVote:             "RequestVote",
		MsgRequestVoteResponse:     "RequestVoteResponse",
		MsgAppendEntries:           "AppendEntries",
		MsgAppendEntriesResponse:   "AppendEntriesResponse",
		MsgInstallSnapshot:         "InstallSnapshot",
		MsgInstallSnapshotResponse: "InstallSnapshotResponse",
		MessageType(99):            "Unknown",
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

// --- ProposeBatch --------------------------------------------------------------

func TestProposeBatch_AppendsAllEntriesInOrder(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	r.becomeLeader() // single call sequence to force leadership without a real election
	r.Ready()
	r.Advance()

	indices, err := r.ProposeBatch([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil {
		t.Fatal(err)
	}
	if len(indices) != 3 || indices[0] != 1 || indices[1] != 2 || indices[2] != 3 {
		t.Fatalf("indices = %v, want [1 2 3]", indices)
	}
	entries := r.Entries(0, 3)
	if len(entries) != 3 || string(entries[0].Data) != "a" || string(entries[1].Data) != "b" || string(entries[2].Data) != "c" {
		t.Fatalf("entries = %+v, want [a b c] in order", entries)
	}
}

func TestProposeBatch_NotLeaderReturnsErrorAndAppendsNothing(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	_, err := r.ProposeBatch([][]byte{[]byte("a"), []byte("b")})
	if err != ErrNotLeader {
		t.Fatalf("err = %v, want ErrNotLeader", err)
	}
	if r.Status().LastLogIndex != 0 {
		t.Fatalf("LastLogIndex = %d, want 0 (nothing should have been appended)", r.Status().LastLogIndex)
	}
}

// TestProposeBatch_SendsOneMessagePerPeerNotOnePerEntry is the actual
// point of ProposeBatch existing at all: verifying that batching several
// proposals together produces ONE AppendEntries per peer carrying all of
// them, rather than the N separate messages N individual Propose() calls
// would produce — which is what an earlier version of this project
// measured as producing almost no real throughput improvement under
// concurrent load, despite batching the leader's own WAL persistence.
func TestProposeBatch_SendsOneMessagePerPeerNotOnePerEntry(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	r.becomeLeader()
	r.Ready() // drain the become-leader heartbeat messages
	r.Advance()

	// Simulate both peers acknowledging that initial heartbeat, as a
	// real cluster would — otherwise ProposeBatch's own eager send
	// would correctly (per the fix this test exists to verify) skip
	// sending while that heartbeat is still outstanding.
	for _, peer := range []uint64{2, 3} {
		r.Step(Message{Type: MsgAppendEntriesResponse, To: r.id, From: peer, Term: r.currentTerm, Success: true, MatchIndex: 0})
	}
	r.Ready()
	r.Advance()

	_, err := r.ProposeBatch([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil {
		t.Fatal(err)
	}
	msgs := r.Ready().Messages

	appendEntriesPerPeer := make(map[uint64]int)
	var entryCountToPeer2 int
	for _, m := range msgs {
		if m.Type != MsgAppendEntries {
			continue
		}
		appendEntriesPerPeer[m.To]++
		if m.To == 2 {
			entryCountToPeer2 = len(m.Entries)
		}
	}
	for _, peer := range []uint64{2, 3} {
		if appendEntriesPerPeer[peer] != 1 {
			t.Fatalf("peer %d received %d AppendEntries messages, want exactly 1 (all 3 proposals batched into one)", peer, appendEntriesPerPeer[peer])
		}
	}
	if entryCountToPeer2 != 3 {
		t.Fatalf("the single AppendEntries to peer 2 carried %d entries, want 3", entryCountToPeer2)
	}
}

func TestProposeBatch_EmptyBatchStillSucceedsAsLeader(t *testing.T) {
	r, _ := New(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate() // no peers: becomes leader immediately
	indices, err := r.ProposeBatch(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(indices) != 0 {
		t.Fatalf("indices = %v, want empty", indices)
	}
}

// TestProposeBatch_RepeatedCallsBeforeAckSendOnlyTheNewDelta is the
// direct regression test for the real O(N^2) bug this project found and
// fixed: sendAppendEntries used to always resend EVERYTHING from
// nextIndex onward, and nextIndex only advances on an acknowledgment —
// so calling ProposeBatch repeatedly before any response arrives (many
// proposals arriving faster than one round trip completes) resent an
// ever-growing, mostly-redundant prefix every time. Without the fix,
// three such calls would transmit 1+2+3=6 entry-equivalents for 3 real
// proposals; with it, exactly 3 — each call sends only what hasn't been
// sent yet.
func TestProposeBatch_RepeatedCallsBeforeAckSendOnlyTheNewDelta(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	r.becomeLeader()
	r.Ready()
	r.Advance()
	// Ack the become-leader heartbeat so we start from a clean, settled
	// state before the real test begins.
	r.Step(Message{Type: MsgAppendEntriesResponse, To: r.id, From: 2, Term: r.currentTerm, Success: true, MatchIndex: 0})
	r.Ready()
	r.Advance()

	// Three proposals in a row, with no response delivered in between —
	// exactly the scenario that caused the original bug.
	for _, data := range [][]byte{[]byte("a"), []byte("b"), []byte("c")} {
		if _, err := r.ProposeBatch([][]byte{data}); err != nil {
			t.Fatal(err)
		}
	}

	var totalEntriesSent int
	for _, m := range r.Ready().Messages {
		if m.Type == MsgAppendEntries && m.To == 2 {
			totalEntriesSent += len(m.Entries)
		}
	}
	if totalEntriesSent != 3 {
		t.Fatalf("total entries transmitted across 3 separate proposals = %d, want exactly 3 (no redundant retransmission)", totalEntriesSent)
	}
}

// TestSendAppendEntries_ConflictRetryUsesAuthoritativeNextIndexNotStaleSentIndex
// verifies the other correctness-critical half of the delta-sending
// design: after a follower rejects an AppendEntries (a log conflict),
// the retry must restart from the backed-off, authoritative nextIndex,
// not from whatever had been optimistically sent (and now might not
// actually be valid) before the rejection.
func TestSendAppendEntries_ConflictRetryUsesAuthoritativeNextIndexNotStaleSentIndex(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	r.becomeLeader()
	r.Ready()
	r.Advance()

	// Propose twice without ever acking: sentIndex[2] advances to 2,
	// while nextIndex[2] stays at its initial value of 1.
	r.ProposeBatch([][]byte{[]byte("a"), []byte("b")})
	r.Ready()
	r.Advance()

	// The follower rejects (a conflict).
	r.Step(Message{Type: MsgAppendEntriesResponse, To: r.id, From: 2, Term: r.currentTerm, Success: false})

	var retry *Message
	for _, m := range r.Ready().Messages {
		m := m
		if m.Type == MsgAppendEntries && m.To == 2 {
			retry = &m
		}
	}
	if retry == nil {
		t.Fatal("expected a retry AppendEntries after the conflict response")
	}
	// nextIndex[2] was 1 (never having reached >1, the backoff guard
	// left it unchanged), so the retry's PrevLogIndex must be 0 — the
	// authoritative nextIndex-1. Without resetting sentIndex on this
	// path, it would incorrectly be 2 (the stale, now-distrusted point
	// from the earlier optimistic sends).
	if retry.PrevLogIndex != 0 {
		t.Fatalf("retry PrevLogIndex = %d, want 0 (authoritative nextIndex-1, not a stale sentIndex)", retry.PrevLogIndex)
	}
}

func TestStep_IgnoresMisaddressedMessage(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.Step(Message{Type: MsgRequestVote, From: 2, To: 99, Term: 1}) // To != our ID
	if msgs := readyMessages(r); len(msgs) != 0 {
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

func TestAppendEntriesResponse_HigherTermCausesStepDown(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	r.becomeLeader()
	term := r.Status().Term

	r.Step(Message{Type: MsgAppendEntriesResponse, From: 2, To: 1, Term: term + 5, Success: false})
	s := r.Status()
	if s.Role != Follower {
		t.Fatalf("role = %v, want Follower after seeing a higher term in an AppendEntriesResponse", s.Role)
	}
	if s.Term != term+5 {
		t.Fatalf("term = %d, want %d", s.Term, term+5)
	}
}

func TestAppendEntriesResponse_FailureBacksOffNextIndexAndRetries(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.becomeCandidate()
	r.becomeLeader()
	r.Ready() // drain the become-leader heartbeat
	r.Advance()
	r.nextIndex[2] = 5 // simulate having advanced past index 1, needing to back off on conflict

	r.Step(Message{Type: MsgAppendEntriesResponse, From: 2, To: 1, Term: r.currentTerm, Success: false})
	if got := r.nextIndex[2]; got != 4 {
		t.Fatalf("nextIndex[2] = %d after a conflict response, want 4 (backed off by one)", got)
	}
	msgs := r.Ready().Messages
	var retried bool
	for _, m := range msgs {
		if m.Type == MsgAppendEntries && m.To == 2 && m.PrevLogIndex == 3 {
			retried = true
		}
	}
	if !retried {
		t.Fatalf("expected an immediate retry AppendEntries with PrevLogIndex=3, got %+v", msgs)
	}
}

func TestAppendEntries_LowerTermRejected(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.currentTerm = 5
	r.Step(Message{Type: MsgAppendEntries, From: 2, To: 1, Term: 3})
	msgs := readyMessages(r)
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

	msgs := readyMessages(r)
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
	msgs := readyMessages(r)
	if len(msgs) != 1 || msgs[0].Success {
		t.Fatalf("expected failure when PrevLogIndex is beyond our log, got %+v", msgs)
	}
}

func TestAppendEntries_PrevLogTermMismatch(t *testing.T) {
	r, _ := New(Config{ID: 1, Peers: []uint64{2}, ElectionTick: 10, HeartbeatTick: 1})
	r.log = append(r.log, LogEntry{Term: 1, Index: 1})
	r.Step(Message{Type: MsgAppendEntries, From: 2, To: 1, Term: 2, PrevLogIndex: 1, PrevLogTerm: 99})
	msgs := readyMessages(r)
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

	leaderTermBeforeHeal := c.nodes[initialLeader].Status().Term

	c.heal(4)
	c.heal(5)
	// Before the Pre-Vote extension, a node that kept timing out while
	// isolated would have an inflated term and could force the current
	// leader to step down on rejoining, even though it can't actually
	// win an election (its log is behind) — a known, documented vanilla-
	// Raft characteristic, not a bug, but a real, measurable disruption.
	// With Pre-Vote (see becomePreCandidate's doc), an isolated node
	// never gathers enough pre-vote grants to increment its term while
	// cut off, so healing should cause NO disruption at all: the SAME
	// leader stays leader, converging in the time for one ordinary
	// heartbeat/replication round — not the "give it 200 ticks to
	// eventually resettle" room a pre-vote-less implementation needs.
	c.ticks(5)

	finalLeader, ok := c.leader()
	if !ok {
		t.Fatal("expected exactly one leader after healing")
	}
	if finalLeader != initialLeader {
		t.Fatalf("leader changed from %d to %d on a healthy partition healing — Pre-Vote should have prevented any disruption", initialLeader, finalLeader)
	}
	if got := c.nodes[finalLeader].Status().Term; got != leaderTermBeforeHeal {
		t.Fatalf("leader's term changed from %d to %d on healing — Pre-Vote should mean the previously-isolated nodes never inflated their term while cut off, so rejoining shouldn't force a new term either", leaderTermBeforeHeal, got)
	}

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
