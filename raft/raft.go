package raft

import (
	"errors"
	"math/rand"
)

// ErrNotLeader is returned by Propose when called on a node that isn't
// currently the leader. The caller is expected to redirect the proposal
// elsewhere (to whichever node it currently believes is leader, via
// Status().Leader, or by retrying against a different node).
var ErrNotLeader = errors.New("raft: not the leader")

// Config configures a new Raft node.
type Config struct {
	// ID is this node's identifier. Must be nonzero — 0 is reserved
	// internally to mean "no vote cast yet this term."
	ID uint64
	// Peers lists every OTHER node in the cluster (not including ID).
	// Every peer ID must also be nonzero.
	Peers []uint64
	// ElectionTick is the minimum number of Tick() calls a follower or
	// candidate waits, without hearing from a valid leader or granting a
	// vote, before starting an election. The actual timeout used is
	// randomized to [ElectionTick, 2*ElectionTick) each time it resets,
	// per the Raft paper's §5.2 guidance — this is what makes split
	// votes rare rather than making every candidate time out in lockstep.
	ElectionTick int
	// HeartbeatTick is how often (in ticks) a leader sends AppendEntries
	// heartbeats to each follower. Should be well under ElectionTick —
	// New returns an error if it isn't, since a heartbeat interval close
	// to (or exceeding) the election timeout defeats the point of a
	// heartbeat.
	HeartbeatTick int
}

func (c Config) validate() error {
	if c.ID == 0 {
		return errors.New("raft: Config.ID must be nonzero")
	}
	for _, p := range c.Peers {
		if p == 0 {
			return errors.New("raft: peer IDs must be nonzero")
		}
		if p == c.ID {
			return errors.New("raft: Peers must not include this node's own ID")
		}
	}
	if c.ElectionTick <= 0 || c.HeartbeatTick <= 0 {
		return errors.New("raft: ElectionTick and HeartbeatTick must be positive")
	}
	if c.ElectionTick < c.HeartbeatTick*2 {
		return errors.New("raft: ElectionTick should be at least 2x HeartbeatTick, or heartbeats won't reliably prevent spurious elections")
	}
	return nil
}

// HardState is the subset of Raft's state that MUST be durable before any
// message depending on it is allowed to leave this node: currentTerm and
// votedFor. (The log entries themselves are the other piece that needs
// persisting — tracked separately in Ready, since they're append-only
// and typically much larger.)
//
// Concretely, the safety rule this exists to satisfy: granting a vote
// must be on disk before the vote-granted response is sent, or a crash
// between "decided to grant" and "wrote it down" followed by a restart
// could grant the same term's vote to a second candidate — a genuine
// safety violation, not just a liveness hiccup.
type HardState struct {
	Term uint64
	Vote uint64
}

// Ready bundles everything that changed as a result of one or more
// Tick()/Step() calls: state that must be durably persisted, and messages
// that are safe to send only AFTER that persistence completes.
//
// The contract (matching etcd/raft's Ready/Advance convention): call
// Ready(), persist HardState (if non-nil) and UnstableEntries to stable
// storage — truncating any previously-persisted entries from
// FirstUnstableIndex onward first, since a log conflict may have
// invalidated them — THEN send Messages, THEN call Advance(). Doing any
// step out of order (especially sending before persisting) reopens the
// safety hole this whole mechanism exists to close.
type Ready struct {
	// HardState is non-nil only if Term or Vote changed since the last
	// Advance() call.
	HardState *HardState
	// UnstableEntries are log entries from FirstUnstableIndex onward that
	// aren't yet known to be durable. Empty if there's nothing new.
	UnstableEntries []LogEntry
	// FirstUnstableIndex is the index UnstableEntries starts at; only
	// meaningful when UnstableEntries is non-empty. A previous
	// Ready/Advance cycle may have reported (and the caller may have
	// persisted) entries at or after this index that a subsequent log
	// conflict has since invalidated — the caller must discard any
	// persisted entries from this index onward before appending
	// UnstableEntries.
	FirstUnstableIndex uint64
	// Messages are outbound messages, safe to send once the above is
	// durable.
	Messages []Message

	// ReadStates are ReadIndex requests (see Raft.RequestReadIndex) that
	// a majority have now confirmed. Unlike everything else in Ready,
	// these carry no durability requirement of their own — nothing here
	// needs to be persisted before use, since they don't represent new
	// durable state, only a majority's confirmation of something already
	// true. The caller should, for each one, wait until its own
	// locally-applied state has caught up to at least Index, then serve
	// the read it was tracking on behalf of Context with a
	// linearizability guarantee.
	ReadStates []ReadState
}

// tests, observability, and callers deciding where to route a proposal.
type Status struct {
	ID           uint64
	Term         uint64
	Role         Role
	Leader       uint64 // 0 if unknown
	CommitIndex  uint64
	LastLogIndex uint64
}

// Raft is one node's consensus state machine. It has no goroutines and
// performs no I/O; see the package doc for how it's meant to be driven.
type Raft struct {
	id    uint64
	peers []uint64

	role        Role
	currentTerm uint64
	votedFor    uint64 // 0 = no vote cast this term
	leaderID    uint64 // 0 = unknown

	// log[0] is a dummy sentinel entry (Term 0, Index 0), so log[i].Index
	// == i always holds with no special-casing at the boundary — a
	// standard Raft implementation trick that keeps prevLogIndex==0 (the
	// "before the first real entry") a valid, unremarkable case rather
	// than one needing its own branch everywhere.
	log []LogEntry

	commitIndex uint64
	lastApplied uint64

	// Tracking for Ready()/Advance(): stableTerm/stableVote are the
	// HardState values as of the last Advance() call; unstableIndex is
	// the lowest log index not yet known to be durable. A log conflict
	// (handleAppendEntries truncating a bad suffix) can only ever LOWER
	// unstableIndex, never raise it — entries once reported as unstable
	// stay considered unstable until an Advance() call confirms them
	// persisted.
	stableTerm    uint64
	stableVote    uint64
	unstableIndex uint64

	// Leader-only volatile state (§5.3). Reset fresh on every transition
	// into Leader; meaningless in any other role.
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	// sentIndex tracks, per peer, the highest log index included in the
	// most recently SENT AppendEntries — distinct from nextIndex, which
	// only advances on an acknowledgment. sendAppendEntries uses this to
	// send only the entries NOT YET transmitted (rather than resending
	// everything from nextIndex again every time), which is what
	// prevents a real, measured O(N^2) bug: without this, N proposals
	// arriving faster than one round trip completes each triggered a
	// full resend of the whole still-unacknowledged backlog, so total
	// data sent across N such calls grew as N(N+1)/2. An earlier version
	// of this fix instead gated sending entirely (skip a peer that
	// already has one outstanding) — simpler, but it collapsed
	// replication to strict stop-and-wait (one outstanding request per
	// peer, no pipelining), which turned out to cost real throughput on
	// real hardware: measurement showed it, a naive "should be strictly
	// better" assumption didn't catch it. Sending only the delta since
	// the last SEND instead eliminates the redundant retransmission
	// without giving up pipelining — multiple outstanding sends to the
	// same peer are fine, since each one's PrevLogIndex is exactly
	// where the previous one left off, and TCP's in-order, reliable
	// delivery on the single persistent connection each peer pair uses
	// (see the transport package) is what makes a follower processing
	// them in the order they were sent - and therefore each one's
	// consistency check - safe.
	sentIndex map[uint64]uint64

	// Candidate-only volatile state. Reset fresh on every new election.
	votesReceived map[uint64]bool

	// PreCandidate-only volatile state. Reset fresh on every new
	// pre-vote round. See becomePreCandidate's doc.
	preVotesReceived map[uint64]bool

	// Leader-only volatile state for the ReadIndex protocol (see
	// RequestReadIndex's doc). Reset fresh on every transition into
	// Leader, matching nextIndex/matchIndex/sentIndex's own pattern.
	// pendingReads tracks each outstanding read confirmation request by
	// its caller-provided context; readStates accumulates confirmed
	// ones until the next Ready()/Advance() cycle drains them, mirroring
	// how msgs works.
	pendingReads map[uint64]*pendingRead
	readStates   []ReadState

	electionTick              int
	heartbeatTick             int
	electionElapsed           int
	heartbeatElapsed          int
	randomizedElectionTimeout int

	rnd *rand.Rand

	msgs []Message
}

// pendingRead tracks one outstanding ReadIndex confirmation request: the
// commitIndex as of when it was requested, and which peers (by ID) have
// since acknowledged this leader's continued legitimacy.
type pendingRead struct {
	index uint64
	acked map[uint64]bool
}

// New constructs a Raft node in the Follower role, term 0, with an empty
// log. It does not start an election on its own — the first Tick() calls
// (up to a randomized timeout) will, unless AppendEntries/RequestVote
// messages arrive first from a real peer.
func New(cfg Config) (*Raft, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	r := &Raft{
		id:            cfg.ID,
		peers:         append([]uint64(nil), cfg.Peers...),
		role:          Follower,
		log:           []LogEntry{{Term: 0, Index: 0}},
		unstableIndex: 1,
		electionTick:  cfg.ElectionTick,
		heartbeatTick: cfg.HeartbeatTick,
		rnd:           rand.New(rand.NewSource(int64(cfg.ID))),
	}
	r.resetElectionTimeout()
	return r, nil
}

// restoreState seeds a freshly constructed Raft with previously persisted
// state, before it starts ticking or stepping — used by the persistent
// storage layer on recovery. entries, if non-empty, must include the
// index-0 dummy sentinel entry (Term 0, Index 0) as log[0], matching
// New()'s own invariant.
func (r *Raft) restoreState(hs HardState, entries []LogEntry) {
	r.currentTerm = hs.Term
	r.votedFor = hs.Vote
	r.stableTerm = hs.Term
	r.stableVote = hs.Vote
	if len(entries) > 0 {
		r.log = entries
	}
	r.unstableIndex = r.lastLogIndex() + 1
}

func (r *Raft) Status() Status {
	return Status{
		ID:           r.id,
		Term:         r.currentTerm,
		Role:         r.role,
		Leader:       r.leaderID,
		CommitIndex:  r.commitIndex,
		LastLogIndex: r.lastLogIndex(),
	}
}

// Ready returns everything that changed since the last Advance() call —
// see the Ready type's doc for the persist-then-send-then-Advance
// contract this establishes. Safe to call repeatedly without side
// effects; nothing is cleared until Advance().
func (r *Raft) Ready() Ready {
	rd := Ready{Messages: r.msgs, ReadStates: r.readStates}
	if r.currentTerm != r.stableTerm || r.votedFor != r.stableVote {
		rd.HardState = &HardState{Term: r.currentTerm, Vote: r.votedFor}
	}
	if r.lastLogIndex() >= r.unstableIndex {
		rd.UnstableEntries = append([]LogEntry(nil), r.log[r.unstableIndex:]...)
		rd.FirstUnstableIndex = r.unstableIndex
	}
	return rd
}

// Advance confirms that whatever the most recent Ready() call returned
// has been durably persisted and its Messages sent, allowing Raft to stop
// reporting them on subsequent Ready() calls.
func (r *Raft) Advance() {
	r.stableTerm = r.currentTerm
	r.stableVote = r.votedFor
	if r.lastLogIndex() >= r.unstableIndex {
		r.unstableIndex = r.lastLogIndex() + 1
	}
	r.msgs = nil
	r.readStates = nil
}

// Entries returns the entries in (start, end] — i.e. after index start up
// to and including index end — or nil if the range is empty or invalid.
// Used by a caller applying newly committed entries to a state machine.
func (r *Raft) Entries(start, end uint64) []LogEntry {
	if end > r.lastLogIndex() {
		end = r.lastLogIndex()
	}
	if start >= end {
		return nil
	}
	return append([]LogEntry(nil), r.log[start+1:end+1]...)
}

// send queues an outbound message.
func (r *Raft) send(m Message) {
	m.From = r.id
	m.Term = r.currentTerm
	r.msgs = append(r.msgs, m)
}

func (r *Raft) lastLogIndex() uint64 { return r.log[len(r.log)-1].Index }
func (r *Raft) lastLogTerm() uint64  { return r.log[len(r.log)-1].Term }

// termAt returns the term of the entry at index, or 0 if index is out of
// range (including index 0, the dummy sentinel, which legitimately has
// term 0).
func (r *Raft) termAt(index uint64) uint64 {
	if index > r.lastLogIndex() {
		return 0
	}
	return r.log[index].Term
}

func (r *Raft) clusterSize() int { return len(r.peers) + 1 }
func (r *Raft) hasMajorityCount(n int) bool {
	return n >= r.clusterSize()/2+1
}

func (r *Raft) resetElectionTimeout() {
	r.electionElapsed = 0
	r.randomizedElectionTimeout = r.electionTick + r.rnd.Intn(r.electionTick)
}

// Tick advances the node's logical clock by one unit. Call this at a
// regular wall-clock interval of your choosing (the interval itself is
// what gives ElectionTick/HeartbeatTick real-world meaning).
func (r *Raft) Tick() {
	if r.role == Leader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTick {
			r.heartbeatElapsed = 0
			r.sendHeartbeats()
		}
		return
	}
	r.electionElapsed++
	if r.electionElapsed >= r.randomizedElectionTimeout {
		r.becomePreCandidate()
	}
}

// Step processes one incoming message: an RPC request from a peer, or a
// response to an RPC this node previously sent. Messages not addressed to
// this node are ignored (defensive: a correct transport should never
// misroute, but Step doesn't trust that blindly).
func (r *Raft) Step(m Message) {
	if m.To != r.id {
		return
	}
	switch m.Type {
	case MsgPreVote:
		r.handlePreVote(m)
	case MsgPreVoteResponse:
		r.handlePreVoteResponse(m)
	case MsgRequestVote:
		r.handleRequestVote(m)
	case MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case MsgAppendEntries:
		r.handleAppendEntries(m)
	case MsgAppendEntriesResponse:
		r.handleAppendEntriesResponse(m)
	}
}

// becomeFollower transitions to Follower. If term is newer than the
// node's current term, it's a genuinely new term: advance to it and
// clear the recorded vote (a vote is only valid within the term it was
// cast in). If term equals the current term, this is a same-term
// transition (e.g. a candidate discovering the real leader for this
// term already exists) and the vote record must be left alone.
func (r *Raft) becomeFollower(term uint64) {
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = 0
	}
	r.role = Follower
	r.resetElectionTimeout()
}

// becomePreCandidate starts a Pre-Vote round: the standard extension
// (from the Raft dissertation, §9.6) to the base algorithm that prevents
// a node cut off from the cluster — but still very much alive and
// ticking — from disrupting a healthy leader the moment its network
// partition heals. Without this, a partitioned node's election timeout
// fires repeatedly with nothing to interrupt it, each time incrementing
// its term further via becomeCandidate; when the partition heals, that
// inflated term causes every other node to see "a higher term than
// mine" and step down — even the current, perfectly healthy leader —
// triggering a real, disruptive election for no good reason.
//
// The fix: before incrementing the term for real, ask every peer "if I
// asked for your vote at term+1 right now, would you grant it?" — using
// the exact same log-up-to-date criteria a real RequestVote would, but
// crucially causing NO state mutation on either side (see handlePreVote).
// Only once a majority say yes does this proceed to becomeCandidate and
// start a real, term-incrementing election. A node that's genuinely
// partitioned from the majority never gets enough pre-vote grants, so
// it never increments its term — its own election timeout keeps firing,
// but every attempt is a no-op round of pre-votes, not a real election.
// When the partition heals, it rejoins at the same term everyone else
// is at, disrupting nothing.
//
// Because no state changes, a pre-vote round requires no HardState
// persistence at all — Ready()'s change-detection correctly reports no
// new HardState for it, unlike becomeCandidate's real term/vote change,
// which the caller must persist before its RequestVote messages are
// safe to send. Tick()'s timeout-driven path always goes through here
// now, including retrying a PreCandidate or Candidate whose own timeout
// fires again without success — every path that could start or restart
// an election goes through a pre-vote check first.
func (r *Raft) becomePreCandidate() {
	r.role = PreCandidate
	r.leaderID = 0
	r.preVotesReceived = map[uint64]bool{r.id: true}
	r.resetElectionTimeout()

	if len(r.peers) == 0 {
		// No one to ask, so a majority of one is already trivially
		// satisfied — proceed straight to a real (but, for a
		// single-node cluster, uncontested and instant) election.
		r.becomeCandidate()
		return
	}
	for _, p := range r.peers {
		r.send(Message{
			Type:         MsgPreVote,
			To:           p,
			LastLogIndex: r.lastLogIndex(),
			LastLogTerm:  r.lastLogTerm(),
		})
	}
}

// becomeCandidate starts a real election: increments the term, votes for
// itself, and requests votes from every peer. Only reached via a
// successful Pre-Vote round (see becomePreCandidate) — or, for a
// single-node cluster with no peers to ask, immediately and
// unconditionally, since a majority of one needs no votes at all.
func (r *Raft) becomeCandidate() {
	r.currentTerm++
	r.role = Candidate
	r.votedFor = r.id
	r.leaderID = 0
	r.votesReceived = map[uint64]bool{r.id: true}
	r.resetElectionTimeout()

	if len(r.peers) == 0 {
		r.becomeLeader()
		return
	}
	for _, p := range r.peers {
		r.send(Message{
			Type:         MsgRequestVote,
			To:           p,
			LastLogIndex: r.lastLogIndex(),
			LastLogTerm:  r.lastLogTerm(),
		})
	}
}

// becomeLeader transitions to Leader: resets per-follower replication
// progress and immediately sends a heartbeat to assert leadership right
// away, rather than waiting up to a full heartbeat interval.
func (r *Raft) becomeLeader() {
	r.role = Leader
	r.leaderID = r.id
	r.nextIndex = make(map[uint64]uint64, len(r.peers))
	r.matchIndex = make(map[uint64]uint64, len(r.peers))
	r.sentIndex = make(map[uint64]uint64, len(r.peers))
	r.pendingReads = make(map[uint64]*pendingRead)
	for _, p := range r.peers {
		r.nextIndex[p] = r.lastLogIndex() + 1
		r.matchIndex[p] = 0
	}
	r.heartbeatElapsed = 0
	r.sendHeartbeats()
	if len(r.peers) == 0 {
		r.maybeAdvanceCommitIndex()
	}
}

func (r *Raft) sendHeartbeats() {
	for _, p := range r.peers {
		r.sendAppendEntries(p, 0)
	}
}

// sendAppendEntries sends peer only the entries not yet transmitted to
// it: everything from max(sentIndex[peer], nextIndex[peer]-1) onward
// (empty, i.e. a pure heartbeat, if there's nothing new). Using
// nextIndex-1 as a floor — rather than always trusting sentIndex — is
// what makes a conflict-retry (see handleAppendEntriesResponse's failure
// path, which explicitly resets sentIndex first) correctly restart from
// the authoritative acknowledged point instead of a stale optimistic one.
//
// readContext, if nonzero, piggybacks a ReadIndex confirmation probe
// onto this same message (see RequestReadIndex's doc) — reusing the
// exact same AppendEntries a normal heartbeat or replication send would
// produce, rather than a separate RPC, since "does this peer still ack
// me as leader" is exactly what an ordinary AppendEntries round trip
// already establishes.
func (r *Raft) sendAppendEntries(peer uint64, readContext uint64) {
	prevIndex := r.nextIndex[peer] - 1
	if r.sentIndex[peer] > prevIndex {
		prevIndex = r.sentIndex[peer]
	}
	prevTerm := r.termAt(prevIndex)
	entries := r.Entries(prevIndex, r.lastLogIndex())
	r.send(Message{
		Type:         MsgAppendEntries,
		To:           peer,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
		ReadContext:  readContext,
	})
	if r.lastLogIndex() > r.sentIndex[peer] {
		r.sentIndex[peer] = r.lastLogIndex()
	}
}

// handlePreVote answers "if you asked for a real vote right now, would
// I grant it" — WITHOUT mutating currentTerm or votedFor, which is the
// entire point (see becomePreCandidate's doc): a partitioned candidate's
// escalating hypothetical term can't disrupt anyone just by asking about
// it. send() always stamps an outgoing message's Term with this node's
// own current (real) term, so a candidate mid-pre-vote — whose own term
// hasn't incremented yet — necessarily sends m.Term equal to its actual
// current term; the term it's really asking about is m.Term+1 (what it
// would become upon starting a real election), computed here rather
// than carried as a separate field.
func (r *Raft) handlePreVote(m Message) {
	prospectiveTerm := m.Term + 1
	if prospectiveTerm < r.currentTerm {
		// Even a real election at this term couldn't beat where we
		// already are.
		r.send(Message{Type: MsgPreVoteResponse, To: m.From, VoteGranted: false})
		return
	}

	logOK := m.LastLogTerm > r.lastLogTerm() ||
		(m.LastLogTerm == r.lastLogTerm() && m.LastLogIndex >= r.lastLogIndex())

	// canVote mirrors handleRequestVote's own check, with one addition:
	// even if we've already voted for someone else this term, we can
	// truthfully say yes to a pre-vote at a STRICTLY higher prospective
	// term, because a real request at that term would first advance us
	// to it and clear our stale vote (see becomeFollower) — so answering
	// honestly here isn't premature or inconsistent with what we'd
	// actually do.
	canVote := r.votedFor == 0 || r.votedFor == m.From || prospectiveTerm > r.currentTerm

	r.send(Message{Type: MsgPreVoteResponse, To: m.From, VoteGranted: canVote && logOK})
}

// handlePreVoteResponse counts a pre-vote grant. No term-based state
// transition happens here at all (unlike handleRequestVoteResponse) —
// by design: a pre-vote round is purely a non-binding poll, so nothing
// either side learns from it should mutate any durable state. Once a
// majority have granted, this proceeds to becomeCandidate — the one
// point where a pre-vote round's success actually does something.
func (r *Raft) handlePreVoteResponse(m Message) {
	if r.role != PreCandidate {
		return // stale: a previous round, or we've since moved on
	}
	if !m.VoteGranted {
		return
	}
	r.preVotesReceived[m.From] = true
	if r.hasMajorityCount(len(r.preVotesReceived)) {
		r.becomeCandidate()
	}
}

func (r *Raft) handleRequestVote(m Message) {
	if m.Term > r.currentTerm {
		r.becomeFollower(m.Term)
	}
	if m.Term < r.currentTerm {
		r.send(Message{Type: MsgRequestVoteResponse, To: m.From, VoteGranted: false})
		return
	}

	canVote := r.votedFor == 0 || r.votedFor == m.From
	logOK := m.LastLogTerm > r.lastLogTerm() ||
		(m.LastLogTerm == r.lastLogTerm() && m.LastLogIndex >= r.lastLogIndex())

	granted := canVote && logOK
	if granted {
		r.votedFor = m.From
		// Granting a vote means we believe an election is legitimately
		// underway; reset our own timeout so we don't immediately start
		// a competing one.
		r.resetElectionTimeout()
	}
	r.send(Message{Type: MsgRequestVoteResponse, To: m.From, VoteGranted: granted})
}

func (r *Raft) handleRequestVoteResponse(m Message) {
	if m.Term > r.currentTerm {
		r.becomeFollower(m.Term)
		return
	}
	if m.Term < r.currentTerm || r.role != Candidate {
		return // stale response: from an earlier term, or we're no longer a candidate
	}
	if !m.VoteGranted {
		return
	}
	r.votesReceived[m.From] = true
	if r.hasMajorityCount(len(r.votesReceived)) {
		r.becomeLeader()
	}
}

func (r *Raft) handleAppendEntries(m Message) {
	if m.Term < r.currentTerm {
		r.send(Message{Type: MsgAppendEntriesResponse, To: m.From, Success: false})
		return
	}

	// A valid AppendEntries at term >= ours means m.From is (or just
	// became, in this term) the legitimate leader: adopt Follower
	// regardless of our previous role, even if our term already matches
	// (e.g. we were a candidate and just discovered the real leader).
	r.becomeFollower(m.Term)
	r.leaderID = m.From

	reply := Message{Type: MsgAppendEntriesResponse, To: m.From, ReadContext: m.ReadContext}

	switch {
	case m.PrevLogIndex > r.lastLogIndex():
		reply.Success = false
	case r.termAt(m.PrevLogIndex) != m.PrevLogTerm:
		reply.Success = false
	default:
		for i, e := range m.Entries {
			idx := m.PrevLogIndex + 1 + uint64(i)
			if idx <= r.lastLogIndex() && r.termAt(idx) != e.Term {
				// Conflicting entry: this and everything after it in our
				// log is wrong (came from a different, non-leader
				// history) and must be discarded before appending the
				// leader's version.
				r.log = r.log[:idx]
				if idx < r.unstableIndex {
					r.unstableIndex = idx
				}
			}
			if idx > r.lastLogIndex() {
				r.log = append(r.log, e)
			}
		}
		if m.LeaderCommit > r.commitIndex {
			r.commitIndex = min(m.LeaderCommit, r.lastLogIndex())
		}
		reply.Success = true
		reply.MatchIndex = m.PrevLogIndex + uint64(len(m.Entries))
	}
	r.send(reply)
}

func (r *Raft) handleAppendEntriesResponse(m Message) {
	if m.Term > r.currentTerm {
		r.becomeFollower(m.Term)
		return
	}
	if m.Term < r.currentTerm || r.role != Leader {
		return
	}

	// A response at our own current term, from a leader's-eye view,
	// means m.From still recognizes us as leader as of right now —
	// exactly the signal a pending ReadIndex confirmation needs, whether
	// or not the AppendEntries itself succeeded (its success or failure
	// is about log content, orthogonal to "does this peer still ack me
	// as leader").
	if m.ReadContext != 0 {
		r.ackReadIndex(m.ReadContext, m.From)
	}

	if m.Success {
		if m.MatchIndex > r.matchIndex[m.From] {
			r.matchIndex[m.From] = m.MatchIndex
		}
		r.nextIndex[m.From] = m.MatchIndex + 1
		r.maybeAdvanceCommitIndex()
		return
	}

	// Log inconsistency: back nextIndex up by one and retry immediately.
	// Also reset sentIndex so the retry starts from the authoritative,
	// acknowledged-safe nextIndex-1 rather than a stale, possibly-
	// conflicting point sendAppendEntries had optimistically sent
	// before this rejection. This is the simple, always-correct backoff
	// the Raft paper describes; it also describes an optional
	// optimization (the follower reporting the conflicting term so the
	// leader can skip back a whole term at once instead of one entry at
	// a time), which we don't implement — a deliberate scope decision,
	// since it only affects how fast a badly-lagging follower catches
	// up, not correctness, and is a natural target to revisit if the
	// benchmarking phase shows slow catch-up after a partition heals.
	if r.nextIndex[m.From] > 1 {
		r.nextIndex[m.From]--
	}
	r.sentIndex[m.From] = 0
	r.sendAppendEntries(m.From, 0)
}

// maybeAdvanceCommitIndex implements the Raft paper's §5.4.2 safety rule:
// a leader may only commit an entry once it's replicated on a majority,
// AND that entry must be from the leader's OWN current term. Committing
// an entry from an earlier term the moment it merely reaches a majority
// (without waiting for a current-term entry to do the same) is exactly
// the unsafe case the paper's Figure 8 walks through: an old-term entry
// can be silently overwritten by a future leader even after apparently
// reaching a majority, if that leader never itself confirmed it via a
// current-term entry. Requiring a current-term entry to also reach a
// majority is what makes the commit truly durable.
func (r *Raft) maybeAdvanceCommitIndex() {
	for n := r.lastLogIndex(); n > r.commitIndex; n-- {
		if r.termAt(n) != r.currentTerm {
			continue
		}
		count := 1 // self
		for _, p := range r.peers {
			if r.matchIndex[p] >= n {
				count++
			}
		}
		if r.hasMajorityCount(count) {
			r.commitIndex = n
			return
		}
	}
}

// Propose appends data as a new log entry, valid only when this node is
// the current leader, and immediately begins replicating it to every
// peer rather than waiting for the next heartbeat. It does not block
// until the entry commits — callers observe that via Status().CommitIndex
// (or Entries, once it advances past the entry's index).
func (r *Raft) Propose(data []byte) error {
	_, err := r.ProposeBatch([][]byte{data})
	return err
}

// ProposeBatch appends every element of datas as a new log entry, in
// order, in a single call — then sends each peer exactly ONE
// AppendEntries reflecting the complete new tail, rather than the N
// separate messages N individual Propose() calls would generate.
//
// This is the reason ProposeBatch exists as its own method rather than
// Propose just being called in a loop by a caller wanting to batch
// several proposals: Propose's per-call sendAppendEntries means the
// network fan-out (and therefore every follower's own append+persist+
// response cycle) never actually batches even if the leader's own log
// append does — calling Propose N times back-to-back still produces N
// messages per peer, each redundantly carrying the growing tail. This
// was discovered, not assumed: an earlier version of this project's
// server package batched multiple client writes into back-to-back
// Propose() calls expecting reduced fsync overhead throughout the
// cluster, and measured almost no throughput improvement under
// concurrent load — investigation traced it to exactly this eager
// per-call send behavior. ProposeBatch defers sending until every entry
// in the batch is already appended, so followers receive one message
// carrying all of them.
//
// Returns the log index assigned to each element of datas, in the same
// order, or ErrNotLeader (with no entries appended and no messages sent)
// if this node isn't currently leader.
func (r *Raft) ProposeBatch(datas [][]byte) ([]uint64, error) {
	if r.role != Leader {
		return nil, ErrNotLeader
	}
	indices := make([]uint64, len(datas))
	for i, data := range datas {
		entry := LogEntry{Term: r.currentTerm, Index: r.lastLogIndex() + 1, Data: data}
		r.log = append(r.log, entry)
		indices[i] = entry.Index
	}
	for _, p := range r.peers {
		r.sendAppendEntries(p, 0)
	}
	if len(r.peers) == 0 {
		r.maybeAdvanceCommitIndex()
	}
	return indices, nil
}

// RequestReadIndex asks this leader to confirm, via a fresh round of
// AppendEntries to a majority of the cluster, that it's still the
// legitimate leader as of right now — the ReadIndex protocol (Raft
// paper §8) that makes a linearizable read safe without routing the
// read itself through the replicated log. The commitIndex at the
// moment this is called is guaranteed to reflect every write that had
// already committed before the read began; once a majority confirm
// (reported via a ReadState in a later Ready() call — see its own doc),
// a caller that waits for its own locally-applied state to reach at
// least that index before actually reading its state machine is
// guaranteed a linearizable result.
//
// ctx is an opaque, caller-chosen correlation ID — it must be unique
// among this caller's own concurrently outstanding requests, since it's
// how the eventual ReadState is matched back to the request that
// started it. Returns ErrNotLeader if this node isn't currently
// leader — only a leader can meaningfully answer "is my state
// authoritative," so like Propose, a read must be routed to the leader
// either way.
//
// Deliberately not optimized to coalesce multiple concurrent calls
// arriving close together into a single shared heartbeat round (a
// well-known real-world optimization — etcd/raft's actual
// implementation does this). A scope decision favoring simplicity and
// testability over the additional throughput this would give under
// very heavy concurrent read load, documented here rather than left to
// look like an oversight.
func (r *Raft) RequestReadIndex(ctx uint64) error {
	if r.role != Leader {
		return ErrNotLeader
	}
	r.pendingReads[ctx] = &pendingRead{index: r.commitIndex, acked: map[uint64]bool{r.id: true}}
	if r.hasMajorityCount(len(r.pendingReads[ctx].acked)) {
		// Single-node cluster: a majority of one is already satisfied
		// by our own implicit self-ack, with no peers to actually ask.
		r.readStates = append(r.readStates, ReadState{Index: r.commitIndex, Context: ctx})
		delete(r.pendingReads, ctx)
		return nil
	}
	for _, p := range r.peers {
		r.sendAppendEntries(p, ctx)
	}
	return nil
}

// ackReadIndex records that peer has, as of right now, confirmed this
// leader's legitimacy for the pending read tracked under ctx — and, once
// a majority (including the leader's own implicit self-ack) have,
// reports it as a confirmed ReadState for the next Ready() call to
// surface. A ctx not found in pendingReads is silently ignored: either
// it was already confirmed and cleared, or it's a stale echo from a
// round tied to a read this node no longer recognizes (e.g. a fresh
// becomeLeader reset cleared the map since) — in neither case is there
// anything left to do.
func (r *Raft) ackReadIndex(ctx, peer uint64) {
	pr, ok := r.pendingReads[ctx]
	if !ok {
		return
	}
	pr.acked[peer] = true
	if r.hasMajorityCount(len(pr.acked)) {
		r.readStates = append(r.readStates, ReadState{Index: pr.index, Context: ctx})
		delete(r.pendingReads, ctx)
	}
}
