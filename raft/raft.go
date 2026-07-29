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

	// Candidate-only volatile state. Reset fresh on every new election.
	votesReceived map[uint64]bool

	electionTick              int
	heartbeatTick             int
	electionElapsed           int
	heartbeatElapsed          int
	randomizedElectionTimeout int

	rnd *rand.Rand

	msgs []Message
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
	rd := Ready{Messages: r.msgs}
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
		r.becomeCandidate()
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

// becomeCandidate starts a new election: increments the term, votes for
// itself, and requests votes from every peer. A single-node cluster
// (no peers) trivially already has a majority and becomes leader at once.
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
		r.sendAppendEntries(p)
	}
}

// sendAppendEntries sends peer everything from its recorded nextIndex
// onward (empty, i.e. a pure heartbeat, if it's already caught up).
func (r *Raft) sendAppendEntries(peer uint64) {
	ni := r.nextIndex[peer]
	prevIndex := ni - 1
	prevTerm := r.termAt(prevIndex)
	entries := r.Entries(prevIndex, r.lastLogIndex())
	r.send(Message{
		Type:         MsgAppendEntries,
		To:           peer,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	})
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

	reply := Message{Type: MsgAppendEntriesResponse, To: m.From}

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

	if m.Success {
		if m.MatchIndex > r.matchIndex[m.From] {
			r.matchIndex[m.From] = m.MatchIndex
		}
		r.nextIndex[m.From] = m.MatchIndex + 1
		r.maybeAdvanceCommitIndex()
		return
	}

	// Log inconsistency: back nextIndex up by one and retry immediately.
	// This is the simple, always-correct backoff the Raft paper
	// describes; it also describes an optional optimization (the
	// follower reporting the conflicting term so the leader can skip
	// back a whole term at once instead of one entry at a time), which
	// we don't implement — a deliberate scope decision, since it only
	// affects how fast a badly-lagging follower catches up, not
	// correctness, and is a natural target to revisit if the
	// benchmarking phase shows slow catch-up after a partition heals.
	if r.nextIndex[m.From] > 1 {
		r.nextIndex[m.From]--
	}
	r.sendAppendEntries(m.From)
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
		r.sendAppendEntries(p)
	}
	if len(r.peers) == 0 {
		r.maybeAdvanceCommitIndex()
	}
	return indices, nil
}
