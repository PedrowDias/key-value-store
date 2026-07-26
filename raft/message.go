// Package raft implements the core Raft consensus algorithm (Diego
// Ongaro and John Ousterhout, "In Search of an Understandable Consensus
// Algorithm") as a synchronous, deterministic state machine, decoupled
// entirely from networking and timing — the same architecture etcd's
// raft library uses, and for the same reason: it's the only way to make
// a distributed consensus algorithm's tricky edge cases (split votes,
// stale leaders, partition recovery, the Figure 8 commit-safety rule)
// testable without flaky sleep-based timing.
//
// The core type, Raft, has no goroutines and makes no network calls. It
// exposes exactly two entry points a surrounding system drives:
//
//   - Tick() advances Raft's internal logical clock by one unit. The
//     caller decides what a "tick" means in wall-clock time (e.g. call it
//     every 100ms) — Raft itself only counts ticks.
//   - Step(msg) feeds in one message, either an incoming RPC request from
//     a peer or a response to an RPC this node previously sent.
//
// Both may cause Raft to want to send messages (a RequestVote to every
// peer on starting an election, an AppendEntries response back to a
// leader, etc.); those accumulate in an internal outbox drained via
// ReadMessages(). Actually delivering them — over a real network, or (in
// tests) directly to another in-memory Raft instance — is entirely the
// caller's responsibility. This package doesn't know what a "peer" is
// beyond an opaque uint64 ID.
package raft

// Role is a node's current position in the Raft state machine.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry is one entry in a node's replicated log. Data is an opaque
// command payload — Raft itself never interprets it; that's the job of
// whatever state machine (here, eventually, the storage engine) applies
// committed entries in order.
type LogEntry struct {
	Term  uint64
	Index uint64
	Data  []byte
}

// MessageType distinguishes the four message shapes Raft's leader
// election and log replication exchange. A single Message struct (rather
// than four separate types) keeps Step()'s dispatch and the outbox
// simple; fields irrelevant to a given Type are left zero.
type MessageType int

const (
	MsgRequestVote MessageType = iota
	MsgRequestVoteResponse
	MsgAppendEntries
	MsgAppendEntriesResponse
)

func (t MessageType) String() string {
	switch t {
	case MsgRequestVote:
		return "RequestVote"
	case MsgRequestVoteResponse:
		return "RequestVoteResponse"
	case MsgAppendEntries:
		return "AppendEntries"
	case MsgAppendEntriesResponse:
		return "AppendEntriesResponse"
	default:
		return "Unknown"
	}
}

// Message is both an RPC request and its response, and both directions
// of both RPC kinds — RequestVote/RequestVoteResponse and
// AppendEntries/AppendEntriesResponse — differentiated by Type. Only the
// fields relevant to a given Type are meaningful; the rest are zero.
type Message struct {
	Type MessageType
	From uint64
	To   uint64
	Term uint64

	// RequestVote request fields.
	LastLogIndex uint64
	LastLogTerm  uint64

	// RequestVoteResponse field.
	VoteGranted bool

	// AppendEntries request fields.
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64

	// AppendEntriesResponse fields. MatchIndex lets the leader update its
	// bookkeeping for this follower directly on success, rather than
	// inferring it; it's meaningless when Success is false.
	Success    bool
	MatchIndex uint64
}
