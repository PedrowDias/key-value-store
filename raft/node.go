package raft

import "fmt"

// Node wraps a Raft state machine with durable storage, handling the
// Ready/Advance persist-then-send contract internally so callers don't
// need to know about it: drive Node with Tick()/Step()/Propose(), then
// call Persist() once after each — it durably saves whatever needs
// saving and hands back the messages that are now safe to send.
type Node struct {
	r       *Raft
	storage *PersistentStorage
}

// OpenNode opens (or creates) durable storage at storagePath, replays it
// to recover any previous term/vote/log/snapshot boundary, and
// constructs a Raft node seeded with that recovered state before it
// participates in the cluster.
//
// The returned Snapshot is non-nil only if one was previously persisted
// (from an earlier CreateSnapshot or InstallSnapshot, before this
// restart) — the caller (ultimately the application driving this node)
// must restore its own state machine from it BEFORE applying anything
// else, since this node's own log no longer has enough history on its
// own to replay from scratch. A nil return means there's nothing to
// restore: build state up from log replay as usual.
func OpenNode(cfg Config, storagePath string) (*Node, *Snapshot, error) {
	storage, hs, log, snap, err := OpenStorage(storagePath)
	if err != nil {
		return nil, nil, err
	}
	r, err := New(cfg)
	if err != nil {
		storage.Close()
		return nil, nil, err
	}
	r.restoreState(hs, log)

	var out *Snapshot
	if snap.LastIncludedIndex > 0 {
		// Seed the freshly-constructed Raft's own snapshot bookkeeping
		// too — both so a peer that's even further behind can still be
		// caught up via InstallSnapshot after this restart, and so
		// Ready()/Advance() correctly treat this as already durably
		// persisted rather than something to report (and re-persist)
		// all over again on the very next cycle.
		r.snapshot = snap
		r.stableSnapshotIndex = snap.LastIncludedIndex
		s := snap
		out = &s
	}

	return &Node{r: r, storage: storage}, out, nil
}

// Tick advances the node's logical clock by one unit.
func (n *Node) Tick() { n.r.Tick() }

// Step processes one incoming message.
func (n *Node) Step(m Message) { n.r.Step(m) }

// Propose appends data as a new log entry, if this node is currently
// leader.
func (n *Node) Propose(data []byte) error { return n.r.Propose(data) }

// ProposeBatch appends multiple entries in one call, sending each peer a
// single AppendEntries covering all of them rather than one message per
// entry. See Raft.ProposeBatch's doc for why this matters beyond just
// convenience.
func (n *Node) ProposeBatch(datas [][]byte) ([]uint64, error) { return n.r.ProposeBatch(datas) }

// RequestReadIndex asks this node (must currently be leader) to confirm
// its continued legitimacy via a fresh round of AppendEntries to a
// majority, the safety check a linearizable read relies on. See
// Raft.RequestReadIndex's own doc for the full protocol and ctx's
// meaning. Confirmation is reported asynchronously, like everything
// else in this package: watch for a matching ReadState in a later
// Persist() call's return.
func (n *Node) RequestReadIndex(ctx uint64) error { return n.r.RequestReadIndex(ctx) }

// CreateSnapshot compacts this node's log up through index, given data
// (a serialized snapshot of the application's own state machine as of
// that index) to persist alongside it. See Raft.CreateSnapshot's own
// doc for the full contract; the durable, on-disk compaction itself
// happens on the next Persist() call, same as any other state change
// here.
func (n *Node) CreateSnapshot(index uint64, data []byte) error {
	return n.r.CreateSnapshot(index, data)
}

// Status returns a snapshot of the node's current state.
func (n *Node) Status() Status { return n.r.Status() }

// Entries returns committed (or any) log entries in (start, end].
func (n *Node) Entries(start, end uint64) []LogEntry { return n.r.Entries(start, end) }

// Persist durably saves whatever changed since the last Persist() call
// and returns the messages that are now safe to send, along with any
// ReadIndex requests a majority have now confirmed (see
// Raft.RequestReadIndex's doc), and a snapshot the application itself
// must restore its own state machine from, if this call installed one
// just received via InstallSnapshot (nil otherwise — in particular, a
// LOCALLY created snapshot, via CreateSnapshot, never produces one here,
// since the application is that snapshot's own source and has nothing
// to restore).
//
// When Ready() reports a new local snapshot boundary (from either
// CreateSnapshot or InstallSnapshot), persisting it takes priority over
// — and supersedes — the normal HardState/UnstableEntries path this
// cycle: SaveSnapshot's rewrite already captures the current HardState
// and every surviving log entry in one consistent operation, so a
// separate append of the same information would be redundant.
//
// Call this exactly once after each Tick(), Step(), Propose(), or
// CreateSnapshot() call, before doing anything else with the node —
// this is what upholds Raft's persist-before-send safety requirement.
func (n *Node) Persist() ([]Message, []ReadState, *Snapshot, error) {
	rd := n.r.Ready()

	if rd.Snapshot != nil {
		hs := HardState{Term: n.r.currentTerm, Vote: n.r.votedFor}
		survivingEntries := n.r.Entries(rd.Snapshot.LastIncludedIndex, n.r.lastLogIndex())
		if err := n.storage.SaveSnapshot(hs, *rd.Snapshot, survivingEntries); err != nil {
			return nil, nil, nil, fmt.Errorf("raft: node persist: %w", err)
		}
	} else {
		if rd.HardState != nil {
			if err := n.storage.SaveHardState(*rd.HardState); err != nil {
				return nil, nil, nil, fmt.Errorf("raft: node persist: %w", err)
			}
		}
		if len(rd.UnstableEntries) > 0 {
			if err := n.storage.SaveEntries(rd.FirstUnstableIndex, rd.UnstableEntries); err != nil {
				return nil, nil, nil, fmt.Errorf("raft: node persist: %w", err)
			}
		}
	}

	n.r.Advance()
	return rd.Messages, rd.ReadStates, rd.SnapshotToApply, nil
}

// Close closes the node's underlying storage.
func (n *Node) Close() error {
	return n.storage.Close()
}
