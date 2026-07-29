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
// to recover any previous term/vote/log, and constructs a Raft node
// seeded with that recovered state before it participates in the cluster.
func OpenNode(cfg Config, storagePath string) (*Node, error) {
	storage, hs, log, err := OpenStorage(storagePath)
	if err != nil {
		return nil, err
	}
	r, err := New(cfg)
	if err != nil {
		storage.Close()
		return nil, err
	}
	r.restoreState(hs, log)
	return &Node{r: r, storage: storage}, nil
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

// Status returns a snapshot of the node's current state.
func (n *Node) Status() Status { return n.r.Status() }

// Entries returns committed (or any) log entries in (start, end].
func (n *Node) Entries(start, end uint64) []LogEntry { return n.r.Entries(start, end) }

// Persist durably saves whatever changed since the last Persist() call
// (HardState and/or new log entries) and returns the messages that are
// now safe to send. Call this exactly once after each Tick(), Step(), or
// Propose() call, before doing anything else with the node — this is
// what upholds Raft's persist-before-send safety requirement.
func (n *Node) Persist() ([]Message, error) {
	rd := n.r.Ready()
	if rd.HardState != nil {
		if err := n.storage.SaveHardState(*rd.HardState); err != nil {
			return nil, fmt.Errorf("raft: node persist: %w", err)
		}
	}
	if len(rd.UnstableEntries) > 0 {
		if err := n.storage.SaveEntries(rd.FirstUnstableIndex, rd.UnstableEntries); err != nil {
			return nil, fmt.Errorf("raft: node persist: %w", err)
		}
	}
	n.r.Advance()
	return rd.Messages, nil
}

// Close closes the node's underlying storage.
func (n *Node) Close() error {
	return n.storage.Close()
}
