package raft

import "testing"

// cluster is a deterministic, in-memory simulation of a group of Raft
// nodes, for tests. There's no real time and no real network: tick()
// advances every node's logical clock by one, then fully drains whatever
// messages that produces — including messages produced BY delivering
// other messages (e.g. a RequestVote triggers a RequestVoteResponse in
// the same tick) — before returning. This models a synchronous network
// as a deliberate simplification for testing the consensus algorithm's
// correctness; message loss/reordering/partition is modeled explicitly
// and separately via isolate/heal, which is the standard way Raft
// implementations (including etcd's) test partition tolerance.
type cluster struct {
	t     *testing.T
	nodes map[uint64]*Raft
	// isolated nodes neither send nor receive any message — a full
	// network partition of exactly that one node from everyone else.
	isolated map[uint64]bool
}

func newCluster(t *testing.T, ids []uint64, electionTick, heartbeatTick int) *cluster {
	t.Helper()
	c := &cluster{
		t:        t,
		nodes:    make(map[uint64]*Raft, len(ids)),
		isolated: make(map[uint64]bool),
	}
	for _, id := range ids {
		var peers []uint64
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		r, err := New(Config{ID: id, Peers: peers, ElectionTick: electionTick, HeartbeatTick: heartbeatTick})
		if err != nil {
			t.Fatalf("New(id=%d): %v", id, err)
		}
		c.nodes[id] = r
	}
	return c
}

// tick advances every node's clock by one and fully delivers all
// resulting traffic before returning.
func (c *cluster) tick() {
	for _, r := range c.nodes {
		r.Tick()
	}
	c.deliverAll()
}

// ticks calls tick n times.
func (c *cluster) ticks(n int) {
	for i := 0; i < n; i++ {
		c.tick()
	}
}

// deliverAll drains every node's outbox and delivers each message to its
// recipient (dropping messages to/from an isolated node), repeating until
// no node has anything left to send — i.e. the cluster reaches a quiet
// state for this round.
func (c *cluster) deliverAll() {
	for {
		var any bool
		for id, r := range c.nodes {
			for _, m := range r.ReadMessages() {
				any = true
				if c.isolated[id] || c.isolated[m.To] {
					continue // dropped: sender or recipient is partitioned away
				}
				recipient, ok := c.nodes[m.To]
				if !ok {
					continue
				}
				recipient.Step(m)
			}
		}
		if !any {
			return
		}
	}
}

func (c *cluster) isolate(id uint64) { c.isolated[id] = true }
func (c *cluster) heal(id uint64)    { delete(c.isolated, id) }

// leader returns the ID of the node this cluster currently agrees is
// leader: exactly one node in Leader role, at the highest term seen, with
// every non-isolated node either agreeing or not yet caught up to a
// stale/nonexistent belief. Returns (0, false) if there's no such
// consensus (e.g. mid-election).
func (c *cluster) leader() (uint64, bool) {
	var leaderID uint64
	var maxTerm uint64
	count := 0
	for id, r := range c.nodes {
		if c.isolated[id] {
			continue
		}
		s := r.Status()
		if s.Role == Leader {
			if s.Term > maxTerm {
				maxTerm = s.Term
				leaderID = s.ID
				count = 1
			} else if s.Term == maxTerm {
				count++
			}
		}
	}
	if count == 1 {
		return leaderID, true
	}
	return 0, false
}

// propose finds the current leader and proposes data through it, failing
// the test if there isn't exactly one.
func (c *cluster) propose(data []byte) {
	c.t.Helper()
	id, ok := c.leader()
	if !ok {
		c.t.Fatalf("propose: no single leader found")
	}
	if err := c.nodes[id].Propose(data); err != nil {
		c.t.Fatalf("propose via node %d: %v", id, err)
	}
}

// allCommitted reports whether every non-isolated node's CommitIndex is
// at least index.
func (c *cluster) allCommitted(index uint64) bool {
	for id, r := range c.nodes {
		if c.isolated[id] {
			continue
		}
		if r.Status().CommitIndex < index {
			return false
		}
	}
	return true
}
