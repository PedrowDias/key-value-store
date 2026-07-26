package server

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/PedrowDias/key-value-store/engine"
	"github.com/PedrowDias/key-value-store/raft"
	"github.com/PedrowDias/key-value-store/transport"
)

// proposeTimeout bounds how long Put/Delete waits for its entry to
// commit before giving up. A real cluster in the middle of a leadership
// change might take a few election-timeout rounds to stabilize; this is
// generous relative to typical tick/election intervals but still finite,
// since a client shouldn't hang forever if this node is partitioned away
// from the majority and can never commit anything.
var proposeTimeout = 5 * time.Second

// applyResult is what Put/Delete/propose ultimately resolve to, once
// their entry either applies successfully or is determined to have
// failed / been superseded.
type applyResult struct {
	err error
}

// pendingPropose tracks a proposer's own copy of the data it proposed,
// alongside the channel to notify. Comparing this against what actually
// gets applied at that log index (see (*Server).applyCommitted) is what
// catches a genuinely subtle correctness pitfall: if leadership changes
// between proposing and committing, a DIFFERENT command can end up at
// the index this proposal expected to land at. Without this check, a
// client could be told its command succeeded when actually someone
// else's did.
type pendingPropose struct {
	data     []byte
	resultCh chan applyResult
}

type proposeRequest struct {
	data     []byte
	resultCh chan applyResult
}

// raftNode is the subset of *raft.Node's methods Server needs. Defined
// as an interface purely so tests can inject a fake that fails at a
// precise point (Persist(), specifically) — forcing that failure through
// a real raft.Node from outside the raft package isn't practical without
// its own injectable seam, and raft.Node already has thorough persist-
// failure coverage in its own package's tests; this interface exists
// only so Server's OWN error-handling code around it gets exercised too.
// *raft.Node satisfies this as-is.
type raftNode interface {
	Tick()
	Step(m raft.Message)
	Propose(data []byte) error
	Status() raft.Status
	Entries(start, end uint64) []raft.LogEntry
	Persist() ([]raft.Message, error)
}

// Server runs one node's full participation in the cluster: driving its
// raft.Node (ticking, stepping incoming messages, persisting, sending
// outbound messages) and applying newly committed entries to its local
// engine.Engine, all from a single goroutine (Run). This single-
// goroutine-owns-the-Raft-node design is deliberate and important:
// raft.Node (like raft.Raft underneath it) is not safe for concurrent
// use by multiple goroutines, matching the library's synchronous,
// single-threaded-event-loop design. Put/Delete calls from other
// goroutines hand off their proposals through a channel rather than
// calling into the node directly.
type Server struct {
	node raftNode
	tr   *transport.Transport
	eng  *engine.Engine

	tickInterval time.Duration

	proposeCh chan proposeRequest
	stopCh    chan struct{}
	stopOnce  sync.Once
	doneCh    chan struct{}

	// Owned exclusively by the Run goroutine; never touched from any
	// other goroutine.
	lastApplied uint64
	waiters     map[uint64]pendingPropose

	// cachedStatus mirrors the underlying raft.Node's status so Status()
	// can be called safely from any goroutine. This matters because
	// raft.Node (like raft.Raft beneath it) is deliberately NOT safe for
	// concurrent access — it's designed to be driven by exactly one
	// goroutine (Run, here). Status() is a public method any caller
	// might reasonably invoke from elsewhere (a test, an HTTP handler,
	// anything wanting observability), so it must not read node's state
	// directly; only Run ever calls node.Status() itself, writing the
	// result here under statusMu for everyone else to read.
	statusMu     sync.RWMutex
	cachedStatus raft.Status
}

// New constructs a Server. tr and eng are assumed already open; Server
// takes ownership of ticking/driving node but does NOT close tr or eng
// itself (the caller opened them and should close them, typically after
// calling Server.Stop()).
func New(node raftNode, tr *transport.Transport, eng *engine.Engine, tickInterval time.Duration) *Server {
	return &Server{
		node:         node,
		tr:           tr,
		eng:          eng,
		tickInterval: tickInterval,
		proposeCh:    make(chan proposeRequest),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
		waiters:      make(map[uint64]pendingPropose),
		cachedStatus: node.Status(),
	}
}

// Run drives the server's event loop until Stop is called. Intended to
// be run in its own goroutine: `go srv.Run()`.
func (s *Server) Run() {
	defer close(s.doneCh)
	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.node.Tick()
			s.pump()

		case m, ok := <-s.tr.Recv():
			if !ok {
				return
			}
			s.node.Step(m)
			s.pump()

		case req := <-s.proposeCh:
			if err := s.node.Propose(req.data); err != nil {
				req.resultCh <- applyResult{err: err}
				continue
			}
			idx := s.node.Status().LastLogIndex
			s.waiters[idx] = pendingPropose{data: req.data, resultCh: req.resultCh}
			s.pump()

		case <-s.stopCh:
			return
		}
	}
}

// Stop signals Run to exit and waits for it to actually do so. Safe to
// call more than once, and safe to call even if Run was never started.
func (s *Server) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	<-s.doneCh
}

// pump persists whatever changed (per raft.Node's Ready/Advance
// contract, handled inside Persist) and sends the resulting messages,
// then applies any newly committed entries. Called after every Tick,
// Step, and Propose, which is the discipline raft.Node's documentation
// requires.
func (s *Server) pump() {
	msgs, err := s.node.Persist()
	if err != nil {
		// A persist failure means this node can no longer safely
		// participate (its durability guarantees are broken) — there's
		// no good local recovery, so surface it loudly. A production
		// system would likely crash the process here rather than limp
		// along with a raft.Node whose disk state may now be out of
		// sync with its in-memory state; for this project, logging is
		// the honest signal without pulling in a logging framework.
		fmt.Printf("server: persist error, node may be unsafe to continue: %v\n", err)
		return
	}
	for _, m := range msgs {
		// Best-effort: Transport itself doesn't retry, and neither do
		// we here — a dropped message just means Raft's own timeout/
		// retry machinery (a future heartbeat, a future election)
		// handles it, exactly as it's designed to.
		s.tr.Send(m)
	}
	s.applyCommitted()
	s.refreshCachedStatus()
}

// refreshCachedStatus updates the status other goroutines read via
// Status(). Only ever called from the Run goroutine, right after any
// operation that might have changed the underlying raft.Node's state.
func (s *Server) refreshCachedStatus() {
	st := s.node.Status()
	s.statusMu.Lock()
	s.cachedStatus = st
	s.statusMu.Unlock()
}

// applyCommitted applies every entry between lastApplied and the
// current CommitIndex, in order, to the local engine — the actual
// "replicated state machine" step. Runs unconditionally regardless of
// this node's role: every node applies every committed entry, which is
// what keeps them all converging on the same state.
func (s *Server) applyCommitted() {
	status := s.node.Status()
	for s.lastApplied < status.CommitIndex {
		nextIndex := s.lastApplied + 1
		entries := s.node.Entries(s.lastApplied, nextIndex)
		if len(entries) == 0 {
			// Shouldn't happen (CommitIndex implies the entry exists),
			// but don't spin forever if it somehow does.
			return
		}
		entry := entries[0]
		s.lastApplied = nextIndex

		res := s.applyEntry(entry)

		if p, ok := s.waiters[entry.Index]; ok {
			delete(s.waiters, entry.Index)
			if !bytes.Equal(p.data, entry.Data) {
				// This proposer's command never actually landed at the
				// index it expected — a leadership change intervened
				// and a DIFFERENT command committed there instead. This
				// is the subtle correctness case: without this check, a
				// client could be told its Put succeeded when in fact
				// someone else's did.
				p.resultCh <- applyResult{err: fmt.Errorf("server: proposal was superseded before committing (leadership likely changed); retry")}
			} else {
				p.resultCh <- res
			}
		}
	}
}

// applyEntry decodes and applies one committed entry to the engine.
func (s *Server) applyEntry(entry raft.LogEntry) applyResult {
	cmd, err := decodeCommand(entry.Data)
	if err != nil {
		return applyResult{err: fmt.Errorf("server: decoding committed entry %d: %w", entry.Index, err)}
	}
	switch cmd.Type {
	case cmdPut:
		if err := s.eng.Put(cmd.Key, cmd.Value); err != nil {
			return applyResult{err: fmt.Errorf("server: applying put for entry %d: %w", entry.Index, err)}
		}
	case cmdDelete:
		if err := s.eng.Delete(cmd.Key); err != nil {
			return applyResult{err: fmt.Errorf("server: applying delete for entry %d: %w", entry.Index, err)}
		}
	default:
		return applyResult{err: fmt.Errorf("server: unknown command type %d in entry %d", cmd.Type, entry.Index)}
	}
	return applyResult{}
}

// propose hands data off to the Run goroutine as a Raft proposal and
// waits for it to either be applied, fail outright (e.g. not leader), be
// superseded by a leadership change, or time out.
func (s *Server) propose(data []byte) error {
	resultCh := make(chan applyResult, 1)
	select {
	case s.proposeCh <- proposeRequest{data: data, resultCh: resultCh}:
	case <-s.doneCh:
		return fmt.Errorf("server: stopped")
	}

	select {
	case res := <-resultCh:
		return res.err
	case <-time.After(proposeTimeout):
		return fmt.Errorf("server: propose timed out waiting for commit")
	case <-s.doneCh:
		return fmt.Errorf("server: stopped")
	}
}

// Put replicates key=value across the cluster via Raft and applies it to
// this node's engine once committed. Returns raft.ErrNotLeader (wrapped)
// if this node isn't currently leader — the caller is expected to find
// and retry against the actual leader.
func (s *Server) Put(key, value []byte) error {
	return s.propose(encodeCommand(command{Type: cmdPut, Key: key, Value: value}))
}

// Delete replicates a deletion of key across the cluster, the same way
// Put does.
func (s *Server) Delete(key []byte) error {
	return s.propose(encodeCommand(command{Type: cmdDelete, Key: key}))
}

// Get reads key directly from this node's local engine, WITHOUT going
// through Raft. This is a deliberate scope decision, not an oversight:
// it means reads are fast and always available (even during a leader
// election), but are only as fresh as this node's applied log —
// possibly stale on a follower, and possibly stale even on a leader that
// has just lost leadership without yet realizing it (a "stale leader"
// serving a read while a new leader has already committed newer writes
// elsewhere). Real linearizable reads need an additional mechanism (a
// "ReadIndex" round confirming current leadership before serving the
// read, described in the Raft paper's extended thesis) — a reasonable
// thing to add later if a client actually needs read-your-writes or
// linearizability guarantees; until then, this is eventually-consistent
// read-from-local-state, which is what most Raft-backed KV stores
// default to for reads that don't need to go through the leader.
func (s *Server) Get(key []byte) ([]byte, bool, error) {
	return s.eng.Get(key)
}

// Status returns a snapshot of the underlying Raft node's status (role,
// term, leader, commit index), safe to call from any goroutine.
func (s *Server) Status() raft.Status {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.cachedStatus
}
