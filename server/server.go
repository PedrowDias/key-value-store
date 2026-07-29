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
	ProposeBatch(datas [][]byte) ([]uint64, error)
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

// proposeChBufferSize bounds how many pending Put/Delete calls can be
// queued waiting for Run() to pick them up. A buffer (rather than an
// unbuffered channel) is what makes batching in Run()'s propose case
// actually effective under concurrent load: with no buffer, only a
// proposal that happens to already be blocked mid-send at the exact
// moment Run() checks would ever be found by the non-blocking drain: a
// buffer lets many concurrent callers' proposals actually accumulate
// while Run() is busy with the previous batch's Persist()/pump() cycle.
const proposeChBufferSize = 256

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
		proposeCh:    make(chan proposeRequest, proposeChBufferSize),
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
			// Group commit: opportunistically drain every OTHER
			// proposal already waiting in the channel right now, so
			// all of them share the single Persist() (and therefore
			// single WAL fsync) call this iteration makes, instead of
			// each paying for its own. Under concurrent write load this
			// is the single highest-leverage change for throughput —
			// fsync latency, not CPU, is the bottleneck, and batching
			// amortizes it across every proposal in the group.
			s.drainAndAcceptProposals(req)
			s.pump()

		case <-s.stopCh:
			return
		}
	}
}

// drainAndAcceptProposals accepts req (already received from proposeCh)
// plus every other proposeRequest immediately available in the channel
// right now, without blocking for more, and submits all of them via a
// SINGLE ProposeBatch call — the other half of this project's group
// commit optimization, alongside applyCommitted's batched ApplyBatch
// call. Using ProposeBatch here (rather than calling Propose in a loop)
// is what actually matters: Propose sends AppendEntries eagerly on every
// call, so a loop of N Propose calls still produces N separate messages
// to every follower — each triggering its own follower-side append,
// persist, and response — even though the LEADER's own log append and
// eventual WAL persist would batch just fine. ProposeBatch defers
// sending until every entry in the group is already appended, so
// followers receive one message carrying all of them.
//
// Split out from Run's select loop as its own method specifically so
// it's directly testable: a test can pre-load several requests into the
// (buffered) channel and call this once, deterministically confirming
// all of them get accepted together, rather than relying on real
// goroutine-scheduling timing to occasionally win the race.
func (s *Server) drainAndAcceptProposals(req proposeRequest) {
	reqs := []proposeRequest{req}
drainLoop:
	for {
		select {
		case req2 := <-s.proposeCh:
			reqs = append(reqs, req2)
		default:
			break drainLoop
		}
	}

	datas := make([][]byte, len(reqs))
	for i, r := range reqs {
		datas[i] = r.data
	}
	indices, err := s.node.ProposeBatch(datas)
	if err != nil {
		for _, r := range reqs {
			r.resultCh <- applyResult{err: err}
		}
		return
	}
	for i, r := range reqs {
		s.waiters[indices[i]] = pendingPropose{data: r.data, resultCh: r.resultCh}
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
// current CommitIndex to the local engine — the actual "replicated
// state machine" step. Runs unconditionally regardless of this node's
// role: every node applies every committed entry, which is what keeps
// them all converging on the same state.
//
// Every newly committed entry in this call is applied via ONE
// engine.ApplyBatch call (one WAL fsync) rather than one Put/Delete call
// (one fsync each) per entry — the other half of this project's group
// commit optimization, complementing Run()'s proposeCh draining: batching
// doesn't help if entries still get committed and applied one at a time
// once they reach this stage.
func (s *Server) applyCommitted() {
	status := s.node.Status()
	if s.lastApplied >= status.CommitIndex {
		return
	}
	entries := s.node.Entries(s.lastApplied, status.CommitIndex)
	if len(entries) == 0 {
		// Shouldn't happen (CommitIndex implies these entries exist),
		// but don't spin forever if it somehow does.
		return
	}

	// Decode every entry up front, classifying each as either a valid
	// op (to include in the shared batch) or a standalone error (a
	// malformed/unknown command, which — being essentially impossible
	// in practice since a proposer's own command was correctly encoded —
	// is handled as an isolated per-entry failure rather than aborting
	// the whole batch over one already-anomalous entry).
	cmds := make([]command, len(entries))
	decodeErrs := make([]error, len(entries))
	var ops []engine.BatchOp
	for i, entry := range entries {
		cmd, err := decodeAndValidateCommand(entry)
		if err != nil {
			decodeErrs[i] = err
			continue
		}
		cmds[i] = cmd
		ops = append(ops, engine.BatchOp{Key: cmd.Key, Value: cmd.Value, Deleted: cmd.Type == cmdDelete})
	}

	var batchErr error
	if len(ops) > 0 {
		batchErr = s.eng.ApplyBatch(ops)
	}

	for i, entry := range entries {
		s.lastApplied = entry.Index

		var res applyResult
		switch {
		case decodeErrs[i] != nil:
			res = applyResult{err: decodeErrs[i]}
		case batchErr != nil:
			res = applyResult{err: fmt.Errorf("server: applying entry %d: %w", entry.Index, batchErr)}
		}

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

// decodeAndValidateCommand decodes entry.Data and confirms it's a known
// command type, returning a descriptive error otherwise. Shared by
// applyCommitted's batch path and applyEntry (kept standalone for
// existing per-entry tests and any caller wanting to apply a single
// entry directly, outside the batched Run() path).
func decodeAndValidateCommand(entry raft.LogEntry) (command, error) {
	cmd, err := decodeCommand(entry.Data)
	if err != nil {
		return command{}, fmt.Errorf("server: decoding committed entry %d: %w", entry.Index, err)
	}
	if cmd.Type != cmdPut && cmd.Type != cmdDelete {
		return command{}, fmt.Errorf("server: unknown command type %d in entry %d", cmd.Type, entry.Index)
	}
	return cmd, nil
}

// applyEntry decodes and applies a single committed entry to the engine.
// Not used by applyCommitted's batched path (see decodeAndValidateCommand
// and the ApplyBatch call above) — kept as a standalone single-entry
// operation for tests and any future caller that needs to apply exactly
// one entry without going through the batching machinery.
func (s *Server) applyEntry(entry raft.LogEntry) applyResult {
	cmd, err := decodeAndValidateCommand(entry)
	if err != nil {
		return applyResult{err: err}
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
