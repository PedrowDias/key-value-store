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
	RequestReadIndex(ctx uint64) error
	CreateSnapshot(index uint64, data []byte) error
	Status() raft.Status
	Entries(start, end uint64) []raft.LogEntry
	Persist() ([]raft.Message, []raft.ReadState, *raft.Snapshot, error)
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

	// batchWindow is how long Run waits, after the first proposal in an
	// otherwise-idle moment arrives, for more proposals to accumulate
	// before submitting them together via one ProposeBatch call — the
	// standard "group commit" / "linger" tuning knob (the same idea as
	// Kafka's linger.ms or CockroachDB's proposal batching). A batch
	// still flushes early, without waiting out the full window, once it
	// reaches maxBatchSize. Zero disables waiting entirely: every
	// proposal is submitted as soon as Run notices it, individually
	// unless others happen to already be queued in the same instant.
	//
	// This exists because of a real, measured finding, not a guess:
	// batching client writes together only helps throughput if enough
	// of them are actually queued up at the moment a batch gets
	// submitted, and on fast hardware individual writes can complete
	// quickly enough that a purely opportunistic (never-wait) drain
	// doesn't reliably accumulate more than one or two — see
	// bench/BENCHMARKS.md's real-hardware section for the measurement
	// that motivated this.
	batchWindow  time.Duration
	maxBatchSize int

	proposeCh   chan proposeRequest
	readIndexCh chan readIndexRequest
	stopCh      chan struct{}
	stopOnce    sync.Once
	doneCh      chan struct{}

	// Owned exclusively by the Run goroutine; never touched from any
	// other goroutine.
	lastApplied uint64
	waiters     map[uint64]pendingPropose

	// snapshotThreshold is how many entries must have been applied
	// since the last snapshot (local or received) before Run
	// automatically triggers a new one — see maybeSnapshot's own doc.
	// lastSnapshotIndex tracks the boundary of that most recent
	// snapshot, local or received, so the threshold check has
	// something to measure forward progress against. Zero
	// snapshotThreshold disables automatic snapshotting entirely.
	snapshotThreshold int
	lastSnapshotIndex uint64

	// pendingReadIndexes and nextReadCtx are also Run-goroutine-only —
	// see LinearizableGet's doc for the full protocol. nextReadCtx is a
	// simple incrementing counter rather than anything fancier, since
	// Run is the only goroutine that ever assigns one, making a data
	// race on it structurally impossible. Starts at 1, not 0: context 0
	// is reserved to mean "no read context attached" at the message
	// level (Message.ReadContext's own doc) — matching this project's
	// existing convention of reserving 0 as a sentinel for uint64 IDs
	// (votedFor, leaderID) — so a real request must never be assigned
	// context 0, or its confirmation would be indistinguishable from an
	// ordinary, unrelated message and silently never counted.
	pendingReadIndexes map[uint64]readIndexRequest
	nextReadCtx        uint64

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

// readIndexRequest is what LinearizableGet hands to Run() via a channel,
// mirroring proposeRequest's own pattern: the actual raft.Node call
// (RequestReadIndex) must happen on Run's single goroutine, since
// raft.Node is not safe for concurrent use.
type readIndexRequest struct {
	resultCh chan readIndexResult
}

type readIndexResult struct {
	index uint64
	err   error
}

// proposeChBufferSize bounds how many pending Put/Delete calls can be
// queued waiting for Run() to pick them up.
const proposeChBufferSize = 256

// Defaults for the batching window (see Server.batchWindow's doc) and
// the safety cap on how large one batch can grow before flushing early
// regardless of the window. These are conservative starting points, not
// claimed-optimal — SetBatchWindow/SetMaxBatchSize exist so a caller
// (or a benchmark sweeping the parameter) can tune them for its own
// hardware and workload shape.
const (
	defaultBatchWindow  = 500 * time.Microsecond
	defaultMaxBatchSize = 64
	// defaultSnapshotThreshold is how many entries accumulate (since
	// the last snapshot, local or received) before Run automatically
	// triggers a new one — see maybeSnapshot's own doc. Deliberately a
	// conservative starting point rather than claimed-optimal, same as
	// the batching defaults above: a real deployment would want to
	// tune this against its own workload's write rate and how large a
	// snapshot the underlying dataset actually produces —
	// SetSnapshotThreshold exists for exactly that. Zero disables
	// automatic snapshotting entirely.
	defaultSnapshotThreshold = 10000
)

// New constructs a Server. tr and eng are assumed already open; Server
// takes ownership of ticking/driving node but does NOT close tr or eng
// itself (the caller opened them and should close them, typically after
// calling Server.Stop()).
func New(node raftNode, tr *transport.Transport, eng *engine.Engine, tickInterval time.Duration) *Server {
	return &Server{
		node:               node,
		tr:                 tr,
		eng:                eng,
		tickInterval:       tickInterval,
		batchWindow:        defaultBatchWindow,
		maxBatchSize:       defaultMaxBatchSize,
		snapshotThreshold:  defaultSnapshotThreshold,
		proposeCh:          make(chan proposeRequest, proposeChBufferSize),
		readIndexCh:        make(chan readIndexRequest, proposeChBufferSize),
		stopCh:             make(chan struct{}),
		doneCh:             make(chan struct{}),
		waiters:            make(map[uint64]pendingPropose),
		pendingReadIndexes: make(map[uint64]readIndexRequest),
		nextReadCtx:        1,
		cachedStatus:       node.Status(),
	}
}

// SetBatchWindow overrides the default group-commit batching window (see
// Server.batchWindow's doc). Must be called before Run(); changing it
// after Run has started is not safe (Run reads it without a lock, since
// under normal use it's set once at startup and never touched again).
func (s *Server) SetBatchWindow(d time.Duration) { s.batchWindow = d }

// SetMaxBatchSize overrides the default cap on how many proposals can
// accumulate in one batch before it flushes early, regardless of
// SetBatchWindow. Must be called before Run(), for the same reason as
// SetBatchWindow.
func (s *Server) SetMaxBatchSize(n int) { s.maxBatchSize = n }

// SetSnapshotThreshold overrides the default automatic-snapshot trigger
// (see Server.snapshotThreshold's doc). Must be called before Run(),
// for the same reason as SetBatchWindow. A value of 0 disables
// automatic snapshotting entirely — CreateSnapshot could still be
// triggered some other way if this project ever added one, but nothing
// currently does.
func (s *Server) SetSnapshotThreshold(n int) { s.snapshotThreshold = n }

// SeedAppliedIndex tells Server that state up through index is already
// reflected in eng (the one passed to New) — used exactly once, at
// startup, when a previously-persisted snapshot was found and restored
// into the engine before Server was even constructed (see
// cmd/kvstore's buildComponents). Without this, applyCommitted would
// try to re-apply entries starting from index 0, which not only
// duplicates work already reflected in the restored engine state but
// would fail outright: this node's own raft log no longer has that
// history to replay — it's exactly what the snapshot superseded. Must
// be called before Run(), and only when index is genuinely already
// applied; calling it with a stale or wrong value would silently skip
// applying real entries.
func (s *Server) SeedAppliedIndex(index uint64) {
	s.lastApplied = index
	s.lastSnapshotIndex = index
}

// Run drives the server's event loop until Stop is called. Intended to
// be run in its own goroutine: `go srv.Run()`.
func (s *Server) Run() {
	defer close(s.doneCh)
	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()

	// pending accumulates proposals across loop iterations between the
	// first one arriving and the batch actually being submitted — see
	// the batchWindow doc on why this waits at all instead of
	// submitting immediately. batchTimerC is nil (blocks forever, i.e.
	// effectively disabled) whenever there's no batch being
	// accumulated; it's armed the moment the first proposal of a new
	// batch arrives.
	var pending []proposeRequest
	var batchTimer *time.Timer
	var batchTimerC <-chan time.Time

	for {
		select {
		case <-ticker.C:
			s.node.Tick()
			s.pump()

		case m, ok := <-s.tr.Recv():
			if !ok {
				s.failPending(pending, fmt.Errorf("server: stopped"))
				return
			}
			s.node.Step(m)
			s.pump()

		case req := <-s.proposeCh:
			pending = append(pending, req)
			switch {
			case s.batchWindow <= 0 || len(pending) >= s.maxBatchSize:
				// No window configured, or the batch is already as
				// large as it's allowed to get: submit right now
				// rather than waiting.
				if batchTimer != nil {
					batchTimer.Stop()
					batchTimerC = nil
				}
				s.submitBatch(pending)
				pending = nil
			case len(pending) == 1:
				// First proposal of a new batch: start the window.
				// Later proposals arriving before it fires just join
				// pending (see the case above and below) without
				// restarting the timer — the window bounds how long
				// the FIRST proposal in a batch waits, not a rolling
				// window per-arrival.
				batchTimer = time.NewTimer(s.batchWindow)
				batchTimerC = batchTimer.C
			}

		case req := <-s.readIndexCh:
			ctx := s.nextReadCtx
			s.nextReadCtx++
			if err := s.node.RequestReadIndex(ctx); err != nil {
				req.resultCh <- readIndexResult{err: err}
				break
			}
			s.pendingReadIndexes[ctx] = req
			s.pump()

		case <-batchTimerC:
			batchTimerC = nil
			s.submitBatch(pending)
			pending = nil

		case <-s.stopCh:
			s.failPending(pending, fmt.Errorf("server: stopped"))
			s.failPendingReadIndexes(fmt.Errorf("server: stopped"))
			return
		}
	}
}

// submitBatch proposes every request in reqs via a single ProposeBatch
// call (see ProposeBatch's own doc for why this — not a loop of
// individual Propose calls — is what makes batching actually reduce
// network and follower-side work, not just the leader's own WAL
// persistence), registers each one's waiter, then pumps the resulting
// state forward. A no-op if reqs is empty (the batch timer can fire
// after pending was already flushed by the max-size path in the same
// iteration it was about to fire in; Go's timer/channel semantics don't
// make that combination impossible to observe).
func (s *Server) submitBatch(reqs []proposeRequest) {
	if len(reqs) == 0 {
		return
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
	s.pump()
}

// failPending immediately notifies every request still waiting to be
// submitted (i.e. still sitting in Run's local `pending` slice, not yet
// even proposed to Raft) that the server is stopping — without this,
// those callers would otherwise hang until their own proposeTimeout
// fires with a generic "timed out" error that doesn't actually explain
// what happened.
func (s *Server) failPending(pending []proposeRequest, err error) {
	for _, r := range pending {
		r.resultCh <- applyResult{err: err}
	}
}

// failPendingReadIndexes notifies every outstanding LinearizableGet
// request that will now never be confirmed (the server is stopping) and
// clears the map — mirroring failPending's role for write proposals.
func (s *Server) failPendingReadIndexes(err error) {
	for ctx, req := range s.pendingReadIndexes {
		req.resultCh <- readIndexResult{err: err}
		delete(s.pendingReadIndexes, ctx)
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
	msgs, readStates, snapshotToApply, err := s.node.Persist()
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
	if snapshotToApply != nil {
		// This node just installed a snapshot received from a leader —
		// raft.Node's own log/commitIndex already reflect it (and that
		// was already made durable inside the Persist() call above).
		// Restoring the actual engine.Engine state from it happens
		// here, BEFORE applyCommitted runs below: lastApplied must
		// already reflect the snapshot's boundary by the time
		// applyCommitted asks for entries starting there, since this
		// node's own raft log no longer has earlier history to replay
		// — it's exactly what the snapshot superseded. A failure here
		// is intentionally non-fatal and simply retried: raft.Node
		// will report the same persisted snapshot again on the very
		// next restart (OpenNode re-reads it from durable storage
		// independent of any in-memory "already reported" tracking),
		// so there's no permanent inconsistency risk, only a delay
		// until the engine catches up.
		if err := s.eng.RestoreSnapshot(snapshotToApply.Data); err != nil {
			fmt.Printf("server: failed to restore a received snapshot (index %d) into the engine, will retry on next restart: %v\n", snapshotToApply.LastIncludedIndex, err)
		} else {
			s.lastApplied = snapshotToApply.LastIncludedIndex
			s.lastSnapshotIndex = snapshotToApply.LastIncludedIndex
		}
	}
	for _, m := range msgs {
		// Best-effort: Transport itself doesn't retry, and neither do
		// we here — a dropped message just means Raft's own timeout/
		// retry machinery (a future heartbeat, a future election)
		// handles it, exactly as it's designed to.
		s.tr.Send(m)
	}
	s.applyCommitted()
	s.maybeSnapshot()

	// Resolve any ReadIndex requests a majority just confirmed. The
	// ReadIndex protocol's other half — waiting for this node's own
	// locally-applied state to catch up to the confirmed index before
	// it's actually safe to read — needs no separate step here: a
	// ReadState's Index is always <= the commitIndex it was confirmed
	// against, which is itself <= this call's CURRENT commitIndex (it
	// only grows), and applyCommitted just unconditionally caught this
	// node up to that current commitIndex. So by construction,
	// s.lastApplied >= rs.Index always holds by the time we get here —
	// a property specific to this project's single-threaded,
	// apply-every-pump-call architecture, not something a more general
	// implementation (e.g. applying on a separate goroutine) could
	// assume for free.
	for _, rs := range readStates {
		req, ok := s.pendingReadIndexes[rs.Context]
		if !ok {
			continue // already resolved some other way, or unrecognized
		}
		delete(s.pendingReadIndexes, rs.Context)
		req.resultCh <- readIndexResult{index: rs.Index}
	}

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

// maybeSnapshot triggers a new snapshot once enough entries have
// accumulated since the last one (local or received) — see
// Server.snapshotThreshold's doc for the threshold itself. Called after
// every applyCommitted, so the check always runs against this node's
// latest locally-applied state, not a stale one.
//
// A failure taking or persisting a snapshot is intentionally
// non-fatal: this node simply tries again on a later call once more
// entries have accumulated (or, in practice, likely resolves itself —
// e.g. a transient engine.Snapshot error from a race with something
// else, or a raft.Node.CreateSnapshot rejection because the target
// index briefly isn't committed yet, which the next attempt at a
// higher index won't hit). The log growing somewhat larger than
// intended in the meantime is a performance concern, not a correctness
// one.
func (s *Server) maybeSnapshot() {
	if s.snapshotThreshold <= 0 {
		return
	}
	if s.lastApplied <= s.lastSnapshotIndex {
		return
	}
	if s.lastApplied-s.lastSnapshotIndex < uint64(s.snapshotThreshold) {
		return
	}

	index := s.lastApplied
	data, err := s.eng.Snapshot()
	if err != nil {
		fmt.Printf("server: failed to take an engine snapshot at index %d: %v\n", index, err)
		return
	}
	if err := s.node.CreateSnapshot(index, data); err != nil {
		fmt.Printf("server: failed to create a raft snapshot at index %d: %v\n", index, err)
		return
	}
	s.lastSnapshotIndex = index
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
// elsewhere). This is eventually-consistent read-from-local-state,
// which is what most Raft-backed KV stores default to for reads that
// don't need to go through the leader; LinearizableGet is the opt-in
// alternative when a caller genuinely needs a stronger guarantee.
func (s *Server) Get(key []byte) ([]byte, bool, error) {
	return s.eng.Get(key)
}

// LinearizableGet reads key with a linearizability guarantee: the
// result is guaranteed to reflect every write that had already
// committed before this call began — never a stale value, unlike Get.
// It does this via the ReadIndex protocol (Raft paper §8,
// raft.Raft.RequestReadIndex): confirm, through a fresh round of
// AppendEntries to a majority, that this node is still the legitimate
// leader as of right now, then read local state once it's caught up to
// that confirmed point.
//
// This only works when called against the current leader — like
// Put/Delete, it returns raft.ErrNotLeader (wrapped) otherwise, and the
// caller is expected to redirect to whichever node it believes is
// leader. Costs a real network round trip to a majority of the
// cluster, unlike Get's purely-local read — pay for it only when the
// guarantee actually matters to the caller.
func (s *Server) LinearizableGet(key []byte) ([]byte, bool, error) {
	resultCh := make(chan readIndexResult, 1)
	select {
	case s.readIndexCh <- readIndexRequest{resultCh: resultCh}:
	case <-s.doneCh:
		return nil, false, fmt.Errorf("server: stopped")
	}

	select {
	case res := <-resultCh:
		if res.err != nil {
			return nil, false, res.err
		}
		// The read is confirmed as of res.index; local state is
		// already guaranteed caught up to at least that point (see
		// pump's doc for why no separate wait is needed here). Reading
		// current state now — possibly even fresher than res.index, if
		// more has committed and applied in the meantime — is still a
		// valid linearizable result: reflecting more than strictly
		// necessary is fine, only reflecting less would be a violation.
		return s.Get(key)
	case <-time.After(proposeTimeout):
		return nil, false, fmt.Errorf("server: linearizable read timed out waiting for read-index confirmation")
	case <-s.doneCh:
		return nil, false, fmt.Errorf("server: stopped")
	}
}

// Status returns a snapshot of the underlying Raft node's status (role,
// term, leader, commit index), safe to call from any goroutine.
func (s *Server) Status() raft.Status {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.cachedStatus
}
