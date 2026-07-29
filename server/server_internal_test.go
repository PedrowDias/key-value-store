package server

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/PedrowDias/key-value-store/engine"
	"github.com/PedrowDias/key-value-store/raft"
	"github.com/PedrowDias/key-value-store/transport"
)

// fakeRaftNode implements raftNode with fully controllable behavior, for
// precisely testing Server's error-handling branches that are
// impractical to force through a live, timing-driven cluster.
type fakeRaftNode struct {
	status       raft.Status
	entries      map[uint64]raft.LogEntry // keyed by Index
	proposeErr   error
	persistErr   error
	persistMsgs  []raft.Message
	proposedData [][]byte
	// autoCommit, if true, makes Propose immediately advance
	// status.CommitIndex to match the newly proposed entry — simulating
	// an instant single-node-style commit so a full Propose-through-
	// Run()-through-applyCommitted round trip completes without needing
	// a second message exchange to simulate. Off by default so existing
	// tests that manipulate status/entries directly (bypassing the real
	// Propose flow) are unaffected.
	autoCommit bool
}

func newFakeRaftNode() *fakeRaftNode {
	return &fakeRaftNode{entries: make(map[uint64]raft.LogEntry)}
}

func (f *fakeRaftNode) Tick()               {}
func (f *fakeRaftNode) Step(m raft.Message) {}
func (f *fakeRaftNode) Status() raft.Status { return f.status }

func (f *fakeRaftNode) Propose(data []byte) error {
	if f.proposeErr != nil {
		return f.proposeErr
	}
	f.proposedData = append(f.proposedData, data)
	f.status.LastLogIndex++
	f.entries[f.status.LastLogIndex] = raft.LogEntry{Term: f.status.Term, Index: f.status.LastLogIndex, Data: data}
	if f.autoCommit {
		f.status.CommitIndex = f.status.LastLogIndex
	}
	return nil
}

// ProposeBatch mirrors Propose for each element, returning the assigned
// indices. The fake doesn't need to model ProposeBatch's real reason for
// existing (sending one combined message instead of one per entry) —
// that property is verified directly against the real raft.Raft in the
// raft package's own tests; here, Server's tests only need consistent
// index assignment and error/waiter bookkeeping.
func (f *fakeRaftNode) ProposeBatch(datas [][]byte) ([]uint64, error) {
	if f.proposeErr != nil {
		return nil, f.proposeErr
	}
	indices := make([]uint64, len(datas))
	for i, data := range datas {
		f.proposedData = append(f.proposedData, data)
		f.status.LastLogIndex++
		f.entries[f.status.LastLogIndex] = raft.LogEntry{Term: f.status.Term, Index: f.status.LastLogIndex, Data: data}
		indices[i] = f.status.LastLogIndex
	}
	if f.autoCommit {
		f.status.CommitIndex = f.status.LastLogIndex
	}
	return indices, nil
}

func (f *fakeRaftNode) Entries(start, end uint64) []raft.LogEntry {
	var out []raft.LogEntry
	for i := start + 1; i <= end; i++ {
		if e, ok := f.entries[i]; ok {
			out = append(out, e)
		}
	}
	return out
}

func (f *fakeRaftNode) Persist() ([]raft.Message, error) {
	if f.persistErr != nil {
		return nil, f.persistErr
	}
	return f.persistMsgs, nil
}

func newTestEngine(t *testing.T) *engine.Engine {
	t.Helper()
	e, err := engine.Open(engine.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func newTestTransport(t *testing.T) *transport.Transport {
	t.Helper()
	tr, err := transport.Listen(1, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Close() })
	return tr
}

// --- pump()'s persist error branch ------------------------------------------

func TestPump_PersistErrorIsHandledWithoutPanicking(t *testing.T) {
	fake := newFakeRaftNode()
	fake.persistErr = errors.New("fake: simulated persist failure")
	srv := New(fake, newTestTransport(t), newTestEngine(t), time.Millisecond)

	// pump() should log and return, not panic or corrupt state.
	srv.pump()
}

// --- applyCommitted's branches -----------------------------------------------

func TestApplyCommitted_AppliesCommandsInOrder(t *testing.T) {
	fake := newFakeRaftNode()
	eng := newTestEngine(t)
	srv := New(fake, newTestTransport(t), eng, time.Millisecond)

	fake.entries[1] = raft.LogEntry{Term: 1, Index: 1, Data: encodeCommand(command{Type: cmdPut, Key: []byte("a"), Value: []byte("1")})}
	fake.entries[2] = raft.LogEntry{Term: 1, Index: 2, Data: encodeCommand(command{Type: cmdPut, Key: []byte("b"), Value: []byte("2")})}
	fake.status.CommitIndex = 2

	srv.applyCommitted()

	val, found, err := eng.Get([]byte("a"))
	if err != nil || !found || string(val) != "1" {
		t.Fatalf("Get(a) = %q found=%v err=%v", val, found, err)
	}
	val, found, err = eng.Get([]byte("b"))
	if err != nil || !found || string(val) != "2" {
		t.Fatalf("Get(b) = %q found=%v err=%v", val, found, err)
	}
	if srv.lastApplied != 2 {
		t.Fatalf("lastApplied = %d, want 2", srv.lastApplied)
	}
}

func TestApplyCommitted_StopsIfEntryMissing(t *testing.T) {
	fake := newFakeRaftNode()
	srv := New(fake, newTestTransport(t), newTestEngine(t), time.Millisecond)

	// CommitIndex claims entry 1 exists, but it was never added to the
	// fake's entries map — applyCommitted must not spin or panic.
	fake.status.CommitIndex = 1
	srv.applyCommitted()
	if srv.lastApplied != 0 {
		t.Fatalf("lastApplied = %d, want 0 (should not advance past a missing entry)", srv.lastApplied)
	}
}

func TestApplyCommitted_NotifiesWaiterOnSuccess(t *testing.T) {
	fake := newFakeRaftNode()
	srv := New(fake, newTestTransport(t), newTestEngine(t), time.Millisecond)

	data := encodeCommand(command{Type: cmdPut, Key: []byte("k"), Value: []byte("v")})
	fake.entries[1] = raft.LogEntry{Term: 1, Index: 1, Data: data}
	fake.status.CommitIndex = 1

	resultCh := make(chan applyResult, 1)
	srv.waiters[1] = pendingPropose{data: data, resultCh: resultCh}

	srv.applyCommitted()

	select {
	case res := <-resultCh:
		if res.err != nil {
			t.Fatalf("unexpected error: %v", res.err)
		}
	default:
		t.Fatal("expected the waiter to be notified")
	}
}

func TestApplyCommitted_SupersededProposalGetsError(t *testing.T) {
	fake := newFakeRaftNode()
	srv := New(fake, newTestTransport(t), newTestEngine(t), time.Millisecond)

	// The entry that actually committed at index 1 is NOT what this
	// waiter proposed — simulating a leadership change where a
	// different command landed at the index this proposer expected.
	actualData := encodeCommand(command{Type: cmdPut, Key: []byte("someone-elses-key"), Value: []byte("v")})
	fake.entries[1] = raft.LogEntry{Term: 2, Index: 1, Data: actualData}
	fake.status.CommitIndex = 1

	myData := encodeCommand(command{Type: cmdPut, Key: []byte("my-key"), Value: []byte("v")})
	resultCh := make(chan applyResult, 1)
	srv.waiters[1] = pendingPropose{data: myData, resultCh: resultCh}

	srv.applyCommitted()

	select {
	case res := <-resultCh:
		if res.err == nil {
			t.Fatal("expected an error: this proposal was superseded by a different command")
		}
	default:
		t.Fatal("expected the waiter to be notified even on supersession")
	}
}

func TestApplyCommitted_MixedBatchAppliesGoodEntriesDespiteOneMalformed(t *testing.T) {
	fake := newFakeRaftNode()
	eng := newTestEngine(t)
	srv := New(fake, newTestTransport(t), eng, time.Millisecond)

	fake.entries[1] = raft.LogEntry{Term: 1, Index: 1, Data: encodeCommand(command{Type: cmdPut, Key: []byte("a"), Value: []byte("1")})}
	fake.entries[2] = raft.LogEntry{Term: 1, Index: 2, Data: []byte{0xFF}} // malformed
	fake.entries[3] = raft.LogEntry{Term: 1, Index: 3, Data: encodeCommand(command{Type: cmdPut, Key: []byte("c"), Value: []byte("3")})}
	fake.status.CommitIndex = 3

	waiter2 := make(chan applyResult, 1)
	srv.waiters[2] = pendingPropose{data: []byte{0xFF}, resultCh: waiter2}

	srv.applyCommitted()

	// The two well-formed entries either side of the malformed one must
	// still have been applied as part of the shared batch.
	val, found, err := eng.Get([]byte("a"))
	if err != nil || !found || string(val) != "1" {
		t.Fatalf("Get(a) = %q found=%v err=%v", val, found, err)
	}
	val, found, err = eng.Get([]byte("c"))
	if err != nil || !found || string(val) != "3" {
		t.Fatalf("Get(c) = %q found=%v err=%v", val, found, err)
	}
	// The malformed entry's own waiter must still get its error.
	select {
	case res := <-waiter2:
		if res.err == nil {
			t.Fatal("expected an error for the malformed entry")
		}
	default:
		t.Fatal("expected the malformed entry's waiter to be notified")
	}
	if srv.lastApplied != 3 {
		t.Fatalf("lastApplied = %d, want 3 (must advance past the malformed entry too)", srv.lastApplied)
	}
}

func TestApplyCommitted_WholeBatchFailureNotifiesWaiterWithError(t *testing.T) {
	fake := newFakeRaftNode()
	eng := newTestEngine(t)
	srv := New(fake, newTestTransport(t), eng, time.Millisecond)

	data := encodeCommand(command{Type: cmdPut, Key: []byte("k"), Value: []byte("v")})
	fake.entries[1] = raft.LogEntry{Term: 1, Index: 1, Data: data}
	fake.status.CommitIndex = 1

	resultCh := make(chan applyResult, 1)
	srv.waiters[1] = pendingPropose{data: data, resultCh: resultCh}

	eng.Close() // forces the whole ApplyBatch call to fail

	srv.applyCommitted()

	select {
	case res := <-resultCh:
		if res.err == nil {
			t.Fatal("expected an error when the whole batch fails to apply")
		}
	default:
		t.Fatal("expected the waiter to be notified of the batch failure")
	}
}

func TestApplyCommitted_NoOpWhenAlreadyUpToDate(t *testing.T) {
	fake := newFakeRaftNode()
	srv := New(fake, newTestTransport(t), newTestEngine(t), time.Millisecond)
	// CommitIndex == lastApplied (both zero): nothing to do.
	srv.applyCommitted()
	if srv.lastApplied != 0 {
		t.Fatalf("lastApplied = %d, want 0", srv.lastApplied)
	}
}

// --- drainAndAcceptProposals (group commit batching) ------------------------

func TestSubmitBatch_ProposesAllAndRegistersWaiters(t *testing.T) {
	fake := newFakeRaftNode()
	srv := New(fake, newTestTransport(t), newTestEngine(t), time.Millisecond)

	reqs := []proposeRequest{
		{data: []byte("first"), resultCh: make(chan applyResult, 1)},
		{data: []byte("second"), resultCh: make(chan applyResult, 1)},
		{data: []byte("third"), resultCh: make(chan applyResult, 1)},
	}
	srv.submitBatch(reqs)

	if len(fake.proposedData) != 3 {
		t.Fatalf("proposedData = %v, want 3 entries (all three submitted together)", fake.proposedData)
	}
	if len(srv.waiters) != 3 {
		t.Fatalf("waiters = %d, want 3", len(srv.waiters))
	}
}

func TestSubmitBatch_EmptyIsNoop(t *testing.T) {
	fake := newFakeRaftNode()
	srv := New(fake, newTestTransport(t), newTestEngine(t), time.Millisecond)
	srv.submitBatch(nil)
	if len(fake.proposedData) != 0 {
		t.Fatalf("proposedData = %v, want empty", fake.proposedData)
	}
}

func TestSubmitBatch_ProposeErrorNotifiesEveryWaiterInTheBatch(t *testing.T) {
	fake := newFakeRaftNode()
	fake.proposeErr = errors.New("boom")
	srv := New(fake, newTestTransport(t), newTestEngine(t), time.Millisecond)

	reqs := []proposeRequest{
		{data: []byte("a"), resultCh: make(chan applyResult, 1)},
		{data: []byte("b"), resultCh: make(chan applyResult, 1)},
	}
	srv.submitBatch(reqs)

	for i, r := range reqs {
		select {
		case res := <-r.resultCh:
			if res.err == nil {
				t.Fatalf("request %d: expected an error", i)
			}
		default:
			t.Fatalf("request %d: expected to be notified", i)
		}
	}
	if len(srv.waiters) != 0 {
		t.Fatalf("waiters = %d, want 0 (nothing should be registered on a failed batch)", len(srv.waiters))
	}
}

func TestFailPending_NotifiesEveryRequest(t *testing.T) {
	srv := New(newFakeRaftNode(), newTestTransport(t), newTestEngine(t), time.Millisecond)
	reqs := []proposeRequest{
		{data: []byte("a"), resultCh: make(chan applyResult, 1)},
		{data: []byte("b"), resultCh: make(chan applyResult, 1)},
	}
	srv.failPending(reqs, errors.New("server: stopped"))
	for i, r := range reqs {
		select {
		case res := <-r.resultCh:
			if res.err == nil {
				t.Fatalf("request %d: expected an error", i)
			}
		default:
			t.Fatalf("request %d: expected to be notified", i)
		}
	}
}

func TestFailPending_EmptyIsNoop(t *testing.T) {
	srv := New(newFakeRaftNode(), newTestTransport(t), newTestEngine(t), time.Millisecond)
	srv.failPending(nil, errors.New("x")) // must not panic on an empty slice
}

// --- applyEntry's branches ---------------------------------------------------

func TestApplyEntry_MalformedCommandReturnsError(t *testing.T) {
	srv := New(newFakeRaftNode(), newTestTransport(t), newTestEngine(t), time.Millisecond)
	res := srv.applyEntry(raft.LogEntry{Index: 1, Data: []byte{0xFF}}) // too short to decode
	if res.err == nil {
		t.Fatal("expected an error applying a malformed command")
	}
}

func TestApplyEntry_UnknownCommandTypeReturnsError(t *testing.T) {
	srv := New(newFakeRaftNode(), newTestTransport(t), newTestEngine(t), time.Millisecond)
	// A structurally valid command with an out-of-range Type byte.
	data := encodeCommand(command{Type: commandType(99), Key: []byte("k")})
	res := srv.applyEntry(raft.LogEntry{Index: 1, Data: data})
	if res.err == nil {
		t.Fatal("expected an error for an unknown command type")
	}
}

func TestApplyEntry_DeleteOfMissingKeyIsNotAnError(t *testing.T) {
	srv := New(newFakeRaftNode(), newTestTransport(t), newTestEngine(t), time.Millisecond)
	data := encodeCommand(command{Type: cmdDelete, Key: []byte("never-existed")})
	res := srv.applyEntry(raft.LogEntry{Index: 1, Data: data})
	if res.err != nil {
		t.Fatalf("unexpected error deleting a nonexistent key: %v", res.err)
	}
}

func TestApplyEntry_PutErrorPropagates(t *testing.T) {
	eng := newTestEngine(t)
	srv := New(newFakeRaftNode(), newTestTransport(t), eng, time.Millisecond)
	eng.Close() // any subsequent engine call now fails

	data := encodeCommand(command{Type: cmdPut, Key: []byte("k"), Value: []byte("v")})
	res := srv.applyEntry(raft.LogEntry{Index: 1, Data: data})
	if res.err == nil {
		t.Fatal("expected an error applying a put against a closed engine")
	}
}

func TestApplyEntry_DeleteErrorPropagates(t *testing.T) {
	eng := newTestEngine(t)
	srv := New(newFakeRaftNode(), newTestTransport(t), eng, time.Millisecond)
	eng.Close()

	data := encodeCommand(command{Type: cmdDelete, Key: []byte("k")})
	res := srv.applyEntry(raft.LogEntry{Index: 1, Data: data})
	if res.err == nil {
		t.Fatal("expected an error applying a delete against a closed engine")
	}
}

// --- propose()'s stopped-mid-flight branches --------------------------------

func TestPropose_StoppedBeforeSubmission(t *testing.T) {
	// Constructed directly (bypassing New()) with an UNBUFFERED
	// proposeCh specifically: with the real, buffered channel New()
	// uses for production throughput, a send here would just succeed
	// immediately into the buffer regardless of whether Run() is still
	// reading, masking the exact branch this test targets (the first
	// select's doneCh case, hit when the channel send itself can't
	// proceed). An unbuffered channel restores that precise scenario.
	srv := &Server{
		node:      newFakeRaftNode(),
		tr:        newTestTransport(t),
		eng:       newTestEngine(t),
		proposeCh: make(chan proposeRequest),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
		waiters:   make(map[uint64]pendingPropose),
	}
	close(srv.doneCh) // simulate Run having already exited

	err := srv.propose([]byte("x"))
	if err == nil {
		t.Fatal("expected an error proposing after the server has stopped")
	}
}

func TestPropose_StoppedWhileWaitingForResult(t *testing.T) {
	srv := New(newFakeRaftNode(), newTestTransport(t), newTestEngine(t), time.Hour)

	// Absorb exactly one request from proposeCh (simulating Run having
	// picked it up) but never respond — then close doneCh directly
	// (bypassing Stop, which would block forever waiting for a Run
	// goroutine that was never started) to simulate the server stopping
	// after submission but before a result arrives.
	go func() { <-srv.proposeCh }()

	errCh := make(chan error, 1)
	go func() { errCh <- srv.propose([]byte("x")) }()

	time.Sleep(50 * time.Millisecond) // give propose time to reach the second select
	close(srv.doneCh)

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected an error when doneCh closes while waiting for a result")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("propose did not return after doneCh closed")
	}
}

// --- Run()'s transport-channel-closed branch --------------------------------

func TestRun_ExitsWhenTransportRecvChannelCloses(t *testing.T) {
	dir := t.TempDir()
	rn, err := raft.OpenNode(raft.Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}, filepath.Join(dir, "raft.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer rn.Close()
	tr, err := transport.Listen(1, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	eng := newTestEngine(t)

	srv := New(rn, tr, eng, time.Hour) // huge tick interval: only the closed channel should end Run
	done := make(chan struct{})
	go func() {
		srv.Run()
		close(done)
	}()

	// Closing the transport (rather than calling srv.Stop()) closes its
	// Recv channel, which Run must detect and exit on cleanly, without
	// ever having Stop() called.
	tr.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after its transport's Recv channel closed")
	}
}

// --- The batching window: timer, max-batch-size, and shutdown behavior -----

func TestRun_BatchWindowTimerFlushesASingleProposal(t *testing.T) {
	fake := newFakeRaftNode()
	fake.autoCommit = true
	srv := New(fake, newTestTransport(t), newTestEngine(t), time.Hour)
	srv.SetBatchWindow(5 * time.Millisecond)
	go srv.Run()
	defer srv.Stop()

	// Nothing else is proposed alongside this one: the only way it can
	// ever get applied is via the timer firing on its own, since it'll
	// never reach maxBatchSize with just one item.
	if err := srv.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	val, found, err := srv.Get([]byte("k"))
	if err != nil || !found || string(val) != "v" {
		t.Fatalf("Get(k) = %q found=%v err=%v", val, found, err)
	}
}

func TestRun_ZeroBatchWindowSubmitsWithoutWaiting(t *testing.T) {
	fake := newFakeRaftNode()
	fake.autoCommit = true
	srv := New(fake, newTestTransport(t), newTestEngine(t), time.Hour)
	srv.SetBatchWindow(0) // disabled: submit as soon as Run notices a proposal
	go srv.Run()
	defer srv.Stop()

	start := time.Now()
	if err := srv.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	// Not a precise timing assertion (inherently flaky) — just confirms
	// this didn't wait anywhere near a "real" window would take, i.e.
	// the zero-window path actually is a distinct, faster code path.
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Put took %v with a zero batch window; expected near-immediate submission", elapsed)
	}
}

func TestRun_MaxBatchSizeFlushesBeforeTheWindowElapses(t *testing.T) {
	fake := newFakeRaftNode()
	fake.autoCommit = true
	srv := New(fake, newTestTransport(t), newTestEngine(t), time.Hour)
	srv.SetBatchWindow(time.Hour) // would never fire on its own within this test
	srv.SetMaxBatchSize(3)
	go srv.Run()
	defer srv.Stop()

	// Fire off 3 concurrent Puts — exactly maxBatchSize — and confirm
	// they all complete quickly. If max-batch-size flushing didn't
	// work, this would hang until the (hour-long) timer, which the
	// test's own timeout below would catch.
	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = srv.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
		}(i)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Puts did not complete promptly; max-batch-size early flush did not trigger")
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
}

func TestRun_StopWithPendingProposalFailsItImmediately(t *testing.T) {
	fake := newFakeRaftNode()
	srv := New(fake, newTestTransport(t), newTestEngine(t), time.Hour)
	srv.SetBatchWindow(time.Hour) // ensure the proposal is still sitting in `pending`, unsubmitted, when Stop is called
	go srv.Run()

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Put([]byte("k"), []byte("v")) }()

	// Give the proposal time to actually be received into Run's pending
	// slice before stopping.
	time.Sleep(50 * time.Millisecond)
	srv.Stop()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected an error: the server stopped before this proposal was ever submitted to Raft")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Put did not return promptly after Stop; failPending did not fire for the still-pending proposal")
	}
}
