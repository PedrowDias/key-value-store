package server

import (
	"errors"
	"path/filepath"
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
	return nil
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
	fake := newFakeRaftNode()
	srv := New(fake, newTestTransport(t), newTestEngine(t), time.Hour) // huge tick interval: Run won't drain proposeCh on its own
	go srv.Run()
	srv.Stop() // closes doneCh before Run ever reads from proposeCh

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
