package transport

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/PedrowDias/key-value-store/raft"
)

// listenLocal starts a Transport on an OS-assigned ephemeral port.
func listenLocal(t *testing.T, id uint64, peerAddrs map[uint64]string) *Transport {
	t.Helper()
	tr, err := Listen(id, "127.0.0.1:0", peerAddrs)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	return tr
}

// recvWithTimeout waits for one message on tr's Recv channel, failing the
// test if none arrives promptly — real network I/O between two localhost
// sockets should be on the order of microseconds, so a generous but
// bounded timeout keeps a genuine bug from hanging the test suite.
func recvWithTimeout(t *testing.T, tr *Transport) raft.Message {
	t.Helper()
	select {
	case m := <-tr.Recv():
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting to receive a message")
		return raft.Message{}
	}
}

func TestTransport_SendReceiveRoundTrip(t *testing.T) {
	b := listenLocal(t, 2, nil)
	defer b.Close()
	a := listenLocal(t, 1, map[uint64]string{2: b.Addr()})
	defer a.Close()

	msg := raft.Message{Type: raft.MsgRequestVote, From: 1, To: 2, Term: 5, LastLogIndex: 3, LastLogTerm: 2}
	if err := a.Send(msg); err != nil {
		t.Fatal(err)
	}
	got := recvWithTimeout(t, b)
	if !messagesEqual(msg, got) {
		t.Fatalf("received %+v, want %+v", got, msg)
	}
}

func TestTransport_SendReceiveWithLogEntries(t *testing.T) {
	b := listenLocal(t, 2, nil)
	defer b.Close()
	a := listenLocal(t, 1, map[uint64]string{2: b.Addr()})
	defer a.Close()

	msg := raft.Message{
		Type: raft.MsgAppendEntries, From: 1, To: 2, Term: 1,
		Entries: []raft.LogEntry{
			{Term: 1, Index: 1, Data: []byte("committed-command")},
		},
	}
	if err := a.Send(msg); err != nil {
		t.Fatal(err)
	}
	got := recvWithTimeout(t, b)
	if !messagesEqual(msg, got) {
		t.Fatalf("received %+v, want %+v", got, msg)
	}
}

func TestTransport_MultipleMessagesInOrder(t *testing.T) {
	b := listenLocal(t, 2, nil)
	defer b.Close()
	a := listenLocal(t, 1, map[uint64]string{2: b.Addr()})
	defer a.Close()

	for i := uint64(1); i <= 5; i++ {
		if err := a.Send(raft.Message{Type: raft.MsgAppendEntriesResponse, From: 1, To: 2, Term: i}); err != nil {
			t.Fatal(err)
		}
	}
	for i := uint64(1); i <= 5; i++ {
		got := recvWithTimeout(t, b)
		if got.Term != i {
			t.Fatalf("message %d: Term = %d, want %d (order not preserved)", i, got.Term, i)
		}
	}
}

func TestTransport_ReusesConnection(t *testing.T) {
	b := listenLocal(t, 2, nil)
	defer b.Close()
	a := listenLocal(t, 1, map[uint64]string{2: b.Addr()})
	defer a.Close()

	a.Send(raft.Message{Type: raft.MsgRequestVote, From: 1, To: 2, Term: 1})
	recvWithTimeout(t, b)
	a.Send(raft.Message{Type: raft.MsgRequestVote, From: 1, To: 2, Term: 2})
	recvWithTimeout(t, b)

	a.mu.Lock()
	numConns := len(a.outConns)
	a.mu.Unlock()
	if numConns != 1 {
		t.Fatalf("outbound connections to peer 2 = %d, want 1 (should be reused, not re-dialed)", numConns)
	}
}

func TestTransport_SendToUnknownPeerErrors(t *testing.T) {
	a := listenLocal(t, 1, nil) // no peer 2 registered
	defer a.Close()

	err := a.Send(raft.Message{Type: raft.MsgRequestVote, From: 1, To: 2, Term: 1})
	if err == nil {
		t.Fatal("expected an error sending to an unregistered peer")
	}
}

func TestTransport_DialFailureErrors(t *testing.T) {
	// Bind and immediately close a listener to get a real, but definitely
	// unreachable, address (nothing listening there anymore).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := ln.Addr().String()
	ln.Close()

	a := listenLocal(t, 1, map[uint64]string{2: deadAddr})
	defer a.Close()

	err = a.Send(raft.Message{Type: raft.MsgRequestVote, From: 1, To: 2, Term: 1})
	if err == nil {
		t.Fatal("expected an error dialing an address nothing is listening on")
	}
}

func TestTransport_ReconnectsAfterConnectionDrop(t *testing.T) {
	b := listenLocal(t, 2, nil)
	defer b.Close()
	a := listenLocal(t, 1, map[uint64]string{2: b.Addr()})
	defer a.Close()

	if err := a.Send(raft.Message{Type: raft.MsgRequestVote, From: 1, To: 2, Term: 1}); err != nil {
		t.Fatal(err)
	}
	recvWithTimeout(t, b)

	// Forcibly sever the connection from A's side, simulating a dropped
	// connection (not a graceful Close), then confirm A transparently
	// redials and delivery still succeeds.
	a.dropConn(2)

	if err := a.Send(raft.Message{Type: raft.MsgRequestVote, From: 1, To: 2, Term: 2}); err != nil {
		t.Fatalf("Send after simulated connection drop: %v", err)
	}
	got := recvWithTimeout(t, b)
	if got.Term != 2 {
		t.Fatalf("Term = %d, want 2", got.Term)
	}
}

func TestTransport_MalformedPeerDataDoesNotCrashOtherConnections(t *testing.T) {
	b := listenLocal(t, 2, nil)
	defer b.Close()

	// A raw, non-protocol-conforming connection sending garbage.
	conn, err := net.Dial("tcp", b.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	conn.Close()

	// A well-behaved peer must still be able to talk to b afterward.
	a := listenLocal(t, 1, map[uint64]string{2: b.Addr()})
	defer a.Close()
	if err := a.Send(raft.Message{Type: raft.MsgRequestVote, From: 1, To: 2, Term: 1}); err != nil {
		t.Fatal(err)
	}
	got := recvWithTimeout(t, b)
	if got.Term != 1 {
		t.Fatalf("Term = %d, want 1", got.Term)
	}
}

func TestTransport_CloseUnblocksRecvAndStopsGoroutines(t *testing.T) {
	tr := listenLocal(t, 1, nil)
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	// The Recv channel must be closed, not just empty, so a `for range`
	// consumer terminates rather than blocking forever.
	select {
	case _, ok := <-tr.Recv():
		if ok {
			t.Fatal("expected the channel to be closed (no value), got a value instead")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Recv channel was not closed within the timeout")
	}
}

func TestTransport_CloseIsIdempotent(t *testing.T) {
	tr := listenLocal(t, 1, nil)
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close() should be a no-op, got: %v", err)
	}
}

func TestTransport_SendAfterCloseErrors(t *testing.T) {
	b := listenLocal(t, 2, nil)
	a := listenLocal(t, 1, map[uint64]string{2: b.Addr()})
	a.Close()
	b.Close()

	err := a.Send(raft.Message{Type: raft.MsgRequestVote, From: 1, To: 2, Term: 1})
	if err == nil {
		t.Fatal("expected an error sending on a closed transport")
	}
}

func TestTransport_ConcurrentSendsToSamePeerAreSafe(t *testing.T) {
	b := listenLocal(t, 2, nil)
	defer b.Close()
	a := listenLocal(t, 1, map[uint64]string{2: b.Addr()})
	defer a.Close()

	const n = 20
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			a.Send(raft.Message{Type: raft.MsgRequestVoteResponse, From: 1, To: 2, Term: uint64(i)})
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < n; i++ {
		<-done
	}
	received := 0
	for i := 0; i < n; i++ {
		select {
		case <-b.Recv():
			received++
		case <-time.After(2 * time.Second):
			t.Fatalf("only received %d/%d messages", received, n)
		}
	}
	if received != n {
		t.Fatalf("received %d messages, want %d", received, n)
	}
}

// TestTransport_ConcurrentSendsProduceUncorruptedFrames is a regression
// test for a real bug: Send originally wrote each frame's length prefix
// and payload as two separate, unsynchronized Write calls. Concurrent
// Send calls to the same peer could interleave those writes on the wire
// — goroutine A's length prefix, then goroutine B's, before A's payload
// — corrupting the frame boundary. The receiving side's decode failure
// then terminates that whole connection's read loop, silently dropping
// every message still in flight behind the corrupted one. This showed up
// as a genuinely flaky test (8-18 out of 20 messages received,
// nondeterministically) that didn't reproduce in every environment,
// exactly the profile of a real concurrency bug rather than a test
// artifact — so this test checks not just that all N messages arrive,
// but that each one decodes to precisely the value it was sent with,
// which a byte-interleaving bug can violate even in the rare case where
// the raw message COUNT happens to still match.
func TestTransport_ConcurrentSendsProduceUncorruptedFrames(t *testing.T) {
	b := listenLocal(t, 2, nil)
	defer b.Close()
	a := listenLocal(t, 1, map[uint64]string{2: b.Addr()})
	defer a.Close()

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Distinctive, easy-to-verify content per message: if frames
			// were corrupted by interleaving, decoded values would come
			// back wrong or fail to decode at all, not just go missing.
			a.Send(raft.Message{
				Type: raft.MsgAppendEntries, From: 1, To: 2, Term: uint64(i),
				Entries: []raft.LogEntry{{Term: uint64(i), Index: uint64(i), Data: []byte(fmt.Sprintf("payload-%03d", i))}},
			})
		}(i)
	}
	wg.Wait()

	seen := make(map[uint64]bool, n)
	for i := 0; i < n; i++ {
		select {
		case m := <-b.Recv():
			if len(m.Entries) != 1 {
				t.Fatalf("message for term %d has %d entries, want 1 (frame corruption?)", m.Term, len(m.Entries))
			}
			want := fmt.Sprintf("payload-%03d", m.Term)
			if string(m.Entries[0].Data) != want {
				t.Fatalf("message for term %d has payload %q, want %q (frame corruption)", m.Term, m.Entries[0].Data, want)
			}
			seen[m.Term] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("only received %d/%d uncorrupted messages", len(seen), n)
		}
	}
	if len(seen) != n {
		t.Fatalf("received %d distinct messages, want %d", len(seen), n)
	}
}

// --- Listen error path -------------------------------------------------------

func TestListen_AddressAlreadyInUseErrors(t *testing.T) {
	holder := listenLocal(t, 1, nil)
	defer holder.Close()

	_, err := Listen(2, holder.Addr(), nil)
	if err == nil {
		t.Fatal("expected an error binding to an address already in use")
	}
}

// --- Directly-testable race-window branches ---------------------------------

func TestRegisterAccepted_FalseWhenClosed(t *testing.T) {
	tr := listenLocal(t, 1, nil)
	tr.Close()

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	conn, _ := net.Dial("tcp", ln.Addr().String())
	defer conn.Close()

	if tr.registerAccepted(conn) {
		t.Fatal("expected registerAccepted to return false on a closed transport")
	}
}

func TestRegisterAccepted_TrueWhenOpen(t *testing.T) {
	tr := listenLocal(t, 1, nil)
	defer tr.Close()

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	conn, _ := net.Dial("tcp", ln.Addr().String())
	defer conn.Close()

	if !tr.registerAccepted(conn) {
		t.Fatal("expected registerAccepted to return true on an open transport")
	}
}

func TestRegisterDialed_ClosedReturnsError(t *testing.T) {
	tr := listenLocal(t, 1, nil)
	tr.Close()

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	conn, _ := net.Dial("tcp", ln.Addr().String())

	_, err := tr.registerDialed(2, conn)
	if err == nil {
		t.Fatal("expected an error registering a dialed connection on a closed transport")
	}
}

func TestRegisterDialed_PrefersExistingOverConcurrentDuplicate(t *testing.T) {
	tr := listenLocal(t, 1, nil)
	defer tr.Close()

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	first, _ := net.Dial("tcp", ln.Addr().String())
	defer first.Close()
	second, _ := net.Dial("tcp", ln.Addr().String())

	// Simulate "another goroutine already won the race": pre-populate
	// outConns for peer 2 with `first`, then register `second` as if it
	// were dialed concurrently by a different caller.
	firstWrapped := &outboundConn{conn: first}
	tr.mu.Lock()
	tr.outConns[2] = firstWrapped
	tr.mu.Unlock()

	got, err := tr.registerDialed(2, second)
	if err != nil {
		t.Fatal(err)
	}
	if got != firstWrapped {
		t.Fatal("expected registerDialed to return the existing (first) connection, not the duplicate")
	}
	// The redundant connection must have been closed, not leaked: a
	// subsequent read on it should observe closure (EOF/error) rather
	// than hang.
	second.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := second.Read(buf); err == nil {
		t.Fatal("expected the redundant duplicate connection to have been closed")
	}
}

func TestDeliver_FalseWhenDoneAndBufferFull(t *testing.T) {
	tr := listenLocal(t, 1, nil)
	defer tr.ln.Close() // avoid the normal Close() path; we manage shutdown by hand below

	// Fill the incoming buffer completely so the delivery case in
	// deliver()'s select can never be ready.
	for i := 0; i < cap(tr.incoming); i++ {
		tr.incoming <- raft.Message{}
	}
	close(tr.done)

	if tr.deliver(raft.Message{Term: 999}) {
		t.Fatal("expected deliver to return false once done is closed and the buffer is full")
	}
}

func TestDeliver_TrueWhenBufferHasRoom(t *testing.T) {
	tr := listenLocal(t, 1, nil)
	defer tr.Close()

	if !tr.deliver(raft.Message{Term: 1}) {
		t.Fatal("expected deliver to succeed with buffer room and done still open")
	}
}

// --- Send's write-failure path (distinct from dial failure) ----------------

func TestSend_WriteFailureDropsConnectionAndReturnsError(t *testing.T) {
	b := listenLocal(t, 2, nil)
	defer b.Close()
	a := listenLocal(t, 1, map[uint64]string{2: b.Addr()})
	defer a.Close()

	if err := a.Send(raft.Message{Type: raft.MsgRequestVote, From: 1, To: 2, Term: 1}); err != nil {
		t.Fatal(err)
	}
	recvWithTimeout(t, b)

	// Close the cached outbound connection's underlying socket directly
	// (bypassing dropConn's bookkeeping), so the next Send finds a
	// stale, already-closed conn in the cache and its Write fails
	// immediately and deterministically.
	a.mu.Lock()
	a.outConns[2].conn.Close()
	a.mu.Unlock()

	if err := a.Send(raft.Message{Type: raft.MsgRequestVote, From: 1, To: 2, Term: 2}); err == nil {
		t.Fatal("expected Send to fail writing to a connection closed out from under it")
	}

	a.mu.Lock()
	_, stillCached := a.outConns[2]
	a.mu.Unlock()
	if stillCached {
		t.Fatal("expected the failed connection to have been dropped from the cache")
	}
}
