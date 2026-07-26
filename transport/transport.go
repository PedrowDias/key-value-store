package transport

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/PedrowDias/key-value-store/raft"
)

const dialTimeout = 2 * time.Second

// outboundConn pairs a dialed connection with a mutex serializing writes
// to it. A single frame is two Write calls (length prefix, then
// payload); without this lock, two goroutines calling Send concurrently
// to the same peer can interleave their length-prefix and payload writes
// on the wire, corrupting both frames. TCP guarantees byte-stream
// ordering for writes issued in program order on ONE goroutine, but
// gives no such guarantee across concurrent writers sharing a
// connection — that atomicity has to be provided at this layer.
type outboundConn struct {
	conn net.Conn
	mu   sync.Mutex
}

// Transport connects one Raft node to its peers over TCP. It's a thin,
// best-effort delivery layer by design: Raft itself is built to tolerate
// dropped messages (a lost AppendEntries just means a retry on the next
// heartbeat, a lost RequestVote just means that election attempt fails
// and a later one succeeds) — so Transport's job is simply "get bytes
// there when the network cooperates," not guaranteed delivery, retries,
// or ordering beyond what a single TCP connection already provides.
type Transport struct {
	selfID    uint64
	ln        net.Listener
	peerAddrs map[uint64]string

	incoming chan raft.Message
	done     chan struct{}
	wg       sync.WaitGroup

	mu       sync.Mutex
	closed   bool
	outConns map[uint64]*outboundConn
	inConns  map[net.Conn]struct{}
}

// Listen starts a Transport for selfID, accepting connections on addr
// (e.g. ":7001") and dialing peerAddrs (peer ID -> "host:port") on demand
// as outbound messages need to be sent.
func Listen(selfID uint64, addr string, peerAddrs map[uint64]string) (*Transport, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("transport: listen on %s: %w", addr, err)
	}
	t := &Transport{
		selfID:    selfID,
		ln:        ln,
		peerAddrs: peerAddrs,
		incoming:  make(chan raft.Message, 256),
		done:      make(chan struct{}),
		outConns:  make(map[uint64]*outboundConn),
		inConns:   make(map[net.Conn]struct{}),
	}
	t.wg.Add(1)
	go t.acceptLoop()
	return t, nil
}

// Addr returns the actual address this Transport is listening on (useful
// in tests that bind to ":0" for an OS-assigned ephemeral port).
func (t *Transport) Addr() string {
	return t.ln.Addr().String()
}

func (t *Transport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			return // listener closed (via Close) or a fatal accept error
		}
		if !t.registerAccepted(conn) {
			conn.Close()
			return
		}
		t.wg.Add(1)
		go t.handleConn(conn)
	}
}

// registerAccepted records a freshly accepted connection, unless the
// transport has since been closed (a real but narrow race: Close() may
// run concurrently with an Accept() that was already in flight). Returns
// false if the transport is closed, telling the caller to discard conn.
func (t *Transport) registerAccepted(conn net.Conn) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return false
	}
	t.inConns[conn] = struct{}{}
	return true
}

// handleConn reads framed messages from one accepted connection until it
// errors or is closed, decoding and forwarding each to Recv()'s channel.
// A malformed message or read error simply ends this connection — no
// panic, no effect on any other connection — since a corrupt or
// disconnected peer shouldn't be able to take down message delivery from
// everyone else.
func (t *Transport) handleConn(conn net.Conn) {
	defer func() {
		t.mu.Lock()
		delete(t.inConns, conn)
		t.mu.Unlock()
		conn.Close()
		t.wg.Done()
	}()

	for {
		payload, err := readFramed(conn)
		if err != nil {
			return
		}
		m, err := decodeMessage(payload)
		if err != nil {
			return
		}
		if !t.deliver(m) {
			return
		}
	}
}

// deliver forwards m to the Recv() channel, or reports false if the
// transport is shutting down (t.done closed) before that could happen —
// relevant when the incoming channel's buffer is full and nothing is
// currently draining it.
func (t *Transport) deliver(m raft.Message) bool {
	select {
	case t.incoming <- m:
		return true
	case <-t.done:
		return false
	}
}

// Recv returns the channel of messages received from peers. Closed once
// Close() completes, so a `for m := range t.Recv()` loop terminates
// cleanly.
func (t *Transport) Recv() <-chan raft.Message {
	return t.incoming
}

// Send delivers m to whichever peer m.To identifies, dialing (and
// caching) a connection on demand. Errors are returned for the caller to
// log/observe, but per the package doc, nothing here retries — that's
// Raft's own responsibility via its normal heartbeat/timeout mechanisms.
//
// Concurrent Send calls to the SAME peer serialize on that peer's
// connection lock, so each call's length-prefix-then-payload write
// completes as one atomic frame on the wire before another can start —
// see outboundConn's doc for why that matters.
func (t *Transport) Send(m raft.Message) error {
	addr, ok := t.peerAddrs[m.To]
	if !ok {
		return fmt.Errorf("transport: unknown peer %d", m.To)
	}
	oc, err := t.getConn(m.To, addr)
	if err != nil {
		return err
	}

	oc.mu.Lock()
	err = writeFramed(oc.conn, encodeMessage(m))
	oc.mu.Unlock()

	if err != nil {
		t.dropConn(m.To)
		return fmt.Errorf("transport: sending to peer %d: %w", m.To, err)
	}
	return nil
}

// getConn returns a cached outbound connection to id, dialing a new one
// if there isn't one yet (or the cached one was dropped after a failure).
func (t *Transport) getConn(id uint64, addr string) (*outboundConn, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, fmt.Errorf("transport: closed")
	}
	if oc, ok := t.outConns[id]; ok {
		t.mu.Unlock()
		return oc, nil
	}
	t.mu.Unlock()

	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("transport: dial peer %d at %s: %w", id, addr, err)
	}
	return t.registerDialed(id, conn)
}

// registerDialed records a freshly dialed outbound connection to id,
// unless the transport has since closed, or another goroutine concurrently
// dialed and cached a connection to the same peer first — in which case
// conn is redundant and closed, and the existing one is returned instead,
// so callers never leak a duplicate connection.
func (t *Transport) registerDialed(id uint64, conn net.Conn) (*outboundConn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		conn.Close()
		return nil, fmt.Errorf("transport: closed")
	}
	if existing, ok := t.outConns[id]; ok {
		conn.Close()
		return existing, nil
	}
	oc := &outboundConn{conn: conn}
	t.outConns[id] = oc
	return oc, nil
}

// dropConn closes and forgets any cached outbound connection to id, so
// the next Send to that peer redials.
func (t *Transport) dropConn(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if oc, ok := t.outConns[id]; ok {
		oc.conn.Close()
		delete(t.outConns, id)
	}
}

// Close shuts down the listener, every outbound and inbound connection,
// and waits for all internal goroutines to exit before returning. Safe
// to call more than once.
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.done)
	t.ln.Close()
	for conn := range t.inConns {
		conn.Close()
	}
	for _, oc := range t.outConns {
		oc.conn.Close()
	}
	t.inConns = nil
	t.outConns = nil
	t.mu.Unlock()

	t.wg.Wait()
	close(t.incoming)
	return nil
}
