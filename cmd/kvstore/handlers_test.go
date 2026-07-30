package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PedrowDias/key-value-store/engine"
	"github.com/PedrowDias/key-value-store/raft"
	"github.com/PedrowDias/key-value-store/server"
	"github.com/PedrowDias/key-value-store/transport"
)

// newTestAPIServer spins up one real, single-node cluster (real
// storage, real Raft, real TCP transport bound to an ephemeral port) and
// returns an httptest.Server fronting it with the real HTTP handlers.
func newTestAPIServer(t *testing.T) *httptest.Server {
	t.Helper()
	dir := t.TempDir()

	rn, _, err := raft.OpenNode(raft.Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}, filepath.Join(dir, "raft.wal"))
	if err != nil {
		t.Fatal(err)
	}
	tr, err := transport.Listen(1, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := engine.Open(engine.Options{Dir: filepath.Join(dir, "kv")})
	if err != nil {
		t.Fatal(err)
	}

	srv := server.New(rn, tr, eng, 10*time.Millisecond)
	go srv.Run()

	t.Cleanup(func() {
		srv.Stop()
		tr.Close()
		rn.Close()
		eng.Close()
	})

	// Wait for the single node to elect itself leader before returning,
	// so tests don't need to poll for this themselves.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Status().Role == raft.Leader {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	return httptest.NewServer(newAPI(srv).routes())
}

func TestHandleKV_MissingKeyReturnsBadRequest(t *testing.T) {
	ts := newTestAPIServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/kv/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestHandleKV_MethodNotAllowed(t *testing.T) {
	ts := newTestAPIServer(t)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPatch, ts.URL+"/kv/somekey", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusMethodNotAllowed)
	}
}

func TestPutGetDelete_RoundTrip(t *testing.T) {
	ts := newTestAPIServer(t)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/mykey", strings.NewReader("myvalue"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	resp, err = http.Get(ts.URL + "/kv/mykey")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if string(body) != "myvalue" {
		t.Fatalf("GET body = %q, want myvalue", body)
	}

	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/kv/mykey", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	resp, err = http.Get(ts.URL + "/kv/mykey")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHandleGet_NotFoundReturns404(t *testing.T) {
	ts := newTestAPIServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/kv/never-existed")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHandlePut_EmptyValueIsValid(t *testing.T) {
	ts := newTestAPIServer(t)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/emptykey", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	resp, err = http.Get(ts.URL + "/kv/emptykey")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if len(body) != 0 {
		t.Fatalf("body = %q, want empty", body)
	}
}

func TestHandleStatus_ReturnsLeaderInfo(t *testing.T) {
	ts := newTestAPIServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		t.Fatal("expected a non-empty JSON status body")
	}
}

// --- Not-leader handling, using a real (non-leader) follower ---------------

func TestPut_OnFollowerReturnsServiceUnavailableWithLeaderHint(t *testing.T) {
	dir := t.TempDir()

	// A 2-node cluster where node 2's peer (node 1) is never actually
	// started: node 2 can never win an election (it needs both votes in
	// a 2-node cluster), so it stays a Follower forever — a reliable,
	// deterministic way to exercise the not-leader path without timing
	// games around a real election.
	rn, _, err := raft.OpenNode(raft.Config{
		ID: 2, Peers: []uint64{1},
		ElectionTick: 10, HeartbeatTick: 1,
	}, filepath.Join(dir, "raft.wal"))
	if err != nil {
		t.Fatal(err)
	}
	tr, err := transport.Listen(2, "127.0.0.1:0", map[uint64]string{1: "127.0.0.1:1"}) // node 1 address is a placeholder; never dialed successfully
	if err != nil {
		t.Fatal(err)
	}
	eng, err := engine.Open(engine.Options{Dir: filepath.Join(dir, "kv")})
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(rn, tr, eng, 10*time.Millisecond)
	go srv.Run()
	t.Cleanup(func() {
		srv.Stop()
		tr.Close()
		rn.Close()
		eng.Close()
	})

	// Give it a couple of election timeouts to confirm it never becomes
	// leader (it shouldn't be able to).
	time.Sleep(300 * time.Millisecond)
	if srv.Status().Role == raft.Leader {
		t.Fatal("test setup invariant violated: a lone node with an unreachable peer must never become leader")
	}

	ts := httptest.NewServer(newAPI(srv).routes())
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/k", strings.NewReader("v"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
}
