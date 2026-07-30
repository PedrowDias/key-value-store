package server

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PedrowDias/key-value-store/raft"
)

// startFakeHTTPServer builds a real *Server backed by a fakeRaftNode
// with autoCommit enabled, runs it, and fronts it with the real
// HTTPAPI — giving full control over Raft-level behavior (errors,
// status) while still exercising the real HTTP layer and the real
// engine underneath.
func startFakeHTTPServer(t *testing.T, fake *fakeRaftNode) (*httptest.Server, *Server) {
	t.Helper()
	fake.autoCommit = true
	srv := New(fake, newTestTransport(t), newTestEngine(t), 10*time.Millisecond)
	go srv.Run()
	t.Cleanup(srv.Stop)
	ts := httptest.NewServer(NewHTTPAPI(srv).Handler())
	t.Cleanup(ts.Close)
	return ts, srv
}

func TestHTTPAPI_PutGetDelete_RoundTrip(t *testing.T) {
	ts, _ := startFakeHTTPServer(t, newFakeRaftNode())

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
	if resp.StatusCode != http.StatusOK || string(body) != "myvalue" {
		t.Fatalf("GET status=%d body=%q, want 200 myvalue", resp.StatusCode, body)
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

func TestHTTPAPI_MissingKeyReturnsBadRequest(t *testing.T) {
	ts, _ := startFakeHTTPServer(t, newFakeRaftNode())
	resp, err := http.Get(ts.URL + "/kv/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestHTTPAPI_MethodNotAllowed(t *testing.T) {
	ts, _ := startFakeHTTPServer(t, newFakeRaftNode())
	req, _ := http.NewRequest(http.MethodPatch, ts.URL+"/kv/k", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusMethodNotAllowed)
	}
}

func TestHTTPAPI_GetNotFound(t *testing.T) {
	ts, _ := startFakeHTTPServer(t, newFakeRaftNode())
	resp, err := http.Get(ts.URL + "/kv/never-existed")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHTTPAPI_GetEngineErrorReturns500(t *testing.T) {
	eng := newTestEngine(t)
	fake := newFakeRaftNode()
	fake.autoCommit = true
	srv := New(fake, newTestTransport(t), eng, 10*time.Millisecond)
	go srv.Run()
	t.Cleanup(srv.Stop)
	ts := httptest.NewServer(NewHTTPAPI(srv).Handler())
	defer ts.Close()

	eng.Close() // any subsequent Get now fails
	resp, err := http.Get(ts.URL + "/kv/k")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}
}

// --- Linearizable reads via ?linearizable=true -------------------------------

func TestHTTPAPI_LinearizableGet_RoundTrip(t *testing.T) {
	fake := newFakeRaftNode()
	fake.autoConfirmReadIndex = true
	ts, _ := startFakeHTTPServer(t, fake)

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/mykey", strings.NewReader("myvalue"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	resp, err = http.Get(ts.URL + "/kv/mykey?linearizable=true")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("linearizable GET status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "myvalue" {
		t.Fatalf("body = %q, want myvalue", body)
	}
}

func TestHTTPAPI_LinearizableGet_NotFound(t *testing.T) {
	fake := newFakeRaftNode()
	fake.autoConfirmReadIndex = true
	ts, _ := startFakeHTTPServer(t, fake)

	resp, err := http.Get(ts.URL + "/kv/never-existed?linearizable=true")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHTTPAPI_LinearizableGet_NotLeaderReturns503WithLeaderHint(t *testing.T) {
	fake := newFakeRaftNode()
	fake.readIndexErr = raft.ErrNotLeader
	fake.status.Leader = 7
	ts, _ := startFakeHTTPServer(t, fake)

	resp, err := http.Get(ts.URL + "/kv/k?linearizable=true")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
	if got := resp.Header.Get("X-Raft-Leader-Id"); got != "7" {
		t.Fatalf("X-Raft-Leader-Id = %q, want 7", got)
	}
}

func TestHTTPAPI_Get_WithoutLinearizableParamUsesLocalReadNotReadIndex(t *testing.T) {
	// autoConfirmReadIndex deliberately left false: if a plain GET (no
	// query param) ever took the LinearizableGet path, this would hang
	// until proposeTimeout and fail as a 500/504, not succeed — passing
	// here is itself the proof the default path is unaffected by
	// LinearizableGet's addition.
	fake := newFakeRaftNode()
	ts, _ := startFakeHTTPServer(t, fake)

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/k", strings.NewReader("v"))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	resp, err := http.Get(ts.URL + "/kv/k")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHTTPAPI_PutNotLeaderReturns503WithLeaderHint(t *testing.T) {
	fake := newFakeRaftNode()
	fake.proposeErr = raft.ErrNotLeader
	fake.status.Leader = 7
	ts, _ := startFakeHTTPServer(t, fake)

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/k", strings.NewReader("v"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
	if got := resp.Header.Get("X-Raft-Leader-Id"); got != "7" {
		t.Fatalf("X-Raft-Leader-Id = %q, want 7", got)
	}
}

func TestHTTPAPI_PutNotLeaderUnknownLeaderOmitsHeader(t *testing.T) {
	fake := newFakeRaftNode()
	fake.proposeErr = raft.ErrNotLeader
	ts, _ := startFakeHTTPServer(t, fake)

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/k", strings.NewReader("v"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
	if got := resp.Header.Get("X-Raft-Leader-Id"); got != "" {
		t.Fatalf("X-Raft-Leader-Id = %q, want empty", got)
	}
}

func TestHTTPAPI_PutGenericErrorReturns500(t *testing.T) {
	fake := newFakeRaftNode()
	fake.proposeErr = errors.New("boom")
	ts, _ := startFakeHTTPServer(t, fake)

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/k", strings.NewReader("v"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}
}

func TestHTTPAPI_PutTimeoutReturns504(t *testing.T) {
	fake := newFakeRaftNode()
	fake.proposeErr = errors.New("server: propose timed out waiting for commit")
	ts, _ := startFakeHTTPServer(t, fake)

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/k", strings.NewReader("v"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusGatewayTimeout)
	}
}

func TestHTTPAPI_DeleteErrorReturns500(t *testing.T) {
	fake := newFakeRaftNode()
	fake.proposeErr = errors.New("boom")
	ts, _ := startFakeHTTPServer(t, fake)

	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/kv/k", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}
}

// brokenReader always fails to read, for testing handlePut's request-body
// read-error branch.
type brokenReader struct{}

func (brokenReader) Read(p []byte) (int, error) {
	return 0, errors.New("simulated read failure")
}

func TestHTTPAPI_PutBodyReadErrorReturns400(t *testing.T) {
	_, srv := startFakeHTTPServer(t, newFakeRaftNode())
	mux := NewHTTPAPI(srv).Handler()

	req := httptest.NewRequest(http.MethodPut, "/kv/k", brokenReader{})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHTTPAPI_PutEmptyValueIsValid(t *testing.T) {
	ts, _ := startFakeHTTPServer(t, newFakeRaftNode())

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
	if resp.StatusCode != http.StatusOK || len(body) != 0 {
		t.Fatalf("status=%d body=%q, want 200 empty", resp.StatusCode, body)
	}
}

func TestHTTPAPI_StatusEndpoint(t *testing.T) {
	ts, _ := startFakeHTTPServer(t, newFakeRaftNode())

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
