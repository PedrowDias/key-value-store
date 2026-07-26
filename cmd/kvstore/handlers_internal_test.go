package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/PedrowDias/key-value-store/raft"
)

// fakeKVServer implements kvServer with fully controllable behavior.
type fakeKVServer struct {
	getVal    []byte
	getFound  bool
	getErr    error
	putErr    error
	deleteErr error
	status    raft.Status
}

func (f *fakeKVServer) Get(key []byte) ([]byte, bool, error) { return f.getVal, f.getFound, f.getErr }
func (f *fakeKVServer) Put(key, value []byte) error          { return f.putErr }
func (f *fakeKVServer) Delete(key []byte) error              { return f.deleteErr }
func (f *fakeKVServer) Status() raft.Status                  { return f.status }

func TestHandleGet_ErrorReturns500(t *testing.T) {
	fake := &fakeKVServer{getErr: errors.New("boom")}
	ts := httptest.NewServer(newAPI(fake).routes())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/kv/k")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}
}

func TestHandleDelete_ErrorPropagatesAsInternalError(t *testing.T) {
	fake := &fakeKVServer{deleteErr: errors.New("boom")}
	ts := httptest.NewServer(newAPI(fake).routes())
	defer ts.Close()

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

func TestHandleDelete_Success(t *testing.T) {
	fake := &fakeKVServer{}
	ts := httptest.NewServer(newAPI(fake).routes())
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/kv/k", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

// brokenReader always fails to read, for testing handlePut's request-body
// read-error branch.
type brokenReader struct{}

func (brokenReader) Read(p []byte) (int, error) {
	return 0, errors.New("simulated read failure")
}

func TestHandlePut_BodyReadErrorReturns400(t *testing.T) {
	fake := &fakeKVServer{}
	mux := newAPI(fake).routes()

	req := httptest.NewRequest(http.MethodPut, "/kv/k", brokenReader{})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

// --- writeProposeError, tested directly (no live server needed) ------------

func TestWriteProposeError_NotLeaderSetsLeaderHintHeader(t *testing.T) {
	fake := &fakeKVServer{status: raft.Status{Leader: 7}}
	a := newAPI(fake)
	rec := httptest.NewRecorder()

	a.writeProposeError(rec, raft.ErrNotLeader)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if got := rec.Header().Get("X-Raft-Leader-Id"); got != "7" {
		t.Fatalf("X-Raft-Leader-Id = %q, want 7", got)
	}
}

func TestWriteProposeError_NotLeaderWithUnknownLeaderOmitsHeader(t *testing.T) {
	fake := &fakeKVServer{status: raft.Status{Leader: 0}}
	a := newAPI(fake)
	rec := httptest.NewRecorder()

	a.writeProposeError(rec, raft.ErrNotLeader)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if got := rec.Header().Get("X-Raft-Leader-Id"); got != "" {
		t.Fatalf("X-Raft-Leader-Id = %q, want empty (leader unknown)", got)
	}
}

func TestWriteProposeError_TimeoutMapsTo504(t *testing.T) {
	a := newAPI(&fakeKVServer{})
	rec := httptest.NewRecorder()

	a.writeProposeError(rec, errors.New("server: propose timed out waiting for commit"))

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusGatewayTimeout)
	}
}

func TestWriteProposeError_GenericErrorMapsTo500(t *testing.T) {
	a := newAPI(&fakeKVServer{})
	rec := httptest.NewRecorder()

	a.writeProposeError(rec, errors.New("something else went wrong"))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}
