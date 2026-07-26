// The kvstore binary runs one node of the distributed key-value store:
// it wires together the storage engine, Raft consensus, and TCP
// transport (via the server package) and exposes a simple HTTP API for
// clients to Put/Get/Delete keys and check cluster status.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/PedrowDias/key-value-store/raft"
)

// kvServer is the subset of *server.Server's methods the HTTP layer
// needs. Defined as an interface purely so tests can inject a fake for
// precise control over error conditions (a specific error message, a
// specific known leader ID) that would otherwise require a slow,
// timing-dependent live cluster to reproduce reliably. *server.Server
// satisfies this as-is.
type kvServer interface {
	Get(key []byte) ([]byte, bool, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	Status() raft.Status
}

// api wraps a kvServer with HTTP handlers. Kept as its own small type
// (rather than free functions closing over a server) specifically so
// it's easy to construct against an in-process test server (or a fake)
// and drive with httptest, without needing a real running binary.
type api struct {
	srv kvServer
}

func newAPI(srv kvServer) *api {
	return &api{srv: srv}
}

// routes returns the HTTP mux; separated from ListenAndServe so tests
// can wrap it with httptest.NewServer directly.
func (a *api) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", a.handleKV)
	mux.HandleFunc("/status", a.handleStatus)
	return mux
}

// handleKV serves GET/PUT/DELETE on /kv/{key}.
func (a *api) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		a.handleGet(w, key)
	case http.MethodPut:
		a.handlePut(w, r, key)
	case http.MethodDelete:
		a.handleDelete(w, key)
	default:
		w.Header().Set("Allow", "GET, PUT, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *api) handleGet(w http.ResponseWriter, key string) {
	val, found, err := a.srv.Get([]byte(key))
	if err != nil {
		http.Error(w, fmt.Sprintf("get: %v", err), http.StatusInternalServerError)
		return
	}
	if !found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(val)
}

func (a *api) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("reading request body: %v", err), http.StatusBadRequest)
		return
	}
	if err := a.srv.Put([]byte(key), value); err != nil {
		a.writeProposeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) handleDelete(w http.ResponseWriter, key string) {
	if err := a.srv.Delete([]byte(key)); err != nil {
		a.writeProposeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writeProposeError classifies a Put/Delete error into an appropriate
// HTTP status: 503 (with a leader hint header, if known) for "you asked
// the wrong node," 504 for "this took too long to commit," 500
// otherwise.
func (a *api) writeProposeError(w http.ResponseWriter, err error) {
	if errors.Is(err, raft.ErrNotLeader) {
		if leader := a.srv.Status().Leader; leader != 0 {
			w.Header().Set("X-Raft-Leader-Id", fmt.Sprintf("%d", leader))
		}
		http.Error(w, "not leader", http.StatusServiceUnavailable)
		return
	}
	if strings.Contains(err.Error(), "timed out") {
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

// statusResponse is the JSON body /status returns.
type statusResponse struct {
	ID           uint64 `json:"id"`
	Term         uint64 `json:"term"`
	Role         string `json:"role"`
	Leader       uint64 `json:"leader"`
	CommitIndex  uint64 `json:"commit_index"`
	LastLogIndex uint64 `json:"last_log_index"`
}

func (a *api) handleStatus(w http.ResponseWriter, r *http.Request) {
	st := a.srv.Status()
	resp := statusResponse{
		ID:           st.ID,
		Term:         st.Term,
		Role:         st.Role.String(),
		Leader:       st.Leader,
		CommitIndex:  st.CommitIndex,
		LastLogIndex: st.LastLogIndex,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
