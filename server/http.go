package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/PedrowDias/key-value-store/raft"
)

// HTTPAPI wraps a *Server with an HTTP API: PUT/GET/DELETE on
// /kv/{key}, and GET /status for cluster observability.
//
// This lives in the server package (rather than in cmd/kvstore, where it
// originally started) specifically so it's importable by more than one
// binary — the kvstore server binary, and the bench package's cluster
// load-test tool both need to stand up a real HTTP-fronted node, and
// neither should have to duplicate this logic to do it.
type HTTPAPI struct {
	srv *Server
}

// NewHTTPAPI wraps srv with HTTP handlers.
func NewHTTPAPI(srv *Server) *HTTPAPI {
	return &HTTPAPI{srv: srv}
}

// Handler returns the http.Handler serving this API; typically passed
// straight to an *http.Server's Handler field, or wrapped with
// httptest.NewServer in tests.
func (a *HTTPAPI) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", a.handleKV)
	mux.HandleFunc("/status", a.handleStatus)
	return mux
}

// handleKV serves GET/PUT/DELETE on /kv/{key}.
func (a *HTTPAPI) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		a.handleGet(w, r, key)
	case http.MethodPut:
		a.handlePut(w, r, key)
	case http.MethodDelete:
		a.handleDelete(w, key)
	default:
		w.Header().Set("Allow", "GET, PUT, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGet serves GET /kv/{key}. By default, reads local state directly
// (Server.Get) — fast and always available, but only eventually
// consistent (see Get's own doc). ?linearizable=true opts into
// Server.LinearizableGet instead: a real network round trip to confirm
// current leadership first, guaranteeing the result reflects every write
// already committed before the read began. Pay for that guarantee only
// when the caller actually asks for it.
func (a *HTTPAPI) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	var val []byte
	var found bool
	var err error
	if r.URL.Query().Get("linearizable") == "true" {
		val, found, err = a.srv.LinearizableGet([]byte(key))
	} else {
		val, found, err = a.srv.Get([]byte(key))
	}
	if err != nil {
		a.writeRaftError(w, err)
		return
	}
	if !found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(val)
}

func (a *HTTPAPI) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("reading request body: %v", err), http.StatusBadRequest)
		return
	}
	if err := a.srv.Put([]byte(key), value); err != nil {
		a.writeRaftError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *HTTPAPI) handleDelete(w http.ResponseWriter, key string) {
	if err := a.srv.Delete([]byte(key)); err != nil {
		a.writeRaftError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writeRaftError classifies an error from any call that needed to reach
// the leader or a majority of the cluster — Put, Delete, or
// LinearizableGet alike — into an appropriate HTTP status: 503 (with a
// leader hint header, if known) for "you asked the wrong node," 504 for
// "this took too long to commit or confirm," 500 otherwise.
func (a *HTTPAPI) writeRaftError(w http.ResponseWriter, err error) {
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

func (a *HTTPAPI) handleStatus(w http.ResponseWriter, r *http.Request) {
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
