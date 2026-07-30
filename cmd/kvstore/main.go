// Command kvstore runs one node of the distributed key-value store: it
// wires together the storage engine, Raft consensus, and TCP transport
// (via the server package) and exposes an HTTP API for clients. See the
// repository's root README for how to start a cluster.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/PedrowDias/key-value-store/engine"
	"github.com/PedrowDias/key-value-store/raft"
	"github.com/PedrowDias/key-value-store/server"
	"github.com/PedrowDias/key-value-store/transport"
)

// main is intentionally the only thing in this file that isn't unit
// tested: it does nothing but wire real os.Args/os.Stderr/OS signals
// into realMain and translate its result into a process exit code.
// Testing a call to os.Exit in-process would terminate the test binary
// itself; the standard, honest way around that is to push every other
// line of logic into a function that RETURNS an exit code instead of
// calling os.Exit directly (realMain, below), and test that function
// directly. That's what every other piece of this file's logic does.
func main() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	os.Exit(realMain(os.Args[1:], os.Stderr, sigCh, nil))
}

// realMain runs the whole program and returns an exit code, taking every
// external dependency (args, where to write errors, the OS signal
// channel, an optional readiness notification) as a parameter rather
// than reaching for globals — which is what makes it callable, and its
// full success/failure lifecycle testable, from an ordinary test.
func realMain(args []string, stderr io.Writer, stopCh <-chan os.Signal, ready chan<- string) int {
	cfg, err := parseArgs(args)
	if err != nil {
		fmt.Fprintln(stderr, "kvstore:", err)
		return 1
	}

	httpListener, err := net.Listen("tcp", cfg.httpAddr)
	if err != nil {
		fmt.Fprintln(stderr, "kvstore: starting http listener:", err)
		return 1
	}

	if err := runServer(cfg, httpListener, stopCh, ready); err != nil {
		fmt.Fprintln(stderr, "kvstore:", err)
		return 1
	}
	return 0
}

// config holds this node's fully parsed configuration.
type config struct {
	id             uint64
	raftAddr       string
	httpAddr       string
	peerAddrs      map[uint64]string
	peerIDs        []uint64
	dataDir        string
	electionTicks  int
	heartbeatTicks int
	tickInterval   time.Duration
	// snapshotThreshold overrides Server's default automatic-snapshot
	// trigger (see Server.SetSnapshotThreshold's doc) when nonzero;
	// zero means "use Server's own default," not "disable" — parseArgs
	// only sets this when the flag was actually given, precisely so
	// omitting it doesn't accidentally turn off snapshotting for every
	// deployment that doesn't know to ask for the default explicitly.
	snapshotThreshold int
}

// parseArgs parses and validates args (typically os.Args[1:]) into a
// config. Uses its own flag.FlagSet rather than the package-level
// flag.CommandLine specifically so it can be called repeatedly from
// tests without global flag-parsing state leaking between calls.
func parseArgs(args []string) (config, error) {
	fs := flag.NewFlagSet("kvstore", flag.ContinueOnError)
	var (
		id                = fs.Uint64("id", 0, "this node's ID (must be nonzero, and match one entry in -peers if peers are given)")
		raftAddr          = fs.String("raft-addr", "", "address to listen on for Raft traffic, e.g. :7001")
		httpAddr          = fs.String("http-addr", "", "address to serve the client HTTP API on, e.g. :8001")
		peers             = fs.String("peers", "", "comma-separated peer list as id=raft-addr, e.g. 2=127.0.0.1:7002,3=127.0.0.1:7003 (omit this node's own entry)")
		dataDir           = fs.String("data-dir", "./data", "directory to store this node's Raft log and KV data")
		electionTicks     = fs.Int("election-ticks", 10, "minimum election timeout, in ticks (randomized up to 2x)")
		heartbeatTick     = fs.Int("heartbeat-ticks", 1, "leader heartbeat interval, in ticks")
		tickInterval      = fs.Duration("tick-interval", 100*time.Millisecond, "wall-clock duration of one tick")
		snapshotThreshold = fs.Int("snapshot-threshold", 0, "entries applied since the last snapshot before triggering a new one (0 = use Server's own default)")
	)
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	if *id == 0 {
		return config{}, fmt.Errorf("-id is required and must be nonzero")
	}
	if *raftAddr == "" {
		return config{}, fmt.Errorf("-raft-addr is required")
	}
	if *httpAddr == "" {
		return config{}, fmt.Errorf("-http-addr is required")
	}

	peerAddrs, peerIDs, err := parsePeers(*peers)
	if err != nil {
		return config{}, fmt.Errorf("parsing -peers: %w", err)
	}

	return config{
		id: *id, raftAddr: *raftAddr, httpAddr: *httpAddr,
		peerAddrs: peerAddrs, peerIDs: peerIDs,
		dataDir:           *dataDir,
		electionTicks:     *electionTicks,
		heartbeatTicks:    *heartbeatTick,
		tickInterval:      *tickInterval,
		snapshotThreshold: *snapshotThreshold,
	}, nil
}

// parsePeers parses "id=addr,id=addr,..." into an id->addr map and the
// list of peer IDs raft.Config expects.
func parsePeers(s string) (map[uint64]string, []uint64, error) {
	addrs := make(map[uint64]string)
	var ids []uint64
	s = strings.TrimSpace(s)
	if s == "" {
		return addrs, ids, nil
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, nil, fmt.Errorf("malformed peer entry %q, want id=addr", part)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(kv[0]), 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("malformed peer ID in %q: %w", part, err)
		}
		if id == 0 {
			return nil, nil, fmt.Errorf("peer ID must be nonzero in %q", part)
		}
		addr := strings.TrimSpace(kv[1])
		if addr == "" {
			return nil, nil, fmt.Errorf("malformed peer address in %q", part)
		}
		addrs[id] = addr
		ids = append(ids, id)
	}
	return addrs, ids, nil
}

// buildComponents opens (or creates) this node's durable storage and
// starts its Raft transport, in the order that keeps partial-failure
// cleanup correct (each step closes whatever the prior steps opened
// before returning an error).
// buildComponents opens (or creates) this node's durable storage, its
// Raft participation, its transport, and its KV engine, in that order.
// The returned uint64 is the index this node's engine state is already
// known to reflect — 0 normally (build up from log replay as usual, via
// server.Server's own applyCommitted), or a persisted snapshot's
// LastIncludedIndex if one existed from before this restart (the caller
// must pass this to server.Server.SeedAppliedIndex before Run(), or
// applyCommitted will incorrectly try to replay log history this node's
// own raft log no longer has — exactly what the snapshot superseded).
//
// A snapshot restoration failure here is fatal (unlike the same
// operation's failure inside server.Server's own pump(), which retries
// on a later restart instead): unlike a live, already-participating
// node where crashing the whole process over a transient issue would
// be needlessly disruptive, this node hasn't started participating at
// all yet, and starting anyway with silently incomplete data would be
// worse than refusing to start.
func buildComponents(cfg config) (*raft.Node, *transport.Transport, *engine.Engine, uint64, error) {
	if err := os.MkdirAll(cfg.dataDir, 0755); err != nil {
		return nil, nil, nil, 0, fmt.Errorf("creating data directory %s: %w", cfg.dataDir, err)
	}

	raftNode, snap, err := raft.OpenNode(raft.Config{
		ID:            cfg.id,
		Peers:         cfg.peerIDs,
		ElectionTick:  cfg.electionTicks,
		HeartbeatTick: cfg.heartbeatTicks,
	}, filepath.Join(cfg.dataDir, "raft.wal"))
	if err != nil {
		return nil, nil, nil, 0, fmt.Errorf("opening raft node: %w", err)
	}

	tr, err := transport.Listen(cfg.id, cfg.raftAddr, cfg.peerAddrs)
	if err != nil {
		raftNode.Close()
		return nil, nil, nil, 0, fmt.Errorf("starting transport: %w", err)
	}

	eng, err := engine.Open(engine.Options{Dir: filepath.Join(cfg.dataDir, "kv")})
	if err != nil {
		tr.Close()
		raftNode.Close()
		return nil, nil, nil, 0, fmt.Errorf("opening storage engine: %w", err)
	}

	var appliedIndex uint64
	if snap != nil {
		if err := eng.RestoreSnapshot(snap.Data); err != nil {
			eng.Close()
			tr.Close()
			raftNode.Close()
			return nil, nil, nil, 0, fmt.Errorf("restoring persisted snapshot (index %d) into the engine: %w", snap.LastIncludedIndex, err)
		}
		appliedIndex = snap.LastIncludedIndex
	}

	return raftNode, tr, eng, appliedIndex, nil
}

// startHTTPServer begins serving handler on ln in the background,
// returning the *http.Server (for graceful Shutdown) and a channel that
// receives at most one error if Serve ever fails for a reason OTHER than
// a graceful Shutdown call (which is the expected, non-error way this
// stops).
func startHTTPServer(ln net.Listener, handler http.Handler) (*http.Server, <-chan error) {
	httpSrv := &http.Server{Handler: handler}
	errCh := make(chan error, 1)
	go func() {
		if err := httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	return httpSrv, errCh
}

// runServer wires cfg's storage/Raft/transport together with the given
// (already bound) HTTP listener, serves until stopCh delivers a signal or
// the HTTP server itself errors, then shuts everything down in order.
// httpListener is a parameter (rather than created internally from
// cfg.httpAddr) specifically so tests can supply one bound to an
// ephemeral port, read its real address, and — by closing it directly —
// deterministically trigger the "HTTP server errored" shutdown path
// without needing to wait for a real OS signal.
func runServer(cfg config, httpListener net.Listener, stopCh <-chan os.Signal, ready chan<- string) error {
	raftNode, tr, eng, appliedIndex, err := buildComponents(cfg)
	if err != nil {
		httpListener.Close()
		return err
	}

	srv := server.New(raftNode, tr, eng, cfg.tickInterval)
	if cfg.snapshotThreshold > 0 {
		srv.SetSnapshotThreshold(cfg.snapshotThreshold)
	}
	if appliedIndex > 0 {
		srv.SeedAppliedIndex(appliedIndex)
	}
	go srv.Run()

	httpSrv, httpErrCh := startHTTPServer(httpListener, server.NewHTTPAPI(srv).Handler())

	log.Printf("kvstore: node %d serving HTTP on %s, Raft on %s", cfg.id, httpListener.Addr().String(), tr.Addr())
	if ready != nil {
		ready <- httpListener.Addr().String()
	}

	select {
	case sig := <-stopCh:
		log.Printf("kvstore: received %v, shutting down", sig)
	case err := <-httpErrCh:
		log.Printf("kvstore: HTTP server error: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpSrv.Shutdown(shutdownCtx)

	srv.Stop()
	tr.Close()
	raftNode.Close()
	eng.Close()

	return nil
}
