package main

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/PedrowDias/key-value-store/engine"
	"github.com/PedrowDias/key-value-store/raft"
)

// --- parseArgs ---------------------------------------------------------------

func TestParseArgs_ValidMinimal(t *testing.T) {
	cfg, err := parseArgs([]string{"-id=1", "-raft-addr=127.0.0.1:7001", "-http-addr=127.0.0.1:8001"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.id != 1 || cfg.raftAddr != "127.0.0.1:7001" || cfg.httpAddr != "127.0.0.1:8001" {
		t.Fatalf("cfg = %+v", cfg)
	}
	if cfg.dataDir != "./data" {
		t.Fatalf("dataDir = %q, want default ./data", cfg.dataDir)
	}
}

func TestParseArgs_WithPeersAndTuning(t *testing.T) {
	cfg, err := parseArgs([]string{
		"-id=1", "-raft-addr=127.0.0.1:7001", "-http-addr=127.0.0.1:8001",
		"-peers=2=127.0.0.1:7002,3=127.0.0.1:7003",
		"-data-dir=/tmp/somedir", "-election-ticks=20", "-heartbeat-ticks=2",
		"-tick-interval=50ms", "-snapshot-threshold=5",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.peerIDs) != 2 {
		t.Fatalf("peerIDs = %v, want 2 entries", cfg.peerIDs)
	}
	if cfg.dataDir != "/tmp/somedir" || cfg.electionTicks != 20 || cfg.heartbeatTicks != 2 {
		t.Fatalf("cfg = %+v", cfg)
	}
	if cfg.tickInterval != 50*time.Millisecond {
		t.Fatalf("tickInterval = %v, want 50ms", cfg.tickInterval)
	}
	if cfg.snapshotThreshold != 5 {
		t.Fatalf("snapshotThreshold = %d, want 5", cfg.snapshotThreshold)
	}
}

func TestParseArgs_MissingID(t *testing.T) {
	_, err := parseArgs([]string{"-raft-addr=127.0.0.1:7001", "-http-addr=127.0.0.1:8001"})
	if err == nil {
		t.Fatal("expected an error for missing -id")
	}
}

func TestParseArgs_MissingRaftAddr(t *testing.T) {
	_, err := parseArgs([]string{"-id=1", "-http-addr=127.0.0.1:8001"})
	if err == nil {
		t.Fatal("expected an error for missing -raft-addr")
	}
}

func TestParseArgs_MissingHTTPAddr(t *testing.T) {
	_, err := parseArgs([]string{"-id=1", "-raft-addr=127.0.0.1:7001"})
	if err == nil {
		t.Fatal("expected an error for missing -http-addr")
	}
}

func TestParseArgs_InvalidPeersPropagatesError(t *testing.T) {
	_, err := parseArgs([]string{"-id=1", "-raft-addr=127.0.0.1:7001", "-http-addr=127.0.0.1:8001", "-peers=garbage"})
	if err == nil {
		t.Fatal("expected an error for malformed -peers")
	}
}

func TestParseArgs_UnknownFlagPropagatesParseError(t *testing.T) {
	_, err := parseArgs([]string{"-not-a-real-flag=1"})
	if err == nil {
		t.Fatal("expected a flag-parse error for an unknown flag")
	}
}

// --- parsePeers ----------------------------------------------------------------

func TestParsePeers_Empty(t *testing.T) {
	addrs, ids, err := parsePeers("")
	if err != nil {
		t.Fatal(err)
	}
	if len(addrs) != 0 || len(ids) != 0 {
		t.Fatalf("addrs=%v ids=%v, want both empty", addrs, ids)
	}
}

func TestParsePeers_WhitespaceOnly(t *testing.T) {
	addrs, ids, err := parsePeers("   ")
	if err != nil {
		t.Fatal(err)
	}
	if len(addrs) != 0 || len(ids) != 0 {
		t.Fatalf("addrs=%v ids=%v, want both empty", addrs, ids)
	}
}

func TestParsePeers_SingleEntry(t *testing.T) {
	addrs, ids, err := parsePeers("2=127.0.0.1:7002")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != 2 {
		t.Fatalf("ids = %v, want [2]", ids)
	}
	if addrs[2] != "127.0.0.1:7002" {
		t.Fatalf("addrs[2] = %q, want 127.0.0.1:7002", addrs[2])
	}
}

func TestParsePeers_MultipleEntries(t *testing.T) {
	addrs, ids, err := parsePeers("2=127.0.0.1:7002,3=127.0.0.1:7003")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("ids = %v, want 2 entries", ids)
	}
	if addrs[2] != "127.0.0.1:7002" || addrs[3] != "127.0.0.1:7003" {
		t.Fatalf("addrs = %v", addrs)
	}
}

func TestParsePeers_TrimsWhitespaceAroundEntries(t *testing.T) {
	addrs, ids, err := parsePeers(" 2 = 127.0.0.1:7002 , 3=127.0.0.1:7003 ")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || addrs[2] != "127.0.0.1:7002" || addrs[3] != "127.0.0.1:7003" {
		t.Fatalf("addrs=%v ids=%v", addrs, ids)
	}
}

func TestParsePeers_SkipsEmptyEntriesBetweenCommas(t *testing.T) {
	_, ids, err := parsePeers("2=127.0.0.1:7002,,3=127.0.0.1:7003")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("ids = %v, want 2 (empty entry between commas should be skipped)", ids)
	}
}

func TestParsePeers_MalformedEntryMissingEquals(t *testing.T) {
	_, _, err := parsePeers("2-127.0.0.1:7002")
	if err == nil {
		t.Fatal("expected an error for an entry missing '='")
	}
}

func TestParsePeers_MalformedNonNumericID(t *testing.T) {
	_, _, err := parsePeers("abc=127.0.0.1:7002")
	if err == nil {
		t.Fatal("expected an error for a non-numeric peer ID")
	}
}

func TestParsePeers_ZeroIDRejected(t *testing.T) {
	_, _, err := parsePeers("0=127.0.0.1:7002")
	if err == nil {
		t.Fatal("expected an error for a zero peer ID")
	}
}

func TestParsePeers_EmptyAddressRejected(t *testing.T) {
	_, _, err := parsePeers("2=")
	if err == nil {
		t.Fatal("expected an error for an empty address")
	}
}

// --- buildComponents ----------------------------------------------------------

func testConfig(t *testing.T, id uint64) config {
	t.Helper()
	return config{
		id:             id,
		raftAddr:       "127.0.0.1:0",
		httpAddr:       "127.0.0.1:0",
		dataDir:        t.TempDir(),
		electionTicks:  10,
		heartbeatTicks: 1,
		tickInterval:   10 * time.Millisecond,
	}
}

func TestBuildComponents_Success(t *testing.T) {
	cfg := testConfig(t, 1)
	rn, tr, eng, _, err := buildComponents(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { tr.Close(); rn.Close(); eng.Close() }()
	if rn == nil || tr == nil || eng == nil {
		t.Fatal("expected all three components to be non-nil")
	}
}

// TestBuildComponents_PersistedSnapshotOnStartupWarns seeds a real
// persisted snapshot at the exact path buildComponents itself will open
// (simulating a previous run that had already compacted its log), then
// calls buildComponents fresh (simulating a restart) — which today
// should reach and pass through the "snapshot exists but can't yet be
// restored into the engine" warning branch without erroring, rather
// than silently ignoring the snapshot's existence. See buildComponents'
// own TODO comment for why actually restoring it isn't implemented yet.
func TestBuildComponents_RestoresPersistedSnapshotOnStartup(t *testing.T) {
	cfg := testConfig(t, 1)
	raftWALPath := filepath.Join(cfg.dataDir, "raft.wal")

	// Generate valid snapshot bytes the same way a real one would be
	// produced: from a real engine's own Snapshot() call.
	sourceEng, err := engine.Open(engine.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceEng.Put([]byte("seeded-key"), []byte("seeded-value")); err != nil {
		t.Fatal(err)
	}
	snapData, err := sourceEng.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sourceEng.Close()

	seedNode, _, err := raft.OpenNode(raft.Config{ID: cfg.id, ElectionTick: cfg.electionTicks, HeartbeatTick: cfg.heartbeatTicks}, raftWALPath)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		seedNode.Tick()
		seedNode.Persist()
	}
	if err := seedNode.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := seedNode.Persist(); err != nil {
		t.Fatal(err)
	}
	if err := seedNode.CreateSnapshot(1, snapData); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := seedNode.Persist(); err != nil {
		t.Fatal(err)
	}
	if err := seedNode.Close(); err != nil {
		t.Fatal(err)
	}

	rn, tr, eng, appliedIndex, err := buildComponents(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { tr.Close(); rn.Close(); eng.Close() }()
	if rn == nil || tr == nil || eng == nil {
		t.Fatal("expected all three components to be non-nil")
	}
	if appliedIndex != 1 {
		t.Fatalf("appliedIndex = %d, want 1 (the persisted snapshot's LastIncludedIndex)", appliedIndex)
	}
	val, found, err := eng.Get([]byte("seeded-key"))
	if err != nil || !found || string(val) != "seeded-value" {
		t.Fatalf("seeded-key = %q found=%v err=%v, want seeded-value true nil — the persisted snapshot should have been restored into the engine", val, found, err)
	}
}

func TestBuildComponents_MalformedPersistedSnapshotFailsStartup(t *testing.T) {
	// The other side of the same behavior: if restoring the persisted
	// snapshot into the engine fails, buildComponents must refuse to
	// start rather than silently proceed with incomplete data (unlike
	// server.pump's own retry-later handling for a LIVE node — this one
	// hasn't started participating at all yet).
	cfg := testConfig(t, 1)
	raftWALPath := filepath.Join(cfg.dataDir, "raft.wal")

	seedNode, _, err := raft.OpenNode(raft.Config{ID: cfg.id, ElectionTick: cfg.electionTicks, HeartbeatTick: cfg.heartbeatTicks}, raftWALPath)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		seedNode.Tick()
		seedNode.Persist()
	}
	if err := seedNode.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := seedNode.Persist(); err != nil {
		t.Fatal(err)
	}
	// Deliberately malformed: not valid length-prefixed snapshot data.
	if err := seedNode.CreateSnapshot(1, []byte("not-valid-snapshot-data")); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := seedNode.Persist(); err != nil {
		t.Fatal(err)
	}
	if err := seedNode.Close(); err != nil {
		t.Fatal(err)
	}

	_, _, _, _, err = buildComponents(cfg)
	if err == nil {
		t.Fatal("expected buildComponents to fail rather than start with a snapshot it couldn't restore")
	}
}

func TestBuildComponents_DataDirIsFileFailsAtMkdir(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	os.WriteFile(blocker, []byte("x"), 0644)

	cfg := testConfig(t, 1)
	cfg.dataDir = filepath.Join(blocker, "sub") // parent path component is a file
	_, _, _, _, err := buildComponents(cfg)
	if err == nil {
		t.Fatal("expected an error when the data directory's parent is a regular file")
	}
}

func TestBuildComponents_RaftOpenNodeErrorPropagates(t *testing.T) {
	cfg := testConfig(t, 0) // invalid: zero ID makes raft.OpenNode fail
	_, _, _, _, err := buildComponents(cfg)
	if err == nil {
		t.Fatal("expected an error for an invalid raft Config")
	}
	if !strings.Contains(err.Error(), "opening raft node") {
		t.Fatalf("error = %v, want it to mention opening the raft node", err)
	}
}

func TestBuildComponents_TransportListenErrorClosesRaftNode(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()

	cfg := testConfig(t, 1)
	cfg.raftAddr = blocker.Addr().String() // already in use
	_, _, _, _, err = buildComponents(cfg)
	if err == nil {
		t.Fatal("expected an error when the raft address is already in use")
	}
	if !strings.Contains(err.Error(), "starting transport") {
		t.Fatalf("error = %v, want it to mention starting the transport", err)
	}
}

func TestBuildComponents_EngineOpenErrorClosesTransportAndRaftNode(t *testing.T) {
	cfg := testConfig(t, 1)
	// engine.Open's data dir is cfg.dataDir + "/kv"; make that path
	// already exist as a regular file so os.MkdirAll (inside engine.Open)
	// fails.
	kvPath := filepath.Join(cfg.dataDir, "kv")
	if err := os.WriteFile(kvPath, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	_, _, _, _, err := buildComponents(cfg)
	if err == nil {
		t.Fatal("expected an error when the engine's data directory is already a file")
	}
	if !strings.Contains(err.Error(), "opening storage engine") {
		t.Fatalf("error = %v, want it to mention opening the storage engine", err)
	}
}

// --- startHTTPServer -----------------------------------------------------------

func TestStartHTTPServer_ServesRequests(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("pong")) })

	httpSrv, errCh := startHTTPServer(ln, mux)
	defer httpSrv.Close()

	resp, err := http.Get("http://" + ln.Addr().String() + "/ping")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	select {
	case err := <-errCh:
		t.Fatalf("unexpected error from a healthy server: %v", err)
	default:
	}
}

func TestStartHTTPServer_ListenerErrorSurfacesOnErrCh(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, errCh := startHTTPServer(ln, http.NewServeMux())

	// Closing the listener out from under Serve makes its Accept loop
	// fail with a real (non-ErrServerClosed) error.
	ln.Close()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected a non-nil error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected an error on errCh after closing the listener directly")
	}
}

// --- runServer: full lifecycle --------------------------------------------------

// fakeSignal satisfies os.Signal for tests that need to trigger a
// shutdown deterministically without waiting for (or sending) a real OS
// signal.
type fakeSignal struct{ name string }

func (f fakeSignal) String() string { return f.name }
func (fakeSignal) Signal()          {}

func TestRunServer_ShutsDownOnStopSignal(t *testing.T) {
	cfg := testConfig(t, 1)
	ln, err := net.Listen("tcp", cfg.httpAddr)
	if err != nil {
		t.Fatal(err)
	}

	stopCh := make(chan os.Signal, 1)
	readyCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() { errCh <- runServer(cfg, ln, stopCh, readyCh) }()

	addr := <-readyCh
	// Confirm it's actually serving before triggering shutdown.
	resp, err := http.Get("http://" + addr + "/status")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	stopCh <- fakeSignal{name: "test-signal"}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runServer returned an error on graceful stop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runServer did not return after a stop signal")
	}
}

func TestRunServer_SeedsAppliedIndexFromPersistedSnapshot(t *testing.T) {
	cfg := testConfig(t, 1)
	cfg.snapshotThreshold = 5 // exercise runServer's SetSnapshotThreshold branch too
	raftWALPath := filepath.Join(cfg.dataDir, "raft.wal")

	sourceEng, err := engine.Open(engine.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceEng.Put([]byte("seeded-key"), []byte("seeded-value")); err != nil {
		t.Fatal(err)
	}
	snapData, err := sourceEng.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sourceEng.Close()

	seedNode, _, err := raft.OpenNode(raft.Config{ID: cfg.id, ElectionTick: cfg.electionTicks, HeartbeatTick: cfg.heartbeatTicks}, raftWALPath)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		seedNode.Tick()
		seedNode.Persist()
	}
	if err := seedNode.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := seedNode.Persist(); err != nil {
		t.Fatal(err)
	}
	if err := seedNode.CreateSnapshot(1, snapData); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := seedNode.Persist(); err != nil {
		t.Fatal(err)
	}
	if err := seedNode.Close(); err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", cfg.httpAddr)
	if err != nil {
		t.Fatal(err)
	}
	stopCh := make(chan os.Signal, 1)
	readyCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() { errCh <- runServer(cfg, ln, stopCh, readyCh) }()
	defer func() {
		stopCh <- fakeSignal{name: "test-signal"}
		<-errCh
	}()

	addr := <-readyCh
	resp, err := http.Get("http://" + addr + "/kv/seeded-key")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET seeded-key status = %d, want 200 — the persisted snapshot should have been restored and applied on startup", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "seeded-value" {
		t.Fatalf("GET seeded-key body = %q, want seeded-value", body)
	}
}

func TestRunServer_ShutsDownOnHTTPServerError(t *testing.T) {
	cfg := testConfig(t, 1)
	ln, err := net.Listen("tcp", cfg.httpAddr)
	if err != nil {
		t.Fatal(err)
	}

	stopCh := make(chan os.Signal, 1) // never sent to
	readyCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() { errCh <- runServer(cfg, ln, stopCh, readyCh) }()

	<-readyCh
	// Force the HTTP server's Accept loop to fail, triggering the
	// httpErrCh branch of runServer's select instead of the stop-signal
	// branch.
	ln.Close()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runServer returned an error: %v (it should still shut down cleanly and return nil even when its own listener failed)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runServer did not return after its HTTP listener failed")
	}
}

func TestRunServer_BuildComponentsErrorClosesListener(t *testing.T) {
	cfg := testConfig(t, 0) // invalid config: buildComponents will fail
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	err = runServer(cfg, ln, make(chan os.Signal, 1), nil)
	if err == nil {
		t.Fatal("expected an error from an invalid config")
	}
	// The listener must have been closed (not leaked): a further Accept
	// should fail immediately.
	_, acceptErr := ln.Accept()
	if acceptErr == nil {
		t.Fatal("expected the listener to already be closed")
	}
}

// --- realMain: full lifecycle, exercised end-to-end ----------------------------

func TestRealMain_ParseArgsErrorReturnsOneAndWritesStderr(t *testing.T) {
	var stderr bytes.Buffer
	code := realMain([]string{}, &stderr, make(chan os.Signal, 1), nil)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "-id") {
		t.Fatalf("stderr = %q, want it to mention -id", stderr.String())
	}
}

func TestRealMain_HTTPListenErrorReturnsOne(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()

	var stderr bytes.Buffer
	args := []string{
		"-id=1", "-raft-addr=127.0.0.1:0",
		"-http-addr=" + blocker.Addr().String(), // already in use
	}
	code := realMain(args, &stderr, make(chan os.Signal, 1), nil)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "http listener") {
		t.Fatalf("stderr = %q, want it to mention the http listener", stderr.String())
	}
}

func TestRealMain_RunServerErrorReturnsOne(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	os.WriteFile(blocker, []byte("x"), 0644)

	var stderr bytes.Buffer
	args := []string{
		"-id=1", "-raft-addr=127.0.0.1:0", "-http-addr=127.0.0.1:0",
		"-data-dir=" + filepath.Join(blocker, "sub"),
	}
	code := realMain(args, &stderr, make(chan os.Signal, 1), nil)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
}

func TestRealMain_FullLifecycleGracefulShutdown(t *testing.T) {
	dir := t.TempDir()
	args := []string{
		"-id=1", "-raft-addr=127.0.0.1:0", "-http-addr=127.0.0.1:0",
		"-data-dir=" + dir, "-tick-interval=10ms",
	}

	stopCh := make(chan os.Signal, 1)
	readyCh := make(chan string, 1)
	var stderr bytes.Buffer
	codeCh := make(chan int, 1)
	go func() { codeCh <- realMain(args, &stderr, stopCh, readyCh) }()

	addr := <-readyCh
	resp, err := http.Get("http://" + addr + "/status")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	stopCh <- syscall.SIGTERM

	select {
	case code := <-codeCh:
		if code != 0 {
			t.Fatalf("exit code = %d, want 0 (graceful shutdown), stderr: %s", code, stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("realMain did not return after a stop signal")
	}
}
