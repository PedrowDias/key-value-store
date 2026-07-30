package raft

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PedrowDias/key-value-store/wal"
)

// --- Snapshot payload encode/decode -----------------------------------------

func TestSnapshotPayload_RoundTrip(t *testing.T) {
	snap := Snapshot{LastIncludedIndex: 42, LastIncludedTerm: 7, Data: []byte("some-snapshot-bytes")}
	decoded, err := decodeSnapshotPayload(encodeSnapshotPayload(snap))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.LastIncludedIndex != snap.LastIncludedIndex || decoded.LastIncludedTerm != snap.LastIncludedTerm || string(decoded.Data) != string(snap.Data) {
		t.Fatalf("decoded = %+v, want %+v", decoded, snap)
	}
}

func TestSnapshotPayload_RoundTrip_EmptyData(t *testing.T) {
	snap := Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1}
	decoded, err := decodeSnapshotPayload(encodeSnapshotPayload(snap))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.LastIncludedIndex != 1 || decoded.LastIncludedTerm != 1 || len(decoded.Data) != 0 {
		t.Fatalf("decoded = %+v, want {Index:1 Term:1 Data:[]}", decoded)
	}
}

func TestDecodeSnapshotPayload_TooShortIsError(t *testing.T) {
	if _, err := decodeSnapshotPayload([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected an error decoding a too-short snapshot payload")
	}
}

func TestDecodeSnapshotPayload_LengthMismatchIsError(t *testing.T) {
	buf := encodeSnapshotPayload(Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, Data: []byte("hello")})
	// Truncate just the trailing data, leaving the declared length
	// prefix (still claiming 5 bytes) inconsistent with what's present.
	truncated := buf[:len(buf)-2]
	if _, err := decodeSnapshotPayload(truncated); err == nil {
		t.Fatal("expected an error when the declared data length doesn't match what's present")
	}
}

// --- OpenStorage: reconstructing a persisted snapshot boundary --------------

func TestOpenStorage_SnapshotPersistsAcrossReopen(t *testing.T) {
	path := tempStoragePath(t)
	s, _, _, _, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveHardState(HardState{Term: 3, Vote: 7}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveEntries(0, []LogEntry{
		{Term: 1, Index: 1, Data: []byte("a")},
		{Term: 1, Index: 2, Data: []byte("b")},
		{Term: 2, Index: 3, Data: []byte("c")},
	}); err != nil {
		t.Fatal(err)
	}

	snap := Snapshot{LastIncludedIndex: 2, LastIncludedTerm: 1, Data: []byte("snapshot-through-2")}
	survivingEntries := []LogEntry{{Term: 2, Index: 3, Data: []byte("c")}}
	if err := s.SaveSnapshot(HardState{Term: 3, Vote: 7}, snap, survivingEntries); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	_, hs, log, gotSnap, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if hs.Term != 3 || hs.Vote != 7 {
		t.Fatalf("HardState = %+v, want {Term:3 Vote:7}", hs)
	}
	if gotSnap.LastIncludedIndex != 2 || gotSnap.LastIncludedTerm != 1 || string(gotSnap.Data) != "snapshot-through-2" {
		t.Fatalf("Snapshot = %+v, want {Index:2 Term:1 Data:snapshot-through-2}", gotSnap)
	}
	if len(log) != 2 || log[0].Index != 2 || log[0].Term != 1 {
		t.Fatalf("log[0] (sentinel) = %+v, want {Index:2 Term:1}, len(log)=%d want 2", log[0], len(log))
	}
	if log[1].Index != 3 || string(log[1].Data) != "c" {
		t.Fatalf("log[1] = %+v, want the surviving entry 3 (Data: c)", log[1])
	}
}

func TestOpenStorage_NoSnapshotMeansOriginalZeroSentinel(t *testing.T) {
	path := tempStoragePath(t)
	s, _, _, _, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveEntries(0, []LogEntry{{Term: 1, Index: 1, Data: []byte("a")}}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	_, _, log, snap, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if snap.LastIncludedIndex != 0 {
		t.Fatalf("Snapshot = %+v, want the zero value (none ever persisted)", snap)
	}
	if log[0].Index != 0 || log[0].Term != 0 {
		t.Fatalf("log[0] (sentinel) = %+v, want the original {Index:0 Term:0}", log[0])
	}
}

// --- SaveSnapshot: failure injection at each step ---------------------------

func TestSaveSnapshot_TempFileOpenErrorPropagates(t *testing.T) {
	s, _, _, _, err := OpenStorage(tempStoragePath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	orig := openWALLog
	openWALLog = func(path string, opts wal.Options) (*wal.WAL, error) {
		return nil, errors.New("openWALLog: simulated failure")
	}
	defer func() { openWALLog = orig }()

	err = s.SaveSnapshot(HardState{}, Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1}, nil)
	if err == nil {
		t.Fatal("expected an error when the compaction temp file fails to open")
	}
	if !strings.Contains(err.Error(), "compaction temp file") {
		t.Fatalf("error = %v, want it to mention the compaction temp file", err)
	}
}

func TestSaveSnapshot_RenameErrorPropagates(t *testing.T) {
	s, _, _, _, err := OpenStorage(tempStoragePath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	origRename := renameFile
	renameFile = func(oldpath, newpath string) error {
		return errors.New("renameFile: simulated failure")
	}
	defer func() { renameFile = origRename }()

	err = s.SaveSnapshot(HardState{}, Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1}, nil)
	if err == nil {
		t.Fatal("expected an error when the atomic rename fails")
	}
	if !strings.Contains(err.Error(), "replacing storage log") {
		t.Fatalf("error = %v, want it to mention replacing the storage log", err)
	}
}

func TestSaveSnapshot_ReopenAfterCompactionErrorPropagates(t *testing.T) {
	s, _, _, _, err := OpenStorage(tempStoragePath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	orig := openWALLog
	callCount := 0
	openWALLog = func(path string, opts wal.Options) (*wal.WAL, error) {
		callCount++
		if callCount == 1 {
			// The temp-file open: let it succeed normally.
			return orig(path, opts)
		}
		// The reopen after a successful rename: fail it.
		return nil, errors.New("openWALLog: simulated failure on reopen")
	}
	defer func() { openWALLog = orig }()

	err = s.SaveSnapshot(HardState{}, Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1}, nil)
	if err == nil {
		t.Fatal("expected an error when reopening the storage log after compaction fails")
	}
	if !strings.Contains(err.Error(), "reopening storage log") {
		t.Fatalf("error = %v, want it to mention reopening the storage log", err)
	}
}

func TestSaveSnapshot_LeftoverTempFileFromPreviousCrashIsRemoved(t *testing.T) {
	path := tempStoragePath(t)
	s, _, _, _, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Simulate a leftover temp file from a previously crashed compaction
	// attempt, containing garbage that would fail to decode if it were
	// ever mistakenly replayed.
	tmpPath := path + ".compact.tmp"
	if f, ferr := os.Create(tmpPath); ferr == nil {
		f.WriteString("garbage-from-a-previous-crash")
		f.Close()
	} else {
		t.Fatal(ferr)
	}

	if err := s.SaveSnapshot(HardState{Term: 1}, Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, Data: []byte("real")}, nil); err != nil {
		t.Fatalf("SaveSnapshot should succeed despite a leftover temp file, got: %v", err)
	}
}

// --- Node: snapshot restoration across a restart, and CreateSnapshot -------

func TestNode_CreateSnapshotPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	n := openTestNode(t, Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}, path)

	for i := 0; i < 20; i++ {
		n.Tick()
		n.Persist()
	}

	if _, err := n.ProposeBatch([][]byte{[]byte("a"), []byte("b"), []byte("c")}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := n.Persist(); err != nil {
		t.Fatal(err)
	}
	if n.Status().CommitIndex != 3 {
		t.Fatalf("CommitIndex = %d, want 3", n.Status().CommitIndex)
	}

	if err := n.CreateSnapshot(2, []byte("snapshot-through-2")); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := n.Persist(); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	n2, snap, err := OpenNode(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}, path)
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()

	if snap == nil {
		t.Fatal("expected a persisted snapshot to be returned on reopen")
	}
	if snap.LastIncludedIndex != 2 || string(snap.Data) != "snapshot-through-2" {
		t.Fatalf("snap = %+v, want {Index:2 Data:snapshot-through-2}", snap)
	}
	// commitIndex is restored to AT LEAST the snapshot boundary (2) —
	// provably safe regardless of cluster size, since a snapshot's
	// entire meaning is "this index is committed." It's NOT necessarily
	// restored all the way to 3 just because entry 3 happens to still
	// be present in the persisted log: whether an old-term entry beyond
	// the snapshot boundary is truly committed is the separate, larger
	// question restoreState's own doc explains it deliberately doesn't
	// try to answer (see maybeAdvanceCommitIndex's Figure 8 rule).
	if n2.Status().CommitIndex < 2 {
		t.Fatalf("recovered CommitIndex = %d, want at least 2 (the snapshot boundary)", n2.Status().CommitIndex)
	}
	entries := n2.Entries(2, 3)
	if len(entries) != 1 || string(entries[0].Data) != "c" {
		t.Fatalf("recovered Entries(2,3) = %+v, want [{Data: c}] (the surviving entry)", entries)
	}
}

func TestNode_OpenNodeReturnsNilSnapshotWhenNoneWasPersisted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	n := openTestNode(t, Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}, path)
	n.Propose([]byte("x"))
	n.Persist()
	n.Close()

	n2, snap, err := OpenNode(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}, path)
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	if snap != nil {
		t.Fatalf("snap = %+v, want nil (no snapshot was ever created)", snap)
	}
}

func TestNode_PersistPropagatesSaveSnapshotError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	n := openTestNode(t, Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}, path)
	defer n.Close()

	for i := 0; i < 20; i++ {
		n.Tick()
		n.Persist()
	}
	if err := n.Propose([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := n.Persist(); err != nil {
		t.Fatal(err)
	}
	if err := n.CreateSnapshot(1, []byte("snap")); err != nil {
		t.Fatal(err)
	}

	origRename := renameFile
	renameFile = func(oldpath, newpath string) error {
		return errors.New("renameFile: simulated failure")
	}
	defer func() { renameFile = origRename }()

	_, _, _, err := n.Persist()
	if err == nil {
		t.Fatal("expected Node.Persist to propagate a SaveSnapshot failure")
	}
	if !strings.Contains(err.Error(), "node persist") {
		t.Fatalf("error = %v, want it to mention node persist", err)
	}
}

func TestNode_PersistUsesSaveSnapshotWhenReadyReportsOne(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	n := openTestNode(t, Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}, path)
	defer n.Close()

	for i := 0; i < 20; i++ {
		n.Tick()
		n.Persist()
	}

	if _, err := n.ProposeBatch([][]byte{[]byte("a"), []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := n.Persist(); err != nil {
		t.Fatal(err)
	}
	if err := n.CreateSnapshot(1, []byte("snap")); err != nil {
		t.Fatal(err)
	}
	msgs, _, snapToApply, err := n.Persist()
	if err != nil {
		t.Fatal(err)
	}
	// A LOCALLY created snapshot must never be reported back as
	// something the application needs to apply — it's the snapshot's
	// own source, with nothing to restore.
	if snapToApply != nil {
		t.Fatalf("SnapshotToApply = %+v, want nil for a local CreateSnapshot", snapToApply)
	}
	_ = msgs
}
