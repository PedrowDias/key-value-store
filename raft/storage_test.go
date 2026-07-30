package raft

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PedrowDias/key-value-store/wal"
)

func tempStoragePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "raft.wal")
}

// --- Round-trip correctness ---------------------------------------------------

func TestStorage_FreshOpenReturnsEmptyState(t *testing.T) {
	s, hs, log, _, err := OpenStorage(tempStoragePath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if hs != (HardState{}) {
		t.Fatalf("HardState = %+v, want zero value", hs)
	}
	if len(log) != 1 || log[0].Term != 0 || log[0].Index != 0 || len(log[0].Data) != 0 {
		t.Fatalf("log = %+v, want just the dummy sentinel", log)
	}
}

func TestStorage_HardStatePersistsAcrossReopen(t *testing.T) {
	path := tempStoragePath(t)
	s, _, _, _, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveHardState(HardState{Term: 7, Vote: 3}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, hs, _, _, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if hs.Term != 7 || hs.Vote != 3 {
		t.Fatalf("recovered HardState = %+v, want {7 3}", hs)
	}
}

func TestStorage_HardStateOverwritePersistsLatest(t *testing.T) {
	path := tempStoragePath(t)
	s, _, _, _, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	s.SaveHardState(HardState{Term: 1, Vote: 1})
	s.SaveHardState(HardState{Term: 2, Vote: 2})
	s.SaveHardState(HardState{Term: 5, Vote: 9})
	s.Close()

	_, hs, _, _, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if hs.Term != 5 || hs.Vote != 9 {
		t.Fatalf("recovered HardState = %+v, want the last write {5 9}", hs)
	}
}

func TestStorage_EntriesPersistAcrossReopen(t *testing.T) {
	path := tempStoragePath(t)
	s, _, _, _, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	entries := []LogEntry{
		{Term: 1, Index: 1, Data: []byte("a")},
		{Term: 1, Index: 2, Data: []byte("b")},
		{Term: 2, Index: 3, Data: []byte("c")},
	}
	if err := s.SaveEntries(1, entries); err != nil {
		t.Fatal(err)
	}
	s.Close()

	_, _, log, _, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(log) != 4 { // dummy sentinel + 3 entries
		t.Fatalf("recovered log has %d entries, want 4", len(log))
	}
	for i, want := range entries {
		got := log[i+1]
		if got.Term != want.Term || got.Index != want.Index || string(got.Data) != string(want.Data) {
			t.Fatalf("entry %d = %+v, want %+v", i, got, want)
		}
	}
}

func TestStorage_SaveEntriesEmptyIsNoop(t *testing.T) {
	path := tempStoragePath(t)
	s, _, _, _, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveEntries(1, nil); err != nil {
		t.Fatal(err)
	}
	s.Close()

	_, _, log, _, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(log) != 1 {
		t.Fatalf("log = %+v, want just the dummy sentinel (nothing was saved)", log)
	}
}

// --- Truncation ----------------------------------------------------------------

func TestStorage_TruncationDiscardsConflictingEntriesOnRecovery(t *testing.T) {
	path := tempStoragePath(t)
	s, _, _, _, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	// First "leader" persists entries 1-3.
	s.SaveEntries(1, []LogEntry{
		{Term: 1, Index: 1, Data: []byte("old-1")},
		{Term: 1, Index: 2, Data: []byte("old-2")},
		{Term: 1, Index: 3, Data: []byte("old-3")},
	})
	// A new leader's AppendEntries conflicts starting at index 2: the
	// caller (Node, in real use) would call SaveEntries(2, [...]) to
	// truncate from 2 onward and persist the real version.
	s.SaveEntries(2, []LogEntry{
		{Term: 2, Index: 2, Data: []byte("real-2")},
	})
	s.Close()

	_, _, log, _, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	// Expect: dummy sentinel, old-1 (untouched, before the truncation
	// point), real-2 (replacing old-2; old-3 discarded entirely since
	// nothing re-wrote it).
	if len(log) != 3 {
		t.Fatalf("recovered log = %+v, want 3 entries (sentinel + old-1 + real-2)", log)
	}
	if string(log[1].Data) != "old-1" {
		t.Fatalf("log[1] = %+v, want old-1 (before the truncation point, untouched)", log[1])
	}
	if log[2].Term != 2 || string(log[2].Data) != "real-2" {
		t.Fatalf("log[2] = %+v, want the real-2 entry from the new leader", log[2])
	}
}

// --- Record encoding malformed-input handling (direct, whitebox) -----------

func TestDecodeRecord_EmptyIsError(t *testing.T) {
	if _, _, err := decodeRecord(nil); err == nil {
		t.Fatal("expected an error decoding an empty record")
	}
}

func TestDecodeHardStatePayload_WrongLengthIsError(t *testing.T) {
	if _, _, err := decodeHardStatePayload([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected an error for a malformed hard state payload")
	}
}

func TestDecodeLogEntryPayload_TooShortIsError(t *testing.T) {
	if _, err := decodeLogEntryPayload([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected an error for a too-short log entry payload")
	}
}

func TestDecodeLogEntryPayload_LengthMismatchIsError(t *testing.T) {
	payload := encodeLogEntryPayload(LogEntry{Term: 1, Index: 1, Data: []byte("hello")})
	truncated := payload[:len(payload)-2] // chop off part of the data
	if _, err := decodeLogEntryPayload(truncated); err == nil {
		t.Fatal("expected an error when the data length header doesn't match actual bytes present")
	}
}

func TestDecodeTruncateFromPayload_WrongLengthIsError(t *testing.T) {
	if _, err := decodeTruncateFromPayload([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected an error for a malformed truncate-from payload")
	}
}

func TestOpenStorage_UnknownRecordKindIsError(t *testing.T) {
	// Hand-write a record with an out-of-range kind byte directly via a
	// raw WAL, bypassing PersistentStorage's own encoder entirely.
	path := tempStoragePath(t)
	w, err := wal.Open(path, wal.Options{SyncOnWrite: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(wal.Record{SeqNum: 1, Type: wal.RecordPut, Value: []byte{99}}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	_, _, _, _, err = OpenStorage(path)
	if err == nil {
		t.Fatal("expected an error opening storage with an unknown record kind")
	}
}

func TestOpenStorage_MalformedHardStateRecordPropagatesError(t *testing.T) {
	path := tempStoragePath(t)
	w, err := wal.Open(path, wal.Options{SyncOnWrite: true})
	if err != nil {
		t.Fatal(err)
	}
	w.Append(wal.Record{SeqNum: 1, Type: wal.RecordPut, Value: encodeRecord(recordHardState, []byte{1, 2})})
	w.Close()

	_, _, _, _, err = OpenStorage(path)
	if err == nil {
		t.Fatal("expected an error opening storage with a malformed hard state record")
	}
}

func TestOpenStorage_MalformedLogEntryRecordPropagatesError(t *testing.T) {
	path := tempStoragePath(t)
	w, err := wal.Open(path, wal.Options{SyncOnWrite: true})
	if err != nil {
		t.Fatal(err)
	}
	w.Append(wal.Record{SeqNum: 1, Type: wal.RecordPut, Value: encodeRecord(recordLogEntry, []byte{1, 2})})
	w.Close()

	_, _, _, _, err = OpenStorage(path)
	if err == nil {
		t.Fatal("expected an error opening storage with a malformed log entry record")
	}
}

func TestOpenStorage_MalformedTruncateRecordPropagatesError(t *testing.T) {
	path := tempStoragePath(t)
	w, err := wal.Open(path, wal.Options{SyncOnWrite: true})
	if err != nil {
		t.Fatal(err)
	}
	w.Append(wal.Record{SeqNum: 1, Type: wal.RecordPut, Value: encodeRecord(recordTruncateFrom, []byte{1, 2})})
	w.Close()

	_, _, _, _, err = OpenStorage(path)
	if err == nil {
		t.Fatal("expected an error opening storage with a malformed truncate record")
	}
}

func TestOpenStorage_ReplayErrorPropagates(t *testing.T) {
	// A directory instead of a file makes wal.Replay itself fail, which
	// OpenStorage must propagate rather than papering over.
	dir := t.TempDir()
	_, _, _, _, err := OpenStorage(dir)
	if err == nil {
		t.Fatal("expected an error when the storage path is a directory")
	}
}

func TestOpenStorage_EmptyRecordValuePropagatesDecodeError(t *testing.T) {
	path := tempStoragePath(t)
	w, err := wal.Open(path, wal.Options{SyncOnWrite: true})
	if err != nil {
		t.Fatal(err)
	}
	// An empty Value has no kind byte at all: decodeRecord's own
	// malformed-input case, reached here via a real replayed WAL record
	// rather than a direct unit test of decodeRecord.
	if err := w.Append(wal.Record{SeqNum: 1, Type: wal.RecordPut, Value: []byte{}}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	_, _, _, _, err = OpenStorage(path)
	if err == nil {
		t.Fatal("expected an error opening storage with an empty record value")
	}
}

func TestOpenStorage_WALOpenErrorPropagates(t *testing.T) {
	orig := openWALLog
	openWALLog = func(path string, opts wal.Options) (*wal.WAL, error) {
		return nil, errors.New("openWALLog: simulated failure")
	}
	defer func() { openWALLog = orig }()

	_, _, _, _, err := OpenStorage(tempStoragePath(t))
	if err == nil {
		t.Fatal("expected an error propagated from a failing wal.Open")
	}
	if !strings.Contains(err.Error(), "opening storage log") {
		t.Fatalf("error = %v, want it to mention opening the storage log", err)
	}
}
