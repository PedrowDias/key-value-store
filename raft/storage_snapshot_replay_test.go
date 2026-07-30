package raft

import (
	"testing"

	"github.com/PedrowDias/key-value-store/wal"
)

func TestOpenStorage_MalformedSnapshotRecordPropagatesError(t *testing.T) {
	path := tempStoragePath(t)
	w, err := wal.Open(path, wal.Options{SyncOnWrite: true})
	if err != nil {
		t.Fatal(err)
	}
	w.Append(wal.Record{SeqNum: 1, Type: wal.RecordPut, Value: encodeRecord(recordSnapshot, []byte{1, 2})})
	w.Close()

	if _, _, _, _, err := OpenStorage(path); err == nil {
		t.Fatal("expected an error opening storage with a malformed snapshot record")
	}
}

func TestOpenStorage_SnapshotDiscardsOlderEntriesDuringReplay(t *testing.T) {
	// Directly exercises the defensive filtering inside OpenStorage's
	// own recordSnapshot handling — independent of whether
	// SaveSnapshot's rewrite would ever actually produce this exact
	// ordering in practice (entries after the snapshot boundary
	// normally come AFTER the snapshot record in the file, not before),
	// this manually constructs a storage log with entries BOTH before
	// AND after the boundary already present by the time a snapshot
	// record is replayed — the scenario that filtering loop exists to
	// handle correctly regardless: discard what's covered, keep what
	// isn't, even if it arrived earlier in the stream.
	path := tempStoragePath(t)
	w, err := wal.Open(path, wal.Options{SyncOnWrite: true})
	if err != nil {
		t.Fatal(err)
	}
	w.Append(wal.Record{SeqNum: 1, Type: wal.RecordPut, Value: encodeRecord(recordLogEntry, encodeLogEntryPayload(LogEntry{Term: 1, Index: 1, Data: []byte("old")}))})
	w.Append(wal.Record{SeqNum: 2, Type: wal.RecordPut, Value: encodeRecord(recordLogEntry, encodeLogEntryPayload(LogEntry{Term: 1, Index: 2, Data: []byte("also-old")}))})
	w.Append(wal.Record{SeqNum: 3, Type: wal.RecordPut, Value: encodeRecord(recordLogEntry, encodeLogEntryPayload(LogEntry{Term: 2, Index: 3, Data: []byte("new")}))})
	snap := Snapshot{LastIncludedIndex: 2, LastIncludedTerm: 1, Data: []byte("snap")}
	w.Append(wal.Record{SeqNum: 4, Type: wal.RecordPut, Value: encodeRecord(recordSnapshot, encodeSnapshotPayload(snap))})
	w.Close()

	_, _, log, gotSnap, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if gotSnap.LastIncludedIndex != 2 {
		t.Fatalf("Snapshot = %+v, want LastIncludedIndex 2", gotSnap)
	}
	if len(log) != 2 {
		t.Fatalf("len(log) = %d, want 2 (sentinel + entry 3 only — entries 1 and 2 must be discarded as covered by the snapshot)", len(log))
	}
	if log[0].Index != 2 || log[1].Index != 3 || string(log[1].Data) != "new" {
		t.Fatalf("log = %+v, want [{Index:2} {Index:3 Data:new}]", log)
	}
}
