package wal

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func tempWALPath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "test.wal")
}

func mustOpen(t *testing.T, path string, sync bool) *WAL {
	t.Helper()
	w, err := Open(path, Options{SyncOnWrite: sync})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return w
}

// --- Basic correctness ---------------------------------------------------

func TestAppendAndReplay_RoundTrip(t *testing.T) {
	path := tempWALPath(t)
	w := mustOpen(t, path, true)

	want := []Record{
		{SeqNum: 1, Type: RecordPut, Key: []byte("k1"), Value: []byte("v1")},
		{SeqNum: 2, Type: RecordPut, Key: []byte("k2"), Value: []byte("v2-longer-value")},
		{SeqNum: 3, Type: RecordDelete, Key: []byte("k1")},
	}
	for _, rec := range want {
		if err := w.Append(rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, stats, err := Replay(path)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if stats.TornWriteTruncated {
		t.Fatalf("unexpected torn write on clean close: %+v", stats)
	}
	if stats.RecordsRecovered != len(want) {
		t.Fatalf("recovered %d records, want %d", stats.RecordsRecovered, len(want))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replayed records mismatch:\ngot:  %+v\nwant: %+v", got, want)
	}
}

func TestReplay_EmptyOrMissingFile(t *testing.T) {
	path := tempWALPath(t) // never created
	got, stats, err := Replay(path)
	if err != nil {
		t.Fatalf("Replay on missing file should not error: %v", err)
	}
	if len(got) != 0 || stats.RecordsRecovered != 0 {
		t.Fatalf("expected no records, got %+v / %+v", got, stats)
	}

	// Now create it empty and replay again.
	w := mustOpen(t, path, true)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got, stats, err = Replay(path)
	if err != nil {
		t.Fatalf("Replay on empty file: %v", err)
	}
	if len(got) != 0 || stats.TornWriteTruncated {
		t.Fatalf("expected clean empty replay, got %+v / %+v", got, stats)
	}
}

func TestAppendBatch_GroupCommit(t *testing.T) {
	path := tempWALPath(t)
	w := mustOpen(t, path, true)

	batch := []Record{
		{SeqNum: 1, Type: RecordPut, Key: []byte("a"), Value: []byte("1")},
		{SeqNum: 2, Type: RecordPut, Key: []byte("b"), Value: []byte("2")},
		{SeqNum: 3, Type: RecordPut, Key: []byte("c"), Value: []byte("3")},
	}
	if err := w.AppendBatch(batch); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, _, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, batch) {
		t.Fatalf("got %+v, want %+v", got, batch)
	}
}

func TestReplay_PreservesOrderAndSeqNums(t *testing.T) {
	path := tempWALPath(t)
	w := mustOpen(t, path, true)
	for i := uint64(1); i <= 50; i++ {
		rec := Record{SeqNum: i, Type: RecordPut, Key: []byte{byte(i)}, Value: []byte{byte(i * 2)}}
		if err := w.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	got, _, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 50 {
		t.Fatalf("got %d records, want 50", len(got))
	}
	for i, rec := range got {
		if rec.SeqNum != uint64(i+1) {
			t.Fatalf("record %d has SeqNum %d, want %d", i, rec.SeqNum, i+1)
		}
	}
}

func TestEmptyAndNilValues(t *testing.T) {
	path := tempWALPath(t)
	w := mustOpen(t, path, true)
	recs := []Record{
		{SeqNum: 1, Type: RecordPut, Key: []byte("k"), Value: []byte{}},   // empty value
		{SeqNum: 2, Type: RecordDelete, Key: []byte("k"), Value: nil},     // nil value (delete)
		{SeqNum: 3, Type: RecordPut, Key: []byte(""), Value: []byte("v")}, // empty key
	}
	for _, r := range recs {
		if err := w.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	got, _, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d records, want 3", len(got))
	}
	if len(got[0].Value) != 0 {
		t.Fatalf("expected empty value, got %v", got[0].Value)
	}
	if len(got[2].Key) != 0 {
		t.Fatalf("expected empty key, got %v", got[2].Key)
	}
}

// --- Crash / corruption recovery -----------------------------------------

// TestReplay_CrashMidWrite_TruncatedPayload simulates the core WAL failure
// mode: the process crashed (or lost power) while the OS was still flushing
// a record's payload to disk. The header (length prefix) made it, but the
// full payload did not. Replay must recover every prior valid record and
// silently discard the torn one — NOT error out, and NOT lose earlier data.
func TestReplay_CrashMidWrite_TruncatedPayload(t *testing.T) {
	path := tempWALPath(t)
	w := mustOpen(t, path, true)

	good := []Record{
		{SeqNum: 1, Type: RecordPut, Key: []byte("k1"), Value: []byte("v1")},
		{SeqNum: 2, Type: RecordPut, Key: []byte("k2"), Value: []byte("v2")},
	}
	for _, r := range good {
		if err := w.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	// Close cleanly so file size reflects exactly the two good records.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	cleanSize := fileSize(t, path)

	// Now simulate a crash mid-append of a third record: write a full,
	// correctly-checksummed record's header+payload, but then reopen the
	// raw file and chop off the last few bytes of the payload, exactly as
	// would happen if the OS only flushed part of the write() before a
	// power loss. We do this by re-encoding a record and manually writing
	// a truncated version directly to the file.
	torn := encode(Record{SeqNum: 3, Type: RecordPut, Key: []byte("k3"), Value: []byte("this-value-will-be-cut-short")})
	tornPrefix := torn[:len(torn)-10] // drop the last 10 bytes of the payload

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(tornPrefix); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	got, stats, err := Replay(path)
	if err != nil {
		t.Fatalf("Replay must not error on a torn tail, got: %v", err)
	}
	if !stats.TornWriteTruncated {
		t.Fatalf("expected TornWriteTruncated=true, got stats=%+v", stats)
	}
	if !reflect.DeepEqual(got, good) {
		t.Fatalf("expected exactly the 2 pre-crash records recovered:\ngot:  %+v\nwant: %+v", got, good)
	}

	// The file on disk should have been truncated back to the clean size,
	// so that a subsequent process can reopen and append cleanly right
	// after the last good record instead of leaving a corrupt gap.
	if got := fileSize(t, path); got != cleanSize {
		t.Fatalf("file not truncated to clean size: got %d, want %d", got, cleanSize)
	}

	// And appending after recovery must work normally.
	w2 := mustOpen(t, path, true)
	if err := w2.Append(Record{SeqNum: 3, Type: RecordPut, Key: []byte("k3"), Value: []byte("v3-retry")}); err != nil {
		t.Fatal(err)
	}
	w2.Close()

	final, _, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(final) != 3 {
		t.Fatalf("expected 3 records after post-crash append, got %d", len(final))
	}
	if string(final[2].Value) != "v3-retry" {
		t.Fatalf("unexpected recovered value: %q", final[2].Value)
	}
}

// TestReplay_CrashMidWrite_TruncatedHeader covers the narrower case where
// the crash happened even earlier — only part of the 8-byte length/CRC
// header itself made it to disk.
func TestReplay_CrashMidWrite_TruncatedHeader(t *testing.T) {
	path := tempWALPath(t)
	w := mustOpen(t, path, true)
	good := Record{SeqNum: 1, Type: RecordPut, Key: []byte("k1"), Value: []byte("v1")}
	if err := w.Append(good); err != nil {
		t.Fatal(err)
	}
	w.Close()
	cleanSize := fileSize(t, path)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	// Write only 3 of 8 header bytes for a would-be next record.
	if _, err := f.Write([]byte{0x01, 0x02, 0x03}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	got, stats, err := Replay(path)
	if err != nil {
		t.Fatalf("Replay must not error: %v", err)
	}
	if !stats.TornWriteTruncated {
		t.Fatalf("expected torn write detected, got %+v", stats)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], good) {
		t.Fatalf("got %+v, want [%+v]", got, good)
	}
	if fs := fileSize(t, path); fs != cleanSize {
		t.Fatalf("file not truncated: got %d want %d", fs, cleanSize)
	}
}

// TestReplay_BitCorruption_DetectedViaChecksum ensures that a flipped bit in
// an otherwise complete, correctly-sized record is caught by the checksum
// (not silently accepted as valid data), and treated the same as a torn tail.
func TestReplay_BitCorruption_DetectedViaChecksum(t *testing.T) {
	path := tempWALPath(t)
	w := mustOpen(t, path, true)
	good := Record{SeqNum: 1, Type: RecordPut, Key: []byte("k1"), Value: []byte("v1")}
	if err := w.Append(good); err != nil {
		t.Fatal(err)
	}
	bad := Record{SeqNum: 2, Type: RecordPut, Key: []byte("k2"), Value: []byte("v2")}
	if err := w.Append(bad); err != nil {
		t.Fatal(err)
	}
	w.Close()

	// Flip a bit inside the second record's payload (well past the first
	// record's framing) without updating its checksum, simulating disk
	// corruption of an already-written record.
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := f.Stat()
	// Flip the last byte of the file (part of record 2's value).
	offset := info.Size() - 1
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, offset); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b, offset); err != nil {
		t.Fatal(err)
	}
	f.Close()

	got, stats, err := Replay(path)
	if err != nil {
		t.Fatalf("Replay must not error on checksum mismatch: %v", err)
	}
	if !stats.TornWriteTruncated {
		t.Fatalf("expected corruption to be reported as truncation, got %+v", stats)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], good) {
		t.Fatalf("expected only the first, uncorrupted record recovered, got %+v", got)
	}
}

func TestAppendToClosedWAL_Errors(t *testing.T) {
	path := tempWALPath(t)
	w := mustOpen(t, path, true)
	w.Close()
	err := w.Append(Record{SeqNum: 1, Type: RecordPut, Key: []byte("k"), Value: []byte("v")})
	if err == nil {
		t.Fatal("expected error appending to closed WAL")
	}
}

func TestSyncOnWriteFalse_ExplicitSyncStillDurable(t *testing.T) {
	path := tempWALPath(t)
	w := mustOpen(t, path, false)
	for i := uint64(1); i <= 5; i++ {
		if err := w.Append(Record{SeqNum: i, Type: RecordPut, Key: []byte{byte(i)}, Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Close()

	got, _, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d records, want 5", len(got))
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}
