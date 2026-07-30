package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/PedrowDias/key-value-store/sstable"
	"github.com/PedrowDias/key-value-store/wal"
)

// --- Snapshot() --------------------------------------------------------------

func TestSnapshot_RoundTrip(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()

	e.Put([]byte("k1"), []byte("v1"))
	e.Put([]byte("k2"), []byte("v2"))
	e.Put([]byte("k3"), []byte("v3"))
	e.Delete([]byte("k2")) // must NOT appear in the snapshot

	data, err := e.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	e2 := mustOpen(t, Options{Dir: tempDir(t)})
	defer e2.Close()
	if err := e2.RestoreSnapshot(data); err != nil {
		t.Fatal(err)
	}

	val, found, err := e2.Get([]byte("k1"))
	if err != nil || !found || string(val) != "v1" {
		t.Fatalf("k1 = %q found=%v err=%v, want v1 true nil", val, found, err)
	}
	val, found, err = e2.Get([]byte("k3"))
	if err != nil || !found || string(val) != "v3" {
		t.Fatalf("k3 = %q found=%v err=%v, want v3 true nil", val, found, err)
	}
	_, found, err = e2.Get([]byte("k2"))
	if err != nil || found {
		t.Fatalf("k2 found=%v err=%v, want false nil (deleted, must not survive the snapshot)", found, err)
	}
}

func TestSnapshot_CoversDataAcrossMemtableAndFlushedSSTable(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t), MemtableSizeThreshold: 1})
	defer e.Close()

	// This Put crosses the (tiny) threshold and gets flushed to a real
	// SSTable; a subsequent one stays in the fresh active memtable —
	// Snapshot must gather live data from BOTH sources, not just one.
	if err := e.Put([]byte("flushed-key"), []byte("flushed-value")); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := e.Put([]byte("memtable-key"), []byte("memtable-value")); err != nil {
		t.Fatal(err)
	}

	data, err := e.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	e2 := mustOpen(t, Options{Dir: tempDir(t)})
	defer e2.Close()
	if err := e2.RestoreSnapshot(data); err != nil {
		t.Fatal(err)
	}
	val, found, _ := e2.Get([]byte("flushed-key"))
	if !found || string(val) != "flushed-value" {
		t.Fatalf("flushed-key found=%v val=%q, want true flushed-value", found, val)
	}
	val, found, _ = e2.Get([]byte("memtable-key"))
	if !found || string(val) != "memtable-value" {
		t.Fatalf("memtable-key found=%v val=%q, want true memtable-value", found, val)
	}
}

func TestSnapshot_ClosedEngineReturnsError(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	e.Close()
	if _, err := e.Snapshot(); err == nil {
		t.Fatal("expected an error taking a snapshot of a closed engine")
	}
}

func TestSnapshot_EmptyEngineProducesEmptyRestorable(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()
	data, err := e.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	e2 := mustOpen(t, Options{Dir: tempDir(t)})
	defer e2.Close()
	if err := e2.RestoreSnapshot(data); err != nil {
		t.Fatal(err)
	}
	_, found, err := e2.Get([]byte("anything"))
	if err != nil || found {
		t.Fatalf("found=%v err=%v, want false nil for an empty restored snapshot", found, err)
	}
}

func TestSnapshot_FakeSSTableReaderReturnsError(t *testing.T) {
	// Snapshot requires a real *sstable.Reader to iterate (see its own
	// doc); a fake substituted via the DI seam is correctly rejected
	// rather than silently skipped or panicking.
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()
	e.sstables = append(e.sstables, &sstableEntry{path: "fake", reader: &fakeSSTableReader{}})

	if _, err := e.Snapshot(); err == nil {
		t.Fatal("expected an error when an sstable reader isn't a real *sstable.Reader")
	}
}

// --- RestoreSnapshot() ---------------------------------------------------------

func TestRestoreSnapshot_ReplacesExistingData(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()
	e.Put([]byte("old-key"), []byte("old-value"))

	snap := encodeTestSnapshot(t, map[string]string{"new-key": "new-value"})
	if err := e.RestoreSnapshot(snap); err != nil {
		t.Fatal(err)
	}

	_, found, _ := e.Get([]byte("old-key"))
	if found {
		t.Fatal("expected old-key to be gone after RestoreSnapshot replaced all state")
	}
	val, found, _ := e.Get([]byte("new-key"))
	if !found || string(val) != "new-value" {
		t.Fatalf("new-key found=%v val=%q, want true new-value", found, val)
	}
}

func TestRestoreSnapshot_ClosedEngineReturnsError(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	e.Close()
	if err := e.RestoreSnapshot(nil); err == nil {
		t.Fatal("expected an error restoring a snapshot into a closed engine")
	}
}

func TestRestoreSnapshot_FlushInProgressReturnsError(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t), MemtableSizeThreshold: 10})
	defer e.Close()

	started := make(chan struct{})
	block := make(chan struct{})
	orig := newSSTableWriter
	newSSTableWriter = func(path string, opts sstable.Options) (sstableWriter, error) {
		real, err := sstable.NewWriter(path, opts)
		if err != nil {
			return nil, err
		}
		close(started)
		return &fakeSSTableWriter{block: block, real: real}, nil
	}

	if err := e.Put([]byte("k"), []byte("value-long-enough-to-cross-the-tiny-threshold")); err != nil {
		t.Fatal(err)
	}
	<-started
	newSSTableWriter = orig

	err := e.RestoreSnapshot(nil)
	if err == nil {
		t.Fatal("expected RestoreSnapshot to reject while a background flush is in progress")
	}

	close(block)
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreSnapshot_MalformedDataReturnsError(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()
	if err := e.RestoreSnapshot([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected an error restoring malformed snapshot data")
	}
}

func TestRestoreSnapshot_SSTableWriterErrorPropagates(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()

	orig := newSSTableWriter
	newSSTableWriter = func(path string, opts sstable.Options) (sstableWriter, error) {
		return nil, errors.New("newSSTableWriter: simulated failure")
	}
	defer func() { newSSTableWriter = orig }()

	err := e.RestoreSnapshot(encodeTestSnapshot(t, map[string]string{"k": "v"}))
	if err == nil {
		t.Fatal("expected an error when creating the snapshot sstable writer fails")
	}
	if !strings.Contains(err.Error(), "snapshot sstable writer") {
		t.Fatalf("error = %v, want it to mention the snapshot sstable writer", err)
	}
}

func TestRestoreSnapshot_WALOpenErrorPropagates(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()

	orig := openWAL
	callCount := 0
	openWAL = func(path string, opts wal.Options) (walHandle, error) {
		callCount++
		return nil, errors.New("openWAL: simulated failure")
	}
	defer func() { openWAL = orig }()

	err := e.RestoreSnapshot(encodeTestSnapshot(t, map[string]string{"k": "v"}))
	if err == nil {
		t.Fatal("expected an error when opening the fresh wal fails")
	}
	if !strings.Contains(err.Error(), "fresh wal") {
		t.Fatalf("error = %v, want it to mention the fresh wal", err)
	}
}

func TestRestoreSnapshot_MarkerWriteErrorPropagates(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()

	orig := renameFile
	renameFile = func(oldpath, newpath string) error {
		return errors.New("renameFile: simulated failure")
	}
	defer func() { renameFile = orig }()

	err := e.RestoreSnapshot(encodeTestSnapshot(t, map[string]string{"k": "v"}))
	if err == nil {
		t.Fatal("expected an error when committing the restore marker fails")
	}
	if !strings.Contains(err.Error(), "committing snapshot restore") {
		t.Fatalf("error = %v, want it to mention committing the snapshot restore", err)
	}
}

// TestRestoreSnapshot_PersistsAcrossReopen is the actual crash-safety
// claim: not just that RestoreSnapshot updates in-memory state
// correctly, but that a real Close() followed by a fresh Open() (a
// real restart, not a simulated one) discovers the RESTORED data, not
// whatever was there before, via the restore marker mechanism.
func TestRestoreSnapshot_SSTableAddErrorPropagates(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()

	orig := newSSTableWriter
	newSSTableWriter = func(path string, opts sstable.Options) (sstableWriter, error) {
		return &fakeSSTableWriter{failAdd: true}, nil
	}
	defer func() { newSSTableWriter = orig }()

	err := e.RestoreSnapshot(encodeTestSnapshot(t, map[string]string{"k": "v"}))
	if err == nil {
		t.Fatal("expected an error when adding a snapshot entry to the sstable writer fails")
	}
	if !strings.Contains(err.Error(), "writing snapshot entry") {
		t.Fatalf("error = %v, want it to mention writing the snapshot entry", err)
	}
}

func TestRestoreSnapshot_SSTableFinishErrorPropagates(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()

	orig := newSSTableWriter
	newSSTableWriter = func(path string, opts sstable.Options) (sstableWriter, error) {
		return &fakeSSTableWriter{failFinish: true}, nil
	}
	defer func() { newSSTableWriter = orig }()

	err := e.RestoreSnapshot(encodeTestSnapshot(t, map[string]string{"k": "v"}))
	if err == nil {
		t.Fatal("expected an error when finishing the snapshot sstable fails")
	}
	if !strings.Contains(err.Error(), "finishing snapshot sstable") {
		t.Fatalf("error = %v, want it to mention finishing the snapshot sstable", err)
	}
}

func TestRestoreSnapshot_ReopenSSTableForReadErrorPropagates(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()

	orig := openSSTableForRead
	openSSTableForRead = func(path string, cache *sstable.BlockCache) (sstableReader, error) {
		return nil, errors.New("openSSTableForRead: simulated failure")
	}
	defer func() { openSSTableForRead = orig }()

	err := e.RestoreSnapshot(encodeTestSnapshot(t, map[string]string{"k": "v"}))
	if err == nil {
		t.Fatal("expected an error when reopening the snapshot sstable for read fails")
	}
	if !strings.Contains(err.Error(), "reopening snapshot sstable") {
		t.Fatalf("error = %v, want it to mention reopening the snapshot sstable", err)
	}
}

func TestRestoreSnapshot_RemovesOldSSTableFilesAfterSuccess(t *testing.T) {
	// Exercises the post-commit cleanup loop specifically, which needs
	// at least one PRE-EXISTING, actually-flushed SSTable to iterate —
	// a fresh engine with only unflushed memtable data (as most other
	// tests here use) never populates e.sstables at all, so this loop
	// would otherwise go untested.
	e := mustOpen(t, Options{Dir: tempDir(t), MemtableSizeThreshold: 1})
	defer e.Close()
	if err := e.Put([]byte("old-flushed-key"), []byte("old-flushed-value")); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(e.sstables) == 0 {
		t.Fatal("precondition failed: expected at least one flushed sstable before RestoreSnapshot")
	}
	oldPaths := make([]string, len(e.sstables))
	for i, ss := range e.sstables {
		oldPaths[i] = ss.path
	}

	if err := e.RestoreSnapshot(encodeTestSnapshot(t, map[string]string{"new-key": "new-value"})); err != nil {
		t.Fatal(err)
	}
	for _, p := range oldPaths {
		if _, statErr := os.Stat(p); !os.IsNotExist(statErr) {
			t.Fatalf("old sstable %s still exists on disk after RestoreSnapshot, want it removed", p)
		}
	}
}

func TestDecodeSnapshotEntries_MalformedValueLengthPrefixIsError(t *testing.T) {
	var buf bytes.Buffer
	writeLenPrefixed(&buf, []byte("valid-key")) // key parses fine
	buf.Write([]byte{1, 2})                     // then a truncated (too-short) value length prefix

	if _, err := decodeSnapshotEntries(buf.Bytes()); err == nil {
		t.Fatal("expected an error when the value's length prefix is truncated")
	}
}

func TestReadLenPrefixed_DeclaredLengthExceedsAvailableIsError(t *testing.T) {
	var buf bytes.Buffer
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], 999999) // declares far more than follows
	buf.Write(lenBuf[:])
	buf.WriteString("short")

	if _, _, err := readLenPrefixed(buf.Bytes(), 0); err == nil {
		t.Fatal("expected an error when the declared length exceeds what's actually available")
	}
}

func TestRestoreSnapshot_PersistsAcrossReopen(t *testing.T) {
	dir := tempDir(t)
	e := mustOpen(t, Options{Dir: dir})
	e.Put([]byte("old-key"), []byte("old-value"))

	snap := encodeTestSnapshot(t, map[string]string{"restored-key": "restored-value"})
	if err := e.RestoreSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	_, found, _ := e2.Get([]byte("old-key"))
	if found {
		t.Fatal("expected old-key to be gone after reopening — the restore marker should have excluded it from discovery")
	}
	val, found, _ := e2.Get([]byte("restored-key"))
	if !found || string(val) != "restored-value" {
		t.Fatalf("restored-key found=%v val=%q, want true restored-value", found, val)
	}
}

// encodeTestSnapshot builds valid Snapshot-format bytes directly (the
// same length-prefixed encoding Snapshot itself produces) without
// needing a whole separate Engine to generate them from.
func encodeTestSnapshot(t *testing.T, kvs map[string]string) []byte {
	t.Helper()
	var buf []byte
	for k, v := range kvs {
		buf = append(buf, lenPrefixedBytes([]byte(k))...)
		buf = append(buf, lenPrefixedBytes([]byte(v))...)
	}
	return buf
}

func lenPrefixedBytes(b []byte) []byte {
	var out bytes.Buffer
	writeLenPrefixed(&out, b)
	return out.Bytes()
}
