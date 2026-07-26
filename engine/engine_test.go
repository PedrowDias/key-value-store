package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/PedrowDias/key-value-store/wal"
)

func tempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func mustOpen(t *testing.T, opts Options) *Engine {
	t.Helper()
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return e
}

// --- Basic correctness -------------------------------------------------------

func TestPutGet_Basic(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()

	if err := e.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	val, found, err := e.Get([]byte("k1"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(val) != "v1" {
		t.Fatalf("Get(k1) = %q, found=%v, want v1, true", val, found)
	}
}

func TestGet_MissingKey(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()
	_, found, err := e.Get([]byte("nope"))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected not found")
	}
}

func TestPut_OverwriteUpdatesValue(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()
	e.Put([]byte("k"), []byte("v1"))
	e.Put([]byte("k"), []byte("v2"))
	val, found, err := e.Get([]byte("k"))
	if err != nil || !found || string(val) != "v2" {
		t.Fatalf("Get(k) = %q found=%v err=%v, want v2 true nil", val, found, err)
	}
}

func TestDelete_ThenGet_NotFound(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()
	e.Put([]byte("k"), []byte("v"))
	if err := e.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	_, found, err := e.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected not found after delete")
	}
}

func TestDelete_NeverWrittenKey(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()
	if err := e.Delete([]byte("ghost")); err != nil {
		t.Fatal(err)
	}
	_, found, err := e.Get([]byte("ghost"))
	if err != nil || found {
		t.Fatalf("found=%v err=%v, want false nil", found, err)
	}
}

// --- Flush and tombstone/value shadowing across SSTables -------------------

func TestFlush_MovesDataToSSTable(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()

	e.Put([]byte("k"), []byte("v"))
	if got := e.Stats().NumSSTables; got != 0 {
		t.Fatalf("NumSSTables before flush = %d, want 0", got)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	stats := e.Stats()
	if stats.NumSSTables != 1 {
		t.Fatalf("NumSSTables after flush = %d, want 1", stats.NumSSTables)
	}
	if stats.MemtableEntries != 0 {
		t.Fatalf("MemtableEntries after flush = %d, want 0", stats.MemtableEntries)
	}

	// Still readable, now falling through to the SSTable.
	val, found, err := e.Get([]byte("k"))
	if err != nil || !found || string(val) != "v" {
		t.Fatalf("Get(k) after flush = %q found=%v err=%v", val, found, err)
	}
}

func TestFlush_EmptyMemtableIsNoop(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := e.Stats().NumSSTables; got != 0 {
		t.Fatalf("NumSSTables = %d, want 0 (flushing an empty memtable must not create a file)", got)
	}
}

func TestGet_NewerSSTableValueShadowsOlder(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()

	e.Put([]byte("k"), []byte("v1"))
	e.Flush()
	e.Put([]byte("k"), []byte("v2"))
	e.Flush()

	val, found, err := e.Get([]byte("k"))
	if err != nil || !found || string(val) != "v2" {
		t.Fatalf("Get(k) = %q found=%v err=%v, want v2 true nil (newest sstable must win)", val, found, err)
	}
}

func TestGet_NewerSSTableTombstoneShadowsOlderValue(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()

	e.Put([]byte("k"), []byte("v1"))
	e.Flush()
	e.Delete([]byte("k"))
	e.Flush()

	_, found, err := e.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected not found: a newer sstable's tombstone must shadow an older sstable's value")
	}
}

func TestAutoFlush_TriggersOnSizeThreshold(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t), MemtableSizeThreshold: 200})
	defer e.Close()

	for i := 0; i < 50; i++ {
		if err := e.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte("some-reasonably-sized-value")); err != nil {
			t.Fatal(err)
		}
	}
	if got := e.Stats().NumSSTables; got == 0 {
		t.Fatal("expected at least one auto-triggered flush given the tiny threshold")
	}

	// Everything written should still be readable regardless of which
	// side of a flush boundary it landed on.
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%03d", i)
		val, found, err := e.Get([]byte(key))
		if err != nil || !found || string(val) != "some-reasonably-sized-value" {
			t.Fatalf("Get(%s) = %q found=%v err=%v", key, val, found, err)
		}
	}
}

// --- Recovery across restarts ------------------------------------------------

func TestRecovery_UnflushedWritesSurviveRestart(t *testing.T) {
	dir := tempDir(t)
	e := mustOpen(t, Options{Dir: dir})
	e.Put([]byte("a"), []byte("va"))
	e.Put([]byte("b"), []byte("vb"))
	// No explicit Flush: these writes only exist in the WAL + memtable.
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2 := mustOpen(t, Options{Dir: dir})
	defer e2.Close()
	for _, kv := range [][2]string{{"a", "va"}, {"b", "vb"}} {
		val, found, err := e2.Get([]byte(kv[0]))
		if err != nil || !found || string(val) != kv[1] {
			t.Fatalf("Get(%s) after restart = %q found=%v err=%v", kv[0], val, found, err)
		}
	}
}

func TestRecovery_FlushedAndUnflushedDataBothSurvive(t *testing.T) {
	dir := tempDir(t)
	e := mustOpen(t, Options{Dir: dir})
	e.Put([]byte("flushed"), []byte("v1"))
	e.Flush()
	e.Put([]byte("unflushed"), []byte("v2"))
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2 := mustOpen(t, Options{Dir: dir})
	defer e2.Close()

	val, found, err := e2.Get([]byte("flushed"))
	if err != nil || !found || string(val) != "v1" {
		t.Fatalf("Get(flushed) = %q found=%v err=%v", val, found, err)
	}
	val, found, err = e2.Get([]byte("unflushed"))
	if err != nil || !found || string(val) != "v2" {
		t.Fatalf("Get(unflushed) = %q found=%v err=%v", val, found, err)
	}
	if got := e2.Stats().NumSSTables; got != 1 {
		t.Fatalf("NumSSTables after restart = %d, want 1", got)
	}
}

func TestRecovery_SequenceNumbersContinueAcrossRestart(t *testing.T) {
	dir := tempDir(t)
	e := mustOpen(t, Options{Dir: dir})
	e.Put([]byte("k"), []byte("v1"))
	e.Flush()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2 := mustOpen(t, Options{Dir: dir})
	// Overwrite the same key post-restart; if sequence numbers reset to 0
	// and somehow collided/misordered with the flushed table's recorded
	// seq, this could resolve incorrectly. Correctness here is the signal.
	if err := e2.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	val, found, err := e2.Get([]byte("k"))
	if err != nil || !found || string(val) != "v2" {
		t.Fatalf("Get(k) = %q found=%v err=%v, want v2 true nil", val, found, err)
	}
	e2.Flush()
	val, found, err = e2.Get([]byte("k"))
	if err != nil || !found || string(val) != "v2" {
		t.Fatalf("after second flush: Get(k) = %q found=%v err=%v, want v2 true nil (newer flush must still win)", val, found, err)
	}
	e2.Close()
}

func TestRecovery_MultipleRestartCycles(t *testing.T) {
	dir := tempDir(t)
	for i := 0; i < 5; i++ {
		e := mustOpen(t, Options{Dir: dir})
		key := fmt.Sprintf("k%d", i)
		if err := e.Put([]byte(key), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
		if i%2 == 0 {
			e.Flush()
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
	}
	e := mustOpen(t, Options{Dir: dir})
	defer e.Close()
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("k%d", i)
		want := fmt.Sprintf("v%d", i)
		val, found, err := e.Get([]byte(key))
		if err != nil || !found || string(val) != want {
			t.Fatalf("Get(%s) = %q found=%v err=%v, want %s true nil", key, val, found, err, want)
		}
	}
}

func TestRecovery_IgnoresUnrelatedFilesInDirectory(t *testing.T) {
	dir := tempDir(t)
	e := mustOpen(t, Options{Dir: dir})
	e.Put([]byte("k"), []byte("v"))
	e.Flush()
	e.Close()

	// Sprinkle in files that must not be mistaken for sstables or crash
	// the directory scan.
	os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("hi"), 0644)
	os.WriteFile(filepath.Join(dir, "000001.sst.bak"), []byte("junk"), 0644)
	os.Mkdir(filepath.Join(dir, "subdir"), 0755)

	e2 := mustOpen(t, Options{Dir: dir})
	defer e2.Close()
	val, found, err := e2.Get([]byte("k"))
	if err != nil || !found || string(val) != "v" {
		t.Fatalf("Get(k) = %q found=%v err=%v", val, found, err)
	}
	if got := e2.Stats().NumSSTables; got != 1 {
		t.Fatalf("NumSSTables = %d, want 1 (stray files must be ignored)", got)
	}
}

// --- Error paths --------------------------------------------------------------

func TestOpen_MkdirAllErrorPropagates(t *testing.T) {
	// Create a plain file, then try to Open an engine "inside" it as if
	// it were a directory: MkdirAll fails identically and portably on
	// Linux and macOS.
	dir := tempDir(t)
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(Options{Dir: filepath.Join(blocker, "sub")})
	if err == nil {
		t.Fatal("expected an error when Dir's parent path is a regular file")
	}
}

func TestOpen_CorruptedExistingSSTablePropagatesError(t *testing.T) {
	dir := tempDir(t)
	e := mustOpen(t, Options{Dir: dir})
	e.Put([]byte("k"), []byte("v"))
	e.Flush()
	e.Close()

	// Corrupt the flushed sstable's magic number.
	sstPath := filepath.Join(dir, "000000.sst")
	f, err := os.OpenFile(sstPath, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := f.Stat()
	f.WriteAt([]byte{0, 0, 0, 0, 0, 0, 0, 0}, info.Size()-8)
	f.Close()

	_, err = Open(Options{Dir: dir})
	if err == nil {
		t.Fatal("expected Open to fail given a corrupted existing sstable")
	}
}

func TestOpen_CorruptedWALPropagatesError(t *testing.T) {
	dir := tempDir(t)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	// wal.log as a directory instead of a file: wal.Replay's os.OpenFile
	// will fail with a non-IsNotExist error.
	if err := os.Mkdir(filepath.Join(dir, walFileName), 0755); err != nil {
		t.Fatal(err)
	}
	_, err := Open(Options{Dir: dir})
	if err == nil {
		t.Fatal("expected Open to propagate a wal.Replay error")
	}
}

func TestWrite_OnClosedEngine(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	e.Close()
	if err := e.Put([]byte("k"), []byte("v")); err == nil {
		t.Fatal("expected an error writing to a closed engine")
	}
	if err := e.Delete([]byte("k")); err == nil {
		t.Fatal("expected an error deleting on a closed engine")
	}
}

func TestGet_OnClosedEngine(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	e.Close()
	if _, _, err := e.Get([]byte("k")); err == nil {
		t.Fatal("expected an error reading a closed engine")
	}
}

func TestFlush_OnClosedEngine(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	e.Close()
	if err := e.Flush(); err == nil {
		t.Fatal("expected an error flushing a closed engine")
	}
}

func TestClose_Idempotent(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("second Close() should be a no-op, got: %v", err)
	}
}

func TestGet_PropagatesSSTableReadError(t *testing.T) {
	dir := tempDir(t)
	e := mustOpen(t, Options{Dir: dir})
	e.Put([]byte("a"), []byte("value-long-enough-to-flip-a-byte-in"))
	e.Flush()

	// Corrupt the data block of the (now-closed-and-reopened-for-reading)
	// sstable, which the engine still has an open Reader for. We corrupt
	// the file on disk directly; the engine's Reader will re-read it from
	// disk on the next Get since it doesn't cache decoded blocks.
	sstPath := filepath.Join(dir, "000000.sst")
	f, err := os.OpenFile(sstPath, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteAt([]byte{0xFF}, 2)
	f.Close()

	_, _, err = e.Get([]byte("a"))
	if err == nil {
		t.Fatal("expected Get to propagate a corrupted-sstable read error")
	}
	e.Close()
}

// --- WAL/SSTable wiring sanity checks (using the real wal package types) ----

func TestEngine_DeleteRecordTypeUsedCorrectly(t *testing.T) {
	// Sanity check that write() picks wal.RecordDelete vs wal.RecordPut
	// correctly by inspecting what actually lands in the WAL via Replay.
	dir := tempDir(t)
	e := mustOpen(t, Options{Dir: dir})
	e.Put([]byte("a"), []byte("va"))
	e.Delete([]byte("b"))
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	records, _, err := wal.Replay(filepath.Join(dir, walFileName))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}
	if records[0].Type != wal.RecordPut {
		t.Fatalf("records[0].Type = %v, want RecordPut", records[0].Type)
	}
	if records[1].Type != wal.RecordDelete {
		t.Fatalf("records[1].Type = %v, want RecordDelete", records[1].Type)
	}
}
