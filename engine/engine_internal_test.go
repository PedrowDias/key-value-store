package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PedrowDias/key-value-store/sstable"
	"github.com/PedrowDias/key-value-store/wal"
)

// --- fakes for the DI seams --------------------------------------------------

type fakeWALHandle struct {
	failAppend bool
	failClose  bool
}

func (f *fakeWALHandle) Append(rec wal.Record) error {
	if f.failAppend {
		return errors.New("fakeWALHandle: simulated append failure")
	}
	return nil
}
func (f *fakeWALHandle) Close() error {
	if f.failClose {
		return errors.New("fakeWALHandle: simulated close failure")
	}
	return nil
}

type fakeSSTableWriter struct {
	failAdd    bool
	failFinish bool
}

func (f *fakeSSTableWriter) Add(key, value []byte, seq uint64, deleted bool) error {
	if f.failAdd {
		return errors.New("fakeSSTableWriter: simulated add failure")
	}
	return nil
}
func (f *fakeSSTableWriter) Finish() (*sstable.Meta, error) {
	if f.failFinish {
		return nil, errors.New("fakeSSTableWriter: simulated finish failure")
	}
	return &sstable.Meta{}, nil
}

type fakeSSTableReader struct {
	failClose bool
}

func (f *fakeSSTableReader) Get(key []byte) ([]byte, uint64, bool, bool, error) {
	return nil, 0, false, false, nil
}
func (f *fakeSSTableReader) Close() error {
	if f.failClose {
		return errors.New("fakeSSTableReader: simulated close failure")
	}
	return nil
}
func (f *fakeSSTableReader) MaxSeq() uint64 { return 0 }

func TestClose_WALCloseErrorPropagates(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	e.w = &fakeWALHandle{failClose: true}

	err := e.Close()
	if err == nil {
		t.Fatal("expected Close to propagate a WAL close error")
	}
	if !strings.Contains(err.Error(), "closing wal") {
		t.Fatalf("error = %v, want it to mention closing the wal", err)
	}
}

func TestClose_SSTableCloseErrorPropagates(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	e.sstables = append(e.sstables, &sstableEntry{path: "fake.sst", reader: &fakeSSTableReader{failClose: true}})

	err := e.Close()
	if err == nil {
		t.Fatal("expected Close to propagate an sstable close error")
	}
	if !strings.Contains(err.Error(), "closing sstable") {
		t.Fatalf("error = %v, want it to mention closing the sstable", err)
	}
}

// --- write()'s WAL append error branch ---------------------------------------

func TestWrite_WALAppendErrorPropagates(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()
	e.w = &fakeWALHandle{failAppend: true}

	err := e.Put([]byte("k"), []byte("v"))
	if err == nil {
		t.Fatal("expected Put to propagate a WAL append error")
	}
	if !strings.Contains(err.Error(), "wal append") {
		t.Fatalf("error = %v, want it to mention wal append", err)
	}
}

// --- Auto-flush trigger propagating a flush error, vs. manual Flush() -------

func TestAutoFlush_ErrorPropagatesThroughWrite(t *testing.T) {
	dir := tempDir(t)
	e := mustOpen(t, Options{Dir: dir, MemtableSizeThreshold: 10})

	// Pre-create the path the *next* flush will use as a directory, so
	// sstable creation fails the moment auto-flush fires — portable and
	// deterministic on both Linux and macOS, no permission tricks needed.
	if err := os.Mkdir(filepath.Join(dir, fmt.Sprintf(sstableFileNamePattern, e.nextFlushSeq)), 0755); err != nil {
		t.Fatal(err)
	}

	err := e.Put([]byte("k"), []byte("this-value-is-long-enough-to-exceed-the-tiny-threshold"))
	if err == nil {
		t.Fatal("expected the write to fail via the auto-flush error path")
	}
	if !strings.Contains(err.Error(), "auto-flush") {
		t.Fatalf("error = %v, want it to mention auto-flush", err)
	}
	e.Close()
}

func TestFlush_SSTableWriterCreationErrorPropagates(t *testing.T) {
	dir := tempDir(t)
	e := mustOpen(t, Options{Dir: dir})
	e.Put([]byte("k"), []byte("v"))

	if err := os.Mkdir(filepath.Join(dir, fmt.Sprintf(sstableFileNamePattern, e.nextFlushSeq)), 0755); err != nil {
		t.Fatal(err)
	}

	err := e.Flush()
	if err == nil {
		t.Fatal("expected Flush to fail when the target sstable path is already a directory")
	}
	if !strings.Contains(err.Error(), "creating sstable writer") {
		t.Fatalf("error = %v, want it to mention sstable writer creation", err)
	}
	e.Close()
}

// --- flushLocked's remaining branches, via the writer/reader/WAL fakes ------

func TestFlush_SSTableAddErrorPropagates(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()
	e.Put([]byte("k"), []byte("v"))

	orig := newSSTableWriter
	newSSTableWriter = func(path string, opts sstable.Options) (sstableWriter, error) {
		return &fakeSSTableWriter{failAdd: true}, nil
	}
	defer func() { newSSTableWriter = orig }()

	err := e.Flush()
	if err == nil {
		t.Fatal("expected Flush to propagate an sstable Add error")
	}
	if !strings.Contains(err.Error(), "writing sstable entry") {
		t.Fatalf("error = %v, want it to mention writing the sstable entry", err)
	}
}

func TestFlush_SSTableFinishErrorPropagates(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()
	e.Put([]byte("k"), []byte("v"))

	orig := newSSTableWriter
	newSSTableWriter = func(path string, opts sstable.Options) (sstableWriter, error) {
		return &fakeSSTableWriter{failFinish: true}, nil
	}
	defer func() { newSSTableWriter = orig }()

	err := e.Flush()
	if err == nil {
		t.Fatal("expected Flush to propagate an sstable Finish error")
	}
	if !strings.Contains(err.Error(), "finishing sstable") {
		t.Fatalf("error = %v, want it to mention finishing the sstable", err)
	}
}

func TestFlush_ReopenAfterFinishErrorPropagates(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()
	e.Put([]byte("k"), []byte("v"))

	// Let the writer succeed normally, but fail the "reopen for reading"
	// step immediately afterward — otherwise unreachable, since a file
	// Finish() just wrote successfully essentially always reopens fine.
	orig := openSSTableForRead
	calls := 0
	openSSTableForRead = func(path string) (sstableReader, error) {
		calls++
		return nil, errors.New("openSSTableForRead: simulated failure")
	}
	defer func() { openSSTableForRead = orig }()

	err := e.Flush()
	if err == nil {
		t.Fatal("expected Flush to propagate a reopen-after-finish error")
	}
	if !strings.Contains(err.Error(), "reopening flushed sstable") {
		t.Fatalf("error = %v, want it to mention reopening the flushed sstable", err)
	}
	if calls == 0 {
		t.Fatal("expected openSSTableForRead to have been called")
	}
}

func TestFlush_WALCloseDuringRotationErrorPropagates(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	e.Put([]byte("k"), []byte("v"))
	e.w = &fakeWALHandle{failClose: true}

	err := e.Flush()
	if err == nil {
		t.Fatal("expected Flush to propagate a WAL close-for-rotation error")
	}
	if !strings.Contains(err.Error(), "closing wal for rotation") {
		t.Fatalf("error = %v, want it to mention closing the wal for rotation", err)
	}
}

func TestFlush_RemoveOldWALErrorPropagates(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()
	e.Put([]byte("k"), []byte("v"))

	orig := removeFile
	removeFile = func(path string) error { return errors.New("removeFile: simulated failure") }
	defer func() { removeFile = orig }()

	err := e.Flush()
	if err == nil {
		t.Fatal("expected Flush to propagate a wal removal error")
	}
	if !strings.Contains(err.Error(), "removing old wal") {
		t.Fatalf("error = %v, want it to mention removing the old wal", err)
	}
}

func TestFlush_ReopenWALAfterRotationErrorPropagates(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t)})
	defer e.Close()
	e.Put([]byte("k"), []byte("v"))

	orig := openWAL
	openWAL = func(path string, opts wal.Options) (walHandle, error) {
		return nil, errors.New("openWAL: simulated failure")
	}
	defer func() { openWAL = orig }()

	err := e.Flush()
	if err == nil {
		t.Fatal("expected Flush to propagate a wal reopen-after-rotation error")
	}
	if !strings.Contains(err.Error(), "reopening wal after rotation") {
		t.Fatalf("error = %v, want it to mention reopening the wal", err)
	}
}

func TestOpen_WALOpenErrorPropagates(t *testing.T) {
	orig := openWAL
	openWAL = func(path string, opts wal.Options) (walHandle, error) {
		return nil, errors.New("openWAL: simulated failure")
	}
	defer func() { openWAL = orig }()

	_, err := Open(Options{Dir: tempDir(t)})
	if err == nil {
		t.Fatal("expected Open to propagate a wal.Open error")
	}
	if !strings.Contains(err.Error(), "opening wal") {
		t.Fatalf("error = %v, want it to mention opening the wal", err)
	}
}

// --- discoverSSTables, direct and closeAll with real entries ---------------

func TestDiscoverSSTables_ReadDirErrorOnNonexistentPath(t *testing.T) {
	_, _, _, err := discoverSSTables(filepath.Join(t.TempDir(), "does-not-exist"))
	if err == nil {
		t.Fatal("expected an error listing a nonexistent directory")
	}
}

func TestOpen_CorruptedOlderSSTable_ClosesNewerSuccessfully(t *testing.T) {
	// Two flushed sstables; corrupt the OLDER one (000000.sst) so that by
	// the time discoverSSTables (iterating newest-first) hits the error,
	// it has already successfully opened the newer one — meaning closeAll
	// actually has a real, open reader to close, exercising its loop body.
	dir := tempDir(t)
	e := mustOpen(t, Options{Dir: dir})
	e.Put([]byte("a"), []byte("va"))
	e.Flush()
	e.Put([]byte("b"), []byte("vb"))
	e.Flush()
	e.Close()

	f, err := os.OpenFile(filepath.Join(dir, "000000.sst"), os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := f.Stat()
	f.WriteAt([]byte{0, 0, 0, 0, 0, 0, 0, 0}, info.Size()-8)
	f.Close()

	_, err = Open(Options{Dir: dir})
	if err == nil {
		t.Fatal("expected Open to fail given a corrupted older sstable")
	}
}

// --- recovery covering the WAL-delete-record replay branch ------------------

func TestRecovery_DeleteRecordInWALIsReplayed(t *testing.T) {
	dir := tempDir(t)
	e := mustOpen(t, Options{Dir: dir})
	e.Put([]byte("k"), []byte("v"))
	e.Delete([]byte("k")) // stays unflushed: only in the WAL
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2 := mustOpen(t, Options{Dir: dir})
	defer e2.Close()
	_, found, err := e2.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected the replayed delete record to leave the key not found")
	}
}
