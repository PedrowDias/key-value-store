package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PedrowDias/key-value-store/sstable"
	"github.com/PedrowDias/key-value-store/wal"
)

// --- fakes for the DI seams --------------------------------------------------

type fakeWALHandle struct {
	failAppend bool
	failClose  bool
	path       string
}

func (f *fakeWALHandle) Append(rec wal.Record) error {
	if f.failAppend {
		return errors.New("fakeWALHandle: simulated append failure")
	}
	return nil
}
func (f *fakeWALHandle) AppendBatch(recs []wal.Record) error {
	if f.failAppend {
		return errors.New("fakeWALHandle: simulated append failure")
	}
	return nil
}
func (f *fakeWALHandle) Path() string { return f.path }
func (f *fakeWALHandle) Close() error {
	if f.failClose {
		return errors.New("fakeWALHandle: simulated close failure")
	}
	return nil
}

type fakeSSTableWriter struct {
	failAdd    bool
	failFinish bool
	// block, if non-nil, is waited on inside Finish before proceeding —
	// lets a test hold a background flush open for exactly as long as
	// it needs, deterministically, rather than racing against how fast
	// a real flush actually completes.
	block <-chan struct{}
	// real, if non-nil, is delegated to for both Add and (after
	// blocking, assuming failFinish is false) Finish — lets a test hold
	// a flush open at a controlled point while still producing a
	// genuinely valid SSTable file on disk (openSSTableForRead, called
	// right after a successful flush, needs a real file to open).
	real sstableWriter
}

func (f *fakeSSTableWriter) Add(key, value []byte, seq uint64, deleted bool) error {
	if f.failAdd {
		return errors.New("fakeSSTableWriter: simulated add failure")
	}
	if f.real != nil {
		return f.real.Add(key, value, seq, deleted)
	}
	return nil
}
func (f *fakeSSTableWriter) Finish() (*sstable.Meta, error) {
	if f.block != nil {
		<-f.block
	}
	if f.failFinish {
		return nil, errors.New("fakeSSTableWriter: simulated finish failure")
	}
	if f.real != nil {
		return f.real.Finish()
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
	// sstable creation fails once the background flush actually runs.
	if err := os.Mkdir(filepath.Join(dir, fmt.Sprintf(sstableFileNamePattern, e.nextFlushSeq)), 0755); err != nil {
		t.Fatal(err)
	}

	// This write triggers the flush but — since it's now a background
	// operation — succeeds itself; the failure surfaces later, not here.
	if err := e.Put([]byte("k"), []byte("this-value-is-long-enough-to-exceed-the-tiny-threshold")); err != nil {
		t.Fatalf("the triggering write itself should succeed (the flush failure is asynchronous): %v", err)
	}

	// Flush waits for any in-progress background flush before doing
	// anything else, and returns its sticky error immediately if one
	// occurred — the natural way to synchronously observe an async
	// failure in a test.
	err := e.Flush()
	if err == nil {
		t.Fatal("expected the background flush to have failed")
	}
	if !strings.Contains(err.Error(), "background flush") {
		t.Fatalf("error = %v, want it to mention the background flush", err)
	}

	// A subsequent write must also refuse, now that the engine has a
	// sticky flush error.
	if err := e.Put([]byte("k2"), []byte("v")); err == nil {
		t.Fatal("expected a subsequent write to refuse due to the sticky flush error")
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
	openSSTableForRead = func(path string, cache *sstable.BlockCache) (sstableReader, error) {
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
	e.w = &fakeWALHandle{failClose: true, path: "fake.wal"}

	err := e.Flush()
	if err == nil {
		t.Fatal("expected Flush to propagate a WAL close error")
	}
	if !strings.Contains(err.Error(), "closing old wal") {
		t.Fatalf("error = %v, want it to mention closing the old wal", err)
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

func TestFlush_OpenNewWALForBackgroundFlushErrorPropagates(t *testing.T) {
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
		t.Fatal("expected Flush to propagate a new-WAL-open error")
	}
	if !strings.Contains(err.Error(), "opening new wal for background flush") {
		t.Fatalf("error = %v, want it to mention opening the new wal", err)
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

func TestOpen_RestoreMarkerErrorPropagates(t *testing.T) {
	dir := tempDir(t)
	// Make the marker's own path a directory rather than a file, so
	// reading it fails with something other than "not exist."
	if err := os.Mkdir(filepath.Join(dir, restoreMarkerFileName), 0755); err != nil {
		t.Fatal(err)
	}
	_, err := Open(Options{Dir: dir})
	if err == nil {
		t.Fatal("expected Open to propagate a restore marker read error")
	}
	if !strings.Contains(err.Error(), "restore marker") {
		t.Fatalf("error = %v, want it to mention the restore marker", err)
	}
}

// --- discoverSSTables, direct and closeAll with real entries ---------------

func TestDiscoverSSTables_ReadDirErrorOnNonexistentPath(t *testing.T) {
	_, _, _, err := discoverSSTables(filepath.Join(t.TempDir(), "does-not-exist"), sstable.NewBlockCache(defaultBlockCacheSize), 0)
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

// --- Async flush: reads and shutdown while a flush is genuinely in progress -

func TestGet_FindsValueInImmutableMemtableDuringInFlightFlush(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t), MemtableSizeThreshold: 10})

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

	if err := e.Put([]byte("frozen-key"), []byte("frozen-value-long-enough-to-cross-the-tiny-threshold")); err != nil {
		t.Fatal(err)
	}
	<-started // the flush goroutine has now read newSSTableWriter and captured its own local reference — safe to restore the global immediately
	newSSTableWriter = orig

	val, found, err := e.Get([]byte("frozen-key"))
	if err != nil || !found || string(val) != "frozen-value-long-enough-to-cross-the-tiny-threshold" {
		t.Fatalf("Get(frozen-key) while a flush is in progress = %q found=%v err=%v", val, found, err)
	}

	close(block)
	if err := e.Flush(); err != nil { // wait for the (already-started) flush to fully finish
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGet_FindsTombstoneInImmutableMemtableDuringInFlightFlush(t *testing.T) {
	// A threshold big enough that the small put+delete below don't
	// themselves cross it (which would deadlock: that flush would then
	// need `block` to be closed before the *next* write — Delete — could
	// even proceed, but this test doesn't close it until later). Only
	// the padding write is meant to cross it.
	e := mustOpen(t, Options{Dir: tempDir(t), MemtableSizeThreshold: 1000})

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

	if err := e.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := e.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	// Crosses the threshold and freezes a memtable containing BOTH the
	// put and the delete tombstone for "k" above — the tombstone itself
	// is what Get must find inside the frozen (not the fresh active)
	// memtable.
	if err := e.Put([]byte("padding"), bytes.Repeat([]byte("x"), 2000)); err != nil {
		t.Fatal(err)
	}
	<-started
	newSSTableWriter = orig

	_, found, err := e.Get([]byte("k"))
	if err != nil || found {
		t.Fatalf("Get(k) found=%v err=%v, want false nil (tombstone lives in the frozen memtable)", found, err)
	}

	close(block)
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestApplyBatch_ClosedWhileWaitingForFlushReturnsError(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t), MemtableSizeThreshold: 10})

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

	// A second write now blocks waiting for the in-progress flush.
	putErrCh := make(chan error, 1)
	go func() { putErrCh <- e.Put([]byte("k2"), []byte("v2")) }()
	time.Sleep(50 * time.Millisecond) // give it time to actually reach the wait

	// Close marks the engine closed immediately, then itself waits for
	// the same flush to finish before it can return.
	closeErrCh := make(chan error, 1)
	go func() { closeErrCh <- e.Close() }()
	time.Sleep(50 * time.Millisecond) // give Close time to set e.closed and start its own wait

	close(block) // let the blocked flush actually finish now

	select {
	case err := <-putErrCh:
		if err == nil {
			t.Fatal("expected the waiting Put to return an error once the engine closed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the waiting Put did not return after Close")
	}
	if err := <-closeErrCh; err != nil {
		t.Fatalf("Close itself returned an unexpected error: %v", err)
	}
}

// --- discoverWALs, direct -----------------------------------------------------

func TestDiscoverWALs_ReadDirErrorOnNonexistentPath(t *testing.T) {
	_, _, err := discoverWALs(filepath.Join(t.TempDir(), "does-not-exist"), 0)
	if err == nil {
		t.Fatal("expected an error listing a nonexistent directory")
	}
}

func TestDiscoverWALs_IgnoresFilenameNotMatchingPatternExactly(t *testing.T) {
	dir := t.TempDir()
	// "000001.wal.bak" would otherwise Sscanf-match the "%06d.wal"
	// prefix; must be filtered out, the same way an analogous stray
	// SSTable-like filename already is.
	if err := os.WriteFile(filepath.Join(dir, "000001.wal.bak"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	paths, nextWALSeq, err := discoverWALs(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 0 {
		t.Fatalf("paths = %v, want empty (the .bak file must be ignored)", paths)
	}
	if nextWALSeq != 0 {
		t.Fatalf("nextWALSeq = %d, want 0", nextWALSeq)
	}
}

func TestOpen_RemovingOldWALAfterRelogErrorPropagates(t *testing.T) {
	dir := tempDir(t)
	e := mustOpen(t, Options{Dir: dir})
	e.Put([]byte("k"), []byte("v")) // leaves something for recovery to replay and re-log
	e.Close()

	orig := removeFile
	removeFile = func(path string) error { return errors.New("removeFile: simulated failure") }
	defer func() { removeFile = orig }()

	_, err := Open(Options{Dir: dir})
	if err == nil {
		t.Fatal("expected Open to propagate an old-wal removal error")
	}
	if !strings.Contains(err.Error(), "removing old wal") {
		t.Fatalf("error = %v, want it to mention removing the old wal", err)
	}
}

func TestApplyBatch_StartingBackgroundFlushErrorPropagates(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t), MemtableSizeThreshold: 10})
	defer e.Close()

	orig := openWAL
	openWAL = func(path string, opts wal.Options) (walHandle, error) {
		return nil, errors.New("openWAL: simulated failure")
	}
	defer func() { openWAL = orig }()

	err := e.Put([]byte("k"), []byte("value-long-enough-to-cross-the-tiny-threshold"))
	if err == nil {
		t.Fatal("expected the write to propagate a background-flush-start error")
	}
	if !strings.Contains(err.Error(), "starting background flush") {
		t.Fatalf("error = %v, want it to mention starting the background flush", err)
	}
}

func TestFlush_ClosedWhileWaitingForPriorFlushReturnsError(t *testing.T) {
	e := mustOpen(t, Options{Dir: tempDir(t), MemtableSizeThreshold: 10})

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

	// Flush, called now, must wait for the already-in-progress flush.
	flushErrCh := make(chan error, 1)
	go func() { flushErrCh <- e.Flush() }()
	time.Sleep(50 * time.Millisecond)

	closeErrCh := make(chan error, 1)
	go func() { closeErrCh <- e.Close() }()
	time.Sleep(50 * time.Millisecond)

	close(block)

	select {
	case err := <-flushErrCh:
		if err == nil {
			t.Fatal("expected the waiting Flush to return an error once the engine closed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the waiting Flush did not return after Close")
	}
	if err := <-closeErrCh; err != nil {
		t.Fatalf("Close itself returned an unexpected error: %v", err)
	}
}
