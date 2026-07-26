package wal

import (
	"bufio"
	"fmt"
	"os"
	"sync"
)

// WAL is an append-only log file. It is safe for concurrent use.
//
// Durability model: Append (and AppendBatch) write into a buffered writer
// and then, if SyncOnWrite is enabled, flush the buffer to the OS and
// fsync the file descriptor before returning. Only after that fsync
// returns is a write considered durable — i.e. safe to acknowledge to a
// caller, and guaranteed to survive a process crash or power loss
// (barring disk hardware failure). Until fsync, the write may exist only
// in the OS page cache and would be lost on a crash, which is exactly the
// scenario Replay's torn-write handling is designed to tolerate.
type WAL struct {
	mu          sync.Mutex
	file        *os.File
	writer      *bufio.Writer
	path        string
	syncOnWrite bool
	closed      bool
	lastSeq     uint64
}

// Options configures a WAL on Open.
type Options struct {
	// SyncOnWrite, if true, fsyncs after every Append/AppendBatch call.
	// This is the safe default: it guarantees a completed call is durable.
	// Setting it false trades durability for throughput (writes are only
	// as durable as the OS page cache until an explicit Sync() call) —
	// useful for benchmarking the cost of fsync itself.
	SyncOnWrite bool
}

// Open opens (creating if necessary) the WAL file at path for appending.
// It does NOT replay existing contents; call Replay(path) separately to
// recover prior records before Open, since replay may need to truncate a
// torn tail — doing that on an already-open handle would race with it.
func Open(path string, opts Options) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	return &WAL{
		file:        f,
		writer:      bufio.NewWriter(f),
		path:        path,
		syncOnWrite: opts.SyncOnWrite,
	}, nil
}

// Append writes a single record. If opened with SyncOnWrite, the record is
// guaranteed durable when Append returns nil.
func (w *WAL) Append(rec Record) error {
	return w.AppendBatch([]Record{rec})
}

// AppendBatch writes multiple records with a single flush/fsync at the end
// (group commit), amortizing the fsync cost across the batch. This is the
// primitive the storage engine should use for buffered/pipelined writers.
func (w *WAL) AppendBatch(recs []Record) error {
	if len(recs) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("wal: append to closed log %s", w.path)
	}

	for _, rec := range recs {
		buf := encode(rec)
		if _, err := w.writer.Write(buf); err != nil {
			return fmt.Errorf("wal: write: %w", err)
		}
		if rec.SeqNum > w.lastSeq {
			w.lastSeq = rec.SeqNum
		}
	}

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if w.syncOnWrite {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("wal: fsync: %w", err)
		}
	}
	return nil
}

// Sync flushes buffered data and fsyncs the underlying file. Callers using
// SyncOnWrite: false should call this explicitly at whatever batching
// boundary they consider a durability point (e.g. before acknowledging a
// client write, or on a timer for group commit).
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	return w.file.Sync()
}

// LastSeq returns the highest sequence number appended so far.
func (w *WAL) LastSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastSeq
}

// Close flushes, fsyncs, and closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.writer.Flush(); err != nil {
		w.file.Close()
		return fmt.Errorf("wal: flush on close: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		w.file.Close()
		return fmt.Errorf("wal: fsync on close: %w", err)
	}
	return w.file.Close()
}

// Path returns the WAL's file path.
func (w *WAL) Path() string {
	return w.path
}
