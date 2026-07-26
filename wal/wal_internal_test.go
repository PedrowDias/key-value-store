package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeWALFile is a minimal in-memory walFile that can be configured to
// fail Write after a chosen number of successful calls, and/or fail Sync
// or Close outright — the same technique used in sstable's fakeFile, for
// the same reason: deterministic, portable coverage of error-handling
// branches that are impractical to trigger via real OS-level faults
// (especially running as root, which bypasses most permission-based
// fault injection).
type fakeWALFile struct {
	buf             bytes.Buffer
	writeCalls      int
	failAfterWrites int // -1 = never fail on Write
	failSync        bool
	failClose       bool
}

func (f *fakeWALFile) Write(p []byte) (int, error) {
	f.writeCalls++
	if f.failAfterWrites >= 0 && f.writeCalls > f.failAfterWrites {
		return 0, errors.New("fakeWALFile: simulated write failure")
	}
	return f.buf.Write(p)
}

func (f *fakeWALFile) Sync() error {
	if f.failSync {
		return errors.New("fakeWALFile: simulated sync failure")
	}
	return nil
}

func (f *fakeWALFile) Close() error {
	if f.failClose {
		return errors.New("fakeWALFile: simulated close failure")
	}
	return nil
}

func newWALWithFake(fake *fakeWALFile, bufSize int, syncOnWrite bool) *WAL {
	return &WAL{
		file:        fake,
		writer:      bufio.NewWriterSize(fake, bufSize),
		path:        "fake-path",
		syncOnWrite: syncOnWrite,
	}
}

func TestOpen_CreateErrorPropagates(t *testing.T) {
	// A directory can't be opened for read/write as a regular file — this
	// fails identically and portably on Linux and macOS.
	dir := t.TempDir()
	_, err := Open(dir, Options{})
	if err == nil {
		t.Fatal("expected an error opening a WAL at a path that is a directory")
	}
}

func TestAppendBatch_WriteErrorPropagates(t *testing.T) {
	// Small buffer forces the write straight through to the fake.
	fake := &fakeWALFile{failAfterWrites: 0}
	w := newWALWithFake(fake, 1, true)

	err := w.Append(Record{SeqNum: 1, Type: RecordPut, Key: []byte("k"), Value: []byte("v")})
	if err == nil {
		t.Fatal("expected Append to propagate the underlying write error")
	}
	if !strings.Contains(err.Error(), "write") {
		t.Fatalf("error = %v, want it to mention the write failure", err)
	}
}

func TestAppendBatch_FlushErrorPropagates(t *testing.T) {
	// Large buffer means the small test record stays buffered; only the
	// explicit Flush() call reaches the fake, isolating this branch from
	// the direct-write-error one above.
	fake := &fakeWALFile{failAfterWrites: 0}
	w := newWALWithFake(fake, 4096, true)

	err := w.Append(Record{SeqNum: 1, Type: RecordPut, Key: []byte("k"), Value: []byte("v")})
	if err == nil {
		t.Fatal("expected Append to propagate a Flush error")
	}
	if !strings.Contains(err.Error(), "flush") {
		t.Fatalf("error = %v, want it to mention flush", err)
	}
}

func TestAppendBatch_FsyncErrorPropagates(t *testing.T) {
	fake := &fakeWALFile{failAfterWrites: -1, failSync: true}
	w := newWALWithFake(fake, 4096, true) // SyncOnWrite: true so fsync is attempted

	err := w.Append(Record{SeqNum: 1, Type: RecordPut, Key: []byte("k"), Value: []byte("v")})
	if err == nil {
		t.Fatal("expected Append to propagate an fsync error")
	}
	if !strings.Contains(err.Error(), "fsync") {
		t.Fatalf("error = %v, want it to mention fsync", err)
	}
}

func TestAppendBatch_EmptyBatchIsNoop(t *testing.T) {
	fake := &fakeWALFile{failAfterWrites: 0} // would fail immediately if it wrote anything
	w := newWALWithFake(fake, 4096, true)
	if err := w.AppendBatch(nil); err != nil {
		t.Fatalf("AppendBatch(nil) should be a no-op, got: %v", err)
	}
	if fake.writeCalls != 0 {
		t.Fatalf("expected no writes for an empty batch, got %d", fake.writeCalls)
	}
}

func TestSync_FlushErrorPropagates(t *testing.T) {
	fake := &fakeWALFile{failAfterWrites: 0}
	w := newWALWithFake(fake, 4096, false)
	// Write directly into the buffered writer (bypassing Append, which
	// always calls Flush itself) so there's genuinely unflushed data for
	// Sync's own Flush call to fail on.
	w.writer.WriteString("pending-unflushed-bytes")

	err := w.Sync()
	if err == nil {
		t.Fatal("expected Sync to propagate a Flush error")
	}
	if !strings.Contains(err.Error(), "flush") {
		t.Fatalf("error = %v, want it to mention flush", err)
	}
}

func TestSync_OnClosedWALIsNoop(t *testing.T) {
	fake := &fakeWALFile{}
	w := newWALWithFake(fake, 4096, false)
	w.closed = true
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync on a closed WAL should be a no-op, got: %v", err)
	}
}

func TestClose_FlushErrorPropagates(t *testing.T) {
	fake := &fakeWALFile{failAfterWrites: 0}
	w := newWALWithFake(fake, 4096, false)
	// Buffer something (stays in bufio's buffer since it's small and
	// under the 4096 threshold), then Close's own Flush call is where it
	// fails.
	w.writer.WriteString("pending-unflushed-bytes")

	err := w.Close()
	if err == nil {
		t.Fatal("expected Close to propagate a Flush error")
	}
	if !strings.Contains(err.Error(), "flush on close") {
		t.Fatalf("error = %v, want it to mention flush on close", err)
	}
}

func TestClose_FsyncErrorPropagates(t *testing.T) {
	fake := &fakeWALFile{failAfterWrites: -1, failSync: true}
	w := newWALWithFake(fake, 4096, false)
	err := w.Close()
	if err == nil {
		t.Fatal("expected Close to propagate an fsync error")
	}
	if !strings.Contains(err.Error(), "fsync on close") {
		t.Fatalf("error = %v, want it to mention fsync on close", err)
	}
}

func TestClose_CloseErrorPropagates(t *testing.T) {
	fake := &fakeWALFile{failAfterWrites: -1, failClose: true}
	w := newWALWithFake(fake, 4096, false)
	err := w.Close()
	if err == nil {
		t.Fatal("expected Close to propagate the underlying Close error")
	}
}

func TestClose_Idempotent(t *testing.T) {
	fake := &fakeWALFile{}
	w := newWALWithFake(fake, 4096, false)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("a second Close() should be a no-op, got: %v", err)
	}
}

// --- trivial accessors, previously never exercised --------------------------

func TestLastSeq(t *testing.T) {
	path := tempWALPath(t)
	w := mustOpen(t, path, true)
	defer w.Close()

	if got := w.LastSeq(); got != 0 {
		t.Fatalf("LastSeq() on an empty WAL = %d, want 0", got)
	}
	w.Append(Record{SeqNum: 5, Type: RecordPut, Key: []byte("k"), Value: []byte("v")})
	w.Append(Record{SeqNum: 3, Type: RecordPut, Key: []byte("k2"), Value: []byte("v2")}) // lower seq, must not regress LastSeq
	if got := w.LastSeq(); got != 5 {
		t.Fatalf("LastSeq() = %d, want 5", got)
	}
}

func TestPath(t *testing.T) {
	path := tempWALPath(t)
	w := mustOpen(t, path, true)
	defer w.Close()
	if got := w.Path(); got != path {
		t.Fatalf("Path() = %q, want %q", got, path)
	}
}

// --- decodePayload malformed inputs (direct, whitebox) ----------------------

func TestDecodePayload_MalformedInputs(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{"too short for header", []byte{0, 1, 2}},
		{"keyLen exceeds remaining", func() []byte {
			p := make([]byte, 13)
			p[0] = byte(RecordPut)
			binary.LittleEndian.PutUint32(p[9:13], 1000) // keyLen far beyond what follows
			return p
		}()},
		{"missing valLen field", func() []byte {
			p := make([]byte, 13) // type+seq+keyLen(=0), nothing after: no room for valLen
			p[0] = byte(RecordPut)
			return p
		}()},
		{"valLen exceeds remaining", func() []byte {
			p := make([]byte, 17) // header(13) + 4-byte valLen field, but no value bytes
			p[0] = byte(RecordPut)
			binary.LittleEndian.PutUint32(p[9:13], 0)    // keyLen=0
			binary.LittleEndian.PutUint32(p[13:17], 100) // valLen claims 100, none present
			return p
		}()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := decodePayload(c.payload); err == nil {
				t.Fatalf("decodePayload(%v) = nil error, want an error", c.payload)
			}
		})
	}
}

// --- Replay error paths not otherwise reachable -----------------------------

func TestReplay_OpenErrorOnDirectory(t *testing.T) {
	// A directory can't be opened for read/write as a regular file — this
	// fails identically and portably on Linux and macOS, no permission
	// tricks needed.
	dir := t.TempDir()
	_, _, err := Replay(dir)
	if err == nil {
		t.Fatal("expected an error replaying a path that is a directory")
	}
}

func TestReplay_DecodePayloadErrorInsideValidChecksum(t *testing.T) {
	// Hand-build a WAL file with one record whose payload is structurally
	// invalid but whose checksum is computed correctly over those exact
	// (invalid) bytes — the only way to reach decodePayload's own error
	// branch inside Replay, since corrupting an already-valid record's
	// bytes always breaks its checksum too, taking a different branch.
	path := filepath.Join(t.TempDir(), "malformed.wal")

	malformedPayload := []byte{0, 1, 2} // too short for decodePayload's own header check
	crc := crc32.Checksum(malformedPayload, crcTable)
	buf := make([]byte, 0, headerSize+len(malformedPayload))
	var crcBytes, lenBytes [4]byte
	binary.LittleEndian.PutUint32(crcBytes[:], crc)
	binary.LittleEndian.PutUint32(lenBytes[:], uint32(len(malformedPayload)))
	buf = append(buf, crcBytes[:]...)
	buf = append(buf, lenBytes[:]...)
	buf = append(buf, malformedPayload...)

	if err := os.WriteFile(path, buf, 0644); err != nil {
		t.Fatal(err)
	}

	records, stats, err := Replay(path)
	if err != nil {
		t.Fatalf("Replay should treat this as a torn/untrusted record, not error: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 recovered records, got %d", len(records))
	}
	if !stats.TornWriteTruncated {
		t.Fatalf("expected the malformed-payload record to be treated as truncatable, got %+v", stats)
	}
}

// --- replayFrom / truncateAndReturn, driven by a fake source ----------------

// fakeReplaySource is a minimal replaySource that can simulate a generic
// (non-EOF) read error at a chosen call, and/or a failing Truncate —
// both essentially impossible to trigger against a real, already-open,
// writable file.
type fakeReplaySource struct {
	data        []byte
	pos         int
	readCalls   int
	failOnRead  int // -1 = never; N = the (N+1)th Read call returns a generic error
	failOnTrunc bool
}

func (f *fakeReplaySource) Read(p []byte) (int, error) {
	f.readCalls++
	if f.failOnRead >= 0 && f.readCalls > f.failOnRead {
		return 0, errors.New("fakeReplaySource: simulated generic read failure")
	}
	if f.pos >= len(f.data) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.pos:])
	f.pos += n
	return n, nil
}

func (f *fakeReplaySource) Truncate(size int64) error {
	if f.failOnTrunc {
		return errors.New("fakeReplaySource: simulated truncate failure")
	}
	return nil
}

func TestReplayFrom_GenericReadErrorPropagates(t *testing.T) {
	// A read error that isn't EOF/ErrUnexpectedEOF and returns n==0: the
	// generic "wal: read header" branch, distinct from the torn-write
	// paths (which only trigger on n>0 partial reads or EOF-family errors).
	fake := &fakeReplaySource{data: []byte{1, 2, 3, 4, 5, 6, 7, 8}, failOnRead: 0}
	_, _, err := replayFrom(fake, int64(len(fake.data)))
	if err == nil {
		t.Fatal("expected replayFrom to propagate a generic read error")
	}
	if !strings.Contains(err.Error(), "read header") {
		t.Fatalf("error = %v, want it to mention reading the header", err)
	}
}

func TestReplayFrom_TruncateErrorPropagates(t *testing.T) {
	// A single well-formed record followed by a torn (too-short) header,
	// so replayFrom reaches truncateAndReturn — which then fails.
	rec := Record{SeqNum: 1, Type: RecordPut, Key: []byte("k"), Value: []byte("v")}
	full := encode(rec)
	data := append(append([]byte{}, full...), 0x01, 0x02) // + a torn partial header

	fake := &fakeReplaySource{data: data, failOnRead: -1, failOnTrunc: true}
	_, _, err := replayFrom(fake, int64(len(data)))
	if err == nil {
		t.Fatal("expected replayFrom to propagate a Truncate error")
	}
	if !strings.Contains(err.Error(), "truncate") {
		t.Fatalf("error = %v, want it to mention truncate", err)
	}
}

func TestReplayFrom_PayloadShortReadDistinctFromSanityBound(t *testing.T) {
	// The "declared length exceeds remaining file size" sanity check (a
	// cheap check against the fileSize the caller reports) and the
	// "io.ReadFull for the payload came up short" check are two separate
	// safeguards. Real torn writes always trip the first (a shrunk file
	// makes fileSize itself reflect the shortfall), so to exercise the
	// second distinctly, we tell replayFrom the file is bigger than the
	// fake source can actually deliver — modeling a genuine read
	// inconsistency (e.g. a racing truncation) rather than a simple torn
	// tail.
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], 0xDEADBEEF) // crc: irrelevant, never reached
	binary.LittleEndian.PutUint32(header[4:8], 20)         // claims a 20-byte payload
	data := append(header, make([]byte, 10)...)            // but only 10 bytes actually follow

	fake := &fakeReplaySource{data: data, failOnRead: -1}
	// Report a fileSize far larger than len(data), so the sanity-bound
	// check (length=20 vs "remaining" computed from this inflated size)
	// passes, and the short physical read is what actually catches it.
	_, stats, err := replayFrom(fake, 1000)
	if err != nil {
		t.Fatalf("a short payload read should be treated as a torn write, not an error: %v", err)
	}
	if !stats.TornWriteTruncated {
		t.Fatalf("expected the short payload read to be treated as a torn write, got %+v", stats)
	}
}
