package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// --- fake fileWriter, for deterministic Finish() error-path tests ----------

// fakeFile is a minimal in-memory fileWriter that can be configured to
// fail its Write call after a chosen number of successful writes, and/or
// fail Sync or Close outright. Using this instead of real OS-level fault
// injection (permissions, disk-full, etc.) makes these tests exact and
// portable: no reliance on root bypassing permission checks (as it does
// in this sandbox) or platform-specific behavior.
type fakeFile struct {
	name            string
	buf             bytes.Buffer
	writeCalls      int
	failAfterWrites int // -1 = never fail on Write; N = the (N+1)th call fails
	failSync        bool
	failClose       bool
}

func (f *fakeFile) Write(p []byte) (int, error) {
	f.writeCalls++
	if f.failAfterWrites >= 0 && f.writeCalls > f.failAfterWrites {
		return 0, errors.New("fakeFile: simulated write failure")
	}
	return f.buf.Write(p)
}

func (f *fakeFile) Sync() error {
	if f.failSync {
		return errors.New("fakeFile: simulated sync failure")
	}
	return nil
}

func (f *fakeFile) Close() error {
	if f.failClose {
		return errors.New("fakeFile: simulated close failure")
	}
	return nil
}

func (f *fakeFile) Name() string { return f.name }

// newWriterWithFake builds a Writer around a fakeFile, bypassing NewWriter
// (which always opens a real *os.File). bufSize controls the underlying
// bufio.Writer's buffer: a small size (e.g. 1) forces every write to pass
// through to the fake immediately, making write-call counts predictable;
// a large size lets writes accumulate so only the explicit Flush() call
// reaches the fake, isolating the Flush()-specific error branch.
func newWriterWithFake(fake *fakeFile, bufSize int, opts Options) *Writer {
	return &Writer{
		f:    fake,
		w:    bufio.NewWriterSize(fake, bufSize),
		opts: opts.withDefaults(),
	}
}

func TestFlushBlock_WriteErrorPropagates(t *testing.T) {
	fake := &fakeFile{failAfterWrites: 0} // fail on the very first write
	w := newWriterWithFake(fake, 1, Options{})
	w.curBlock = []byte("some pending entry bytes")
	w.lastKey = []byte("k")

	err := w.flushBlock()
	if err == nil {
		t.Fatal("expected flushBlock to propagate the underlying write error")
	}
	if !strings.Contains(err.Error(), "write") {
		t.Fatalf("error = %v, want it to mention the write failure", err)
	}
}

func TestFinish_BloomOrIndexWriteErrorPropagates(t *testing.T) {
	// curBlock is empty (zero Add calls), so flushBlock no-ops and the
	// very first real write Finish attempts is the bloom block.
	fake := &fakeFile{failAfterWrites: 0}
	w := newWriterWithFake(fake, 1, Options{})

	_, err := w.Finish()
	if err == nil {
		t.Fatal("expected Finish to propagate a bloom/index block write error")
	}
	if !strings.Contains(err.Error(), "writing block") {
		t.Fatalf("error = %v, want it to mention which block failed", err)
	}
}

func TestFinish_FooterWriteErrorPropagates(t *testing.T) {
	// Allow exactly 2 real writes to succeed (bloom, then index), fail on
	// the 3rd (the footer). Buffer size 1 makes each logical write a
	// distinct, immediate underlying Write call.
	fake := &fakeFile{failAfterWrites: 2}
	w := newWriterWithFake(fake, 1, Options{})

	_, err := w.Finish()
	if err == nil {
		t.Fatal("expected Finish to propagate a footer write error")
	}
	if !strings.Contains(err.Error(), "write footer") {
		t.Fatalf("error = %v, want it to mention the footer write", err)
	}
}

func TestFinish_FlushErrorPropagates(t *testing.T) {
	// A large buffer means bloom+index+footer (all tiny) stay buffered
	// and never individually reach the fake; only the explicit Flush()
	// call at the end forces a real write, so failing the very first
	// write call isolates the Flush() branch specifically.
	fake := &fakeFile{failAfterWrites: 0}
	w := newWriterWithFake(fake, 4096, Options{})

	_, err := w.Finish()
	if err == nil {
		t.Fatal("expected Finish to propagate a Flush error")
	}
	if !strings.Contains(err.Error(), "flush") {
		t.Fatalf("error = %v, want it to mention flush", err)
	}
}

func TestFinish_SyncErrorPropagates(t *testing.T) {
	fake := &fakeFile{failAfterWrites: -1, failSync: true}
	w := newWriterWithFake(fake, 4096, Options{})

	_, err := w.Finish()
	if err == nil {
		t.Fatal("expected Finish to propagate a Sync error")
	}
	if !strings.Contains(err.Error(), "fsync") {
		t.Fatalf("error = %v, want it to mention fsync", err)
	}
}

func TestFinish_CloseErrorPropagates(t *testing.T) {
	fake := &fakeFile{failAfterWrites: -1, failClose: true}
	w := newWriterWithFake(fake, 4096, Options{})

	_, err := w.Finish()
	if err == nil {
		t.Fatal("expected Finish to propagate a Close error")
	}
	if !strings.Contains(err.Error(), "close") {
		t.Fatalf("error = %v, want it to mention close", err)
	}
}

func TestFinish_CalledTwice(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.sst")
	w, err := NewWriter(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Finish(); err == nil {
		t.Fatal("expected an error calling Finish twice")
	}
}

func TestNewWriter_CreateErrorPropagates(t *testing.T) {
	// A directory can't be opened for writing as a regular file — this
	// fails identically and portably on Linux and macOS, no permission
	// tricks needed (root doesn't bypass an ENOTDIR/EISDIR-class error).
	dir := t.TempDir()
	_, err := NewWriter(dir, Options{})
	if err == nil {
		t.Fatal("expected an error creating a Writer at a path that is a directory")
	}
}

// --- Reader.Open error paths not exercised by the black-box tests ----------

func TestOpen_NonexistentFile(t *testing.T) {
	_, err := Open(filepath.Join(t.TempDir(), "does-not-exist.sst"))
	if err == nil {
		t.Fatal("expected an error opening a nonexistent file")
	}
}

func TestOpen_DetectsCorruptedIndexBlock(t *testing.T) {
	path := tempPath(t)
	writeTable(t, path, Options{}, []entry{
		{key: "a", value: "va", seq: 1},
		{key: "b", value: "vb", seq: 2},
	})

	// The index block is the second-to-last thing before the footer;
	// flip a byte a few bytes before EOF-footerSize to land inside it
	// without touching the footer itself.
	f, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := f.Stat()
	if _, err := f.WriteAt([]byte{0xAB}, info.Size()-int64(footerSize)-2); err != nil {
		t.Fatal(err)
	}
	f.Close()

	_, err = Open(path)
	if err == nil {
		t.Fatal("expected Open to detect a corrupted index block")
	}
}

func TestOpen_DetectsCorruptedBloomBlock(t *testing.T) {
	path := tempPath(t)
	meta := writeTable(t, path, Options{}, []entry{
		{key: "a", value: "va", seq: 1},
	})
	_ = meta

	// Corrupt a byte a bit further back, inside the bloom block (which
	// precedes the index block, which precedes the footer).
	f, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := f.Stat()
	// The bloom block for 1 key is small; reach back far enough to land
	// inside it rather than the (also small) index block or footer.
	if _, err := f.WriteAt([]byte{0xCD}, info.Size()-int64(footerSize)-20); err != nil {
		t.Fatal(err)
	}
	f.Close()

	_, err = Open(path)
	if err == nil {
		t.Fatal("expected Open to detect a corrupted bloom block")
	}
}

// --- direct unit tests for decode functions' malformed-input handling ------

func TestDecodeEntry_MalformedInputs(t *testing.T) {
	valid := encodeEntry([]byte("key"), []byte("value"), 42, false)

	cases := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"keyLen varint truncated", []byte{0x80}},
		{"keyLen exceeds remaining", append(appendUvarint(nil, 100), []byte("short")...)},
		{"missing type byte", appendUvarint(nil, 0)}, // keyLen=0, key="", nothing left for type
		{"seq varint missing", append(append(appendUvarint(nil, 0), byte(entryPut)), []byte{}...)},
		{"valLen varint missing", func() []byte {
			// keyLen=3, key="key", type=Put, seq=42, then nothing at all
			// for the valLen varint that should follow.
			d := appendUvarint(nil, 3)
			d = append(d, []byte("key")...)
			d = append(d, byte(entryPut))
			d = appendUvarint(d, 42)
			return d
		}()},
		{"value shorter than valLen", truncateBefore(valid, 1)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, _, err := decodeEntry(c.data); err == nil {
				t.Fatalf("decodeEntry(%v) = nil error, want an error", c.data)
			}
		})
	}
}

// appendUvarint is a small local helper mirroring binary.AppendUvarint,
// used to hand-build malformed entry byte sequences for the table above.
func appendUvarint(buf []byte, x uint64) []byte {
	var tmp [10]byte
	n := 0
	for x >= 0x80 {
		tmp[n] = byte(x) | 0x80
		x >>= 7
		n++
	}
	tmp[n] = byte(x)
	n++
	return append(buf, tmp[:n]...)
}

// truncateBefore cuts n bytes off the end of a valid encoded entry, to
// simulate a torn/short read at various points.
func truncateBefore(data []byte, n int) []byte {
	if n >= len(data) {
		return nil
	}
	return data[:len(data)-n]
}

func TestDecodeIndex_MalformedInputs(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"keyLen varint truncated", []byte{0x80}},
		{"keyLen exceeds remaining", appendUvarint(nil, 100)},
		{"missing offset/length bytes", append(appendUvarint(nil, 1), 'k')},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := decodeIndex(c.data); err == nil {
				t.Fatalf("decodeIndex(%v) = nil error, want an error", c.data)
			}
		})
	}
}

func TestDecodeIndex_EmptyInputIsNotAnError(t *testing.T) {
	entries, err := decodeIndex(nil)
	if err != nil {
		t.Fatalf("decodeIndex(nil) should succeed with zero entries, got: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
}

func TestDecodeBloomFilter_MalformedInputs(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"too short for header", []byte{1, 2, 3}},
		{"bits length mismatch", func() []byte {
			bf := newBloomFilter(100, 0.01)
			enc := bf.encode()
			return enc[:len(enc)-1] // chop one byte off the bitset
		}()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := decodeBloomFilter(c.data); err == nil {
				t.Fatal("expected an error decoding malformed bloom filter data")
			}
		})
	}
}

// --- bloomParams clamping ---------------------------------------------------

func TestBloomParams_ZeroEntries(t *testing.T) {
	m, k := bloomParams(0, 0.01)
	if m != 0 || k != 0 {
		t.Fatalf("bloomParams(0, ...) = (%d, %d), want (0, 0)", m, k)
	}
}

func TestBloomParams_ClampsMinimumM(t *testing.T) {
	// n=1 with a very loose (large) target FP rate drives the raw
	// computed m below the 8-bit floor.
	m, _ := bloomParams(1, 0.99)
	if m < 8 {
		t.Fatalf("m = %d, want the floor of 8 to have been applied", m)
	}
}

func TestBloomParams_ClampsMinimumK(t *testing.T) {
	// n=100 with a loose target FP rate: the m-floor of 8 combined with
	// a large n drives the raw computed k below 1.
	m, k := bloomParams(100, 0.99)
	if m != 8 {
		t.Fatalf("m = %d, want 8 (floor)", m)
	}
	if k != 1 {
		t.Fatalf("k = %d, want the floor of 1 to have been applied", k)
	}
}

func TestBloomParams_ClampsMaximumK(t *testing.T) {
	// An extremely strict target FP rate drives the raw computed k above
	// the 30-hash-function ceiling.
	_, k := bloomParams(10000, 1e-15)
	if k != 30 {
		t.Fatalf("k = %d, want the ceiling of 30 to have been applied", k)
	}
}

func TestBloomFilter_EmptyFilterAlwaysMayContain(t *testing.T) {
	bf := newBloomFilter(0, 0.01)
	bf.add([]byte("anything")) // no-op: m==0 means add() has nothing to set
	if !bf.mayContain([]byte("anything")) {
		t.Fatal("a zero-sized filter (empty table) must never rule out a key")
	}
}

// --- Small accessor coverage -------------------------------------------------

func TestReader_MaxSeqAndIteratorAccessors(t *testing.T) {
	path := tempPath(t)
	writeTable(t, path, Options{}, []entry{
		{key: "a", value: "va", seq: 5},
		{key: "b", deleted: true, seq: 9},
		{key: "c", value: "vc", seq: 3},
	})
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if got := r.MaxSeq(); got != 9 {
		t.Fatalf("MaxSeq() = %d, want 9", got)
	}

	it := r.NewIterator()
	it.SeekToFirst()
	if it.SeqNum() != 5 {
		t.Fatalf("first entry SeqNum() = %d, want 5", it.SeqNum())
	}
	if it.Deleted() {
		t.Fatal("first entry should not be deleted")
	}
	it.Next()
	if it.SeqNum() != 9 {
		t.Fatalf("second entry SeqNum() = %d, want 9", it.SeqNum())
	}
	if !it.Deleted() {
		t.Fatal("second entry should be deleted")
	}
}

func TestIterator_NextAfterErrorIsNoop(t *testing.T) {
	path := buildRawSSTable(t, rawBlockSpec{
		data:  []byte{0xFF}, // malformed: unterminated varint, self-consistently checksummed
		bloom: validBloomBlockFor(t, []byte("z")),
		index: validIndexFor(t, "z", 0, 0),
	})
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	it := r.NewIterator()
	it.SeekToFirst()
	if it.Valid() {
		t.Fatal("expected the handcrafted corrupt table to fail on first block load")
	}
	if it.Err() == nil {
		t.Fatal("expected an iteration error from the corrupt block")
	}
	errBefore := it.Err()
	it.Next() // must be a no-op once it.err is set
	if it.Err() != errBefore {
		t.Fatalf("Next() after an error must not change or clear it.err: before=%v after=%v", errBefore, it.Err())
	}
	if it.Valid() {
		t.Fatal("still must not be valid after a no-op Next()")
	}
}

func TestGet_DecodeEntryErrorInsideValidBlock(t *testing.T) {
	path := buildRawSSTable(t, rawBlockSpec{
		data:  []byte{0xFF}, // malformed: an unterminated varint, but self-consistently checksummed
		bloom: validBloomBlockFor(t, []byte("z")),
		index: validIndexFor(t, "z", 0, 5), // length filled in by buildRawSSTable
	})
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// The block's checksum is internally consistent (computed over the
	// same malformed bytes it contains), so Get gets past the checksum
	// check and must fail specifically at entry decoding.
	_, _, _, _, err = r.Get([]byte("z"))
	if err == nil {
		t.Fatal("expected Get to surface a decode error from a structurally malformed (but checksum-valid) block")
	}
}

func TestOpen_DecodeBloomStructuralError(t *testing.T) {
	// A too-short-for-its-own-header bloom block: checksum computed over
	// these exact (invalid) bytes, so it passes readChecksummed, but
	// decodeBloomFilter itself must reject it.
	path := buildRawSSTable(t, rawBlockSpec{
		data:  []byte("irrelevant"),
		bloom: []byte{1, 2, 3},
		index: validIndexFor(t, "z", 0, len("irrelevant")),
	})
	_, err := Open(path)
	if err == nil {
		t.Fatal("expected Open to reject a structurally invalid (but checksum-valid) bloom block")
	}
}

func TestOpen_DecodeIndexStructuralError(t *testing.T) {
	// An index block containing an unterminated varint: checksum-valid
	// over those bytes, but decodeIndex must reject the structure.
	path := buildRawSSTable(t, rawBlockSpec{
		data:  []byte("irrelevant"),
		bloom: validBloomBlockFor(t, []byte("z")),
		index: []byte{0x80},
	})
	_, err := Open(path)
	if err == nil {
		t.Fatal("expected Open to reject a structurally invalid (but checksum-valid) index block")
	}
}

func TestOpen_BloomBlockChecksumMismatch(t *testing.T) {
	path := buildRawSSTableWithBadChecksum(t, blockKindBloom)
	_, err := Open(path)
	if err == nil {
		t.Fatal("expected Open to detect a bloom block checksum mismatch")
	}
	if !strings.Contains(err.Error(), "bloom filter block") {
		t.Fatalf("error = %v, want it to specifically mention the bloom filter block", err)
	}
}

func TestOpen_IndexBlockChecksumMismatch(t *testing.T) {
	path := buildRawSSTableWithBadChecksum(t, blockKindIndex)
	_, err := Open(path)
	if err == nil {
		t.Fatal("expected Open to detect an index block checksum mismatch")
	}
	if !strings.Contains(err.Error(), "index block") {
		t.Fatalf("error = %v, want it to specifically mention the index block", err)
	}
}

func TestOpen_RejectsFileSmallerThanFooter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tiny.sst")
	if err := os.WriteFile(path, make([]byte, 10), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(path)
	if err == nil {
		t.Fatal("expected an error opening a file smaller than the footer itself")
	}
	if !strings.Contains(err.Error(), "too small") {
		t.Fatalf("error = %v, want it to mention the file being too small", err)
	}
}

func TestGet_KeySmallerThanAllEntriesInCandidateBlock(t *testing.T) {
	// Hand-build the data block from real, valid entries (not going
	// through Writer), paired with a zero-sized bloom filter (m=0, which
	// always answers "maybe present" per bloom.go's mayContain) so the
	// lookup deterministically reaches the in-block scan rather than
	// depending on landing a probabilistic bloom false positive.
	var data []byte
	data = append(data, encodeEntry([]byte("b"), []byte("vb"), 1, false)...)
	data = append(data, encodeEntry([]byte("d"), []byte("vd"), 2, false)...)
	data = append(data, encodeEntry([]byte("f"), []byte("vf"), 3, false)...)

	path := buildRawSSTable(t, rawBlockSpec{
		data:  data,
		bloom: newBloomFilter(0, 0.01).encode(), // m=0: mayContain is always true
		index: validIndexFor(t, "f", 0, 0),
	})
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// "a" sorts before every real key, but the index still selects this
	// (only) block since its lastKey ("f") >= "a". The very first
	// in-block comparison then finds an entry greater than the target,
	// exercising the "passed where it would be" early-return.
	_, _, _, found, err := r.Get([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected not found")
	}
}

func TestGet_FallsThroughEmptyDataBlock(t *testing.T) {
	// A block with zero entries is never produced by Writer (flushBlock
	// no-ops on an empty pending block), but Get must still handle one
	// gracefully if the index ever points at one — exercising the
	// "scanned the whole block, matched nothing" fall-through.
	path := buildRawSSTable(t, rawBlockSpec{
		data:  []byte{}, // empty: the entry-scan loop never executes
		bloom: validBloomBlockFor(t, []byte("z")),
		index: validIndexFor(t, "z", 0, 0),
	})
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, _, _, found, err := r.Get([]byte("z"))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected not found in an empty data block")
	}
}

func TestFinish_PendingBlockWriteErrorPropagates(t *testing.T) {
	// Unlike TestFlushBlock_WriteErrorPropagates (which calls flushBlock
	// directly), this drives the same failure through Finish() itself,
	// covering Finish's own error-propagation line for a pending block.
	fake := &fakeFile{failAfterWrites: 0}
	w := newWriterWithFake(fake, 1, Options{})
	if err := w.Add([]byte("k"), []byte("v"), 1, false); err != nil {
		t.Fatal(err)
	}
	_, err := w.Finish()
	if err == nil {
		t.Fatal("expected Finish to propagate a pending-block write error")
	}
}

func TestReadChecksummed_TooShortForChecksum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.sst")
	if err := os.WriteFile(path, []byte("abcdef"), 0644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	_, err = readChecksummed(f, 0, 3) // length < 4: too small to hold a checksum at all
	if err == nil {
		t.Fatal("expected an error for a block length too small to contain a checksum")
	}
}

func TestReadChecksummed_ReadPastEOF(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.sst")
	if err := os.WriteFile(path, []byte("abcdef"), 0644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	_, err = readChecksummed(f, 0, 1000) // far beyond the 6-byte file
	if err == nil {
		t.Fatal("expected an error reading a block that extends past EOF")
	}
}

func TestIterator_ChecksumMismatchDuringIteration(t *testing.T) {
	path := tempPath(t)
	writeTable(t, path, Options{}, []entry{
		{key: "a", value: "value-a-long-enough-to-flip-a-byte-in", seq: 1},
	})

	f, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte inside the data block's payload (offset 2 is safely
	// within the entry bytes for this small single-block table).
	if _, err := f.WriteAt([]byte{0xFF}, 2); err != nil {
		t.Fatal(err)
	}
	f.Close()

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	it := r.NewIterator()
	it.SeekToFirst()
	if it.Valid() {
		t.Fatal("expected the iterator to fail on a checksum-mismatched first block")
	}
	if it.Err() == nil {
		t.Fatal("expected an iteration error from the checksum mismatch")
	}
	if !strings.Contains(it.Err().Error(), "iterator reading block") {
		t.Fatalf("error = %v, want it to specifically mention iterator block reading", it.Err())
	}
}

// --- raw sstable construction helpers, for scenarios only reachable by ----
// --- hand-building a file (checksum-valid but structurally malformed, ----
// --- or with a deliberately wrong checksum) -------------------------------

type rawBlockSpec struct {
	data  []byte
	bloom []byte
	index []byte // if built via validIndexFor, offsets are patched below
}

// buildRawSSTable writes data, bloom, and index blocks exactly as given
// (each with a correctly computed checksum over its own bytes) followed by
// a valid footer pointing at them. Used to construct scenarios where a
// block passes its checksum check but is structurally invalid underneath —
// not reachable by corrupting an already-written real file, since any
// byte change there breaks the checksum too and takes a different branch.
func buildRawSSTable(t *testing.T, spec rawBlockSpec) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "raw.sst")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var offset uint64
	write := func(data []byte) (off, length uint64) {
		off = offset
		crc := crc32.Checksum(data, crcTable)
		buf := make([]byte, len(data)+4)
		copy(buf, data)
		binary.LittleEndian.PutUint32(buf[len(data):], crc)
		if _, err := f.Write(buf); err != nil {
			t.Fatal(err)
		}
		length = uint64(len(buf))
		offset += length
		return off, length
	}

	dataOffset, dataLength := write(spec.data)
	// validIndexFor leaves a placeholder for the data block's real
	// offset/length; patch them in now that we know them.
	index := patchIndexOffsetLength(spec.index, dataOffset, dataLength)

	bloomOffset, bloomLength := write(spec.bloom)
	indexOffset, indexLength := write(index)

	footer := make([]byte, 0, footerSize)
	footer = binary.LittleEndian.AppendUint64(footer, indexOffset)
	footer = binary.LittleEndian.AppendUint64(footer, indexLength)
	footer = binary.LittleEndian.AppendUint64(footer, bloomOffset)
	footer = binary.LittleEndian.AppendUint64(footer, bloomLength)
	footer = binary.LittleEndian.AppendUint64(footer, 1)
	footer = binary.LittleEndian.AppendUint64(footer, 1)
	footer = binary.LittleEndian.AppendUint64(footer, magicNumber)
	if _, err := f.Write(footer); err != nil {
		t.Fatal(err)
	}
	return path
}

type blockKind int

const (
	blockKindBloom blockKind = iota
	blockKindIndex
)

// buildRawSSTableWithBadChecksum writes a normal, structurally valid file
// but deliberately stores the WRONG checksum for one chosen block, to
// exercise readChecksummed's mismatch branch attributed specifically to
// that block (as opposed to a coincidental corruption landing in the
// wrong region, which is fragile to rely on when block sizes shift).
func buildRawSSTableWithBadChecksum(t *testing.T, corrupt blockKind) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "badcrc.sst")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var offset uint64
	write := func(data []byte, badChecksum bool) (off, length uint64) {
		off = offset
		crc := crc32.Checksum(data, crcTable)
		if badChecksum {
			crc ^= 0xFFFFFFFF // guaranteed wrong
		}
		buf := make([]byte, len(data)+4)
		copy(buf, data)
		binary.LittleEndian.PutUint32(buf[len(data):], crc)
		if _, err := f.Write(buf); err != nil {
			t.Fatal(err)
		}
		length = uint64(len(buf))
		offset += length
		return off, length
	}

	dataOffset, dataLength := write([]byte("hello"), false)

	bf := newBloomFilter(1, 0.01)
	bf.add([]byte("z"))
	bloomOffset, bloomLength := write(bf.encode(), corrupt == blockKindBloom)

	idx := encodeIndex([]indexEntry{{lastKey: []byte("z"), offset: dataOffset, length: dataLength}})
	indexOffset, indexLength := write(idx, corrupt == blockKindIndex)

	footer := make([]byte, 0, footerSize)
	footer = binary.LittleEndian.AppendUint64(footer, indexOffset)
	footer = binary.LittleEndian.AppendUint64(footer, indexLength)
	footer = binary.LittleEndian.AppendUint64(footer, bloomOffset)
	footer = binary.LittleEndian.AppendUint64(footer, bloomLength)
	footer = binary.LittleEndian.AppendUint64(footer, 1)
	footer = binary.LittleEndian.AppendUint64(footer, 1)
	footer = binary.LittleEndian.AppendUint64(footer, magicNumber)
	if _, err := f.Write(footer); err != nil {
		t.Fatal(err)
	}
	return path
}

// validBloomBlockFor returns correctly encoded bloom filter bytes
// containing the given key, for use as the "this part is fine" block in a
// rawBlockSpec that's testing a different block.
func validBloomBlockFor(t *testing.T, key []byte) []byte {
	t.Helper()
	bf := newBloomFilter(1, 0.01)
	bf.add(key)
	return bf.encode()
}

// validIndexFor returns index block bytes with one entry for lastKey,
// with offset/length as placeholders patched in by buildRawSSTable once
// the actual data block's position on disk is known.
func validIndexFor(t *testing.T, lastKey string, _ uint64, _ int) []byte {
	t.Helper()
	return encodeIndex([]indexEntry{{lastKey: []byte(lastKey), offset: 0, length: 0}})
}

// patchIndexOffsetLength rewrites the single index entry produced by
// validIndexFor with the real offset/length of the data block it should
// point at (known only after that block has actually been written).
func patchIndexOffsetLength(index []byte, dataOffset, dataLength uint64) []byte {
	entries, err := decodeIndex(index)
	if err != nil || len(entries) != 1 {
		return index // malformed-on-purpose index bytes: leave untouched
	}
	entries[0].offset = dataOffset
	entries[0].length = dataLength
	return encodeIndex(entries)
}
