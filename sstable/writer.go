// Package sstable implements the on-disk sorted string table format: an
// immutable file holding a sorted run of key/value entries, produced by
// flushing a memtable or by compacting several existing SSTables together.
//
// File layout:
//
//	[data block 0][data block 1]...[data block N]
//	[bloom filter block]
//	[index block]
//	[footer, fixed 56 bytes at EOF]
//
// Every block (data, bloom, index) is followed by a 4-byte CRC32C
// checksum over its own bytes, checked on every read. The footer is a
// fixed-size trailer at the very end of the file recording where the
// index and bloom blocks are, so opening a table means: seek to
// (file size - 56), read the footer, then jump straight to the index —
// no need to scan the file to find anything. This mirrors the
// footer-at-EOF design LevelDB/RocksDB use for the same reason.
package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const (
	defaultBlockSize = 4096
	defaultBloomFPR  = 0.01
	footerSize       = 56
	magicNumber      = 0x4B56535331 // arbitrary fixed constant identifying this file format/version
)

// fileWriter is the subset of *os.File operations Writer needs. Defined as
// an interface (rather than using *os.File directly) purely so tests can
// inject a fake that fails at a precise, chosen point — a write, a Sync,
// or a Close — deterministically and portably. Simulating those same
// failures against a real file would mean OS-specific tricks (permission
// games root ignores, disk-full conditions, timing races) that are either
// unreliable or don't work the same on Linux and macOS. A real *os.File
// satisfies this interface as-is.
type fileWriter interface {
	io.Writer
	Sync() error
	Close() error
	Name() string
}

// Options configures a Writer.
type Options struct {
	// BlockSize is the approximate size, in bytes, at which a data block
	// is flushed. Entries are never split across blocks, so actual block
	// sizes vary a bit above this target depending on entry sizes.
	BlockSize int
	// BloomFPRate is the target false-positive rate for the table's bloom
	// filter, e.g. 0.01 for ~1%. Lower rates use more bits per key.
	BloomFPRate float64
}

func (o Options) withDefaults() Options {
	if o.BlockSize <= 0 {
		o.BlockSize = defaultBlockSize
	}
	if o.BloomFPRate <= 0 {
		o.BloomFPRate = defaultBloomFPR
	}
	return o
}

// Meta describes a completed SSTable file, returned by Finish. The engine
// uses this to track which files exist and what key range each covers,
// without needing to reopen and re-scan them.
type Meta struct {
	Path       string
	NumEntries int
	MinKey     []byte
	MaxKey     []byte
	MaxSeq     uint64
	FileSize   uint64
}

// Writer builds a new SSTable file. Entries must be added in strictly
// increasing key order — the same order a memtable's Iterator or a
// compaction merge produces — since the writer streams entries straight
// into blocks and never sorts or buffers the full data set.
type Writer struct {
	f    fileWriter
	w    *bufio.Writer
	opts Options

	offset uint64 // current write position in the file

	curBlock []byte // pending, not-yet-flushed data block bytes
	index    []indexEntry

	keys [][]byte // every key seen, retained only to size/build the bloom filter in Finish

	lastKey     []byte
	haveLastKey bool
	minKey      []byte
	maxKey      []byte
	numEntries  int
	maxSeq      uint64

	closed bool
}

// NewWriter creates path (truncating it if it already exists) and returns
// a Writer ready for Add calls.
func NewWriter(path string, opts Options) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("sstable: create %s: %w", path, err)
	}
	return &Writer{
		f:    f,
		w:    bufio.NewWriter(f),
		opts: opts.withDefaults(),
	}, nil
}

// Add appends one entry. Keys must be strictly increasing across calls;
// Add returns an error rather than silently accepting an out-of-order or
// duplicate key, since either would indicate a bug upstream (e.g. a
// memtable iterator not actually being sorted) that's far better caught
// here than as a subtly wrong SSTable on disk.
func (w *Writer) Add(key, value []byte, seq uint64, deleted bool) error {
	if w.closed {
		return fmt.Errorf("sstable: Add called after Finish")
	}
	if w.haveLastKey {
		switch bytes.Compare(key, w.lastKey) {
		case 0:
			return fmt.Errorf("sstable: duplicate key %q passed to Add (caller must dedupe before writing)", key)
		case -1:
			return fmt.Errorf("sstable: keys out of order: %q was added after %q", key, w.lastKey)
		}
	}

	w.curBlock = append(w.curBlock, encodeEntry(key, value, seq, deleted)...)

	keyCopy := append([]byte(nil), key...)
	w.keys = append(w.keys, keyCopy)
	w.lastKey = keyCopy
	w.haveLastKey = true
	if w.minKey == nil {
		w.minKey = keyCopy
	}
	w.maxKey = keyCopy
	w.numEntries++
	if seq > w.maxSeq {
		w.maxSeq = seq
	}

	if len(w.curBlock) >= w.opts.BlockSize {
		return w.flushBlock()
	}
	return nil
}

// flushBlock writes out the pending data block (if any) with its checksum
// trailer and records an index entry pointing at it.
func (w *Writer) flushBlock() error {
	if len(w.curBlock) == 0 {
		return nil
	}
	blockStart := w.offset
	length, err := w.writeChecksummed(w.curBlock)
	if err != nil {
		return err
	}
	w.index = append(w.index, indexEntry{
		lastKey: append([]byte(nil), w.lastKey...),
		offset:  blockStart,
		length:  length,
	})
	w.curBlock = w.curBlock[:0]
	return nil
}

// writeChecksummed writes data followed by a 4-byte CRC32C trailer over
// it in a single Write call, advancing w.offset, and returns the total
// bytes written (data + 4).
func (w *Writer) writeChecksummed(data []byte) (uint64, error) {
	crc := crc32.Checksum(data, crcTable)
	buf := make([]byte, len(data)+4)
	copy(buf, data)
	binary.LittleEndian.PutUint32(buf[len(data):], crc)
	if _, err := w.w.Write(buf); err != nil {
		return 0, fmt.Errorf("sstable: write: %w", err)
	}
	length := uint64(len(buf))
	w.offset += length
	return length, nil
}

// Finish flushes any pending data, writes the bloom filter, index, and
// footer, fsyncs, and closes the file. The Writer must not be used again
// afterward.
func (w *Writer) Finish() (*Meta, error) {
	if w.closed {
		return nil, fmt.Errorf("sstable: Finish called twice")
	}
	w.closed = true

	if err := w.flushBlock(); err != nil {
		return nil, err
	}

	// Bloom filter: sized for the exact key count now that we've seen
	// every key, built in one pass over the (small) retained key list.
	bf := newBloomFilter(len(w.keys), w.opts.BloomFPRate)
	for _, k := range w.keys {
		bf.add(k)
	}

	// Bloom and index blocks share one write-and-checksum code path (and
	// its error handling) rather than two near-identical copies.
	blocks := [2][]byte{bf.encode(), encodeIndex(w.index)}
	var blockOffsets, blockLengths [2]uint64
	for i, data := range blocks {
		blockOffsets[i] = w.offset
		length, err := w.writeChecksummed(data)
		if err != nil {
			return nil, fmt.Errorf("sstable: writing block %d of 2 (0=bloom, 1=index): %w", i, err)
		}
		blockLengths[i] = length
	}
	bloomOffset, indexOffset := blockOffsets[0], blockOffsets[1]
	bloomLength, indexLength := blockLengths[0], blockLengths[1]

	footer := make([]byte, 0, footerSize)
	footer = binary.LittleEndian.AppendUint64(footer, indexOffset)
	footer = binary.LittleEndian.AppendUint64(footer, indexLength)
	footer = binary.LittleEndian.AppendUint64(footer, bloomOffset)
	footer = binary.LittleEndian.AppendUint64(footer, bloomLength)
	footer = binary.LittleEndian.AppendUint64(footer, uint64(w.numEntries))
	footer = binary.LittleEndian.AppendUint64(footer, w.maxSeq)
	footer = binary.LittleEndian.AppendUint64(footer, magicNumber)
	if _, err := w.w.Write(footer); err != nil {
		return nil, fmt.Errorf("sstable: write footer: %w", err)
	}
	w.offset += uint64(len(footer))

	if err := w.w.Flush(); err != nil {
		w.f.Close()
		return nil, fmt.Errorf("sstable: flush: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return nil, fmt.Errorf("sstable: fsync: %w", err)
	}
	path := w.f.Name()
	if err := w.f.Close(); err != nil {
		return nil, fmt.Errorf("sstable: close: %w", err)
	}

	return &Meta{
		Path:       path,
		NumEntries: w.numEntries,
		MinKey:     w.minKey,
		MaxKey:     w.maxKey,
		MaxSeq:     w.maxSeq,
		FileSize:   w.offset,
	}, nil
}
