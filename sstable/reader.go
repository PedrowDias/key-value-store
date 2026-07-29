package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
)

// Reader opens an existing SSTable file for point lookups and iteration.
// The index and bloom filter are loaded fully into memory on Open (they're
// small relative to the data); data blocks are read from disk on demand,
// or served from cache — see OpenWithCache.
type Reader struct {
	f          *os.File
	path       string
	cache      *BlockCache
	index      []indexEntry
	bloom      *bloomFilter
	numEntries int
	maxSeq     uint64
}

// Open opens the SSTable at path with no block cache — equivalent to
// OpenWithCache(path, nil). See OpenWithCache for the caching variant.
func Open(path string) (*Reader, error) {
	return OpenWithCache(path, nil)
}

// OpenWithCache opens the SSTable at path, reading its footer, index,
// and bloom filter. It returns an error if the file is too small, has a
// bad magic number, or either block fails its checksum — any of which
// means the file is not a valid, uncorrupted SSTable produced by this
// package.
//
// If cache is non-nil, data blocks this Reader reads are served from
// (and populate) it, shared with any other Reader given the same cache
// instance — see BlockCache's own doc for why sharing across files is
// what makes this actually useful. A nil cache means no caching: every
// Get and iteration reads from disk every time, exactly as before this
// existed.
func OpenWithCache(path string, cache *BlockCache) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: stat %s: %w", path, err)
	}
	if info.Size() < footerSize {
		f.Close()
		return nil, fmt.Errorf("sstable: %s is too small (%d bytes) to contain a valid footer", path, info.Size())
	}

	footer := make([]byte, footerSize)
	if _, err := f.ReadAt(footer, info.Size()-footerSize); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: read footer: %w", err)
	}
	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	indexLength := binary.LittleEndian.Uint64(footer[8:16])
	bloomOffset := binary.LittleEndian.Uint64(footer[16:24])
	bloomLength := binary.LittleEndian.Uint64(footer[24:32])
	numEntries := binary.LittleEndian.Uint64(footer[32:40])
	maxSeq := binary.LittleEndian.Uint64(footer[40:48])
	magic := binary.LittleEndian.Uint64(footer[48:56])
	if magic != magicNumber {
		f.Close()
		return nil, fmt.Errorf("sstable: %s has bad magic number (not an sstable file, or corrupt)", path)
	}

	bloomRaw, err := readChecksummed(f, bloomOffset, bloomLength)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: reading bloom filter block: %w", err)
	}
	bloom, err := decodeBloomFilter(bloomRaw)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: decoding bloom filter: %w", err)
	}

	indexRaw, err := readChecksummed(f, indexOffset, indexLength)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: reading index block: %w", err)
	}
	index, err := decodeIndex(indexRaw)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: decoding index: %w", err)
	}

	return &Reader{f: f, path: path, cache: cache, index: index, bloom: bloom, numEntries: int(numEntries), maxSeq: maxSeq}, nil
}

// readChecksummed reads a length-prefixed-by-the-caller block at the given
// file offset and verifies its trailing CRC32C, returning the payload with
// the checksum stripped off.
func readChecksummed(f *os.File, offset, length uint64) ([]byte, error) {
	if length < 4 {
		return nil, fmt.Errorf("block at offset %d is too small (%d bytes) to contain a checksum", offset, length)
	}
	buf := make([]byte, length)
	if _, err := f.ReadAt(buf, int64(offset)); err != nil {
		return nil, fmt.Errorf("read at offset %d: %w", offset, err)
	}
	data := buf[:len(buf)-4]
	want := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	got := crc32.Checksum(data, crcTable)
	if got != want {
		return nil, fmt.Errorf("checksum mismatch at offset %d: file is corrupt", offset)
	}
	return data, nil
}

// NumEntries returns the number of entries the table was built with.
func (r *Reader) NumEntries() int {
	return r.numEntries
}

// MaxSeq returns the highest WAL sequence number among this table's
// entries. The engine uses this on restart to resume sequence-number
// allocation past every value that's ever been durably assigned, across
// both existing SSTables and the WAL.
func (r *Reader) MaxSeq() uint64 {
	return r.maxSeq
}

// readDataBlock returns the checksum-verified bytes of the data block at
// blk, from the cache if present there, otherwise from disk — populating
// the cache on a miss so a subsequent read of the same block (via this
// Reader or any other sharing the same cache) is a map lookup instead of
// a disk read and CRC32C verification.
func (r *Reader) readDataBlock(blk indexEntry) ([]byte, error) {
	if data, ok := r.cache.get(r.path, blk.offset); ok {
		return data, nil
	}
	data, err := readChecksummed(r.f, blk.offset, blk.length)
	if err != nil {
		return nil, err
	}
	r.cache.put(r.path, blk.offset, data)
	return data, nil
}

// Get looks up key. found is false if the key isn't in this table at all.
// If found is true and deleted is true, this table holds a tombstone for
// the key — the caller (the engine, consulting multiple SSTables newest
// to oldest) must treat that as "deleted," not fall through to an older
// table's stale value.
//
// The bloom filter is checked first: on a "definitely not present"
// answer, Get returns immediately without any disk I/O at all, which is
// the entire point of shipping one per table.
func (r *Reader) Get(key []byte) (value []byte, seq uint64, deleted bool, found bool, err error) {
	if !r.bloom.mayContain(key) {
		return nil, 0, false, false, nil
	}

	i := sort.Search(len(r.index), func(i int) bool {
		return bytes.Compare(r.index[i].lastKey, key) >= 0
	})
	if i == len(r.index) {
		// key is greater than every block's last key: not in this table.
		return nil, 0, false, false, nil
	}

	blk := r.index[i]
	data, rerr := r.readDataBlock(blk)
	if rerr != nil {
		return nil, 0, false, false, fmt.Errorf("sstable: reading data block: %w", rerr)
	}

	off := 0
	for off < len(data) {
		e, n, derr := decodeEntry(data[off:])
		if derr != nil {
			return nil, 0, false, false, fmt.Errorf("sstable: decoding entry: %w", derr)
		}
		switch bytes.Compare(e.key, key) {
		case 0:
			return e.value, e.seq, e.deleted, true, nil
		case 1:
			// entries within a block are sorted; we've passed where
			// key would be, so it isn't in this table.
			return nil, 0, false, false, nil
		}
		off += n
	}
	return nil, 0, false, false, nil
}

// Close closes the underlying file.
func (r *Reader) Close() error {
	return r.f.Close()
}

// Iterator provides forward, in-order traversal of every entry in the
// table, block by block, reading each data block from disk lazily as
// iteration reaches it. Used for compaction (merging several tables) and
// for any range scan that needs to walk a table in full.
type Iterator struct {
	r        *Reader
	blockIdx int
	entries  []decodedEntry
	entryIdx int
	err      error
}

// NewIterator returns an Iterator; call SeekToFirst before reading.
func (r *Reader) NewIterator() *Iterator {
	return &Iterator{r: r, blockIdx: -1, entryIdx: -1}
}

func (it *Iterator) loadBlock(idx int) {
	if idx < 0 || idx >= len(it.r.index) {
		it.entries = nil
		it.entryIdx = -1
		it.blockIdx = idx
		return
	}
	blk := it.r.index[idx]
	data, err := it.r.readDataBlock(blk)
	if err != nil {
		it.err = fmt.Errorf("sstable: iterator reading block %d: %w", idx, err)
		it.entries = nil
		it.entryIdx = -1
		return
	}
	var entries []decodedEntry
	off := 0
	for off < len(data) {
		e, n, derr := decodeEntry(data[off:])
		if derr != nil {
			it.err = fmt.Errorf("sstable: iterator decoding block %d: %w", idx, derr)
			it.entries = nil
			it.entryIdx = -1
			return
		}
		entries = append(entries, e)
		off += n
	}
	it.entries = entries
	it.entryIdx = 0
	it.blockIdx = idx
}

// SeekToFirst positions the iterator at the table's first entry.
func (it *Iterator) SeekToFirst() {
	it.err = nil
	it.loadBlock(0)
}

// Valid reports whether the iterator is positioned at a readable entry.
// It returns false both at true end-of-table and after an error — check
// Err() to distinguish the two.
func (it *Iterator) Valid() bool {
	return it.err == nil && it.entryIdx >= 0 && it.entryIdx < len(it.entries)
}

// Next advances to the next entry, loading the next data block from disk
// if the current one is exhausted.
func (it *Iterator) Next() {
	if it.err != nil {
		return
	}
	it.entryIdx++
	if it.entryIdx >= len(it.entries) {
		it.loadBlock(it.blockIdx + 1)
	}
}

// Err returns the first error encountered during iteration, if any.
func (it *Iterator) Err() error { return it.err }

// Key returns the current entry's key. Only valid when Valid() is true.
func (it *Iterator) Key() []byte { return it.entries[it.entryIdx].key }

// Value returns the current entry's value. Only valid when Valid() is true.
func (it *Iterator) Value() []byte { return it.entries[it.entryIdx].value }

// Deleted reports whether the current entry is a tombstone. Only valid
// when Valid() is true.
func (it *Iterator) Deleted() bool { return it.entries[it.entryIdx].deleted }

// SeqNum returns the current entry's sequence number. Only valid when
// Valid() is true.
func (it *Iterator) SeqNum() uint64 { return it.entries[it.entryIdx].seq }
