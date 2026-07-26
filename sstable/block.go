package sstable

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

var errCorruptEntry = errors.New("sstable: corrupt entry")
var errCorruptIndex = errors.New("sstable: corrupt index block")

// entryType distinguishes a value write from a tombstone, same idea as
// wal.RecordType — kept as its own small type here rather than importing
// the wal package, since an SSTable's on-disk format needs to be stable
// and self-describing independent of the WAL's framing.
type entryType uint8

const (
	entryPut entryType = iota
	entryDelete
)

// decodedEntry is one key/value record as read back out of a data block.
type decodedEntry struct {
	key     []byte
	value   []byte
	seq     uint64
	deleted bool
}

// encodeEntry serializes one entry into a data block's entry format:
//
//	[uvarint keyLen][key][1B type][uvarint seq][uvarint valLen][value]
//
// Varint encoding for the length/seq fields (rather than fixed-width
// integers) noticeably shrinks the on-disk size for the common case of
// small keys and sequence numbers, which matters once we're measuring
// storage overhead in the benchmark phase.
func encodeEntry(key, value []byte, seq uint64, deleted bool) []byte {
	buf := make([]byte, 0, len(key)+len(value)+1+3*binary.MaxVarintLen64)
	buf = binary.AppendUvarint(buf, uint64(len(key)))
	buf = append(buf, key...)
	if deleted {
		buf = append(buf, byte(entryDelete))
	} else {
		buf = append(buf, byte(entryPut))
	}
	buf = binary.AppendUvarint(buf, seq)
	buf = binary.AppendUvarint(buf, uint64(len(value)))
	buf = append(buf, value...)
	return buf
}

// decodeEntry parses one entry starting at data[0], returning the entry
// and the number of bytes consumed so the caller can advance to the next
// one. It never returns partial/best-effort data on error.
func decodeEntry(data []byte) (decodedEntry, int, error) {
	var e decodedEntry

	keyLen, n := binary.Uvarint(data)
	if n <= 0 {
		return decodedEntry{}, 0, errCorruptEntry
	}
	off := n
	if uint64(len(data)-off) < keyLen {
		return decodedEntry{}, 0, errCorruptEntry
	}
	e.key = append([]byte(nil), data[off:off+int(keyLen)]...)
	off += int(keyLen)

	if off >= len(data) {
		return decodedEntry{}, 0, errCorruptEntry
	}
	e.deleted = entryType(data[off]) == entryDelete
	off++

	seq, n2 := binary.Uvarint(data[off:])
	if n2 <= 0 {
		return decodedEntry{}, 0, errCorruptEntry
	}
	e.seq = seq
	off += n2

	valLen, n3 := binary.Uvarint(data[off:])
	if n3 <= 0 {
		return decodedEntry{}, 0, errCorruptEntry
	}
	off += n3
	if uint64(len(data)-off) < valLen {
		return decodedEntry{}, 0, errCorruptEntry
	}
	if valLen > 0 {
		e.value = append([]byte(nil), data[off:off+int(valLen)]...)
	}
	off += int(valLen)

	return e, off, nil
}

// indexEntry records, for one data block, the largest key it contains and
// where to find it in the file — enough for a binary search over the
// (small, always in-memory) index to find the one candidate data block for
// a given key without touching the rest of the file.
type indexEntry struct {
	lastKey []byte
	offset  uint64
	length  uint64
}

func encodeIndex(entries []indexEntry) []byte {
	var buf []byte
	for _, e := range entries {
		buf = binary.AppendUvarint(buf, uint64(len(e.lastKey)))
		buf = append(buf, e.lastKey...)
		buf = binary.LittleEndian.AppendUint64(buf, e.offset)
		buf = binary.LittleEndian.AppendUint64(buf, e.length)
	}
	return buf
}

func decodeIndex(data []byte) ([]indexEntry, error) {
	var entries []indexEntry
	off := 0
	for off < len(data) {
		keyLen, n := binary.Uvarint(data[off:])
		if n <= 0 {
			return nil, errCorruptIndex
		}
		off += n
		if uint64(len(data)-off) < keyLen {
			return nil, errCorruptIndex
		}
		key := append([]byte(nil), data[off:off+int(keyLen)]...)
		off += int(keyLen)

		if len(data)-off < 16 {
			return nil, errCorruptIndex
		}
		offset := binary.LittleEndian.Uint64(data[off : off+8])
		length := binary.LittleEndian.Uint64(data[off+8 : off+16])
		off += 16

		entries = append(entries, indexEntry{lastKey: key, offset: offset, length: length})
	}
	return entries, nil
}
