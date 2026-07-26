// Package wal implements a write-ahead log: an append-only, crash-safe
// on-disk log of mutations. Every write to the storage engine is first
// durably recorded here before it is applied to the in-memory memtable,
// so that if the process crashes, the memtable can be reconstructed by
// replaying the log.
package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// RecordType distinguishes a normal write from a tombstone (deletion).
type RecordType uint8

const (
	RecordPut RecordType = iota
	RecordDelete
)

// Record is a single logical mutation: either "set key=value" or "delete key".
type Record struct {
	SeqNum uint64
	Type   RecordType
	Key    []byte
	Value  []byte // unused (nil) for RecordDelete
}

// On-disk framing for one record:
//
//	[4 bytes  CRC32C(payload)]
//	[4 bytes  payload length (uint32, little-endian)]
//	payload:
//	  [1 byte   type]
//	  [8 bytes  seq number]
//	  [4 bytes  key length]
//	  [key bytes]
//	  [4 bytes  value length]   (0 for deletes)
//	  [value bytes]
//
// The checksum covers only the payload, not the length prefix. This layout
// mirrors the framing used by LevelDB/RocksDB write-ahead logs: a length +
// checksum pair per record lets a reader detect a record that was only
// partially flushed to disk before a crash.
const headerSize = 4 + 4 // crc + length

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrCorruptRecord is returned internally by decode when a payload's
// checksum does not match its stored CRC. It never escapes the package;
// callers get corruption reported via Replay's return value instead.
var errCorruptRecord = errors.New("wal: corrupt record")

// encode serializes a record into its full on-disk framing (header + payload).
func encode(rec Record) []byte {
	keyLen := len(rec.Key)
	valLen := len(rec.Value)
	payloadLen := 1 + 8 + 4 + keyLen + 4 + valLen

	buf := make([]byte, headerSize+payloadLen)
	payload := buf[headerSize:]

	payload[0] = byte(rec.Type)
	binary.LittleEndian.PutUint64(payload[1:9], rec.SeqNum)
	binary.LittleEndian.PutUint32(payload[9:13], uint32(keyLen))
	copy(payload[13:13+keyLen], rec.Key)
	valOff := 13 + keyLen
	binary.LittleEndian.PutUint32(payload[valOff:valOff+4], uint32(valLen))
	copy(payload[valOff+4:], rec.Value)

	crc := crc32.Checksum(payload, crcTable)
	binary.LittleEndian.PutUint32(buf[0:4], crc)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(payloadLen))

	return buf
}

// decodePayload parses a payload (post-checksum-verification) into a Record.
// It assumes the caller has already validated len(payload) and its checksum.
func decodePayload(payload []byte) (Record, error) {
	if len(payload) < 1+8+4 {
		return Record{}, errCorruptRecord
	}
	rec := Record{}
	rec.Type = RecordType(payload[0])
	rec.SeqNum = binary.LittleEndian.Uint64(payload[1:9])
	keyLen := binary.LittleEndian.Uint32(payload[9:13])

	off := 13
	if uint32(len(payload)-off) < keyLen {
		return Record{}, errCorruptRecord
	}
	rec.Key = append([]byte(nil), payload[off:off+int(keyLen)]...)
	off += int(keyLen)

	if len(payload)-off < 4 {
		return Record{}, errCorruptRecord
	}
	valLen := binary.LittleEndian.Uint32(payload[off : off+4])
	off += 4
	if uint32(len(payload)-off) < valLen {
		return Record{}, errCorruptRecord
	}
	if valLen > 0 {
		rec.Value = append([]byte(nil), payload[off:off+int(valLen)]...)
	}

	return rec, nil
}
