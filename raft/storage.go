// Persistence for Raft's durable state: currentTerm, votedFor, and the
// log entries themselves. Built directly on the wal package — the same
// write-ahead log used by the storage engine — rather than inventing a
// second crash-recovery scheme: a torn write here needs exactly the same
// handling (detect it, discard the incomplete tail, keep everything
// before it) that the KV engine's WAL already implements and tests
// thoroughly.
//
// Three record shapes are multiplexed onto wal.Record's opaque Value
// bytes (wal.Record's own Put/Delete distinction is KV-specific and
// unused here — every record is written as a RecordPut with a one-byte
// kind tag inside Value):
//
//   - hardState: {Term, Vote} — the two values that must be durable
//     before any message depending on them is sent.
//   - logEntry: one log entry.
//   - truncateFrom: "discard any previously persisted entries with
//     Index >= this value." Always emitted immediately before a batch of
//     new entries, whether or not there's actually anything to discard —
//     a uniform, always-safe operation rather than two different code
//     paths for "plain append" vs. "conflict-caused rewrite."
package raft

import (
	"encoding/binary"
	"fmt"

	"github.com/PedrowDias/key-value-store/wal"
)

type recordKind byte

const (
	recordHardState recordKind = iota
	recordLogEntry
	recordTruncateFrom
)

// PersistentStorage durably stores a Raft node's HardState and log via a
// write-ahead log on disk.
type PersistentStorage struct {
	w   *wal.WAL
	seq uint64
}

// OpenStorage opens (creating if necessary) the storage log at path,
// replaying any existing records to reconstruct the HardState and log as
// of the last successful persist. The returned log always includes the
// index-0 dummy sentinel entry, matching Raft's own invariant, ready to
// hand directly to restoreState.
func OpenStorage(path string) (*PersistentStorage, HardState, []LogEntry, error) {
	records, _, err := wal.Replay(path)
	if err != nil {
		return nil, HardState{}, nil, fmt.Errorf("raft: replaying storage log: %w", err)
	}

	var hs HardState
	var log []LogEntry
	var maxSeq uint64

	for _, rec := range records {
		if rec.SeqNum > maxSeq {
			maxSeq = rec.SeqNum
		}
		kind, payload, derr := decodeRecord(rec.Value)
		if derr != nil {
			return nil, HardState{}, nil, fmt.Errorf("raft: decoding persisted record: %w", derr)
		}
		switch kind {
		case recordHardState:
			term, vote, herr := decodeHardStatePayload(payload)
			if herr != nil {
				return nil, HardState{}, nil, herr
			}
			hs = HardState{Term: term, Vote: vote}
		case recordLogEntry:
			e, eerr := decodeLogEntryPayload(payload)
			if eerr != nil {
				return nil, HardState{}, nil, eerr
			}
			log = append(log, e)
		case recordTruncateFrom:
			from, terr := decodeTruncateFromPayload(payload)
			if terr != nil {
				return nil, HardState{}, nil, terr
			}
			kept := log[:0]
			for _, e := range log {
				if e.Index < from {
					kept = append(kept, e)
				}
			}
			log = kept
		default:
			return nil, HardState{}, nil, fmt.Errorf("raft: unknown persisted record kind %d", kind)
		}
	}

	fullLog := append([]LogEntry{{Term: 0, Index: 0}}, log...)

	w, err := openWALLog(path, wal.Options{SyncOnWrite: true})
	if err != nil {
		return nil, HardState{}, nil, fmt.Errorf("raft: opening storage log: %w", err)
	}

	return &PersistentStorage{w: w, seq: maxSeq}, hs, fullLog, nil
}

// openWALLog is a package-level indirection over wal.Open purely so tests
// can simulate it failing at a specific point (after wal.Replay has
// already succeeded) — a scenario essentially impossible to construct
// against a real filesystem, since both calls target the same path with
// no intervening state change.
var openWALLog = wal.Open

func (s *PersistentStorage) nextSeq() uint64 {
	s.seq++
	return s.seq
}

// SaveHardState durably persists term and vote. Must complete before any
// message that depends on this HardState (e.g. a granted vote's response)
// is sent.
func (s *PersistentStorage) SaveHardState(hs HardState) error {
	rec := wal.Record{
		SeqNum: s.nextSeq(),
		Type:   wal.RecordPut,
		Value:  encodeRecord(recordHardState, encodeHardStatePayload(hs.Term, hs.Vote)),
	}
	if err := s.w.Append(rec); err != nil {
		return fmt.Errorf("raft: persisting hard state: %w", err)
	}
	return nil
}

// SaveEntries durably persists entries as the new log suffix starting at
// firstIndex, first recording a truncation marker for firstIndex so that
// recovery correctly discards any previously persisted entries there or
// later (relevant when this call represents a conflict-caused rewrite,
// harmless when it's a plain append with nothing to actually discard).
// A no-op if entries is empty.
func (s *PersistentStorage) SaveEntries(firstIndex uint64, entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	recs := make([]wal.Record, 0, len(entries)+1)
	recs = append(recs, wal.Record{
		SeqNum: s.nextSeq(),
		Type:   wal.RecordPut,
		Value:  encodeRecord(recordTruncateFrom, encodeTruncateFromPayload(firstIndex)),
	})
	for _, e := range entries {
		recs = append(recs, wal.Record{
			SeqNum: s.nextSeq(),
			Type:   wal.RecordPut,
			Value:  encodeRecord(recordLogEntry, encodeLogEntryPayload(e)),
		})
	}
	if err := s.w.AppendBatch(recs); err != nil {
		return fmt.Errorf("raft: persisting log entries: %w", err)
	}
	return nil
}

// Close closes the underlying storage log.
func (s *PersistentStorage) Close() error {
	return s.w.Close()
}

// --- record encoding ---------------------------------------------------------

func encodeRecord(kind recordKind, payload []byte) []byte {
	buf := make([]byte, 0, 1+len(payload))
	buf = append(buf, byte(kind))
	return append(buf, payload...)
}

func decodeRecord(data []byte) (recordKind, []byte, error) {
	if len(data) < 1 {
		return 0, nil, fmt.Errorf("raft: empty persisted record")
	}
	return recordKind(data[0]), data[1:], nil
}

func encodeHardStatePayload(term, vote uint64) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], term)
	binary.LittleEndian.PutUint64(buf[8:16], vote)
	return buf
}

func decodeHardStatePayload(data []byte) (term, vote uint64, err error) {
	if len(data) != 16 {
		return 0, 0, fmt.Errorf("raft: malformed hard state record (%d bytes, want 16)", len(data))
	}
	return binary.LittleEndian.Uint64(data[0:8]), binary.LittleEndian.Uint64(data[8:16]), nil
}

func encodeLogEntryPayload(e LogEntry) []byte {
	buf := make([]byte, 0, 20+len(e.Data))
	var b8 [8]byte
	binary.LittleEndian.PutUint64(b8[:], e.Term)
	buf = append(buf, b8[:]...)
	binary.LittleEndian.PutUint64(b8[:], e.Index)
	buf = append(buf, b8[:]...)
	var b4 [4]byte
	binary.LittleEndian.PutUint32(b4[:], uint32(len(e.Data)))
	buf = append(buf, b4[:]...)
	return append(buf, e.Data...)
}

func decodeLogEntryPayload(data []byte) (LogEntry, error) {
	if len(data) < 20 {
		return LogEntry{}, fmt.Errorf("raft: malformed log entry record (%d bytes, want >= 20)", len(data))
	}
	term := binary.LittleEndian.Uint64(data[0:8])
	index := binary.LittleEndian.Uint64(data[8:16])
	dlen := binary.LittleEndian.Uint32(data[16:20])
	if uint32(len(data)-20) != dlen {
		return LogEntry{}, fmt.Errorf("raft: log entry data length mismatch: header says %d, have %d", dlen, len(data)-20)
	}
	var d []byte
	if dlen > 0 {
		d = append([]byte(nil), data[20:20+dlen]...)
	}
	return LogEntry{Term: term, Index: index, Data: d}, nil
}

func encodeTruncateFromPayload(from uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, from)
	return buf
}

func decodeTruncateFromPayload(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("raft: malformed truncate record (%d bytes, want 8)", len(data))
	}
	return binary.LittleEndian.Uint64(data), nil
}
