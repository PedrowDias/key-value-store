// Persistence for Raft's durable state: currentTerm, votedFor, the log
// entries themselves, and — since compaction (see Raft.CreateSnapshot's
// doc) — the current snapshot boundary. Built directly on the wal
// package — the same write-ahead log used by the storage engine —
// rather than inventing a second crash-recovery scheme: a torn write
// here needs exactly the same handling (detect it, discard the
// incomplete tail, keep everything before it) that the KV engine's WAL
// already implements and tests thoroughly.
//
// Four record shapes are multiplexed onto wal.Record's opaque Value
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
//   - snapshot: {LastIncludedIndex, LastIncludedTerm, Data} — a
//     CreateSnapshot boundary. Unlike the other three, this isn't just
//     appended: SaveSnapshot rewrites the entire storage file from
//     scratch (HardState, this snapshot record, then only the log
//     entries that survive past it) and atomically replaces the old one
//     — the entire point of compaction is bounding this file's own
//     on-disk size, not just the in-memory log's, so entries the
//     snapshot now covers need to actually stop existing on disk, not
//     merely be marked as ignorable during replay.
package raft

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/PedrowDias/key-value-store/wal"
)

type recordKind byte

const (
	recordHardState recordKind = iota
	recordLogEntry
	recordTruncateFrom
	recordSnapshot
)

// PersistentStorage durably stores a Raft node's HardState, log, and
// current snapshot boundary via a write-ahead log on disk.
type PersistentStorage struct {
	w    *wal.WAL
	seq  uint64
	path string
}

// OpenStorage opens (creating if necessary) the storage log at path,
// replaying any existing records to reconstruct the HardState, log, and
// current snapshot boundary (if any) as of the last successful persist.
// The returned log always includes a dummy sentinel entry as its first
// element — (Index 0, Term 0) if no snapshot has ever been taken, or
// (Snapshot.LastIncludedIndex, Snapshot.LastIncludedTerm) if one has —
// matching Raft's own invariant, ready to hand directly to restoreState.
// The returned Snapshot is the zero value if none was ever persisted;
// otherwise the caller (raft.OpenNode, and beyond it the application)
// needs its Data to actually restore a state machine that no longer has
// enough log history to replay from scratch.
func OpenStorage(path string) (*PersistentStorage, HardState, []LogEntry, Snapshot, error) {
	records, _, err := wal.Replay(path)
	if err != nil {
		return nil, HardState{}, nil, Snapshot{}, fmt.Errorf("raft: replaying storage log: %w", err)
	}

	var hs HardState
	var log []LogEntry
	var snap Snapshot
	var maxSeq uint64

	for _, rec := range records {
		if rec.SeqNum > maxSeq {
			maxSeq = rec.SeqNum
		}
		kind, payload, derr := decodeRecord(rec.Value)
		if derr != nil {
			return nil, HardState{}, nil, Snapshot{}, fmt.Errorf("raft: decoding persisted record: %w", derr)
		}
		switch kind {
		case recordHardState:
			term, vote, herr := decodeHardStatePayload(payload)
			if herr != nil {
				return nil, HardState{}, nil, Snapshot{}, herr
			}
			hs = HardState{Term: term, Vote: vote}
		case recordLogEntry:
			e, eerr := decodeLogEntryPayload(payload)
			if eerr != nil {
				return nil, HardState{}, nil, Snapshot{}, eerr
			}
			log = append(log, e)
		case recordTruncateFrom:
			from, terr := decodeTruncateFromPayload(payload)
			if terr != nil {
				return nil, HardState{}, nil, Snapshot{}, terr
			}
			kept := log[:0]
			for _, e := range log {
				if e.Index < from {
					kept = append(kept, e)
				}
			}
			log = kept
		case recordSnapshot:
			s, serr := decodeSnapshotPayload(payload)
			if serr != nil {
				return nil, HardState{}, nil, Snapshot{}, serr
			}
			snap = s
			// Everything at or before the snapshot boundary is gone —
			// a snapshot supersedes whatever entries existed there
			// before, matching CreateSnapshot's own in-memory
			// truncation. In practice SaveSnapshot rewrites the whole
			// file so no earlier entries are normally even present by
			// the time this replays, but staying correct here doesn't
			// depend on that.
			kept := log[:0]
			for _, e := range log {
				if e.Index > snap.LastIncludedIndex {
					kept = append(kept, e)
				}
			}
			log = kept
		default:
			return nil, HardState{}, nil, Snapshot{}, fmt.Errorf("raft: unknown persisted record kind %d", kind)
		}
	}

	var sentinel LogEntry
	if snap.LastIncludedIndex > 0 {
		sentinel = LogEntry{Index: snap.LastIncludedIndex, Term: snap.LastIncludedTerm}
	} else {
		sentinel = LogEntry{Term: 0, Index: 0}
	}
	fullLog := append([]LogEntry{sentinel}, log...)

	w, err := openWALLog(path, wal.Options{SyncOnWrite: true})
	if err != nil {
		return nil, HardState{}, nil, Snapshot{}, fmt.Errorf("raft: opening storage log: %w", err)
	}

	return &PersistentStorage{w: w, seq: maxSeq, path: path}, hs, fullLog, snap, nil
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

// SaveSnapshot durably persists a new snapshot boundary, compacting the
// storage log in the process: unlike SaveHardState/SaveEntries, this
// doesn't just append — the entire point of compaction (see
// Raft.CreateSnapshot's doc) is bounding this file's own on-disk size,
// not just the in-memory log's, so this rewrites the whole file from
// scratch (hs, then the snapshot record, then only survivingEntries —
// whatever log entries are still needed after the new boundary) into a
// temporary file and atomically replaces the original with it.
//
// If this crashes at any point before the atomic rename completes, the
// ORIGINAL file is untouched and still fully valid on its own — recovery
// just proceeds without having benefited from this particular
// compaction attempt yet, a safe, conservative failure mode rather than
// a corruption risk. A leftover temp file from a previous crashed
// attempt is removed (best-effort) before starting a fresh one.
func (s *PersistentStorage) SaveSnapshot(hs HardState, snap Snapshot, survivingEntries []LogEntry) error {
	tmpPath := s.path + ".compact.tmp"
	removeFile(tmpPath)

	tmpW, err := openWALLog(tmpPath, wal.Options{SyncOnWrite: true})
	if err != nil {
		return fmt.Errorf("raft: opening compaction temp file: %w", err)
	}

	var tmpSeq uint64
	nextTmpSeq := func() uint64 {
		tmpSeq++
		return tmpSeq
	}

	if err := tmpW.Append(wal.Record{
		SeqNum: nextTmpSeq(),
		Type:   wal.RecordPut,
		Value:  encodeRecord(recordHardState, encodeHardStatePayload(hs.Term, hs.Vote)),
	}); err != nil {
		// Accepted gap: Append/Close failing on a WAL this same
		// function just successfully opened moments ago isn't
		// portably triggerable against a real filesystem — the same
		// class of OS-level branch this project has consistently left
		// untested elsewhere (e.g. sstable's Stat/ReadAt failures on
		// an already-opened fd, or wal.Replay's own truncation-not-
		// error contract). Every such branch below shares this note.
		tmpW.Close()
		removeFile(tmpPath)
		return fmt.Errorf("raft: writing hard state to compaction temp file: %w", err)
	}

	if err := tmpW.Append(wal.Record{
		SeqNum: nextTmpSeq(),
		Type:   wal.RecordPut,
		Value:  encodeRecord(recordSnapshot, encodeSnapshotPayload(snap)),
	}); err != nil {
		tmpW.Close()
		removeFile(tmpPath)
		return fmt.Errorf("raft: writing snapshot to compaction temp file: %w", err)
	}

	if len(survivingEntries) > 0 {
		recs := make([]wal.Record, len(survivingEntries))
		for i, e := range survivingEntries {
			recs[i] = wal.Record{
				SeqNum: nextTmpSeq(),
				Type:   wal.RecordPut,
				Value:  encodeRecord(recordLogEntry, encodeLogEntryPayload(e)),
			}
		}
		if err := tmpW.AppendBatch(recs); err != nil {
			tmpW.Close()
			removeFile(tmpPath)
			return fmt.Errorf("raft: writing surviving entries to compaction temp file: %w", err)
		}
	}

	if err := tmpW.Close(); err != nil {
		removeFile(tmpPath)
		return fmt.Errorf("raft: closing compaction temp file: %w", err)
	}

	if err := renameFile(tmpPath, s.path); err != nil {
		return fmt.Errorf("raft: replacing storage log with its compacted version: %w", err)
	}

	// The rename is already durable at this point; re-pointing our own
	// handle at the (now-compacted) file under its original path is
	// just so subsequent SaveHardState/SaveEntries calls keep appending
	// to the right place.
	if err := s.w.Close(); err != nil {
		return fmt.Errorf("raft: closing pre-compaction storage log handle: %w", err)
	}
	newW, err := openWALLog(s.path, wal.Options{SyncOnWrite: true})
	if err != nil {
		return fmt.Errorf("raft: reopening storage log after compaction: %w", err)
	}
	s.w = newW
	s.seq = tmpSeq

	return nil
}

// removeFile and renameFile are package-level indirections over
// os.Remove/os.Rename purely so tests can simulate either failing at a
// precise point during SaveSnapshot's compaction — not practical to
// trigger against a real filesystem on demand.
var removeFile = func(path string) error { return os.Remove(path) }
var renameFile = func(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }

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

func encodeSnapshotPayload(snap Snapshot) []byte {
	buf := make([]byte, 0, 20+len(snap.Data))
	var b8 [8]byte
	binary.LittleEndian.PutUint64(b8[:], snap.LastIncludedIndex)
	buf = append(buf, b8[:]...)
	binary.LittleEndian.PutUint64(b8[:], snap.LastIncludedTerm)
	buf = append(buf, b8[:]...)
	var b4 [4]byte
	binary.LittleEndian.PutUint32(b4[:], uint32(len(snap.Data)))
	buf = append(buf, b4[:]...)
	return append(buf, snap.Data...)
}

func decodeSnapshotPayload(data []byte) (Snapshot, error) {
	if len(data) < 20 {
		return Snapshot{}, fmt.Errorf("raft: malformed snapshot record (%d bytes, want >= 20)", len(data))
	}
	index := binary.LittleEndian.Uint64(data[0:8])
	term := binary.LittleEndian.Uint64(data[8:16])
	dlen := binary.LittleEndian.Uint32(data[16:20])
	if uint32(len(data)-20) != dlen {
		return Snapshot{}, fmt.Errorf("raft: snapshot data length mismatch: header says %d, have %d", dlen, len(data)-20)
	}
	var d []byte
	if dlen > 0 {
		d = append([]byte(nil), data[20:20+dlen]...)
	}
	return Snapshot{LastIncludedIndex: index, LastIncludedTerm: term, Data: d}, nil
}
