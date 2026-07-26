package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// RecoveryStats reports what Replay found, mainly for logging/observability
// and for tests to assert on.
type RecoveryStats struct {
	RecordsRecovered int
	// TornWriteTruncated is true if replay stopped early because it found
	// an incomplete or checksum-mismatched record at the tail and
	// truncated the file to discard it. This is the expected, non-error
	// outcome of a crash that occurred mid-append.
	TornWriteTruncated bool
	// BytesTruncated is how many trailing bytes were discarded.
	BytesTruncated int64
}

// replaySource is the subset of *os.File operations the core replay loop
// needs. Defined as an interface purely so tests can inject a fake that
// fails a Read or a Truncate at a precise point — both are otherwise
// impractical to trigger deterministically against a real file (a mid-
// stream read error needs a faulty disk or device; Truncate essentially
// never fails on a normal writable file you already have open).
type replaySource interface {
	io.Reader
	Truncate(size int64) error
}

// Replay reads every well-formed record from the WAL file at path, in the
// order they were written, and returns them along with recovery stats.
//
// Crash-safety contract: a WAL is only ever appended to by a single writer,
// so the sole way a record can be malformed is if the process crashed (or
// the machine lost power) partway through writing it — the header claims
// more bytes than exist, or the payload's checksum doesn't match because
// only some of its bytes made it to disk before the crash. Replay treats
// the FIRST such malformed record as the torn tail: it stops there,
// discards that record and everything after it (truncating the file on
// disk to match), and returns every valid record that preceded it. This
// mirrors the recovery behavior of LevelDB/RocksDB write-ahead logs.
//
// A record that decodes and checksums cleanly is trusted completely —
// Replay does not attempt to detect bit-rot in already-fsynced, validly
// framed records; that is a different concern (e.g. block-level scrubbing
// or checksums over the whole file), out of scope for a per-record WAL.
func Replay(path string) ([]Record, RecoveryStats, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if os.IsNotExist(err) {
		return nil, RecoveryStats{}, nil
	}
	if err != nil {
		return nil, RecoveryStats{}, fmt.Errorf("wal: open for replay: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, RecoveryStats{}, fmt.Errorf("wal: stat: %w", err)
	}

	return replayFrom(f, info.Size())
}

// replayFrom holds the actual replay loop, decoupled from file-opening so
// it can be driven by a fake source in tests.
func replayFrom(f replaySource, fileSize int64) ([]Record, RecoveryStats, error) {
	var records []Record
	var offset int64
	header := make([]byte, headerSize)

	for {
		n, err := io.ReadFull(f, header)
		if err == io.EOF {
			// Clean end: no partial header at all. Nothing to truncate.
			break
		}
		if err == io.ErrUnexpectedEOF || (err != nil && n > 0 && n < headerSize) {
			// Fewer than headerSize bytes remain: a torn write caught
			// mid-header.
			return truncateAndReturn(f, records, offset, fileSize)
		}
		if err != nil {
			return nil, RecoveryStats{}, fmt.Errorf("wal: read header at offset %d: %w", offset, err)
		}

		crc := binary.LittleEndian.Uint32(header[0:4])
		length := binary.LittleEndian.Uint32(header[4:8])

		// Sanity bound: a claimed length larger than the remaining file
		// can only happen if the header itself was the last thing
		// fsynced before a crash, with the payload never (fully) written.
		remaining := fileSize - offset - headerSize
		if int64(length) > remaining {
			return truncateAndReturn(f, records, offset, fileSize)
		}

		payload := make([]byte, length)
		n, err = io.ReadFull(f, payload)
		if err != nil || n < int(length) {
			return truncateAndReturn(f, records, offset, fileSize)
		}

		if crc32.Checksum(payload, crcTable) != crc {
			// Torn write: header landed on disk but payload bytes did
			// not (or were only partially written) before the crash.
			return truncateAndReturn(f, records, offset, fileSize)
		}

		rec, err := decodePayload(payload)
		if err != nil {
			// Checksum matched but payload is structurally invalid.
			// Extremely unlikely (would need a CRC collision), but
			// treat it the same way: don't trust it, truncate here.
			return truncateAndReturn(f, records, offset, fileSize)
		}

		records = append(records, rec)
		offset += headerSize + int64(length)
	}

	return records, RecoveryStats{RecordsRecovered: len(records)}, nil
}

// truncateAndReturn discards everything from validOffset onward, both from
// the returned record set (already implicit, since we stop appending) and
// from the on-disk file, so that future appends continue cleanly right
// after the last valid record instead of leaving a corrupt gap.
func truncateAndReturn(f replaySource, records []Record, validOffset, fileSize int64) ([]Record, RecoveryStats, error) {
	truncated := fileSize - validOffset
	if err := f.Truncate(validOffset); err != nil {
		return nil, RecoveryStats{}, fmt.Errorf("wal: truncate torn tail: %w", err)
	}
	return records, RecoveryStats{
		RecordsRecovered:   len(records),
		TornWriteTruncated: truncated > 0,
		BytesTruncated:     truncated,
	}, nil
}
