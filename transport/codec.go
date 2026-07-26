// Package transport connects real Raft nodes over TCP: it turns
// raft.Message values into bytes on a wire and back, and manages the
// actual socket connections between peers. raft.Raft/raft.Node know
// nothing about any of this — they only produce and consume
// raft.Message values, which is exactly the seam this package plugs
// into.
package transport

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/PedrowDias/key-value-store/raft"
)

// Wire format for one message (everything after the 4-byte length
// prefix framing adds):
//
//	[1B  Type]
//	[8B  From]        [8B  To]          [8B  Term]
//	[8B  LastLogIndex][8B  LastLogTerm] [1B  VoteGranted]
//	[8B  PrevLogIndex][8B  PrevLogTerm]
//	[4B  entry count][entries...]
//	[8B  LeaderCommit][1B Success][8B MatchIndex]
//
// Each entry: [8B Term][8B Index][4B data length][data].
//
// Every field is always present regardless of message Type (a
// RequestVote, say, encodes unused AppendEntries fields as zero) —
// simpler and more robust than a variable layout per type, at the cost
// of a few wasted bytes per message. A reasonable target to revisit in
// the benchmarking phase if per-message overhead ever shows up as a
// real cost; not worth the complexity until it does.
const (
	fixedFieldsSize = 1 + 8 + 8 + 8 + 8 + 8 + 1 + 8 + 8 + 4 + 8 + 1 + 8
	entryHeaderSize = 8 + 8 + 4
)

var errMalformedMessage = fmt.Errorf("transport: malformed message")

func encodeMessage(m raft.Message) []byte {
	size := fixedFieldsSize
	for _, e := range m.Entries {
		size += entryHeaderSize + len(e.Data)
	}
	buf := make([]byte, size)
	off := 0

	buf[off] = byte(m.Type)
	off++
	off += putUint64(buf[off:], m.From)
	off += putUint64(buf[off:], m.To)
	off += putUint64(buf[off:], m.Term)
	off += putUint64(buf[off:], m.LastLogIndex)
	off += putUint64(buf[off:], m.LastLogTerm)
	buf[off] = boolByte(m.VoteGranted)
	off++
	off += putUint64(buf[off:], m.PrevLogIndex)
	off += putUint64(buf[off:], m.PrevLogTerm)

	binary.LittleEndian.PutUint32(buf[off:], uint32(len(m.Entries)))
	off += 4
	for _, e := range m.Entries {
		off += putUint64(buf[off:], e.Term)
		off += putUint64(buf[off:], e.Index)
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(e.Data)))
		off += 4
		off += copy(buf[off:], e.Data)
	}

	off += putUint64(buf[off:], m.LeaderCommit)
	buf[off] = boolByte(m.Success)
	off++
	off += putUint64(buf[off:], m.MatchIndex)

	return buf
}

func decodeMessage(data []byte) (raft.Message, error) {
	if len(data) < fixedFieldsSize {
		return raft.Message{}, errMalformedMessage
	}
	var m raft.Message
	off := 0

	m.Type = raft.MessageType(data[off])
	off++
	m.From, off = getUint64(data, off)
	m.To, off = getUint64(data, off)
	m.Term, off = getUint64(data, off)
	m.LastLogIndex, off = getUint64(data, off)
	m.LastLogTerm, off = getUint64(data, off)
	m.VoteGranted = data[off] != 0
	off++
	m.PrevLogIndex, off = getUint64(data, off)
	m.PrevLogTerm, off = getUint64(data, off)

	count := binary.LittleEndian.Uint32(data[off:])
	off += 4

	// Bound count against what could possibly remain before allocating
	// anything: each entry needs at least entryHeaderSize bytes, so a
	// count implying more than that is definitely corrupt or malicious.
	// Validating BEFORE allocating matters — trusting an attacker- or
	// corruption-controlled count directly as a slice length lets a
	// single crafted length field trigger a multi-gigabyte allocation
	// and crash the process via OOM, independent of how much data
	// actually follows on the wire.
	maxPossibleEntries := uint32(len(data)-off) / entryHeaderSize
	if count > maxPossibleEntries {
		return raft.Message{}, errMalformedMessage
	}

	if count > 0 {
		m.Entries = make([]raft.LogEntry, count)
	}
	for i := 0; i < int(count); i++ {
		if len(data)-off < entryHeaderSize {
			return raft.Message{}, errMalformedMessage
		}
		var e raft.LogEntry
		e.Term, off = getUint64(data, off)
		e.Index, off = getUint64(data, off)
		dlen := binary.LittleEndian.Uint32(data[off:])
		off += 4
		if uint32(len(data)-off) < dlen {
			return raft.Message{}, errMalformedMessage
		}
		if dlen > 0 {
			e.Data = append([]byte(nil), data[off:off+int(dlen)]...)
		}
		off += int(dlen)
		m.Entries[i] = e
	}

	if len(data)-off < 8+1+8 {
		return raft.Message{}, errMalformedMessage
	}
	m.LeaderCommit, off = getUint64(data, off)
	m.Success = data[off] != 0
	off++
	m.MatchIndex, off = getUint64(data, off)

	return m, nil
}

func putUint64(buf []byte, v uint64) int {
	binary.LittleEndian.PutUint64(buf, v)
	return 8
}

func getUint64(data []byte, off int) (uint64, int) {
	return binary.LittleEndian.Uint64(data[off:]), off + 8
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// --- framing: length-prefixed messages over a stream connection ------------

const maxMessageSize = 64 * 1024 * 1024 // 64 MiB: generous, but bounds a malicious/corrupt length prefix from causing an unbounded allocation

// writeFramed writes one length-prefixed message to w.
func writeFramed(w io.Writer, payload []byte) error {
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("transport: writing frame length: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("transport: writing frame payload: %w", err)
	}
	return nil
}

// readFramed reads one length-prefixed message from r.
func readFramed(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err // includes io.EOF for a clean connection close; callers check for that
	}
	length := binary.LittleEndian.Uint32(lenBuf[:])
	if length > maxMessageSize {
		return nil, fmt.Errorf("transport: frame length %d exceeds max %d", length, maxMessageSize)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("transport: reading frame payload: %w", err)
	}
	return payload, nil
}
