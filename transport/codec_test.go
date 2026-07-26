package transport

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/PedrowDias/key-value-store/raft"
)

func messagesEqual(a, b raft.Message) bool {
	if a.Type != b.Type || a.From != b.From || a.To != b.To || a.Term != b.Term ||
		a.LastLogIndex != b.LastLogIndex || a.LastLogTerm != b.LastLogTerm ||
		a.VoteGranted != b.VoteGranted || a.PrevLogIndex != b.PrevLogIndex ||
		a.PrevLogTerm != b.PrevLogTerm || a.LeaderCommit != b.LeaderCommit ||
		a.Success != b.Success || a.MatchIndex != b.MatchIndex {
		return false
	}
	if len(a.Entries) != len(b.Entries) {
		return false
	}
	for i := range a.Entries {
		if a.Entries[i].Term != b.Entries[i].Term || a.Entries[i].Index != b.Entries[i].Index {
			return false
		}
		if !bytes.Equal(a.Entries[i].Data, b.Entries[i].Data) {
			return false
		}
	}
	return true
}

func TestCodec_RoundTrip_RequestVote(t *testing.T) {
	m := raft.Message{
		Type: raft.MsgRequestVote, From: 1, To: 2, Term: 5,
		LastLogIndex: 10, LastLogTerm: 4,
	}
	decoded, err := decodeMessage(encodeMessage(m))
	if err != nil {
		t.Fatal(err)
	}
	if !messagesEqual(m, decoded) {
		t.Fatalf("decoded = %+v, want %+v", decoded, m)
	}
}

func TestCodec_RoundTrip_RequestVoteResponse(t *testing.T) {
	m := raft.Message{Type: raft.MsgRequestVoteResponse, From: 2, To: 1, Term: 5, VoteGranted: true}
	decoded, err := decodeMessage(encodeMessage(m))
	if err != nil {
		t.Fatal(err)
	}
	if !messagesEqual(m, decoded) {
		t.Fatalf("decoded = %+v, want %+v", decoded, m)
	}
}

func TestCodec_RoundTrip_AppendEntriesWithEntries(t *testing.T) {
	m := raft.Message{
		Type: raft.MsgAppendEntries, From: 1, To: 2, Term: 3,
		PrevLogIndex: 5, PrevLogTerm: 2,
		Entries: []raft.LogEntry{
			{Term: 3, Index: 6, Data: []byte("hello")},
			{Term: 3, Index: 7, Data: []byte("")},
			{Term: 3, Index: 8, Data: []byte("a longer piece of data to make sure sizes vary")},
		},
		LeaderCommit: 4,
	}
	decoded, err := decodeMessage(encodeMessage(m))
	if err != nil {
		t.Fatal(err)
	}
	if !messagesEqual(m, decoded) {
		t.Fatalf("decoded = %+v, want %+v", decoded, m)
	}
}

func TestCodec_RoundTrip_AppendEntriesEmpty(t *testing.T) {
	// A pure heartbeat: no entries at all.
	m := raft.Message{Type: raft.MsgAppendEntries, From: 1, To: 2, Term: 3, PrevLogIndex: 5, PrevLogTerm: 2, LeaderCommit: 4}
	decoded, err := decodeMessage(encodeMessage(m))
	if err != nil {
		t.Fatal(err)
	}
	if !messagesEqual(m, decoded) {
		t.Fatalf("decoded = %+v, want %+v", decoded, m)
	}
	if len(decoded.Entries) != 0 {
		t.Fatalf("Entries = %+v, want empty", decoded.Entries)
	}
}

func TestCodec_RoundTrip_AppendEntriesResponse(t *testing.T) {
	m := raft.Message{Type: raft.MsgAppendEntriesResponse, From: 2, To: 1, Term: 3, Success: true, MatchIndex: 7}
	decoded, err := decodeMessage(encodeMessage(m))
	if err != nil {
		t.Fatal(err)
	}
	if !messagesEqual(m, decoded) {
		t.Fatalf("decoded = %+v, want %+v", decoded, m)
	}
}

func TestCodec_RoundTrip_ZeroValueMessage(t *testing.T) {
	var m raft.Message
	decoded, err := decodeMessage(encodeMessage(m))
	if err != nil {
		t.Fatal(err)
	}
	if !messagesEqual(m, decoded) {
		t.Fatalf("decoded = %+v, want zero-value message", decoded)
	}
}

// --- Malformed input handling ------------------------------------------------

func TestDecodeMessage_TooShortForFixedFields(t *testing.T) {
	if _, err := decodeMessage(make([]byte, fixedFieldsSize-1)); err == nil {
		t.Fatal("expected an error for data shorter than the fixed fields")
	}
}

func TestDecodeMessage_EntryCountExceedsAvailableData(t *testing.T) {
	valid := encodeMessage(raft.Message{Type: raft.MsgAppendEntries})
	// Overwrite the entry-count field (right after the fixed prefix
	// preceding it) to claim far more entries than actually follow.
	corrupted := append([]byte(nil), valid...)
	countOffset := 1 + 8*5 + 1 + 8*2 // Type + From/To/Term/LastLogIndex/LastLogTerm + VoteGranted + PrevLogIndex/PrevLogTerm
	corrupted[countOffset] = 0xFF
	corrupted[countOffset+1] = 0xFF
	corrupted[countOffset+2] = 0xFF
	corrupted[countOffset+3] = 0x7F
	if _, err := decodeMessage(corrupted); err == nil {
		t.Fatal("expected an error when entry count exceeds available data")
	}
}

func TestDecodeMessage_EntryDataLengthExceedsAvailableData(t *testing.T) {
	m := raft.Message{Type: raft.MsgAppendEntries, Entries: []raft.LogEntry{{Term: 1, Index: 1, Data: []byte("hi")}}}
	valid := encodeMessage(m)
	// Truncate the payload so the entry's declared data length claims
	// more bytes than actually remain.
	truncated := valid[:len(valid)-5]
	if _, err := decodeMessage(truncated); err == nil {
		t.Fatal("expected an error when an entry's data length exceeds available data")
	}
}

func TestDecodeMessage_TruncatedAfterEntries(t *testing.T) {
	m := raft.Message{Type: raft.MsgAppendEntries, Entries: []raft.LogEntry{{Term: 1, Index: 1}}}
	valid := encodeMessage(m)
	// Cut off the trailing LeaderCommit/Success/MatchIndex fields.
	truncated := valid[:len(valid)-3]
	if _, err := decodeMessage(truncated); err == nil {
		t.Fatal("expected an error when trailing fields are truncated")
	}
}

func TestDecodeMessage_LaterEntryHeaderStarvedBySpaceHungryEarlierEntry(t *testing.T) {
	// Two entries; the coarse "count vs. total remaining bytes" bound
	// passes, but the FIRST entry's (corrupted) declared data length
	// consumes so much of the buffer that the SECOND entry's own header
	// can't fit in what's left — a distinct, later check than the
	// initial coarse bound, only reachable via genuinely malformed
	// bytes rather than simple truncation (which trims the end, not
	// redistributes space earlier in the buffer).
	m := raft.Message{
		Type: raft.MsgAppendEntries,
		Entries: []raft.LogEntry{
			{Term: 1, Index: 1, Data: []byte("short")},      // 5 bytes
			{Term: 1, Index: 2, Data: []byte("also-short")}, // 10 bytes
		},
	}
	valid := encodeMessage(m)

	const headerBeforeEntries = 1 + 8*5 + 1 + 8*2 + 4
	dataLenOffset := headerBeforeEntries + 8 + 8 // past this entry's Term, Index

	// Corrupt the first entry's declared data length from 5 to 40. This
	// must (a) still pass THIS entry's own "declared length <= all
	// remaining bytes" check — 40 is comfortably under the ~52 bytes
	// left in the buffer at that point — while (b) consuming 35 bytes
	// more than the real 5, which is exactly enough to eat into the
	// space the second entry's 20-byte header needs, without changing
	// the buffer's total length at all.
	corrupted := append([]byte(nil), valid...)
	binary.LittleEndian.PutUint32(corrupted[dataLenOffset:], 40)

	if _, err := decodeMessage(corrupted); err == nil {
		t.Fatal("expected an error when an earlier entry's inflated data length starves a later entry's header")
	}
}

func TestDecodeMessage_EntryDataLengthExceedsRemainingWithinBuffer(t *testing.T) {
	// A single entry whose declared data length is corrupted to exceed
	// what's actually present, WITHOUT truncating the buffer (so this
	// hits the entry's own data-length check specifically, not the
	// trailing-fixed-fields check that a simple end-truncation would
	// hit instead).
	m := raft.Message{
		Type:    raft.MsgAppendEntries,
		Entries: []raft.LogEntry{{Term: 1, Index: 1, Data: []byte("hi")}},
	}
	valid := encodeMessage(m)
	const headerBeforeEntries = 1 + 8*5 + 1 + 8*2 + 4
	dataLenOffset := headerBeforeEntries + 8 + 8
	corrupted := append([]byte(nil), valid...)
	binary.LittleEndian.PutUint32(corrupted[dataLenOffset:], 1000)

	if _, err := decodeMessage(corrupted); err == nil {
		t.Fatal("expected an error when an entry's data length exceeds what's actually present")
	}
}

// --- Framing ------------------------------------------------------------------

func TestFramed_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte("some framed payload")
	if err := writeFramed(&buf, payload); err != nil {
		t.Fatal(err)
	}
	got, err := readFramed(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("got %q, want %q", got, payload)
	}
}

func TestFramed_EmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	if err := writeFramed(&buf, nil); err != nil {
		t.Fatal(err)
	}
	got, err := readFramed(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("got %v, want empty", got)
	}
}

func TestReadFramed_EOFOnEmptyStream(t *testing.T) {
	var buf bytes.Buffer
	_, err := readFramed(&buf)
	if err == nil {
		t.Fatal("expected an error (EOF) reading from an empty stream")
	}
}

func TestReadFramed_TruncatedPayload(t *testing.T) {
	var buf bytes.Buffer
	writeFramed(&buf, []byte("hello world"))
	truncated := buf.Bytes()[:6] // length prefix + a few payload bytes, not all
	_, err := readFramed(bytes.NewReader(truncated))
	if err == nil {
		t.Fatal("expected an error reading a truncated frame payload")
	}
}

func TestReadFramed_ExceedsMaxSize(t *testing.T) {
	var lenBuf [4]byte
	// Encode a length far beyond maxMessageSize.
	lenBuf[0], lenBuf[1], lenBuf[2], lenBuf[3] = 0xFF, 0xFF, 0xFF, 0x7F
	_, err := readFramed(bytes.NewReader(lenBuf[:]))
	if err == nil {
		t.Fatal("expected an error for a frame length exceeding the maximum")
	}
}

// failAfterWriter fails its Write call after a configured number of
// prior successful writes, used to exercise writeFramed's two distinct
// write error branches (the length prefix write, and the payload write).
type failAfterWriter struct {
	allowedWrites int
}

func (w *failAfterWriter) Write(p []byte) (int, error) {
	if w.allowedWrites <= 0 {
		return 0, fmt.Errorf("failAfterWriter: simulated write failure")
	}
	w.allowedWrites--
	return len(p), nil
}

func TestWriteFramed_LengthPrefixWriteErrorPropagates(t *testing.T) {
	w := &failAfterWriter{allowedWrites: 0}
	if err := writeFramed(w, []byte("payload")); err == nil {
		t.Fatal("expected an error when writing the length prefix fails")
	}
}

func TestWriteFramed_PayloadWriteErrorPropagates(t *testing.T) {
	w := &failAfterWriter{allowedWrites: 1} // length prefix succeeds, payload write fails
	if err := writeFramed(w, []byte("payload")); err == nil {
		t.Fatal("expected an error when writing the payload fails")
	}
}
