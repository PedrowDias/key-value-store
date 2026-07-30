package transport

import (
	"testing"

	"github.com/PedrowDias/key-value-store/raft"
)

func TestCodec_RoundTrip_InstallSnapshot(t *testing.T) {
	m := raft.Message{
		Type: raft.MsgInstallSnapshot, From: 1, To: 2, Term: 4,
		Snapshot: raft.Snapshot{LastIncludedIndex: 100, LastIncludedTerm: 3, Data: []byte("the-actual-snapshot-bytes")},
	}
	decoded, err := decodeMessage(encodeMessage(m))
	if err != nil {
		t.Fatal(err)
	}
	if !messagesEqual(m, decoded) {
		t.Fatalf("decoded = %+v, want %+v", decoded, m)
	}
}

func TestCodec_RoundTrip_InstallSnapshotResponse(t *testing.T) {
	m := raft.Message{Type: raft.MsgInstallSnapshotResponse, From: 2, To: 1, Term: 4, MatchIndex: 100}
	decoded, err := decodeMessage(encodeMessage(m))
	if err != nil {
		t.Fatal(err)
	}
	if !messagesEqual(m, decoded) {
		t.Fatalf("decoded = %+v, want %+v", decoded, m)
	}
}

func TestCodec_RoundTrip_EmptySnapshot(t *testing.T) {
	// A Snapshot with no Data at all (the zero value) must still
	// round-trip cleanly — distinct from the "some real Data" case
	// above, since a zero-length data section exercises the boundary of
	// the length-prefixed encoding differently.
	m := raft.Message{Type: raft.MsgInstallSnapshot, From: 1, To: 2, Term: 1}
	decoded, err := decodeMessage(encodeMessage(m))
	if err != nil {
		t.Fatal(err)
	}
	if !messagesEqual(m, decoded) {
		t.Fatalf("decoded = %+v, want %+v", decoded, m)
	}
}

func TestCodec_RoundTrip_ReadContext(t *testing.T) {
	// messagesEqual now checks ReadContext (it silently didn't before —
	// see its own update); this exercises that the field itself actually
	// survives encode/decode, not just that the comparison can detect it
	// if it didn't.
	m := raft.Message{Type: raft.MsgAppendEntries, From: 1, To: 2, Term: 1, ReadContext: 42}
	decoded, err := decodeMessage(encodeMessage(m))
	if err != nil {
		t.Fatal(err)
	}
	if !messagesEqual(m, decoded) {
		t.Fatalf("decoded = %+v, want %+v", decoded, m)
	}
	if decoded.ReadContext != 42 {
		t.Fatalf("decoded.ReadContext = %d, want 42", decoded.ReadContext)
	}
}
