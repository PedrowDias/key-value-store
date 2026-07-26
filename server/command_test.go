package server

import (
	"bytes"
	"testing"
)

func TestCommand_RoundTrip_Put(t *testing.T) {
	c := command{Type: cmdPut, Key: []byte("k"), Value: []byte("v")}
	decoded, err := decodeCommand(encodeCommand(c))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Type != cmdPut || !bytes.Equal(decoded.Key, c.Key) || !bytes.Equal(decoded.Value, c.Value) {
		t.Fatalf("decoded = %+v, want %+v", decoded, c)
	}
}

func TestCommand_RoundTrip_Delete(t *testing.T) {
	c := command{Type: cmdDelete, Key: []byte("k")}
	decoded, err := decodeCommand(encodeCommand(c))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Type != cmdDelete || !bytes.Equal(decoded.Key, c.Key) {
		t.Fatalf("decoded = %+v, want %+v", decoded, c)
	}
	if len(decoded.Value) != 0 {
		t.Fatalf("decoded.Value = %v, want empty for a delete", decoded.Value)
	}
}

func TestCommand_RoundTrip_EmptyKeyAndValue(t *testing.T) {
	c := command{Type: cmdPut, Key: []byte{}, Value: []byte{}}
	decoded, err := decodeCommand(encodeCommand(c))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Key) != 0 || len(decoded.Value) != 0 {
		t.Fatalf("decoded = %+v, want empty key/value", decoded)
	}
}

func TestDecodeCommand_TooShortForHeader(t *testing.T) {
	if _, err := decodeCommand([]byte{0, 1, 2}); err == nil {
		t.Fatal("expected an error for data shorter than the fixed header")
	}
}

func TestDecodeCommand_KeyLengthExceedsAvailableData(t *testing.T) {
	// type(1) + keyLen claiming 100 bytes, but nothing follows.
	data := append([]byte{byte(cmdPut)}, encodeUint32ForTest(100)...)
	if _, err := decodeCommand(data); err == nil {
		t.Fatal("expected an error when keyLen exceeds available data")
	}
}

func TestDecodeCommand_MissingValueLengthField(t *testing.T) {
	// type(1) + keyLen(0): valid so far, but nothing left for valLen.
	data := append([]byte{byte(cmdPut)}, encodeUint32ForTest(0)...)
	if _, err := decodeCommand(data); err == nil {
		t.Fatal("expected an error when the value-length field itself is missing")
	}
}

func TestDecodeCommand_ValueLengthExceedsAvailableData(t *testing.T) {
	data := append([]byte{byte(cmdPut)}, encodeUint32ForTest(0)...) // keyLen=0
	data = append(data, encodeUint32ForTest(100)...)                // valLen=100, nothing follows
	if _, err := decodeCommand(data); err == nil {
		t.Fatal("expected an error when valLen exceeds available data")
	}
}

func encodeUint32ForTest(v uint32) []byte {
	return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)}
}
