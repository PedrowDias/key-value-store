// Package server ties raft.Node, transport.Transport, and engine.Engine
// together into an actual running cluster node: client Put/Delete calls
// become proposed Raft log entries, and once (and only once) Raft
// reports them committed, they're applied to the local storage engine —
// the standard "replicated state machine" pattern. Every node in the
// cluster runs the same sequence of committed commands in the same
// order, which is what makes them end up with the same data.
package server

import (
	"encoding/binary"
	"fmt"
)

// commandType distinguishes a Put from a Delete inside a proposed Raft
// log entry's opaque Data payload.
type commandType byte

const (
	cmdPut commandType = iota
	cmdDelete
)

// command is one client operation, proposed as a single Raft log entry.
type command struct {
	Type  commandType
	Key   []byte
	Value []byte // unused for cmdDelete
}

// encodeCommand serializes a command for use as a raft.LogEntry's Data.
// Format: [1B type][4B keyLen][key][4B valLen][value].
func encodeCommand(c command) []byte {
	buf := make([]byte, 0, 1+4+len(c.Key)+4+len(c.Value))
	buf = append(buf, byte(c.Type))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(c.Key)))
	buf = append(buf, c.Key...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(c.Value)))
	buf = append(buf, c.Value...)
	return buf
}

var errMalformedCommand = fmt.Errorf("server: malformed command")

func decodeCommand(data []byte) (command, error) {
	if len(data) < 1+4 {
		return command{}, errMalformedCommand
	}
	var c command
	c.Type = commandType(data[0])
	off := 1

	keyLen := binary.LittleEndian.Uint32(data[off:])
	off += 4
	if uint32(len(data)-off) < keyLen {
		return command{}, errMalformedCommand
	}
	if keyLen > 0 {
		c.Key = append([]byte(nil), data[off:off+int(keyLen)]...)
	}
	off += int(keyLen)

	if len(data)-off < 4 {
		return command{}, errMalformedCommand
	}
	valLen := binary.LittleEndian.Uint32(data[off:])
	off += 4
	if uint32(len(data)-off) < valLen {
		return command{}, errMalformedCommand
	}
	if valLen > 0 {
		c.Value = append([]byte(nil), data[off:off+int(valLen)]...)
	}

	return c, nil
}
