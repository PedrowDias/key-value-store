package memtable

import (
	"bytes"
	"sort"
)

// Iterator provides ordered, forward-only traversal of a Memtable's
// entries, in sorted key order — exactly the order an SSTable file needs
// its entries written in, and exactly the order a range scan needs to
// return them in.
//
// It is a point-in-time snapshot taken when NewIterator is called: the
// entry data is copied out under a read lock up front rather than walked
// live, so it's safe to hold and use even while other goroutines continue
// writing to the memtable, and its view never changes mid-iteration —
// including a later Put that overwrites an existing key's value in
// place, which is why the iterator copies out each entry's fields rather
// than keeping pointers into the live skip-list nodes. This mirrors how
// real LSM engines treat a memtable being flushed: RocksDB "switches" a
// memtable to immutable before handing it to a flush goroutine, for the
// same reason — a flush needs a stable, complete view.
type Iterator struct {
	entries []entrySnapshot
	idx     int // -1 before first use; points at entries[idx] once positioned
}

type entrySnapshot struct {
	key     []byte
	value   []byte
	deleted bool
	seq     uint64
}

// NewIterator returns an Iterator positioned before the first entry; call
// SeekToFirst (or Seek) before reading.
func (m *Memtable) NewIterator() *Iterator {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([]entrySnapshot, 0, m.list.length)
	for n := m.list.head.forward[0]; n != nil; n = n.forward[0] {
		entries = append(entries, entrySnapshot{key: n.key, value: n.value, deleted: n.deleted, seq: n.seq})
	}
	return &Iterator{entries: entries, idx: -1}
}

// SeekToFirst positions the iterator at the smallest key.
func (it *Iterator) SeekToFirst() {
	it.idx = 0
}

// Seek positions the iterator at the smallest key >= target. If no such
// key exists, the iterator becomes invalid (as if iteration ran off the
// end).
func (it *Iterator) Seek(target []byte) {
	it.idx = sort.Search(len(it.entries), func(i int) bool {
		return bytes.Compare(it.entries[i].key, target) >= 0
	})
}

// Valid reports whether the iterator is currently positioned at a valid
// entry.
func (it *Iterator) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.entries)
}

// Next advances to the next entry. Calling it when !Valid() is a no-op.
func (it *Iterator) Next() {
	if it.idx < len(it.entries) {
		it.idx++
	}
}

// Key returns the current entry's key. Only valid when Valid() is true.
func (it *Iterator) Key() []byte {
	return it.entries[it.idx].key
}

// Value returns the current entry's value (meaningless/nil for a
// tombstone). Only valid when Valid() is true.
func (it *Iterator) Value() []byte {
	return it.entries[it.idx].value
}

// Deleted reports whether the current entry is a tombstone. Only valid
// when Valid() is true.
func (it *Iterator) Deleted() bool {
	return it.entries[it.idx].deleted
}

// SeqNum returns the current entry's sequence number. Only valid when
// Valid() is true.
func (it *Iterator) SeqNum() uint64 {
	return it.entries[it.idx].seq
}
