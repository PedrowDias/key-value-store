package memtable

import (
	"bytes"
	"math/rand"
)

// A skip list is a linked structure with multiple "levels" of forward
// pointers: level 0 links every entry in sorted order, and each higher
// level skips over a geometrically-decreasing fraction of entries,
// letting search/insert/delete jump ahead instead of scanning linearly.
// Expected time for all three operations is O(log n), matching a balanced
// tree, but the implementation is a fraction of the code and needs no
// rebalancing — which is why LevelDB, RocksDB, and Redis all use one for
// exactly this role (an ordered, mutable, in-memory buffer of recent
// writes).
const (
	maxHeight = 16   // supports ~ p^-maxHeight = 4^16 entries before height is a bottleneck
	p         = 0.25 // probability of promoting a node to the next level up
)

// nodeOverheadBytes is a rough per-entry accounting of the Go runtime
// overhead a skip list node carries beyond its raw key/value bytes
// (struct headers, the forward-pointer slice, slice headers for key and
// value). It's an estimate, not a precise measurement, but it's enough to
// make ApproxSize a reasonable trigger for "this memtable is big enough
// to flush," which is the only thing that number is used for.
const nodeOverheadBytes = 48

type node struct {
	key     []byte
	value   []byte
	deleted bool
	seq     uint64
	forward []*node // forward[i] is this node's successor at level i
}

func entrySize(key, value []byte) int64 {
	return int64(len(key)+len(value)) + nodeOverheadBytes
}

type skipList struct {
	head   *node
	height int // number of levels currently in use, 1 <= height <= maxHeight
	length int
	rnd    *rand.Rand
}

func newSkipList(seed int64) *skipList {
	return &skipList{
		head:   &node{forward: make([]*node, maxHeight)},
		height: 1,
		rnd:    rand.New(rand.NewSource(seed)),
	}
}

func (s *skipList) randomLevel() int {
	level := 1
	for level < maxHeight && s.rnd.Float64() < p {
		level++
	}
	return level
}

// put inserts or updates the entry for key and returns the signed change
// in size this caused, for the caller to maintain a running total. Not
// safe for concurrent use; callers must hold their own lock.
func (s *skipList) put(key, value []byte, seq uint64, deleted bool) int64 {
	var update [maxHeight]*node
	x := s.head
	for i := s.height - 1; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
		update[i] = x
	}
	existing := x.forward[0]

	if existing != nil && bytes.Equal(existing.key, key) {
		oldSize := entrySize(existing.key, existing.value)
		existing.value = append([]byte(nil), value...)
		existing.deleted = deleted
		existing.seq = seq
		return entrySize(existing.key, existing.value) - oldSize
	}

	level := s.randomLevel()
	if level > s.height {
		for i := s.height; i < level; i++ {
			update[i] = s.head
		}
		s.height = level
	}

	n := &node{
		key:     append([]byte(nil), key...),
		value:   append([]byte(nil), value...),
		deleted: deleted,
		seq:     seq,
		forward: make([]*node, level),
	}
	for i := 0; i < level; i++ {
		n.forward[i] = update[i].forward[i]
		update[i].forward[i] = n
	}
	s.length++
	return entrySize(n.key, n.value)
}

// get returns the node for key, or nil if absent. Safe to call concurrently
// with other reads, but not with put (callers arrange locking).
func (s *skipList) get(key []byte) *node {
	x := s.head
	for i := s.height - 1; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
	}
	x = x.forward[0]
	if x != nil && bytes.Equal(x.key, key) {
		return x
	}
	return nil
}
