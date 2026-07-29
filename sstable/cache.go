package sstable

import (
	"container/list"
	"sync"
)

// BlockCache is a small, size-bounded LRU cache mapping (file, byte
// offset) to the checksum-verified raw bytes of a data block — shared
// across every *Reader constructed with OpenWithCache using the same
// cache instance, which is what makes it actually useful: a real
// workload reads across many SSTable files, not just one.
//
// This exists to close a real, measured gap this project's own
// benchmarks found (see bench/BENCHMARKS.md): once a workload's data
// exceeds the memtable's flush threshold, reads that fall through to an
// SSTable pay a real disk read (and CRC32C verification) on every
// single call, even for a key that was just read a moment ago. Caching
// verified block bytes turns a repeated read of hot data into a map
// lookup, at the cost of the memory the cache holds.
//
// Safe for concurrent use by multiple goroutines — a nil *BlockCache is
// also valid and behaves as "no caching" throughout (every method is a
// nil-safe no-op/always-miss), which is what lets OpenWithCache's cache
// parameter be optional without every caller needing its own nil check.
type BlockCache struct {
	mu       sync.Mutex
	maxBytes int64
	curBytes int64
	ll       *list.List
	items    map[cacheKey]*list.Element
}

type cacheKey struct {
	file   string
	offset uint64
}

type cacheEntry struct {
	key  cacheKey
	data []byte
}

// NewBlockCache returns a BlockCache holding at most maxBytes of block
// data (by the sum of cached blocks' lengths), evicting the
// least-recently-used block to make room for a new one once full. A
// non-positive maxBytes means "cache nothing" — get always misses, put
// is always a no-op.
func NewBlockCache(maxBytes int64) *BlockCache {
	return &BlockCache{
		maxBytes: maxBytes,
		ll:       list.New(),
		items:    make(map[cacheKey]*list.Element),
	}
}

// get returns the cached bytes for (file, offset), if present, moving
// that entry to the front of the LRU order.
func (c *BlockCache) get(file string, offset uint64) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[cacheKey{file: file, offset: offset}]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*cacheEntry).data, true
}

// put stores data for (file, offset), evicting least-recently-used
// entries as needed to stay within maxBytes. A block larger than the
// entire cache is never stored (it could never fit without evicting
// everything else and still not fitting). Storing a key that's already
// present just refreshes its LRU position rather than double-counting
// its bytes — this can legitimately happen if two callers race a cache
// miss for the same block and both go to disk.
func (c *BlockCache) put(file string, offset uint64, data []byte) {
	if c == nil || c.maxBytes <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	key := cacheKey{file: file, offset: offset}
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		return
	}
	if int64(len(data)) > c.maxBytes {
		return
	}

	el := c.ll.PushFront(&cacheEntry{key: key, data: data})
	c.items[key] = el
	c.curBytes += int64(len(data))

	for c.curBytes > c.maxBytes {
		c.evictOldest()
	}
}

func (c *BlockCache) evictOldest() {
	el := c.ll.Back()
	if el == nil {
		return
	}
	c.ll.Remove(el)
	entry := el.Value.(*cacheEntry)
	delete(c.items, entry.key)
	c.curBytes -= int64(len(entry.data))
}

// entryCount reports how many blocks are currently cached. Unexported —
// this package's own tests use it to verify eviction behavior directly;
// it's not meant as a public introspection API.
func (c *BlockCache) entryCount() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}
