package sstable

import "testing"

func TestBlockCache_PutThenGetHits(t *testing.T) {
	c := NewBlockCache(1024)
	c.put("a.sst", 0, []byte("hello"))
	data, ok := c.get("a.sst", 0)
	if !ok || string(data) != "hello" {
		t.Fatalf("get = %q, %v; want hello, true", data, ok)
	}
}

func TestBlockCache_MissForUnknownKey(t *testing.T) {
	c := NewBlockCache(1024)
	_, ok := c.get("a.sst", 0)
	if ok {
		t.Fatal("expected a miss for a key never put")
	}
}

func TestBlockCache_DistinguishesByFileAndOffset(t *testing.T) {
	c := NewBlockCache(1024)
	c.put("a.sst", 0, []byte("from-a"))
	c.put("b.sst", 0, []byte("from-b"))
	c.put("a.sst", 100, []byte("from-a-offset-100"))

	data, ok := c.get("a.sst", 0)
	if !ok || string(data) != "from-a" {
		t.Fatalf("a.sst@0 = %q, %v", data, ok)
	}
	data, ok = c.get("b.sst", 0)
	if !ok || string(data) != "from-b" {
		t.Fatalf("b.sst@0 = %q, %v", data, ok)
	}
	data, ok = c.get("a.sst", 100)
	if !ok || string(data) != "from-a-offset-100" {
		t.Fatalf("a.sst@100 = %q, %v", data, ok)
	}
}

func TestBlockCache_EvictsLeastRecentlyUsed(t *testing.T) {
	// Room for exactly 2 five-byte blocks.
	c := NewBlockCache(10)
	c.put("f", 1, []byte("aaaaa"))
	c.put("f", 2, []byte("bbbbb"))
	// Touch block 1 so it's now more-recently-used than block 2.
	c.get("f", 1)
	// A third block forces an eviction; block 2 (now the LRU one) should go.
	c.put("f", 3, []byte("ccccc"))

	if _, ok := c.get("f", 2); ok {
		t.Fatal("expected block 2 to have been evicted as least-recently-used")
	}
	if _, ok := c.get("f", 1); !ok {
		t.Fatal("expected block 1 to still be cached (it was touched more recently)")
	}
	if _, ok := c.get("f", 3); !ok {
		t.Fatal("expected block 3 (just inserted) to be cached")
	}
	if n := c.entryCount(); n != 2 {
		t.Fatalf("entryCount = %d, want 2", n)
	}
}

func TestBlockCache_ReputtingExistingKeyRefreshesPositionWithoutDoubleCounting(t *testing.T) {
	c := NewBlockCache(10)
	c.put("f", 1, []byte("aaaaa"))
	c.put("f", 2, []byte("bbbbb"))
	// Re-put block 1 (simulating two racing callers both fetching the
	// same block on a miss) — must not double-count its bytes toward
	// the size limit, and must not evict anything as a result.
	c.put("f", 1, []byte("aaaaa"))

	if n := c.entryCount(); n != 2 {
		t.Fatalf("entryCount = %d, want 2 (re-putting an existing key must not grow the cache)", n)
	}
	if _, ok := c.get("f", 2); !ok {
		t.Fatal("block 2 should not have been evicted by re-putting block 1")
	}
}

func TestBlockCache_BlockLargerThanWholeCacheIsNeverStored(t *testing.T) {
	c := NewBlockCache(4)
	c.put("f", 1, []byte("way-too-big"))
	if _, ok := c.get("f", 1); ok {
		t.Fatal("expected a block larger than the whole cache to never be stored")
	}
	if n := c.entryCount(); n != 0 {
		t.Fatalf("entryCount = %d, want 0", n)
	}
}

func TestBlockCache_NonPositiveMaxBytesCachesNothing(t *testing.T) {
	c := NewBlockCache(0)
	c.put("f", 1, []byte("x"))
	if _, ok := c.get("f", 1); ok {
		t.Fatal("expected a zero-capacity cache to never retain anything")
	}
}

func TestBlockCache_NilCacheIsSafeNoOp(t *testing.T) {
	var c *BlockCache
	c.put("f", 1, []byte("x")) // must not panic
	if _, ok := c.get("f", 1); ok {
		t.Fatal("expected a nil cache to always miss")
	}
	if n := c.entryCount(); n != 0 {
		t.Fatalf("entryCount on a nil cache = %d, want 0", n)
	}
}

func TestBlockCache_EvictionKeepsWithinByteBudgetAcrossManyInserts(t *testing.T) {
	c := NewBlockCache(50)
	for i := 0; i < 20; i++ {
		c.put("f", uint64(i), []byte("0123456789")) // 10 bytes each
	}
	// At most 5 ten-byte blocks fit in a 50-byte budget.
	if n := c.entryCount(); n > 5 {
		t.Fatalf("entryCount = %d, want <= 5 (budget is 50 bytes, 10 bytes/entry)", n)
	}
	// The most recently inserted must still be present.
	if _, ok := c.get("f", 19); !ok {
		t.Fatal("expected the most recently inserted block to still be cached")
	}
}

func TestBlockCache_EvictOldestOnEmptyListIsNoop(t *testing.T) {
	c := NewBlockCache(1024)
	c.evictOldest() // must not panic on an empty cache
	if n := c.entryCount(); n != 0 {
		t.Fatalf("entryCount = %d, want 0", n)
	}
}

// --- Integration: a real Reader actually serving reads from the cache ------

func TestOpenWithCache_SecondGetIsServedFromCacheNotDisk(t *testing.T) {
	path := tempPath(t)
	writeTable(t, path, Options{}, []entry{
		{key: "a", value: "1", seq: 1},
		{key: "b", value: "2", seq: 2},
		{key: "c", value: "3", seq: 3},
	})

	cache := NewBlockCache(1 << 20)
	r, err := OpenWithCache(path, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if n := cache.entryCount(); n != 0 {
		t.Fatalf("entryCount before any Get = %d, want 0", n)
	}

	val, _, _, found, err := r.Get([]byte("a"))
	if err != nil || !found || string(val) != "1" {
		t.Fatalf("first Get(a) = %q found=%v err=%v", val, found, err)
	}
	if n := cache.entryCount(); n != 1 {
		t.Fatalf("entryCount after first Get = %d, want 1 (the block should now be cached)", n)
	}

	// A second Get for a DIFFERENT key that lives in the SAME block must
	// hit the cache rather than reading the file again — exercised
	// indirectly by confirming the cache's entry count doesn't grow (a
	// second miss populating a NEW entry would raise it) and the value
	// is still correct.
	val, _, _, found, err = r.Get([]byte("b"))
	if err != nil || !found || string(val) != "2" {
		t.Fatalf("second Get(b) = %q found=%v err=%v", val, found, err)
	}
	if n := cache.entryCount(); n != 1 {
		t.Fatalf("entryCount after second Get (same block) = %d, want still 1 (should have hit the cache)", n)
	}
}

func TestOpenWithCache_SharedAcrossTwoReadersOfTheSameFile(t *testing.T) {
	path := tempPath(t)
	writeTable(t, path, Options{}, []entry{{key: "k", value: "v", seq: 1}})

	cache := NewBlockCache(1 << 20)
	r1, err := OpenWithCache(path, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Close()
	r2, err := OpenWithCache(path, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()

	if _, _, _, found, err := r1.Get([]byte("k")); err != nil || !found {
		t.Fatalf("r1.Get: found=%v err=%v", found, err)
	}
	if n := cache.entryCount(); n != 1 {
		t.Fatalf("entryCount after r1's Get = %d, want 1", n)
	}

	// r2 is a completely separate Reader (its own *os.File) sharing the
	// same cache instance — its Get should be served from what r1
	// already populated, without growing the cache further.
	val, _, _, found, err := r2.Get([]byte("k"))
	if err != nil || !found || string(val) != "v" {
		t.Fatalf("r2.Get(k) = %q found=%v err=%v", val, found, err)
	}
	if n := cache.entryCount(); n != 1 {
		t.Fatalf("entryCount after r2's Get = %d, want still 1 (shared cache, should have hit)", n)
	}
}

func TestOpenWithCache_IteratorAlsoUsesTheCache(t *testing.T) {
	path := tempPath(t)
	writeTable(t, path, Options{}, []entry{
		{key: "a", value: "1", seq: 1},
		{key: "b", value: "2", seq: 2},
	})

	cache := NewBlockCache(1 << 20)
	r, err := OpenWithCache(path, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	it := r.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	if n := cache.entryCount(); n == 0 {
		t.Fatal("expected the iterator's block reads to have populated the cache")
	}
}
