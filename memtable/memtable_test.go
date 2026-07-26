package memtable

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// --- Basic correctness ---------------------------------------------------

func TestPutGet_Basic(t *testing.T) {
	m := New()
	m.Put([]byte("k1"), []byte("v1"), 1)
	m.Put([]byte("k2"), []byte("v2"), 2)

	v, seq, deleted, found := m.Get([]byte("k1"))
	if !found || deleted || string(v) != "v1" || seq != 1 {
		t.Fatalf("Get(k1) = %q, seq=%d, deleted=%v, found=%v; want v1, 1, false, true", v, seq, deleted, found)
	}

	v, seq, deleted, found = m.Get([]byte("k2"))
	if !found || deleted || string(v) != "v2" || seq != 2 {
		t.Fatalf("Get(k2) = %q, seq=%d, deleted=%v, found=%v; want v2, 2, false, true", v, seq, deleted, found)
	}
}

func TestGet_MissingKey(t *testing.T) {
	m := New()
	m.Put([]byte("k1"), []byte("v1"), 1)
	_, _, _, found := m.Get([]byte("nope"))
	if found {
		t.Fatal("expected found=false for a key that was never written")
	}
}

func TestPut_OverwriteUpdatesValueAndSeq(t *testing.T) {
	m := New()
	m.Put([]byte("k"), []byte("v1"), 1)
	m.Put([]byte("k"), []byte("v2-longer"), 5)

	v, seq, deleted, found := m.Get([]byte("k"))
	if !found || deleted {
		t.Fatalf("found=%v deleted=%v, want true, false", found, deleted)
	}
	if string(v) != "v2-longer" {
		t.Fatalf("Get = %q, want v2-longer (overwrite should win)", v)
	}
	if seq != 5 {
		t.Fatalf("seq = %d, want 5", seq)
	}
	if m.Len() != 1 {
		t.Fatalf("Len() = %d, want 1 (overwrite must not create a second entry)", m.Len())
	}
}

func TestDelete_Tombstone(t *testing.T) {
	m := New()
	m.Put([]byte("k"), []byte("v"), 1)
	m.Delete([]byte("k"), 2)

	v, seq, deleted, found := m.Get([]byte("k"))
	if !found {
		t.Fatal("a tombstoned key must still be 'found' (distinguishes deleted from never-written)")
	}
	if !deleted {
		t.Fatal("expected deleted=true")
	}
	if seq != 2 {
		t.Fatalf("seq = %d, want 2", seq)
	}
	_ = v // value content after delete is unspecified/irrelevant
}

func TestDelete_ThenPut_Resurrects(t *testing.T) {
	m := New()
	m.Put([]byte("k"), []byte("v1"), 1)
	m.Delete([]byte("k"), 2)
	m.Put([]byte("k"), []byte("v3"), 3)

	v, seq, deleted, found := m.Get([]byte("k"))
	if !found || deleted {
		t.Fatalf("found=%v deleted=%v, want true, false", found, deleted)
	}
	if string(v) != "v3" || seq != 3 {
		t.Fatalf("Get = %q seq=%d, want v3, 3", v, seq)
	}
}

func TestDelete_NeverWrittenKey_StillRecordsTombstone(t *testing.T) {
	m := New()
	m.Delete([]byte("ghost"), 1)
	_, _, deleted, found := m.Get([]byte("ghost"))
	if !found || !deleted {
		t.Fatalf("found=%v deleted=%v, want true, true (a delete of an unseen key is still a tombstone the engine must remember)", found, deleted)
	}
}

// --- Ordering / iteration --------------------------------------------------

func TestIterator_SortedOrder(t *testing.T) {
	m := New()
	keys := []string{"delta", "alpha", "charlie", "echo", "bravo"}
	for i, k := range keys {
		m.Put([]byte(k), []byte(fmt.Sprintf("v%d", i)), uint64(i+1))
	}

	it := m.NewIterator()
	it.SeekToFirst()
	var got []string
	for it.Valid() {
		got = append(got, string(it.Key()))
		it.Next()
	}

	want := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestIterator_EmptyMemtable(t *testing.T) {
	m := New()
	it := m.NewIterator()
	it.SeekToFirst()
	if it.Valid() {
		t.Fatal("expected an empty memtable's iterator to be immediately invalid")
	}
}

func TestIterator_ReflectsTombstonesAndValues(t *testing.T) {
	m := New()
	m.Put([]byte("a"), []byte("va"), 1)
	m.Delete([]byte("b"), 2)
	m.Put([]byte("c"), []byte("vc"), 3)

	it := m.NewIterator()
	it.SeekToFirst()

	type ent struct {
		key     string
		val     string
		deleted bool
		seq     uint64
	}
	var got []ent
	for it.Valid() {
		got = append(got, ent{string(it.Key()), string(it.Value()), it.Deleted(), it.SeqNum()})
		it.Next()
	}

	want := []ent{{"a", "va", false, 1}, {"b", "", true, 2}, {"c", "vc", false, 3}}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i].key != want[i].key || got[i].deleted != want[i].deleted || got[i].seq != want[i].seq {
			t.Fatalf("entry %d: got %+v, want %+v", i, got[i], want[i])
		}
		if !want[i].deleted && got[i].val != want[i].val {
			t.Fatalf("entry %d: got val %q, want %q", i, got[i].val, want[i].val)
		}
	}
}

func TestIterator_Seek(t *testing.T) {
	m := New()
	for _, k := range []string{"a", "c", "e", "g", "i"} {
		m.Put([]byte(k), []byte(k+"-val"), 1)
	}

	it := m.NewIterator()
	it.Seek([]byte("d")) // between c and e -> should land on e
	if !it.Valid() || string(it.Key()) != "e" {
		t.Fatalf("Seek(d): got key=%q valid=%v, want e", it.Key(), it.Valid())
	}

	it.Seek([]byte("e")) // exact match
	if !it.Valid() || string(it.Key()) != "e" {
		t.Fatalf("Seek(e): got key=%q, want e", it.Key())
	}

	it.Seek([]byte("z")) // past the end
	if it.Valid() {
		t.Fatalf("Seek(z) past the end should be invalid, got key=%q", it.Key())
	}
}

func TestIterator_SnapshotIsolationFromLaterWrites(t *testing.T) {
	m := New()
	m.Put([]byte("a"), []byte("v1"), 1)
	it := m.NewIterator()

	// Mutate the memtable after taking the iterator.
	m.Put([]byte("b"), []byte("v2"), 2)
	m.Put([]byte("a"), []byte("v1-changed"), 3)

	it.SeekToFirst()
	if !it.Valid() || string(it.Key()) != "a" || string(it.Value()) != "v1" {
		t.Fatalf("iterator should see the pre-write snapshot: got key=%q val=%q", it.Key(), it.Value())
	}
	it.Next()
	if it.Valid() {
		t.Fatalf("iterator snapshot should not include writes made after it was created, but found key=%q", it.Key())
	}
}

// --- Size accounting -------------------------------------------------------

func TestApproxSize_GrowsOnPutAndShrinksNotOnOverwriteSmaller(t *testing.T) {
	m := New()
	if m.ApproxSize() != 0 {
		t.Fatalf("empty memtable ApproxSize = %d, want 0", m.ApproxSize())
	}
	m.Put([]byte("key"), []byte("aVeryLongValueHere"), 1)
	sizeAfterFirst := m.ApproxSize()
	if sizeAfterFirst <= 0 {
		t.Fatalf("expected positive size after a put, got %d", sizeAfterFirst)
	}

	m.Put([]byte("key"), []byte("x"), 2) // overwrite with a much shorter value
	sizeAfterShrink := m.ApproxSize()
	if sizeAfterShrink >= sizeAfterFirst {
		t.Fatalf("expected size to shrink after overwriting with a shorter value: before=%d after=%d", sizeAfterFirst, sizeAfterShrink)
	}
}

func TestApproxSize_MatchesManualSum(t *testing.T) {
	m := New()
	entries := map[string]string{"aa": "111", "bb": "22222", "cc": "3"}
	var want int64
	for k, v := range entries {
		m.Put([]byte(k), []byte(v), 1)
		want += int64(len(k)+len(v)) + nodeOverheadBytes
	}
	if got := m.ApproxSize(); got != want {
		t.Fatalf("ApproxSize() = %d, want %d", got, want)
	}
}

// --- Scale / structural correctness ---------------------------------------

func TestSkipList_LargeRandomDataset_SortedAndComplete(t *testing.T) {
	const n = 10000
	m := newForTest(42)

	rnd := rand.New(rand.NewSource(1))
	keys := make(map[string]string, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%08d", rnd.Intn(1000000))
		v := fmt.Sprintf("val-%d", i)
		keys[k] = v // last write for a given random key wins, matching memtable semantics
		m.Put([]byte(k), []byte(v), uint64(i))
	}

	if m.Len() != len(keys) {
		t.Fatalf("Len() = %d, want %d", m.Len(), len(keys))
	}

	it := m.NewIterator()
	it.SeekToFirst()
	var prev []byte
	count := 0
	for it.Valid() {
		if prev != nil && bytes.Compare(prev, it.Key()) >= 0 {
			t.Fatalf("iteration order violated: %q came at or after %q", it.Key(), prev)
		}
		want, ok := keys[string(it.Key())]
		if !ok {
			t.Fatalf("iterator produced unexpected key %q", it.Key())
		}
		if string(it.Value()) != want {
			t.Fatalf("key %q: got value %q, want %q", it.Key(), it.Value(), want)
		}
		prev = append([]byte(nil), it.Key()...)
		count++
		it.Next()
	}
	if count != len(keys) {
		t.Fatalf("iterator yielded %d entries, want %d", count, len(keys))
	}
}

// --- Concurrency ------------------------------------------------------------

func TestConcurrentPutsAndGets(t *testing.T) {
	m := New()
	const goroutines = 16
	const opsPerGoroutine = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("g%d-k%d", g, i%50))
				m.Put(key, []byte("v"), uint64(i))
				m.Get(key)
				if i%10 == 0 {
					m.Delete(key, uint64(i))
				}
				_ = m.ApproxSize()
				_ = m.Len()
			}
		}(g)
	}
	wg.Wait()

	// Sanity: every distinct key across all goroutines should be present.
	for g := 0; g < goroutines; g++ {
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("g%d-k%d", g, i))
			_, _, _, found := m.Get(key)
			if !found {
				t.Fatalf("expected key %s to be present after concurrent writes", key)
			}
		}
	}
}

func TestConcurrentIteratorDuringWrites(t *testing.T) {
	m := New()
	for i := 0; i < 100; i++ {
		m.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("v"), uint64(i))
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 100; i < 1000; i++ {
			m.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("v"), uint64(i))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			it := m.NewIterator()
			it.SeekToFirst()
			var prev []byte
			for it.Valid() {
				if prev != nil && bytes.Compare(prev, it.Key()) >= 0 {
					t.Errorf("iterator order violated concurrently with writes")
				}
				prev = it.Key()
				it.Next()
			}
		}
	}()
	wg.Wait()
}
