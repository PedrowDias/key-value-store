package sstable

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func tempPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.sst")
}

type entry struct {
	key     string
	value   string
	seq     uint64
	deleted bool
}

func writeTable(t *testing.T, path string, opts Options, entries []entry) *Meta {
	t.Helper()
	w, err := NewWriter(path, opts)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, e := range entries {
		var val []byte
		if !e.deleted {
			val = []byte(e.value)
		}
		if err := w.Add([]byte(e.key), val, e.seq, e.deleted); err != nil {
			t.Fatalf("Add(%q): %v", e.key, err)
		}
	}
	meta, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	return meta
}

// --- Round-trip correctness -------------------------------------------------

func TestWriteAndRead_RoundTrip(t *testing.T) {
	path := tempPath(t)
	entries := []entry{
		{key: "alpha", value: "v-alpha", seq: 1},
		{key: "bravo", value: "v-bravo", seq: 2},
		{key: "charlie", value: "v-charlie", seq: 3},
		{key: "delta", deleted: true, seq: 4},
		{key: "echo", value: "v-echo", seq: 5},
	}
	meta := writeTable(t, path, Options{}, entries)

	if meta.NumEntries != len(entries) {
		t.Fatalf("meta.NumEntries = %d, want %d", meta.NumEntries, len(entries))
	}
	if string(meta.MinKey) != "alpha" || string(meta.MaxKey) != "echo" {
		t.Fatalf("meta key range = [%q, %q], want [alpha, echo]", meta.MinKey, meta.MaxKey)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	for _, e := range entries {
		val, seq, deleted, found, err := r.Get([]byte(e.key))
		if err != nil {
			t.Fatalf("Get(%q): %v", e.key, err)
		}
		if !found {
			t.Fatalf("Get(%q): not found", e.key)
		}
		if deleted != e.deleted {
			t.Fatalf("Get(%q): deleted=%v, want %v", e.key, deleted, e.deleted)
		}
		if seq != e.seq {
			t.Fatalf("Get(%q): seq=%d, want %d", e.key, seq, e.seq)
		}
		if !e.deleted && string(val) != e.value {
			t.Fatalf("Get(%q): value=%q, want %q", e.key, val, e.value)
		}
	}
}

func TestGet_MissingKey(t *testing.T) {
	path := tempPath(t)
	writeTable(t, path, Options{}, []entry{
		{key: "b", value: "vb", seq: 1},
		{key: "d", value: "vd", seq: 2},
		{key: "f", value: "vf", seq: 3},
	})
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for _, k := range []string{"a", "c", "e", "g", "zzz"} {
		_, _, _, found, err := r.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%q): %v", k, err)
		}
		if found {
			t.Fatalf("Get(%q): expected not found", k)
		}
	}
}

func TestGet_EmptyTable(t *testing.T) {
	path := tempPath(t)
	writeTable(t, path, Options{}, nil)
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open on empty table: %v", err)
	}
	defer r.Close()

	_, _, _, found, err := r.Get([]byte("anything"))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected not found in an empty table")
	}
	if r.NumEntries() != 0 {
		t.Fatalf("NumEntries() = %d, want 0", r.NumEntries())
	}
}

// --- Ordering enforcement ---------------------------------------------------

func TestAdd_RejectsOutOfOrderKeys(t *testing.T) {
	w, err := NewWriter(tempPath(t), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add([]byte("m"), []byte("v"), 1, false); err != nil {
		t.Fatal(err)
	}
	if err := w.Add([]byte("a"), []byte("v"), 2, false); err == nil {
		t.Fatal("expected an error adding a key smaller than the previous one")
	}
}

func TestAdd_RejectsDuplicateKeys(t *testing.T) {
	w, err := NewWriter(tempPath(t), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add([]byte("m"), []byte("v1"), 1, false); err != nil {
		t.Fatal(err)
	}
	if err := w.Add([]byte("m"), []byte("v2"), 2, false); err == nil {
		t.Fatal("expected an error adding a duplicate key")
	}
}

func TestAdd_AfterFinish_Errors(t *testing.T) {
	w, err := NewWriter(tempPath(t), Options{})
	if err != nil {
		t.Fatal(err)
	}
	w.Add([]byte("a"), []byte("v"), 1, false)
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := w.Add([]byte("b"), []byte("v"), 2, false); err == nil {
		t.Fatal("expected error calling Add after Finish")
	}
}

// --- Iterator ----------------------------------------------------------------

func TestIterator_MultiBlock_SortedAndComplete(t *testing.T) {
	path := tempPath(t)
	const n = 500
	var entries []entry
	for i := 0; i < n; i++ {
		entries = append(entries, entry{
			key:   fmt.Sprintf("key-%05d", i),
			value: fmt.Sprintf("value-%05d-%s", i, randomString(20)),
			seq:   uint64(i),
		})
	}
	// Small block size forces many blocks, exercising cross-block iteration.
	writeTable(t, path, Options{BlockSize: 256}, entries)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	it := r.NewIterator()
	it.SeekToFirst()
	count := 0
	for it.Valid() {
		want := entries[count]
		if string(it.Key()) != want.key {
			t.Fatalf("entry %d: key = %q, want %q", count, it.Key(), want.key)
		}
		if string(it.Value()) != want.value {
			t.Fatalf("entry %d: value = %q, want %q", count, it.Value(), want.value)
		}
		count++
		it.Next()
	}
	if it.Err() != nil {
		t.Fatalf("iterator error: %v", it.Err())
	}
	if count != n {
		t.Fatalf("iterated %d entries, want %d", count, n)
	}
}

func randomString(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(b)
}

// --- Bloom filter behavior ---------------------------------------------------

func TestBloomFilter_NoFalseNegatives(t *testing.T) {
	// The bloom filter must never cause a real key to be reported missing.
	// Run with a deliberately loose FP rate isn't the point here — the
	// guarantee that matters is zero false negatives, checked directly
	// against the filter (not just through Get, so a bug in Get's block
	// scan can't mask a bloom filter bug or vice versa).
	rnd := rand.New(rand.NewSource(7))
	const n = 2000
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k-%d-%d", i, rnd.Int()))
	}

	bf := newBloomFilter(n, 0.01)
	for _, k := range keys {
		bf.add(k)
	}
	for _, k := range keys {
		if !bf.mayContain(k) {
			t.Fatalf("false negative for key %q: bloom filter must never say 'definitely absent' for a key that was added", k)
		}
	}
}

func TestBloomFilter_FalsePositiveRateIsReasonable(t *testing.T) {
	const n = 5000
	const targetFPR = 0.01
	bf := newBloomFilter(n, targetFPR)

	inserted := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("present-%d", i))
		bf.add(k)
		inserted[string(k)] = true
	}

	falsePositives := 0
	const trials = 20000
	for i := 0; i < trials; i++ {
		k := []byte(fmt.Sprintf("absent-%d", i))
		if inserted[string(k)] {
			continue // shouldn't happen given disjoint key spaces, but guard anyway
		}
		if bf.mayContain(k) {
			falsePositives++
		}
	}
	rate := float64(falsePositives) / float64(trials)
	// 1.5x slack covers normal statistical variance at this trial count
	// without masking a real problem. This test originally allowed up to
	// 3x and still caught a real bug (see the splitmix64 comment in
	// bloom.go) at ~2.5x target — tightened here now that the fix brings
	// the observed rate back to within a few percent of target.
	if rate > targetFPR*1.5 {
		t.Fatalf("false positive rate %.4f is far above target %.4f (%d/%d) — bloom filter hashing or sizing looks broken", rate, targetFPR, falsePositives, trials)
	}
	t.Logf("observed false positive rate: %.4f (target %.4f)", rate, targetFPR)
}

func TestGet_SkipsDiskReadOnBloomMiss(t *testing.T) {
	// Indirect check: corrupt every data block on disk after writing, then
	// confirm that Get for a key NOT in the table still returns cleanly
	// (no error), which is only possible if it never touched the
	// corrupted data blocks at all — i.e. the bloom filter did its job.
	path := tempPath(t)
	writeTable(t, path, Options{}, []entry{
		{key: "a", value: "va", seq: 1},
		{key: "b", value: "vb", seq: 2},
	})

	f, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt the first data block's bytes (well before the bloom/index
	// blocks, which are near EOF).
	if _, err := f.WriteAt([]byte{0xDE, 0xAD, 0xBE, 0xEF}, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open should succeed (data block corruption doesn't affect footer/index/bloom): %v", err)
	}
	defer r.Close()

	_, _, _, found, err := r.Get([]byte("definitely-not-present"))
	if err != nil {
		t.Fatalf("Get for an absent key touched corrupted data and errored: %v (bloom filter should have short-circuited before reading any data block)", err)
	}
	if found {
		t.Fatal("expected not found")
	}
}

// --- Corruption detection ----------------------------------------------------

func TestOpen_RejectsTruncatedFile(t *testing.T) {
	path := tempPath(t)
	writeTable(t, path, Options{}, []entry{{key: "a", value: "v", seq: 1}})

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-10); err != nil {
		t.Fatal(err)
	}

	_, err = Open(path)
	if err == nil {
		t.Fatal("expected an error opening a truncated sstable file")
	}
}

func TestOpen_RejectsBadMagicNumber(t *testing.T) {
	path := tempPath(t)
	writeTable(t, path, Options{}, []entry{{key: "a", value: "v", seq: 1}})

	f, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := f.Stat()
	// Overwrite the last 8 bytes (the magic number field) with garbage.
	if _, err := f.WriteAt([]byte{1, 2, 3, 4, 5, 6, 7, 8}, info.Size()-8); err != nil {
		t.Fatal(err)
	}
	f.Close()

	_, err = Open(path)
	if err == nil {
		t.Fatal("expected an error opening a file with a corrupted magic number")
	}
}

func TestGet_DetectsCorruptedDataBlock(t *testing.T) {
	path := tempPath(t)
	writeTable(t, path, Options{}, []entry{
		{key: "a", value: "value-a-long-enough-to-matter", seq: 1},
	})

	f, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte within the first data block (offset 0 is safe: the
	// table is tiny, single block, and this byte is inside the entry's
	// key/value payload, not the checksum trailer).
	if _, err := f.WriteAt([]byte{0xFF}, 2); err != nil {
		t.Fatal(err)
	}
	f.Close()

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, _, _, _, err = r.Get([]byte("a"))
	if err == nil {
		t.Fatal("expected an error reading a key whose data block has a corrupted checksum")
	}
}

// --- Scale --------------------------------------------------------------------

func TestLargeTable_RandomKeys(t *testing.T) {
	path := tempPath(t)
	rnd := rand.New(rand.NewSource(99))
	const n = 5000

	keySet := make(map[string]string, n)
	for len(keySet) < n {
		k := fmt.Sprintf("key-%06d", rnd.Intn(1000000))
		keySet[k] = fmt.Sprintf("val-for-%s", k)
	}
	var sortedKeys []string
	for k := range keySet {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	w, err := NewWriter(path, Options{BlockSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	for i, k := range sortedKeys {
		if err := w.Add([]byte(k), []byte(keySet[k]), uint64(i), false); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Spot-check every key via point lookup.
	for k, want := range keySet {
		val, _, deleted, found, err := r.Get([]byte(k))
		if err != nil || !found || deleted {
			t.Fatalf("Get(%q): val=%q err=%v found=%v deleted=%v", k, val, err, found, deleted)
		}
		if string(val) != want {
			t.Fatalf("Get(%q) = %q, want %q", k, val, want)
		}
	}

	// And via full iteration.
	it := r.NewIterator()
	it.SeekToFirst()
	count := 0
	for it.Valid() {
		if string(it.Key()) != sortedKeys[count] {
			t.Fatalf("iterator entry %d: key = %q, want %q", count, it.Key(), sortedKeys[count])
		}
		count++
		it.Next()
	}
	if count != len(sortedKeys) {
		t.Fatalf("iterated %d entries, want %d", count, len(sortedKeys))
	}
}
