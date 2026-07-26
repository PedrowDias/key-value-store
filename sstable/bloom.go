package sstable

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"math"
)

// A Bloom filter answers "might this key be in the set?" using a fixed-size
// bit array instead of storing the keys themselves. False positives are
// possible (it may say "maybe present" for a key that isn't there); false
// negatives are not (it never says "definitely absent" for a key that is
// there). That asymmetry is exactly what an SSTable needs: before touching
// disk to look up a key, check the filter first, and skip the disk read
// entirely on a "definitely absent" answer — which for a typical workload
// is the outcome for the vast majority of point lookups against SSTables
// that don't hold the key being searched for.
//
// This is a per-SSTable filter sized for the exact number of keys the file
// ends up holding (computed in Writer.Finish, once every key has been
// seen), using the standard two-hash technique (Kirsch-Mitzenmacher) to
// simulate k independent hash functions from just two real hash computations.
type bloomFilter struct {
	m    uint64 // number of bits
	k    int    // number of hash functions
	bits []byte // len(bits) == ceil(m/8)
}

var errCorruptBloom = errors.New("sstable: corrupt bloom filter block")

// bloomParams computes the bit-array size m and hash count k that achieve
// approximately targetFPRate for n entries, using the standard formulas:
//
//	m = ceil(-n * ln(p) / ln(2)^2)
//	k = round((m/n) * ln(2))
func bloomParams(n int, targetFPRate float64) (m uint64, k int) {
	if n <= 0 {
		return 0, 0
	}
	nf := float64(n)
	m = uint64(math.Ceil(-nf * math.Log(targetFPRate) / (math.Ln2 * math.Ln2)))
	if m < 8 {
		m = 8
	}
	k = int(math.Round((float64(m) / nf) * math.Ln2))
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return m, k
}

func newBloomFilter(expectedEntries int, targetFPRate float64) *bloomFilter {
	m, k := bloomParams(expectedEntries, targetFPRate)
	return &bloomFilter{m: m, k: k, bits: make([]byte, (m+7)/8)}
}

// hashes derives two 64-bit hashes of key for the Kirsch-Mitzenmacher
// combination below. h1 is FNV-1a of the key; h2 is derived from h1 via
// splitmix64, a well-known strong bit-mixing finalizer (originally from
// the SplitMix PRNG family) rather than by re-hashing a trivially
// modified version of the same input with the same algorithm.
//
// That distinction matters more than it might look: an earlier version
// of this function computed h2 by running FNV-1a a second time over
// key+[0xff]. It passed every functional test, but a dedicated bloom
// filter false-positive-rate test caught it producing a false positive
// rate roughly 2.5x the target — because FNV-1a's mixing isn't strong
// enough to fully decorrelate from itself when given nearly-identical
// input (as most keys here are: a short fixed prefix plus a numeric
// suffix), so the two derived hash values stayed correlated enough that
// the k probe positions for a given key weren't behaving independently.
// Deriving h2 from h1 through splitmix64 (which is designed so that a
// single-bit change in its input flips roughly half of its output bits)
// fixed that: measured false-positive rate came back in line with the
// target. See TestBloomFilter_FalsePositiveRateIsReasonable.
func bloomHashes(key []byte) (h1, h2 uint64) {
	f1 := fnv.New64a()
	f1.Write(key)
	h1 = f1.Sum64()
	h2 = splitmix64(h1)
	return h1, h2
}

// splitmix64 is a fixed-point bit mixer: strong avalanche (each output
// bit depends on most input bits), fast, and stateless — exactly what's
// needed here to turn one hash value into a second, well-decorrelated one.
func splitmix64(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	x = x ^ (x >> 31)
	return x
}

func (b *bloomFilter) add(key []byte) {
	if b.m == 0 {
		return
	}
	h1, h2 := bloomHashes(key)
	for i := 0; i < b.k; i++ {
		idx := (h1 + uint64(i)*h2) % b.m
		b.bits[idx/8] |= 1 << (idx % 8)
	}
}

// mayContain returns false only if key is definitely not in the set. A
// true result means "maybe" — the caller must still check the real data.
func (b *bloomFilter) mayContain(key []byte) bool {
	if b.m == 0 {
		// No filter was built (e.g. an empty SSTable) — can't rule
		// anything out, so defer entirely to the real lookup.
		return true
	}
	h1, h2 := bloomHashes(key)
	for i := 0; i < b.k; i++ {
		idx := (h1 + uint64(i)*h2) % b.m
		if b.bits[idx/8]&(1<<(idx%8)) == 0 {
			return false
		}
	}
	return true
}

// encode serializes the filter as [4B k][8B m][bitset bytes].
func (b *bloomFilter) encode() []byte {
	buf := make([]byte, 0, 12+len(b.bits))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(b.k))
	buf = binary.LittleEndian.AppendUint64(buf, b.m)
	buf = append(buf, b.bits...)
	return buf
}

func decodeBloomFilter(data []byte) (*bloomFilter, error) {
	if len(data) < 12 {
		return nil, errCorruptBloom
	}
	k := binary.LittleEndian.Uint32(data[0:4])
	m := binary.LittleEndian.Uint64(data[4:12])
	bits := data[12:]
	wantLen := int((m + 7) / 8)
	if len(bits) != wantLen {
		return nil, errCorruptBloom
	}
	return &bloomFilter{m: m, k: int(k), bits: append([]byte(nil), bits...)}, nil
}
