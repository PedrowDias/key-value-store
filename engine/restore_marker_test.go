package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/PedrowDias/key-value-store/sstable"
)

// --- readRestoreMarker / writeRestoreMarkerAtomically -----------------------

func TestRestoreMarker_AbsentReturnsZero(t *testing.T) {
	n, err := readRestoreMarker(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("readRestoreMarker on a directory with no marker = %d, want 0", n)
	}
}

func TestRestoreMarker_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	if err := writeRestoreMarkerAtomically(dir, 42); err != nil {
		t.Fatal(err)
	}
	n, err := readRestoreMarker(dir)
	if err != nil {
		t.Fatal(err)
	}
	if n != 42 {
		t.Fatalf("readRestoreMarker = %d, want 42", n)
	}
}

func TestRestoreMarker_OverwritePersistsLatest(t *testing.T) {
	dir := t.TempDir()
	if err := writeRestoreMarkerAtomically(dir, 1); err != nil {
		t.Fatal(err)
	}
	if err := writeRestoreMarkerAtomically(dir, 99); err != nil {
		t.Fatal(err)
	}
	n, err := readRestoreMarker(dir)
	if err != nil {
		t.Fatal(err)
	}
	if n != 99 {
		t.Fatalf("readRestoreMarker after overwrite = %d, want 99", n)
	}
}

func TestRestoreMarker_MalformedContentIsError(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, restoreMarkerFileName), []byte("not-a-number"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readRestoreMarker(dir); err == nil {
		t.Fatal("expected an error reading a malformed restore marker")
	}
}

func TestRestoreMarker_ReadErrorOtherThanNotExistPropagates(t *testing.T) {
	// A directory where the marker's name is itself a directory, not a
	// file: os.ReadFile fails with something other than "not exist,"
	// which readRestoreMarker must propagate rather than treat as "no
	// marker."
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, restoreMarkerFileName), 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := readRestoreMarker(dir); err == nil {
		t.Fatal("expected an error when the marker path is itself a directory")
	}
}

func TestWriteRestoreMarkerAtomically_CreateErrorPropagates(t *testing.T) {
	// A directory that doesn't exist: os.Create for the temp file fails.
	dir := filepath.Join(t.TempDir(), "does-not-exist")
	if err := writeRestoreMarkerAtomically(dir, 1); err == nil {
		t.Fatal("expected an error creating the marker temp file in a nonexistent directory")
	}
}

func TestWriteRestoreMarkerAtomically_RenameErrorPropagates(t *testing.T) {
	dir := t.TempDir()
	orig := renameFile
	renameFile = func(oldpath, newpath string) error {
		return errors.New("renameFile: simulated failure")
	}
	defer func() { renameFile = orig }()

	if err := writeRestoreMarkerAtomically(dir, 1); err == nil {
		t.Fatal("expected an error when the atomic rename fails")
	}
}

// --- Discovery filtering against a restore marker ---------------------------

func TestDiscoverSSTables_FiltersBelowMinValidSeq(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []int{0, 1, 2} {
		w, err := sstable.NewWriter(filepath.Join(dir, sstableName(n)), sstable.Options{})
		if err != nil {
			t.Fatal(err)
		}
		w.Add([]byte("k"), []byte("v"), uint64(n), false)
		if _, err := w.Finish(); err != nil {
			t.Fatal(err)
		}
	}

	sstables, nextFlushSeq, _, err := discoverSSTables(dir, sstable.NewBlockCache(defaultBlockCacheSize), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer closeAll(sstables)
	if len(sstables) != 1 {
		t.Fatalf("len(sstables) = %d, want 1 (only 000002.sst, minValidSeq=2 filters out 0 and 1)", len(sstables))
	}
	if nextFlushSeq != 3 {
		t.Fatalf("nextFlushSeq = %d, want 3", nextFlushSeq)
	}
}

func TestDiscoverWALs_FiltersBelowMinValidSeq(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []int{0, 1, 2} {
		if err := os.WriteFile(filepath.Join(dir, walName(n)), []byte{}, 0644); err != nil {
			t.Fatal(err)
		}
	}

	paths, nextWALSeq, err := discoverWALs(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || paths[0] != filepath.Join(dir, walName(2)) {
		t.Fatalf("paths = %v, want just [%s] (minValidSeq=2 filters out 0 and 1)", paths, filepath.Join(dir, walName(2)))
	}
	if nextWALSeq != 3 {
		t.Fatalf("nextWALSeq = %d, want 3", nextWALSeq)
	}
}

func sstableName(n int) string { return fmtName(sstableFileNamePattern, n) }
func walName(n int) string     { return fmtName(walFileNamePattern, n) }

func fmtName(pattern string, n int) string {
	return filepath.Base(fmt.Sprintf(pattern, n))
}
