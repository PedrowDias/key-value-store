package bench

import (
	"errors"
	"os"
	"testing"
)

func TestNaiveStore_PutGetDelete(t *testing.T) {
	s, err := OpenNaiveStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	val, found, err := s.Get([]byte("k"))
	if err != nil || !found || string(val) != "v" {
		t.Fatalf("Get(k) = %q found=%v err=%v", val, found, err)
	}

	if err := s.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	_, found, err = s.Get([]byte("k"))
	if err != nil || found {
		t.Fatalf("Get(k) after delete: found=%v err=%v", found, err)
	}
}

func TestNaiveStore_GetMissingKey(t *testing.T) {
	s, err := OpenNaiveStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, found, err := s.Get([]byte("never-existed"))
	if err != nil || found {
		t.Fatalf("found=%v err=%v, want false nil", found, err)
	}
}

func TestNaiveStore_DeleteMissingKeyIsNotAnError(t *testing.T) {
	s, err := OpenNaiveStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Delete([]byte("never-existed")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNaiveStore_OverwriteUpdatesValue(t *testing.T) {
	s, err := OpenNaiveStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	s.Put([]byte("k"), []byte("v1"))
	s.Put([]byte("k"), []byte("v2-longer-value"))
	val, found, err := s.Get([]byte("k"))
	if err != nil || !found || string(val) != "v2-longer-value" {
		t.Fatalf("Get(k) = %q found=%v err=%v, want v2-longer-value", val, found, err)
	}
}

func TestNaiveStore_BinaryKeysAndValues(t *testing.T) {
	s, err := OpenNaiveStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	key := []byte{0x00, 0xFF, 0x10, 0xAB}
	value := []byte{0x01, 0x02, 0x00, 0xFF}
	if err := s.Put(key, value); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.Get(key)
	if err != nil || !found || string(got) != string(value) {
		t.Fatalf("Get = %v found=%v err=%v, want %v", got, found, err, value)
	}
}

func TestOpenNaiveStore_ErrorOnUnwritableParent(t *testing.T) {
	dir := t.TempDir()
	blocker := dir + "/blocker"
	if err := os.WriteFile(blocker, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := OpenNaiveStore(blocker + "/sub")
	if err == nil {
		t.Fatal("expected an error when the parent path is a regular file")
	}
}

func TestNaiveStore_PutFailsWhenTargetPathIsDirectory(t *testing.T) {
	s, err := OpenNaiveStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	key := []byte("k")
	// Pre-create a directory at exactly the path Put would open as a
	// file, so the OpenFile call fails (EISDIR) — portable and
	// deterministic on both Linux and macOS.
	if err := os.MkdirAll(s.keyPath(key), 0755); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(key, []byte("v")); err == nil {
		t.Fatal("expected an error opening a directory as if it were a file")
	}
}

func TestNaiveStore_GetFailsWhenTargetPathIsDirectory(t *testing.T) {
	s, err := OpenNaiveStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	key := []byte("k")
	if err := os.MkdirAll(s.keyPath(key), 0755); err != nil {
		t.Fatal(err)
	}
	// Reading a directory as a file fails with a real error, distinct
	// from the "not found" case ReadFile returns for a missing path.
	_, _, err = s.Get(key)
	if err == nil {
		t.Fatal("expected an error reading a directory as if it were a file")
	}
}

func TestNaiveStore_DeleteFailsWhenTargetIsNonEmptyDirectory(t *testing.T) {
	s, err := OpenNaiveStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	key := []byte("k")
	if err := os.MkdirAll(s.keyPath(key), 0755); err != nil {
		t.Fatal(err)
	}
	// A non-empty directory: os.Remove fails with ENOTEMPTY, a real
	// error distinct from the IsNotExist case Delete otherwise ignores.
	if err := os.WriteFile(s.keyPath(key)+"/inner", []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(key); err == nil {
		t.Fatal("expected an error deleting a non-empty directory")
	}
}

// --- Put's write/sync/close error branches, via a fake fileHandle -----------

type fakeFileHandle struct {
	failWrite bool
	failSync  bool
	failClose bool
}

func (f *fakeFileHandle) Write(p []byte) (int, error) {
	if f.failWrite {
		return 0, errors.New("fakeFileHandle: simulated write failure")
	}
	return len(p), nil
}
func (f *fakeFileHandle) Sync() error {
	if f.failSync {
		return errors.New("fakeFileHandle: simulated sync failure")
	}
	return nil
}
func (f *fakeFileHandle) Close() error {
	if f.failClose {
		return errors.New("fakeFileHandle: simulated close failure")
	}
	return nil
}

func TestNaiveStore_PutWriteErrorPropagates(t *testing.T) {
	orig := openFileForWrite
	openFileForWrite = func(path string) (fileHandle, error) {
		return &fakeFileHandle{failWrite: true}, nil
	}
	defer func() { openFileForWrite = orig }()

	s, err := OpenNaiveStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Put([]byte("k"), []byte("v")); err == nil {
		t.Fatal("expected an error when the underlying write fails")
	}
}

func TestNaiveStore_PutSyncErrorPropagates(t *testing.T) {
	orig := openFileForWrite
	openFileForWrite = func(path string) (fileHandle, error) {
		return &fakeFileHandle{failSync: true}, nil
	}
	defer func() { openFileForWrite = orig }()

	s, err := OpenNaiveStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Put([]byte("k"), []byte("v")); err == nil {
		t.Fatal("expected an error when fsync fails")
	}
}

func TestNaiveStore_PutCloseErrorPropagates(t *testing.T) {
	orig := openFileForWrite
	openFileForWrite = func(path string) (fileHandle, error) {
		return &fakeFileHandle{failClose: true}, nil
	}
	defer func() { openFileForWrite = orig }()

	s, err := OpenNaiveStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Put([]byte("k"), []byte("v")); err == nil {
		t.Fatal("expected an error when the final close fails")
	}
}

func TestNaiveStore_PutOpenErrorPropagates(t *testing.T) {
	orig := openFileForWrite
	openFileForWrite = func(path string) (fileHandle, error) {
		return nil, errors.New("openFileForWrite: simulated failure")
	}
	defer func() { openFileForWrite = orig }()

	s, err := OpenNaiveStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Put([]byte("k"), []byte("v")); err == nil {
		t.Fatal("expected an error when opening the file fails")
	}
}
