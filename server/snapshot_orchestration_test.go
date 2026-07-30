package server

import (
	"errors"
	"testing"
	"time"

	"github.com/PedrowDias/key-value-store/raft"
)

// --- SetSnapshotThreshold / SeedAppliedIndex --------------------------------

func TestSetSnapshotThreshold(t *testing.T) {
	srv := New(newFakeRaftNode(), newTestTransport(t), newTestEngine(t), time.Hour)
	srv.SetSnapshotThreshold(5)
	if srv.snapshotThreshold != 5 {
		t.Fatalf("snapshotThreshold = %d, want 5", srv.snapshotThreshold)
	}
}

func TestSeedAppliedIndex(t *testing.T) {
	srv := New(newFakeRaftNode(), newTestTransport(t), newTestEngine(t), time.Hour)
	srv.SeedAppliedIndex(42)
	if srv.lastApplied != 42 {
		t.Fatalf("lastApplied = %d, want 42", srv.lastApplied)
	}
	if srv.lastSnapshotIndex != 42 {
		t.Fatalf("lastSnapshotIndex = %d, want 42", srv.lastSnapshotIndex)
	}
}

// --- maybeSnapshot -----------------------------------------------------------

func TestMaybeSnapshot_TriggersOnceThresholdReached(t *testing.T) {
	fake := newFakeRaftNode()
	fake.autoCommit = true
	eng := newTestEngine(t)
	srv := New(fake, newTestTransport(t), eng, time.Hour)
	srv.SetSnapshotThreshold(2)

	eng.Put([]byte("k1"), []byte("v1"))
	srv.lastApplied = 1
	srv.maybeSnapshot()
	if len(fake.createSnapshotCalls) != 0 {
		t.Fatalf("createSnapshotCalls = %v, want none yet (below threshold)", fake.createSnapshotCalls)
	}

	srv.lastApplied = 2
	srv.maybeSnapshot()
	if len(fake.createSnapshotCalls) != 1 {
		t.Fatalf("createSnapshotCalls = %v, want exactly one call once the threshold is reached", fake.createSnapshotCalls)
	}
	if fake.createSnapshotCalls[0].index != 2 {
		t.Fatalf("CreateSnapshot called with index %d, want 2", fake.createSnapshotCalls[0].index)
	}
	if srv.lastSnapshotIndex != 2 {
		t.Fatalf("lastSnapshotIndex = %d, want 2 after a successful snapshot", srv.lastSnapshotIndex)
	}
}

func TestMaybeSnapshot_DoesNotTriggerBelowThreshold(t *testing.T) {
	fake := newFakeRaftNode()
	srv := New(fake, newTestTransport(t), newTestEngine(t), time.Hour)
	srv.SetSnapshotThreshold(1000)
	srv.lastApplied = 5

	srv.maybeSnapshot()
	if len(fake.createSnapshotCalls) != 0 {
		t.Fatalf("createSnapshotCalls = %v, want none (well below threshold)", fake.createSnapshotCalls)
	}
}

func TestMaybeSnapshot_DisabledWhenThresholdIsZero(t *testing.T) {
	fake := newFakeRaftNode()
	srv := New(fake, newTestTransport(t), newTestEngine(t), time.Hour)
	srv.SetSnapshotThreshold(0)
	srv.lastApplied = 1_000_000

	srv.maybeSnapshot()
	if len(fake.createSnapshotCalls) != 0 {
		t.Fatalf("createSnapshotCalls = %v, want none (threshold 0 disables automatic snapshotting)", fake.createSnapshotCalls)
	}
}

func TestMaybeSnapshot_DoesNotTriggerTwiceForTheSameProgress(t *testing.T) {
	fake := newFakeRaftNode()
	srv := New(fake, newTestTransport(t), newTestEngine(t), time.Hour)
	srv.SetSnapshotThreshold(2)
	srv.lastApplied = 2

	srv.maybeSnapshot()
	srv.maybeSnapshot() // called again with no further progress
	if len(fake.createSnapshotCalls) != 1 {
		t.Fatalf("createSnapshotCalls = %v, want exactly one — a second call with no new progress must not re-trigger", fake.createSnapshotCalls)
	}
}

func TestMaybeSnapshot_EngineSnapshotErrorIsNonFatal(t *testing.T) {
	fake := newFakeRaftNode()
	eng := newTestEngine(t)
	eng.Close() // any subsequent Snapshot call now fails
	srv := New(fake, newTestTransport(t), eng, time.Hour)
	srv.SetSnapshotThreshold(1)
	srv.lastApplied = 1

	srv.maybeSnapshot() // must not panic
	if len(fake.createSnapshotCalls) != 0 {
		t.Fatalf("createSnapshotCalls = %v, want none (engine.Snapshot failed before CreateSnapshot was ever reached)", fake.createSnapshotCalls)
	}
	if srv.lastSnapshotIndex != 0 {
		t.Fatalf("lastSnapshotIndex = %d, want unchanged 0 after a failed attempt", srv.lastSnapshotIndex)
	}
}

func TestMaybeSnapshot_CreateSnapshotErrorIsNonFatal(t *testing.T) {
	fake := newFakeRaftNode()
	fake.createSnapshotErr = errors.New("fakeRaftNode: simulated CreateSnapshot failure")
	srv := New(fake, newTestTransport(t), newTestEngine(t), time.Hour)
	srv.SetSnapshotThreshold(1)
	srv.lastApplied = 1

	srv.maybeSnapshot() // must not panic
	if srv.lastSnapshotIndex != 0 {
		t.Fatalf("lastSnapshotIndex = %d, want unchanged 0 after a failed CreateSnapshot call", srv.lastSnapshotIndex)
	}
}

// --- pump()'s RestoreSnapshot wiring -----------------------------------------

func TestPump_RestoresReceivedSnapshotIntoEngine(t *testing.T) {
	// Generate valid snapshot bytes representing a DIFFERENT dataset
	// from a separate, throwaway engine — this is what a real snapshot
	// received from a leader would look like.
	sourceEng := newTestEngine(t)
	sourceEng.Put([]byte("restored-key"), []byte("restored-value"))
	snapData, err := sourceEng.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sourceEng.Close()

	fake := newFakeRaftNode()
	eng := newTestEngine(t)
	eng.Put([]byte("stale-key"), []byte("stale-value")) // must be gone after restoring
	fake.snapshotToApply = &raft.Snapshot{LastIncludedIndex: 7, LastIncludedTerm: 1, Data: snapData}

	srv := New(fake, newTestTransport(t), eng, time.Hour)
	srv.pump()

	if srv.lastApplied != 7 {
		t.Fatalf("lastApplied = %d, want 7 (the restored snapshot's LastIncludedIndex)", srv.lastApplied)
	}
	if srv.lastSnapshotIndex != 7 {
		t.Fatalf("lastSnapshotIndex = %d, want 7", srv.lastSnapshotIndex)
	}
	_, found, _ := eng.Get([]byte("stale-key"))
	if found {
		t.Fatal("expected stale-key to be gone after pump() restored the received snapshot")
	}
	val, found, _ := eng.Get([]byte("restored-key"))
	if !found || string(val) != "restored-value" {
		t.Fatalf("restored-key found=%v val=%q, want true restored-value", found, val)
	}
}

func TestPump_RestoreSnapshotErrorIsNonFatal(t *testing.T) {
	fake := newFakeRaftNode()
	eng := newTestEngine(t)
	eng.Close() // any subsequent RestoreSnapshot call now fails

	fake.snapshotToApply = &raft.Snapshot{LastIncludedIndex: 7, LastIncludedTerm: 1}
	srv := New(fake, newTestTransport(t), eng, time.Hour)

	srv.pump() // must not panic
	if srv.lastApplied != 0 {
		t.Fatalf("lastApplied = %d, want unchanged 0 after a failed RestoreSnapshot", srv.lastApplied)
	}
}
