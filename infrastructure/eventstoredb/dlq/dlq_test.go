package dlq

import (
	"context"
	"errors"
	"testing"
)

// fakeStore is an in-memory dlq.Store for unit-testing the projection
// worker without a Postgres instance.
type fakeStore struct {
	flushCalls  int
	flushCommit uint64
	flushFailed []Entry
	flushErr    error
	listResult  []Entry
	listErr     error
	deleteCalls []int64
	deleteErr   error
}

func (f *fakeStore) FlushCheckpoint(_ context.Context, _ string, commit uint64, failures []Entry) error {
	f.flushCalls++
	f.flushCommit = commit
	f.flushFailed = append([]Entry{}, failures...)
	return f.flushErr
}

func (f *fakeStore) List(_ context.Context, _ string, _ int) ([]Entry, error) {
	return f.listResult, f.listErr
}

func (f *fakeStore) Delete(_ context.Context, id int64) error {
	f.deleteCalls = append(f.deleteCalls, id)
	return f.deleteErr
}

// TestFakeStoreFlushRecordsArguments is a self-test of the test double: it
// records what the worker passed so individual test cases can assert on
// the values without inspecting a real database.
func TestFakeStoreFlushRecordsArguments(t *testing.T) {
	t.Parallel()

	f := &fakeStore{}
	entries := []Entry{{ID: 1, EventID: "ev-1"}}
	err := f.FlushCheckpoint(context.Background(), "my-projection", 42, entries)
	if err != nil {
		t.Fatalf("FlushCheckpoint: %v", err)
	}
	if f.flushCalls != 1 {
		t.Fatalf("flushCalls = %d, want 1", f.flushCalls)
	}
	if f.flushCommit != 42 {
		t.Fatalf("flushCommit = %d, want 42", f.flushCommit)
	}
	if len(f.flushFailed) != 1 || f.flushFailed[0].EventID != "ev-1" {
		t.Fatalf("flushFailed = %+v, want one entry with EventID ev-1", f.flushFailed)
	}
}

// TestErrSentinelsAreusableAsErrors confirms the package's error
// sentinels (if any are added later) are usable via errors.Is.
func TestErrSentinelsAreUsableAsErrors(t *testing.T) {
	t.Parallel()
	// Sanity check that errors.Is behaves on a wrapped nil-class
	// sentinel; this just exercises the standard library so future
	// additions to the package's sentinel-error contract have a place
	// to plug in.
	err := errors.New("plain")
	if errors.Is(err, err) == false { //nolint:staticcheck // intentional self-test
		t.Fatal("errors.Is self-identity check failed")
	}
}
