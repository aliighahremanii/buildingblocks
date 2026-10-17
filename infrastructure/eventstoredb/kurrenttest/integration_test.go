// Package kurrenttest — integration_test.go contains the smoke test that
// verifies the test harness itself can talk to a real KurrentDB instance.
// It is the minimal end-to-end proof that:
//
//   - The KurrentDB container is up and KURRENTDB_URL is reachable.
//   - AppendToStream + ReadStream round-trip events correctly.
//   - Optimistic concurrency (NoStream vs StreamRevision) is enforced
//     by the server, not just the client.
//
// Module-specific integration tests (concurrent-rename, snapshot round-
// trip, DLQ atomicity) live alongside the modules that wire a real
// client. This file only proves the harness.
package kurrenttest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

// TestLiveClientAppendAndReadRoundTrips is the harness smoke test. If it
// skips, every other live-KurrentDB test will skip too (they all use
// OpenClient). If it fails, the harness wiring is broken and the
// integration CI job is the place to debug it.
func TestLiveClientAppendAndReadRoundTrips(t *testing.T) {
	client := OpenClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream := UniqueStreamName(t, "kurrenttest-roundtrip")

	// Append two events with NoStream (fresh stream).
	events := []kurrentdb.EventData{
		{
			EventID:     uuid.New(),
			EventType:   "test.first",
			ContentType: kurrentdb.ContentTypeJson,
			Data:        []byte(`{"i":1}`),
		},
		{
			EventID:     uuid.New(),
			EventType:   "test.second",
			ContentType: kurrentdb.ContentTypeJson,
			Data:        []byte(`{"i":2}`),
		},
	}
	if _, err := client.AppendToStream(ctx, stream,
		kurrentdb.AppendToStreamOptions{StreamState: kurrentdb.NoStream{}},
		events...,
	); err != nil {
		t.Fatalf("AppendToStream: %v", err)
	}

	// Read back and verify both events survive in order.
	rd, err := client.ReadStream(ctx, stream, kurrentdb.ReadStreamOptions{
		Direction: kurrentdb.Forwards,
		From:      kurrentdb.Start{},
	}, 10)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	defer rd.Close()

	var got []string
	for {
		we, err := rd.Recv()
		if err != nil {
			break
		}
		if we.Event == nil {
			continue
		}
		got = append(got, we.Event.EventType)
	}
	if len(got) != 2 {
		t.Fatalf("read %d events, want 2", len(got))
	}
	if got[0] != "test.first" || got[1] != "test.second" {
		t.Fatalf("event order = %v, want [test.first test.second]", got)
	}
}

// TestLiveClientEnforcesOptimisticConcurrency verifies the property the
// entire OCC fix (P0.2) depends on: when an append specifies NoStream
// on an existing stream, the server MUST reject it with a conflict.
// Without this server-side guarantee, the client-side
// toKurrentStreamState mapping is theatre.
func TestLiveClientEnforcesOptimisticConcurrency(t *testing.T) {
	client := OpenClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream := UniqueStreamName(t, "kurrenttest-occ")
	// Two DIFFERENT events (different EventIDs). If we reuse the same
	// EventID, KurrentDB treats the second append as idempotent (a
	// safety net on top of the NoStream expectation) and returns nil;
	// that would make the test pass for the wrong reason.
	event1 := kurrentdb.EventData{
		EventID:     uuid.New(),
		EventType:   "test.create",
		ContentType: kurrentdb.ContentTypeJson,
		Data:        []byte(`{}`),
	}
	event2 := kurrentdb.EventData{
		EventID:     uuid.New(),
		EventType:   "test.create",
		ContentType: kurrentdb.ContentTypeJson,
		Data:        []byte(`{}`),
	}

	// First NoStream append succeeds (stream does not exist).
	if _, err := client.AppendToStream(ctx, stream,
		kurrentdb.AppendToStreamOptions{StreamState: kurrentdb.NoStream{}}, event1,
	); err != nil {
		t.Fatalf("first NoStream append: %v", err)
	}

	// Second NoStream append MUST fail — the stream now exists at
	// revision 0, and the different EventID prevents KurrentDB's
	// idempotent-append safety net from silently accepting the duplicate.
	_, err := client.AppendToStream(ctx, stream,
		kurrentdb.AppendToStreamOptions{StreamState: kurrentdb.NoStream{}}, event2,
	)
	if err == nil {
		t.Fatal("second NoStream append: want error (server must enforce OCC), got nil")
	}

	// The error must be classifiable as a stream-revision conflict via
	// errors.As against *kurrentdb.Error with the right Code. This is
	// the exact pattern eventstore.mapAppendError uses.
	var esErr *kurrentdb.Error
	if !errors.As(err, &esErr) {
		t.Fatalf("error type = %T, want *kurrentdb.Error", err)
	}
	if esErr.Code() != kurrentdb.ErrorCodeStreamRevisionConflict &&
		esErr.Code() != kurrentdb.ErrorCodeWrongExpectedVersion {
		t.Fatalf("error code = %v, want StreamRevisionConflict or WrongExpectedVersion", esErr.Code())
	}
}
