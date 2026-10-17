package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aliighahremanii/buildingblocks/application/idempotency"
	"github.com/aliighahremanii/buildingblocks/infrastructure/postgres/postgrestest"
)

// TestStoreLookupMissReturnsNotFound verifies the contract that a missing
// record returns found=false with no error. Callers branch on `found`
// rather than inspecting the error string.
func TestStoreLookupMissReturnsNotFound(t *testing.T) {
	t.Parallel()
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	postgrestest.TruncateTables(t, pool, "command_idempotency")

	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, found, err := store.Lookup(ctx, "missing-key", "missing.kind")
	if err != nil {
		t.Fatalf("Lookup miss: %v", err)
	}
	if found {
		t.Fatal("found = true, want false for missing key")
	}
}

// TestStoreInsertThenLookupRoundTrips verifies the happy path: an Insert
// followed by a Lookup with the same (key, commandKind) returns the exact
// record we stored.
func TestStoreInsertThenLookupRoundTrips(t *testing.T) {
	t.Parallel()
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	postgrestest.TruncateTables(t, pool, "command_idempotency")

	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	original := idempotency.Record{
		Key:            "K-roundtrip",
		CommandKind:    "test.kind",
		AggregateID:    "uuid-abc",
		ResponseStatus: 201,
		ResponseBody:   json.RawMessage(`{"id":"uuid-abc","name":"alice"}`),
	}
	if err := store.Insert(ctx, original); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	got, found, err := store.Lookup(ctx, "K-roundtrip", "test.kind")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if !found {
		t.Fatal("found = false, want true after Insert")
	}
	if got.Key != original.Key {
		t.Fatalf("Key = %q, want %q", got.Key, original.Key)
	}
	if got.CommandKind != original.CommandKind {
		t.Fatalf("CommandKind = %q, want %q", got.CommandKind, original.CommandKind)
	}
	if got.AggregateID != original.AggregateID {
		t.Fatalf("AggregateID = %q, want %q", got.AggregateID, original.AggregateID)
	}
	if got.ResponseStatus != original.ResponseStatus {
		t.Fatalf("ResponseStatus = %d, want %d", got.ResponseStatus, original.ResponseStatus)
	}
	// ResponseBody MUST round-trip byte-for-byte. The idempotency wire
	// contract requires a retried request to return the EXACT response body
	// bytes the first attempt produced. When the cache column was JSONB,
	// PostgreSQL canonicalized whitespace on read (adding a space after ':'
	// and ','), breaking byte-identity (D-R2-01). With BYTEA the cached
	// bytes are returned verbatim. See decisions D016 and D021.
	if !bytes.Equal(got.ResponseBody, original.ResponseBody) {
		t.Fatalf("ResponseBody bytes differ:\n  stored: %q\n  read:   %q",
			string(original.ResponseBody), string(got.ResponseBody))
	}
}

// TestStoreLookupReturnsExactCachedBytesNoJsonbCanonicalization is the
// DB-backed regression test for D-R2-01 (regression of D016). It pins the
// exact boundary the defect named: the cached response body must be read
// back BYTE-FOR-BYTE, with no whitespace canonicalization.
//
// When the cache column was JSONB, PostgreSQL's JSONB text output added a
// space after ':' and ',', so a body stored as `{"id":"x"}` was read back as
// `{"id": "x"}` — semantically equal JSON but different wire bytes. That
// broke the idempotency wire contract ("the retried response must be the
// EXACT same JSON body"). The in-memory middleware test
// (TestIdempotencyReplayRawBytesIdentical) could not catch this because its
// fake store returns bytes verbatim and never round-trips through a real
// column; only this live-DB test can.
//
// This test uses a MULTI-FIELD body with NO spaces (the form json.Marshal
// produces and the form the middleware caches), so any space-injecting
// re-serialization would change the bytes. It fails against a JSONB column
// and passes against BYTEA.
func TestStoreLookupReturnsExactCachedBytesNoJsonbCanonicalization(t *testing.T) {
	t.Parallel()
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	postgrestest.TruncateTables(t, pool, "command_idempotency")

	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Multi-field body with NO whitespace — exactly what json.Marshal emits
	// and what the middleware caches. JSONB would read this back as
	// `{"id":"uuid-d021","name":"alice","status":"active"}` only if there
	// were no spaces to inject; the danger is the colon/comma spacing, so
	// we assert the raw bytes including every ':' and ','.
	stored := json.RawMessage(`{"id":"uuid-d021","name":"alice","status":"active"}`)
	record := idempotency.Record{
		Key:            "K-D021-bytes",
		CommandKind:    "test.kind",
		AggregateID:    "uuid-d021",
		ResponseStatus: 201,
		ResponseBody:   stored,
	}
	if err := store.Insert(ctx, record); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	got, found, err := store.Lookup(ctx, "K-D021-bytes", "test.kind")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if !found {
		t.Fatal("found = false, want true after Insert")
	}
	if !bytes.Equal([]byte(got.ResponseBody), []byte(stored)) {
		t.Fatalf("D-R2-01 regression: cached response body bytes changed on read (JSONB-style whitespace canonicalization detected):\n  stored: %q\n  read:   %q",
			string(stored), string(got.ResponseBody))
	}
}

// TestStoreInsertDuplicateReturnsErrConflict verifies the
// unique-violation detection that the middleware relies on for
// parallel-race resolution. A second Insert with the same (key, kind)
// MUST return idempotency.ErrConflict so the middleware can Lookup and
// replay the winner.
func TestStoreInsertDuplicateReturnsErrConflict(t *testing.T) {
	t.Parallel()
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	postgrestest.TruncateTables(t, pool, "command_idempotency")

	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	record := idempotency.Record{
		Key:            "K-conflict",
		CommandKind:    "test.kind",
		AggregateID:    "uuid-1",
		ResponseStatus: 201,
		ResponseBody:   json.RawMessage(`{}`),
	}
	if err := store.Insert(ctx, record); err != nil {
		t.Fatalf("first Insert: %v", err)
	}

	err := store.Insert(ctx, record)
	if !errors.Is(err, idempotency.ErrConflict) {
		t.Fatalf("second Insert: err = %v, want idempotency.ErrConflict", err)
	}
}

// TestStoreInsertDifferentKindsDoNotConflict verifies that the natural key
// is (key, commandKind): the same Idempotency-Key reused across two
// different commands gets two separate records rather than a false
// conflict.
func TestStoreInsertDifferentKindsDoNotConflict(t *testing.T) {
	t.Parallel()
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	postgrestest.TruncateTables(t, pool, "command_idempotency")

	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	record1 := idempotency.Record{
		Key: "K-shared", CommandKind: "test.register", AggregateID: "uuid-A",
		ResponseStatus: 201, ResponseBody: json.RawMessage(`{}`),
	}
	record2 := idempotency.Record{
		Key: "K-shared", CommandKind: "test.rename", AggregateID: "uuid-B",
		ResponseStatus: 204, ResponseBody: json.RawMessage(`{}`),
	}
	if err := store.Insert(ctx, record1); err != nil {
		t.Fatalf("Insert register: %v", err)
	}
	if err := store.Insert(ctx, record2); err != nil {
		t.Fatalf("Insert rename: %v (different kind must not conflict)", err)
	}

	got1, found1, err := store.Lookup(ctx, "K-shared", "test.register")
	if err != nil || !found1 {
		t.Fatalf("Lookup register: found=%v err=%v", found1, err)
	}
	if got1.AggregateID != "uuid-A" {
		t.Fatalf("register record AggregateID = %q, want uuid-A", got1.AggregateID)
	}

	got2, found2, err := store.Lookup(ctx, "K-shared", "test.rename")
	if err != nil || !found2 {
		t.Fatalf("Lookup rename: found=%v err=%v", found2, err)
	}
	if got2.AggregateID != "uuid-B" {
		t.Fatalf("rename record AggregateID = %q, want uuid-B", got2.AggregateID)
	}
}
