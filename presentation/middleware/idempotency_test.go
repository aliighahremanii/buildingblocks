package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	apperrors "github.com/aliighahremanii/buildingblocks/application/errors"
	"github.com/aliighahremanii/buildingblocks/application/idempotency"
	"github.com/aliighahremanii/buildingblocks/infrastructure/webhttp"
	"github.com/aliighahremanii/buildingblocks/presentation/web"
)

// fakeStore is an in-memory idempotency.Store for unit-testing the
// middleware without a Postgres instance. It mirrors the
// unique-violation behavior of the real store: Insert on a duplicate
// (key, commandKind) returns idempotency.ErrConflict.
type fakeStore struct {
	mu      sync.Mutex
	records map[string]idempotency.Record
	// failInsert, if non-nil, makes the next Insert return this error
	// instead of inspecting the map. Used to exercise the failure path.
	failInsert error
}

func newFakeStore() *fakeStore {
	return &fakeStore{records: make(map[string]idempotency.Record)}
}

func keyOf(key, kind string) string { return kind + "/" + key }

func (f *fakeStore) Lookup(_ context.Context, key, kind string) (idempotency.Record, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.records[keyOf(key, kind)]
	return rec, ok, nil
}

func (f *fakeStore) Insert(_ context.Context, record idempotency.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failInsert != nil {
		err := f.failInsert
		f.failInsert = nil
		return err
	}
	k := keyOf(record.Key, record.CommandKind)
	if _, exists := f.records[k]; exists {
		return idempotency.ErrConflict
	}
	f.records[k] = record
	return nil
}

// recordingContext is a fakeContext that captures the status + body written
// to it, so tests can assert on the wire response.
type recordingContext struct {
	ctx      context.Context
	header   string
	status   int
	body     []byte
	bindErr  error
	bindJSON any
	request  *http.Request
	// writer is a real ResponseRecorder used by writeCached, which writes
	// the cached body directly via ResponseWriter().Write.
	writer *httptest.ResponseRecorder
}

// statusOrWriterCode returns the status from the writer if it was written to
// (via ResponseWriter().WriteHeader, the path writeBytes uses), otherwise
// the status set by JSON/NoContent methods.
func (r *recordingContext) statusOrWriterCode() int {
	if r.writer != nil && r.writer.Code != 0 {
		return r.writer.Code
	}
	return r.status
}

func (r *recordingContext) Context() context.Context  { return r.ctx }
func (r *recordingContext) Method() string            { return "POST" }
func (r *recordingContext) Path() string              { return "/test" }
func (r *recordingContext) Query(string) string       { return "" }
func (r *recordingContext) Param(string) string       { return "" }
func (r *recordingContext) Header(name string) string { return r.header }
func (r *recordingContext) SetHeader(name, value string) {
	if r.writer != nil {
		r.writer.Header().Set(name, value)
	}
}
func (r *recordingContext) BindJSON(target any) error { r.bindJSON = target; return r.bindErr }
func (r *recordingContext) JSON(status int, value any) error {
	r.status = status
	out, err := json.Marshal(value)
	if err != nil {
		return err
	}
	r.body = out
	return nil
}
func (r *recordingContext) Text(status int, value string) error {
	r.status = status
	r.body = []byte(value)
	return nil
}
func (r *recordingContext) NoContent(status int) error {
	r.status = status
	return nil
}
func (r *recordingContext) ResponseWriter() http.ResponseWriter {
	if r.writer == nil {
		r.writer = httptest.NewRecorder()
	}
	return r.writer
}
func (r *recordingContext) Request() *http.Request { return r.request }

func newRecordingContext(idempotencyKey string) *recordingContext {
	return &recordingContext{
		ctx:    context.Background(),
		header: idempotencyKey,
		writer: httptest.NewRecorder(),
	}
}

func newBodyRecordingContext(idempotencyKey, body string) *recordingContext {
	ctx := newRecordingContext(idempotencyKey)
	ctx.request = httptest.NewRequest(http.MethodPost, "/test", bytes.NewBufferString(body))
	return ctx
}

// noopInner is a HandlerFunc that always succeeds with a fixed result.
func noopInner(aggrID string, status int, body any) HandlerFunc {
	return func(_ web.Context) (HandlerResult, error) {
		return HandlerResult{AggregateID: aggrID, Status: status, Body: body}, nil
	}
}

// TestNewWithoutKeyRunsHandlerAndDoesNotCache verifies that a request
// WITHOUT an Idempotency-Key header runs the handler normally and the
// store is never queried.
func TestNewWithoutKeyRunsHandlerAndDoesNotCache(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	calls := 0
	inner := func(_ web.Context) (HandlerResult, error) {
		calls++
		return HandlerResult{AggregateID: "uuid-1", Status: 201, Body: map[string]any{"id": "uuid-1"}}, nil
	}

	h := New(store, "test.cmd", inner)
	ctx := newRecordingContext("") // no Idempotency-Key header
	if err := h(ctx); err != nil {
		t.Fatalf("h: %v", err)
	}

	if calls != 1 {
		t.Fatalf("handler called %d times, want 1", calls)
	}
	if ctx.statusOrWriterCode() != 201 {
		t.Fatalf("status = %d, want 201", ctx.statusOrWriterCode())
	}
	if store.records != nil && len(store.records) != 0 {
		t.Fatalf("expected 0 cache entries, got %d", len(store.records))
	}
}

// TestNewWithNilStoreIsPassthrough verifies that a nil store makes the
// middleware a no-op. This is the deployment that opts out of the dedup
// table.
func TestNewWithNilStoreIsPassthrough(t *testing.T) {
	t.Parallel()

	calls := 0
	inner := func(_ web.Context) (HandlerResult, error) {
		calls++
		return HandlerResult{AggregateID: "uuid", Status: 201, Body: map[string]any{"id": "uuid"}}, nil
	}

	h := New(nil, "test.cmd", inner)
	ctx := newRecordingContext("some-key")
	if err := h(ctx); err != nil {
		t.Fatalf("h: %v", err)
	}
	if calls != 1 {
		t.Fatalf("handler called %d times, want 1", calls)
	}
	if ctx.statusOrWriterCode() != 201 {
		t.Fatalf("status = %d, want 201", ctx.statusOrWriterCode())
	}
}

// TestNewWithKeyCachesAndReplays is the headline contract test: a second
// request with the same key must return the cached response WITHOUT
// re-running the inner handler. This is the wire-level idempotency
// guarantee.
func TestNewWithKeyCachesAndReplays(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	calls := 0
	inner := func(_ web.Context) (HandlerResult, error) {
		calls++
		return HandlerResult{AggregateID: "uuid-1", Status: 201, Body: map[string]any{"id": "uuid-1"}}, nil
	}

	h := New(store, "test.cmd", inner)

	// First request with key K1.
	ctx1 := newRecordingContext("K1")
	if err := h(ctx1); err != nil {
		t.Fatalf("h first: %v", err)
	}
	if calls != 1 {
		t.Fatalf("handler called %d times after first, want 1", calls)
	}
	if ctx1.statusOrWriterCode() != 201 {
		t.Fatalf("first status = %d, want 201", ctx1.statusOrWriterCode())
	}
	body1Bytes := ctx1.writer.Body.Bytes()
	var body1 map[string]any
	if err := json.Unmarshal(body1Bytes, &body1); err != nil {
		t.Fatalf("unmarshal first body: %v (body=%q)", err, string(body1Bytes))
	}
	if body1["id"] != "uuid-1" {
		t.Fatalf("first body id = %q, want %q", body1["id"], "uuid-1")
	}

	// Second request with the SAME key K1 must short-circuit.
	ctx2 := newRecordingContext("K1")
	if err := h(ctx2); err != nil {
		t.Fatalf("h second: %v", err)
	}
	if calls != 1 {
		t.Fatalf("handler called %d times after second, want 1 (cached)", calls)
	}
	// writeCached writes through ResponseWriter, so we assert on the
	// writer's recorded status, not ctx2.status.
	if ctx2.writer.Code != 201 {
		t.Fatalf("second status = %d, want 201 (cached)", ctx2.writer.Code)
	}
	var body2 map[string]any
	if err := json.Unmarshal(ctx2.writer.Body.Bytes(), &body2); err != nil {
		t.Fatalf("unmarshal second body: %v", err)
	}
	if body2["id"] != "uuid-1" {
		t.Fatalf("second body id = %q, want %q (cached)", body2["id"], "uuid-1")
	}
}

// TestNewWithDifferentKeysRunsSeparately verifies that two different keys
// produce two cache entries and two handler runs. The natural key is
// (key, commandKind) so different keys must not dedup against each other.
func TestNewWithDifferentKeysRunsSeparately(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	calls := 0
	inner := func(_ web.Context) (HandlerResult, error) {
		calls++
		return HandlerResult{AggregateID: "uuid", Status: 201, Body: map[string]any{"id": "uuid"}}, nil
	}

	h := New(store, "test.cmd", inner)

	ctx1 := newRecordingContext("K1")
	_ = h(ctx1)
	ctx2 := newRecordingContext("K2")
	_ = h(ctx2)

	if calls != 2 {
		t.Fatalf("handler called %d times, want 2 (different keys)", calls)
	}
}

// TestNewWithKeyAndHandlerErrorDoesNotCache verifies the critical
// invariant: errors are NEVER cached. Caching an error would make the
// client's retry see the same error forever, with no chance to recover.
func TestNewWithKeyAndHandlerErrorDoesNotCache(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	calls := 0
	inner := func(_ web.Context) (HandlerResult, error) {
		calls++
		return HandlerResult{}, apperrors.NewValidationError(apperrors.Field("name", "required"))
	}

	h := New(store, "test.cmd", inner)

	ctx1 := newRecordingContext("K1")
	err := h(ctx1)
	if err == nil {
		t.Fatal("h first: want error, got nil")
	}
	if calls != 1 {
		t.Fatalf("handler called %d times after first, want 1", calls)
	}

	// Second request with same key must NOT short-circuit — the first
	// produced an error, no cache entry was written, the handler runs
	// again so the client can retry.
	ctx2 := newRecordingContext("K1")
	_ = h(ctx2)
	if calls != 2 {
		t.Fatalf("handler called %d times after second, want 2 (errors not cached)", calls)
	}
}

// TestNewWithConflictReplaysWinner verifies that when Insert returns
// ErrConflict (a parallel request won the race), the middleware replays
// the winner's cached response rather than our own. This guarantees that
// two parallel POSTs with the same key both see the SAME response, which
// is the strongest idempotency guarantee.
func TestNewWithConflictReplaysWinner(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	// Pre-seed the cache with the winner's response: a different body
	// from what our inner handler would produce.
	winnerRecord := idempotency.Record{
		Key:            "K1",
		CommandKind:    "test.cmd",
		AggregateID:    "winner-id",
		ResponseStatus: 201,
		ResponseBody:   json.RawMessage(`{"id":"winner-id"}`),
	}
	store.records[keyOf("K1", "test.cmd")] = winnerRecord

	inner := func(_ web.Context) (HandlerResult, error) {
		return HandlerResult{AggregateID: "loser-id", Status: 201, Body: map[string]any{"id": "loser-id"}}, nil
	}

	h := New(store, "test.cmd", inner)
	ctx := newRecordingContext("K1")
	if err := h(ctx); err != nil {
		t.Fatalf("h: %v", err)
	}

	// writeCached writes through ResponseWriter, so we assert on the
	// writer's recorded status.
	if ctx.writer.Code != 201 {
		t.Fatalf("status = %d, want 201 (winner)", ctx.writer.Code)
	}
	bodyBytes := ctx.writer.Body.Bytes()
	var body map[string]any
	if err := json.Unmarshal(bodyBytes, &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if body["id"] != "winner-id" {
		t.Fatalf("body id = %q, want %q (replay winner, not our loser)", body["id"], "winner-id")
	}
}

// TestNewCaches204Responses verifies that 204 No Content responses are
// cached and replayed as 204 rather than a JSON body.
func TestNewCaches204Responses(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	calls := 0
	inner := func(_ web.Context) (HandlerResult, error) {
		calls++
		return HandlerResult{AggregateID: "uuid", Status: 204, Body: nil}, nil
	}

	h := New(store, "test.cmd", inner)

	ctx1 := newRecordingContext("K1")
	_ = h(ctx1)
	if ctx1.status != 204 {
		t.Fatalf("first status = %d, want 204", ctx1.status)
	}

	ctx2 := newRecordingContext("K1")
	_ = h(ctx2)
	if calls != 1 {
		t.Fatalf("handler called %d times, want 1", calls)
	}
	// 204 cached responses go through ctx.NoContent, which sets
	// ctx.status (the recordingContext's JSON/NoContent methods update
	// ctx.status; writeCached for 204 also calls NoContent).
	if ctx2.status != 204 && ctx2.writer.Code != 204 {
		t.Fatalf("second status = %d (writer=%d), want 204 (cached)", ctx2.status, ctx2.writer.Code)
	}
}

// TestIsConcurrencyErrorDetectsConcurrencyError verifies the helper used
// by Register handlers to translate post-crash 409s into
// lookup-and-replay-on-retry.
func TestIsConcurrencyErrorDetectsConcurrencyError(t *testing.T) {
	t.Parallel()

	if IsConcurrencyError(nil) {
		t.Fatal("IsConcurrencyError(nil) = true, want false")
	}
	if IsConcurrencyError(errors.New("plain")) {
		t.Fatal("IsConcurrencyError(plain) = true, want false")
	}
	ce := apperrors.NewConcurrencyError("country", "uuid", 1, 2)
	if !IsConcurrencyError(ce) {
		t.Fatal("IsConcurrencyError(ConcurrencyError) = false, want true")
	}
	if !IsConcurrencyError(wrapErr(ce)) {
		t.Fatal("IsConcurrencyError(wrapped ConcurrencyError) = false, want true")
	}
}

func wrapErr(err error) error {
	return &wrapW{cause: err}
}

type wrapW struct{ cause error }

func (w *wrapW) Error() string { return "wrap: " + w.cause.Error() }
func (w *wrapW) Unwrap() error { return w.cause }

// TestNewLookupFailureDoesNotBlock verifies that a store.Lookup failure
// falls through to running the handler. The handler's own OCC controls
// make that safe (the worst case is the command runs twice and the
// NoStream/AtRevision guard rejects the duplicate).
func TestNewLookupFailureDoesNotBlock(t *testing.T) {
	t.Parallel()

	// fakeStore with a tweak: inject a Lookup failure by setting a
	// poisoned key. We use a wrapping store to simulate.
	type failingStore struct{ *fakeStore }
	// Easier: construct a one-off store that always fails Lookup.
	store := failingLookupStore{}

	calls := 0
	inner := func(_ web.Context) (HandlerResult, error) {
		calls++
		return HandlerResult{AggregateID: "uuid", Status: 201, Body: map[string]any{"id": "uuid"}}, nil
	}

	h := New(store, "test.cmd", inner)
	ctx := newRecordingContext("K1")
	if err := h(ctx); err != nil {
		t.Fatalf("h: %v", err)
	}
	if calls != 1 {
		t.Fatalf("handler called %d times, want 1 (lookup failed, fell through)", calls)
	}
	if ctx.statusOrWriterCode() != 201 {
		t.Fatalf("status = %d, want 201", ctx.statusOrWriterCode())
	}
}

type failingLookupStore struct{}

func (failingLookupStore) Lookup(_ context.Context, _, _ string) (idempotency.Record, bool, error) {
	return idempotency.Record{}, false, errors.New("store down")
}
func (failingLookupStore) Insert(_ context.Context, _ idempotency.Record) error {
	return nil
}

// TestNewEndToEndAgainstServeMux verifies the middleware integrates cleanly
// with a real net/http mux via the web.Router — the read path the actual
// API uses. This is a smoke test for the wiring shapes (no dedup logic
// asserted beyond what the unit tests above cover).
func TestNewEndToEndAgainstServeMux(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	router := webhttp.NewRouter(mux)
	store := newFakeStore()
	inner := noopInner("uuid-1", 201, map[string]any{"id": "uuid-1"})
	router.Post("/items", New(store, "sample1.item.register", inner))

	// First request with key.
	req1 := httptest.NewRequest(http.MethodPost, "/items", nil)
	req1.Header.Set(HeaderName, "K1")
	rec1 := httptest.NewRecorder()
	mux.ServeHTTP(rec1, req1)
	if rec1.Code != 201 {
		t.Fatalf("first status = %d, want 201", rec1.Code)
	}

	// Second request with same key must short-circuit to the cached body.
	req2 := httptest.NewRequest(http.MethodPost, "/items", nil)
	req2.Header.Set(HeaderName, "K1")
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != 201 {
		t.Fatalf("second status = %d, want 201 (cached)", rec2.Code)
	}
	body2, err := io.ReadAll(rec2.Body)
	if err != nil {
		t.Fatalf("read second body: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(body2, &body); err != nil {
		t.Fatalf("unmarshal second body: %v", err)
	}
	if body["id"] != "uuid-1" {
		t.Fatalf("second body id = %q, want %q (cached)", body["id"], "uuid-1")
	}
}

func TestNewRejectsChangedBodyForSameKey(t *testing.T) {
	store := newFakeStore()
	calls := 0
	h := New(store, "test.cmd", func(_ web.Context) (HandlerResult, error) {
		calls++
		return HandlerResult{AggregateID: "uuid-1", Status: http.StatusNoContent}, nil
	})

	if err := h(newBodyRecordingContext("K1", `{"warehouse":"W1"}`)); err != nil {
		t.Fatalf("first request: %v", err)
	}
	err := h(newBodyRecordingContext("K1", `{"warehouse":"W2"}`))
	var conflict apperrors.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("second request error = %v, want ConflictError", err)
	}
	if calls != 1 {
		t.Fatalf("handler calls = %d, want 1", calls)
	}
}

// TestIdempotencyReplayRawBytesIdentical (R1-01 regression test) verifies
// that the first response and the cached replay produce BYTE-IDENTICAL
// response bodies. The original bug: the first response was written via
// ctx.JSON (json.Encoder.Encode → trailing newline) while the cached replay
// used json.Marshal (no trailing newline). This test runs through the REAL
// webhttp adapter (not the recordingContext fake) so the Encode path is
// exercised. The fake recordingContext.JSON uses json.Marshal and therefore
// cannot detect the difference.
//
// Scope note (D-R2-01 / D021): this test guards the IN-MEMORY serialization
// path — it uses newFakeStore, which returns cached bytes verbatim and never
// round-trips through a real column. It CANNOT detect storage-layer
// re-serialization (e.g. the JSONB whitespace-canonicalization that caused
// D-R2-01). The DB-backed guard for that class of defect lives in
// internal/buildingblocks/infrastructure/postgres/idempotency
// (TestStoreLookupReturnsExactCachedBytesNoJsonbCanonicalization).
func TestIdempotencyReplayRawBytesIdentical(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	router := webhttp.NewRouter(mux)
	store := newFakeStore()
	inner := noopInner("uuid-1", 201, map[string]any{"id": "uuid-1"})
	router.Post("/items", New(store, "test.cmd", inner))

	// First request with key K1.
	req1 := httptest.NewRequest(http.MethodPost, "/items", nil)
	req1.Header.Set(HeaderName, "K1")
	rec1 := httptest.NewRecorder()
	mux.ServeHTTP(rec1, req1)
	if rec1.Code != 201 {
		t.Fatalf("first status = %d, want 201", rec1.Code)
	}
	body1 := rec1.Body.Bytes()

	// Second request with same key K1 must replay the cached response.
	req2 := httptest.NewRequest(http.MethodPost, "/items", nil)
	req2.Header.Set(HeaderName, "K1")
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != 201 {
		t.Fatalf("second status = %d, want 201 (cached)", rec2.Code)
	}
	body2 := rec2.Body.Bytes()

	// The critical assertion: raw bytes must be identical. Previously the
	// first response had a trailing \n (from json.Encoder.Encode) while the
	// cached replay did not.
	if !bytes.Equal(body1, body2) {
		t.Fatalf("response bodies differ:\n  first:  %q\n  cached: %q", string(body1), string(body2))
	}

	// Content-Type must also match.
	if rec1.Header().Get("Content-Type") != rec2.Header().Get("Content-Type") {
		t.Fatalf("Content-Type mismatch: first=%q cached=%q",
			rec1.Header().Get("Content-Type"), rec2.Header().Get("Content-Type"))
	}
}
