package events

import (
	"encoding/json"
	"testing"
	"time"
)

// fakeEvent is a minimal Event impl for testing the recorder mechanics.
type fakeEvent struct{ Base }

func newFakeEvent(name, id, typ string, now time.Time) fakeEvent {
	return fakeEvent{Base: NewBase(name, id, typ, now)}
}

func TestRecorderRecordsAndPullsEvents(t *testing.T) {
	t.Parallel()

	var r Recorder
	if r.EventCount() != 0 {
		t.Fatalf("zero-value recorder should have 0 events, got %d", r.EventCount())
	}

	now := time.Now().UTC()
	e1 := newFakeEvent("country.registered", "id-1", "country", now)
	e2 := newFakeEvent("country.renamed", "id-1", "country", now.Add(time.Second))

	r.Record(e1)
	r.Record(e2)

	if r.EventCount() != 2 {
		t.Fatalf("EventCount = %d, want 2", r.EventCount())
	}

	pulled := r.PullEvents()
	if len(pulled) != 2 {
		t.Fatalf("PullEvents len = %d, want 2", len(pulled))
	}
	if pulled[0].EventName() != "country.registered" {
		t.Fatalf("first pulled event = %q", pulled[0].EventName())
	}
	if pulled[1].AggregateID() != "id-1" {
		t.Fatalf("second event AggregateID = %q", pulled[1].AggregateID())
	}

	// Pulling clears the recorder.
	if r.EventCount() != 0 {
		t.Fatalf("after PullEvents, EventCount = %d, want 0", r.EventCount())
	}
	if got := r.PullEvents(); len(got) != 0 {
		t.Fatalf("PullEvents after drain should be empty, got %d", len(got))
	}
}

func TestRecorderIgnoresNil(t *testing.T) {
	t.Parallel()

	var r Recorder
	r.Record(nil)
	if r.EventCount() != 0 {
		t.Fatalf("nil event should be ignored, count = %d", r.EventCount())
	}
}

func TestPeekEventsDoesNotClear(t *testing.T) {
	t.Parallel()

	var r Recorder
	now := time.Now().UTC()
	r.Record(newFakeEvent("a.b", "id", "a", now))

	peeked := r.PeekEvents()
	if len(peeked) != 1 {
		t.Fatalf("PeekEvents len = %d", len(peeked))
	}

	// Peek should not drain.
	if r.EventCount() != 1 {
		t.Fatalf("PeekEvents should not drain, count = %d", r.EventCount())
	}

	// Mutating the peeked slice must not affect the recorder.
	peeked[0] = nil
	if r.PeekEvents()[0] == nil {
		t.Fatalf("PeekEvents should return a defensive copy")
	}
}

func TestBaseSatisfiesEventInterface(t *testing.T) {
	t.Parallel()

	var _ Event = Base{}

	now := time.Now().UTC()
	b := NewBase("country.activated", "uuid-123", "country", now)

	if b.EventName() != "country.activated" {
		t.Fatalf("EventName = %q", b.EventName())
	}
	if b.AggregateID() != "uuid-123" {
		t.Fatalf("AggregateID = %q", b.AggregateID())
	}
	if b.AggregateType() != "country" {
		t.Fatalf("AggregateType = %q", b.AggregateType())
	}
	if !b.OccurredAt().Equal(now.UTC()) {
		t.Fatalf("OccurredAt = %v, want %v", b.OccurredAt(), now.UTC())
	}
}

// TestNewBaseStampsCurrentSchemaVersion ensures every freshly-constructed Base
// carries the current schema version so writers never emit an unversioned
// event by accident.
func TestNewBaseStampsCurrentSchemaVersion(t *testing.T) {
	t.Parallel()

	b := NewBase("x.y", "id", "x", time.Now().UTC())
	if b.SchemaVersion != CurrentSchemaVersion {
		t.Fatalf("SchemaVersion = %d, want %d (CurrentSchemaVersion)", b.SchemaVersion, CurrentSchemaVersion)
	}
}

// concreteForJSON is a minimal event struct embedding Base, used to verify
// that SchemaVersion round-trips through JSON exactly like a real module event
// (the embedded Base's exported field is promoted to the enclosing struct).
type concreteForJSON struct {
	Base
	Payload string `json:"payload"`
}

// TestSchemaVersionRoundTripsJSON confirms the schemaVersion field is present
// in the marshaled JSON and survives an unmarshal back into a fresh struct.
func TestSchemaVersionRoundTripsJSON(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	orig := concreteForJSON{
		Base:    NewBase("x.y", "id-1", "x", now),
		Payload: "hello",
	}

	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var peek struct {
		SchemaVersion int `json:"schemaVersion"`
	}
	if err := json.Unmarshal(data, &peek); err != nil {
		t.Fatalf("peek unmarshal: %v", err)
	}
	if peek.SchemaVersion != CurrentSchemaVersion {
		t.Fatalf("peeked schemaVersion = %d, want %d", peek.SchemaVersion, CurrentSchemaVersion)
	}

	var decoded concreteForJSON
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.SchemaVersion != CurrentSchemaVersion {
		t.Fatalf("decoded SchemaVersion = %d, want %d", decoded.SchemaVersion, CurrentSchemaVersion)
	}
	if decoded.Payload != "hello" {
		t.Fatalf("decoded Payload = %q, want %q", decoded.Payload, "hello")
	}
}

// TestAbsentSchemaVersionDefaultsToZero verifies the contract that events
// written before versioning existed (no schemaVersion key in JSON) decode
// with SchemaVersion == 0. The registry layer interprets 0 as version 1.
func TestAbsentSchemaVersionDefaultsToZero(t *testing.T) {
	t.Parallel()

	// JSON payload with no schemaVersion field, mimicking a pre-versioning
	// event persisted in KurrentDB.
	preVersioning := []byte(`{"payload":"old"}`)

	var decoded concreteForJSON
	if err := json.Unmarshal(preVersioning, &decoded); err != nil {
		t.Fatalf("decode pre-versioning payload: %v", err)
	}
	if decoded.SchemaVersion != 0 {
		t.Fatalf("decoded SchemaVersion = %d, want 0 (absent field)", decoded.SchemaVersion)
	}
	if decoded.Payload != "old" {
		t.Fatalf("decoded Payload = %q, want %q", decoded.Payload, "old")
	}
}

// --- Enrich (read-path metadata stamping) ---

// eventWithBase is the same shape used in TestSchemaVersionRoundTripsJSON but
// kept distinct so these tests read as a self-contained group.
type eventWithBase struct {
	Base
	Payload string `json:"payload"`
}

// newEventWithBase constructs a fully-populated event identical to what an
// aggregate's behavior method would produce at write time.
func newEventWithBase(name, id, typ string, now time.Time, payload string) eventWithBase {
	return eventWithBase{
		Base:    NewBase(name, id, typ, now),
		Payload: payload,
	}
}

// TestEnrichRestoresIdentityAfterJSONRoundTrip is the regression test for the
// critical rehydration bug: after JSON marshal/unmarshal, every identity and
// timing field on Base reads as a zero value because the fields are
// unexported and therefore not serialized. Enrich must restore them so that
// Apply methods (which read e.AggregateID() / e.OccurredAt()) see the same
// values the original constructor supplied.
func TestEnrichRestoresIdentityAfterJSONRoundTrip(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	orig := newEventWithBase("country.registered", "uuid-abc", "country", now, "hello")

	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Decode into a pointer so json.Unmarshal populates the struct. This is
	// exactly the shape the Registry produces: factories return pointers.
	var decoded *eventWithBase
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Before Enrich: identity/timing are zero (the bug).
	if decoded.EventName() != "" {
		t.Fatalf("pre-Enrich EventName = %q, want empty (bug)", decoded.EventName())
	}
	if decoded.AggregateID() != "" {
		t.Fatalf("pre-Enrich AggregateID = %q, want empty (bug)", decoded.AggregateID())
	}
	if decoded.AggregateType() != "" {
		t.Fatalf("pre-Enrich AggregateType = %q, want empty (bug)", decoded.AggregateType())
	}
	if !decoded.OccurredAt().IsZero() {
		t.Fatalf("pre-Enrich OccurredAt = %v, want zero (bug)", decoded.OccurredAt())
	}

	// Enrich with the values that the event-store envelope would carry.
	Enrich(decoded, "country.registered", "uuid-abc", "country", now)

	// After Enrich: identity/timing are restored to the original values.
	if decoded.EventName() != "country.registered" {
		t.Fatalf("post-Enrich EventName = %q", decoded.EventName())
	}
	if decoded.AggregateID() != "uuid-abc" {
		t.Fatalf("post-Enrich AggregateID = %q", decoded.AggregateID())
	}
	if decoded.AggregateType() != "country" {
		t.Fatalf("post-Enrich AggregateType = %q", decoded.AggregateType())
	}
	if !decoded.OccurredAt().Equal(now) {
		t.Fatalf("post-Enrich OccurredAt = %v, want %v", decoded.OccurredAt(), now)
	}

	// Payload is unaffected by Enrich (it was carried in the JSON).
	if decoded.Payload != "hello" {
		t.Fatalf("post-Enrich Payload = %q, want %q", decoded.Payload, "hello")
	}
}

// TestEnrichDoesNotOverwriteExistingMetadata guarantees that calling Enrich on
// an event that already has its Base populated (e.g. an event still in an
// aggregate's recorder, or a second Enrich call) is a no-op. This makes
// Enrich safe to call defensively at every read boundary without risk of
// overwriting legitimate state.
func TestEnrichDoesNotOverwriteExistingMetadata(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	evt := newEventWithBase("country.registered", "uuid-orig", "country", now, "x")

	// Attempt to overwrite with different values.
	Enrich(&evt, "DIFFERENT.name", "DIFFERENT-id", "DIFFERENT-type", now.Add(time.Hour))

	if evt.EventName() != "country.registered" {
		t.Fatalf("EventName overwritten: got %q", evt.EventName())
	}
	if evt.AggregateID() != "uuid-orig" {
		t.Fatalf("AggregateID overwritten: got %q", evt.AggregateID())
	}
	if evt.AggregateType() != "country" {
		t.Fatalf("AggregateType overwritten: got %q", evt.AggregateType())
	}
	if !evt.OccurredAt().Equal(now) {
		t.Fatalf("OccurredAt overwritten: got %v, want %v", evt.OccurredAt(), now)
	}
}

// TestEnrichIsNilSafe ensures Enrich does not panic on a nil event. The read
// path calls Enrich unconditionally; a defensive nil-check keeps that safe.
func TestEnrichIsNilSafe(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Enrich(nil) panicked: %v", r)
		}
	}()

	now := time.Now().UTC()
	if got := Enrich(nil, "x.y", "id", "x", now); got != nil {
		t.Fatalf("Enrich(nil) should return nil, got %v", got)
	}
}

// TestEnrichIsIdempotentAcrossCalls verifies that calling Enrich twice with
// the same arguments has the same effect as calling it once. This matters
// because projection workers may enrich events that a future version of the
// store already enriched (e.g. when enrichment is added to additional read
// paths).
func TestEnrichIsIdempotentAcrossCalls(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	data, err := json.Marshal(newEventWithBase("country.activated", "uuid-x", "country", now, "p"))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded *eventWithBase
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	Enrich(decoded, "country.activated", "uuid-x", "country", now)
	first := decoded.OccurredAt()

	Enrich(decoded, "country.activated", "uuid-x", "country", now.Add(time.Hour))
	if !decoded.OccurredAt().Equal(first) {
		t.Fatalf("second Enrich call overwrote OccurredAt: got %v, want %v", decoded.OccurredAt(), first)
	}
}

// TestEnrichAcceptsValueReceiverEvent confirms that an event whose factory
// returns a pointer (the documented Registry contract) is enriched correctly.
// This is a contract test for the metadataSetter interface: pointer-to-struct
// must promote the *Base methods.
func TestEnrichAcceptsValueReceiverEvent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	evt := &eventWithBase{} // pointer, as the Registry produces
	Enrich(evt, "x.activated", "uuid-7", "x", now)

	if evt.EventName() != "x.activated" {
		t.Fatalf("EventName = %q", evt.EventName())
	}
	if evt.AggregateID() != "uuid-7" {
		t.Fatalf("AggregateID = %q", evt.AggregateID())
	}
	if evt.AggregateType() != "x" {
		t.Fatalf("AggregateType = %q", evt.AggregateType())
	}
	if !evt.OccurredAt().Equal(now) {
		t.Fatalf("OccurredAt = %v, want %v", evt.OccurredAt(), now)
	}
}
