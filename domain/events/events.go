// Package events provides the domain-event recording infrastructure used by
// aggregates to signal state changes without depending on any application or
// infrastructure concern.
//
// Aggregates embed Recorder (by value, unexported) and call Record from within
// their behavior methods. Application middleware (commands.PublishDomainEvents)
// pulls recorded events after a successful command and forwards them to an
// events.Publisher — a NoOp in phase 1, an outbox writer in phase 2.
//
// This package is pure Go (only the standard library) so it is safe for the
// domain layer to import.
//
// # Schema versioning
//
// Every event carries a SchemaVersion stamped by NewBase at write time. The
// current schema version is [CurrentSchemaVersion]. When a breaking change is
// made to an event struct's shape (field rename, retype, removal, or semantic
// shift), callers MUST:
//
//  1. Bump [CurrentSchemaVersion] by one.
//  2. Register a v(N)->v(N+1) [upcaster] in the module's eventstore registry
//     for every affected event type.
//
// Events already persisted in KurrentDB under an older schema are upgraded
// transparently on read by the upcaster chain before being unmarshaled into
// the latest struct. An absent schemaVersion field (events written before
// versioning existed) is treated as version 1.
//
// # Read-path enrichment
//
// Base's identity/timing fields (name, aggregateID, aggregateType, occurredAt)
// are unexported and therefore not part of the JSON payload. They are stamped
// at write time by [NewBase], dropped at serialization, and restored at read
// time by [Enrich] using metadata from the event-store envelope (event type,
// stream identity, commit timestamp). Every read path (event store
// ReadStream, projection workers, snapshot replay) MUST call Enrich on
// freshly-decoded events before handing them to aggregate Apply methods or
// downstream consumers. Otherwise Apply methods that read
// [Event.AggregateID] / [Event.OccurredAt] would see empty values, corrupting
// aggregate state on rehydration.
package events

import "time"

// CurrentSchemaVersion is the schema version stamped on every event written by
// the current code. When an event struct's shape changes in a breaking way,
// bump this constant and register an upcaster for the previous version. See
// the package documentation for the full workflow.
//
// History:
//   - v1: initial versioned envelope (introduces the schemaVersion field).
//   - v2: events may carry split Local+Intl names instead of Intl-only;
//     RegisteredEvent variants dropped the redundant concatenated Name field.
//   - v3: BOL RegistrationPendingEvent gains the generated BolNumber field
//     and the new bol.numberAssigned backfill event (customer feedback
//     batch 6); order events gain ExpertId/archived (batch 5). All changes
//     are additive with omitempty, but the bump keeps every event type's
//     upcaster target aligned with the global stamp.
const CurrentSchemaVersion = 3

// Event is the contract every domain event satisfies. Implementations are
// concrete structs defined in each aggregate's package (e.g.
// country.RenamedEvent).
type Event interface {
	EventName() string
	OccurredAt() time.Time
	AggregateID() string
	AggregateType() string
}

// Recorder is embedded by aggregate roots to collect domain events raised
// during behavior methods. The zero value is ready to use.
//
// Embed as an unexported field:
//
//	type Country struct {
//	    recorder Recorder
//	    ...
//	}
type Recorder struct {
	events []Event
}

// Record appends an event to the recorder. Safe to call on a zero-value
// Recorder.
func (r *Recorder) Record(e Event) {
	if e == nil {
		return
	}
	r.events = append(r.events, e)
}

// PullEvents returns all recorded events and clears the recorder. The
// aggregate is returned to a "clean" state, ready for the next unit of work.
func (r *Recorder) PullEvents() []Event {
	out := r.events
	r.events = nil
	return out
}

// PeekEvents returns a copy of recorded events without clearing. Useful in
// tests.
func (r *Recorder) PeekEvents() []Event {
	out := make([]Event, len(r.events))
	copy(out, r.events)
	return out
}

// EventCount returns the number of recorded events.
func (r *Recorder) EventCount() int {
	return len(r.events)
}

// Base provides the common fields embedded in every concrete event struct to
// satisfy the Event interface without repeating boilerplate.
//
// Embed as an unexported field; set the fields in the constructor:
//
//	type RenamedEvent struct {
//	    Base
//	    OldName string
//	    NewName string
//	}
//
// The exported SchemaVersion field is serialized to JSON so that readers can
// detect the schema version of stored events and apply upcasters when needed.
// It is set automatically by [NewBase] to [CurrentSchemaVersion] and should
// not be modified by callers. The remaining fields (name, occurredAt,
// aggregateID, aggregateType) are unexported and therefore not serialized;
// KurrentDB carries them out-of-band via stream identity and event metadata.
// They are stamped back onto a decoded event by [Enrich] on the read path,
// so Apply methods that read [Event.AggregateID] or [Event.OccurredAt] see
// the same values the original constructor supplied.
type Base struct {
	// SchemaVersion is the event-payload schema version. Exported so
	// encoding/json serializes it; the json key is "schemaVersion". Absent on
	// read (older events) decodes as 0, which the registry treats as 1.
	SchemaVersion int `json:"schemaVersion"`

	name          string
	occurredAt    time.Time
	aggregateID   string
	aggregateType string
}

// NewBase constructs a Base with the given fields and the current schema
// version. occurredAt is normalized to UTC.
func NewBase(name, aggregateID, aggregateType string, now time.Time) Base {
	return Base{
		SchemaVersion: CurrentSchemaVersion,
		name:          name,
		occurredAt:    now.UTC(),
		aggregateID:   aggregateID,
		aggregateType: aggregateType,
	}
}

// EventName returns the event type identifier (e.g. "country.renamed").
func (b Base) EventName() string { return b.name }

// OccurredAt returns the UTC timestamp the event was raised.
func (b Base) OccurredAt() time.Time { return b.occurredAt }

// AggregateID returns the UUID of the aggregate that raised the event.
func (b Base) AggregateID() string { return b.aggregateID }

// AggregateType returns the aggregate type name (e.g. "country").
func (b Base) AggregateType() string { return b.aggregateType }

// metadataSetter is implemented by every pointer-to-concrete-event struct
// because each embeds Base by value, so *Base's methods are promoted to
// *RegisteredEvent, *RenamedEvent, etc. The unexported method keeps the seam
// internal to the events package: external callers reach it via [Enrich].
type metadataSetter interface {
	setMetadata(name, aggregateID, aggregateType string, occurredAt time.Time)
}

// setMetadata stamps the identity/timing fields onto the Base. Only zero-valued
// fields are overwritten, so calling this on a freshly-constructed event
// (whose Base was populated by NewBase) is a no-op; calling it on a freshly
// JSON-decoded event (whose Base identity/timing fields are zero because they
// are unexported and therefore not serialized) restores them.
//
// This is the read-path counterpart to NewBase: NewBase stamps the fields at
// write time, JSON drops them at serialization, setMetadata restores them at
// deserialization, and [Enrich] is the public façade that callers use.
func (b *Base) setMetadata(name, aggregateID, aggregateType string, occurredAt time.Time) {
	if b.name == "" {
		b.name = name
	}
	if b.aggregateID == "" {
		b.aggregateID = aggregateID
	}
	if b.aggregateType == "" {
		b.aggregateType = aggregateType
	}
	if b.occurredAt.IsZero() {
		b.occurredAt = occurredAt.UTC()
	}
}

// Enrich stamps a decoded event with the identity/timing metadata that the
// JSON payload did not carry (because Base's identity/timing fields are
// unexported and therefore not serialized). The fields are sourced from the
// event-store envelope: name is the event-type string (e.g. "country.registered"),
// aggregateType and aggregateID together identify the owning stream,
// occurredAt is the event-store commit timestamp.
//
// Enrich is the read-path counterpart to [NewBase]: at write time NewBase
// populates Base from in-memory arguments; JSON serialization drops every
// unexported field; at read time Enrich restores them from the surrounding
// event-store envelope.
//
// Enrich is idempotent: only zero-valued fields are stamped, so calling it on
// an event that was already populated (e.g. a freshly-constructed event still
// in an aggregate's recorder) is a safe no-op. Calling it twice with the same
// arguments is also a no-op after the first call.
//
// Enrich is nil-safe: a nil evt is returned unchanged. If evt does not embed
// Base (and therefore cannot satisfy the internal setter interface), it also
// passes through unchanged — but every event in this codebase embeds Base.
func Enrich(evt Event, name, aggregateID, aggregateType string, occurredAt time.Time) Event {
	if evt == nil {
		return evt
	}
	if m, ok := evt.(metadataSetter); ok {
		m.setMetadata(name, aggregateID, aggregateType, occurredAt)
	}
	return evt
}
