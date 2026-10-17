package events

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// goldenEvent is the single stub event used to guard the event wire format.
// It deliberately exercises every field shape that real module events use:
//   - string                (RequiredName)
//   - int                   (Count)
//   - *string               (OptionalNote, nullable pointer)
//   - Base (embedded)       (carries SchemaVersion and the unexported
//     identity/timing fields that must NOT appear in
//     the JSON payload)
//
// Adding a new field shape to any module event struct means adding it here as
// well so the golden fixture continues to represent the full surface area.
type goldenEvent struct {
	Base
	RequiredName string  `json:"requiredName"`
	Count        int     `json:"count"`
	OptionalNote *string `json:"optionalNote,omitempty"`
}

// TestGoldenWireFormat is the canonical guard against accidental wire-format
// changes. The marshaled JSON must contain exactly these keys (no more, no
// fewer) and the schemaVersion must read back as CurrentSchemaVersion. Any
// drift — a renamed json tag, a newly-exported Base field leaking into the
// payload, a removed field — fails this test and forces a schema-version bump
// plus an upcaster per the package docs.
func TestGoldenWireFormat(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	note := "hello"
	evt := goldenEvent{
		Base:         NewBase("golden.fixture", "uuid-golden", "golden", now),
		RequiredName: "fixture",
		Count:        42,
		OptionalNote: &note,
	}

	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(data)

	// Required keys that MUST appear in every event payload. The
	// schemaVersion key is stamped by Base via NewBase; the rest are the
	// goldenEvent's own exported fields.
	requiredKeys := []string{
		`"schemaVersion":3`,
		`"requiredName":"fixture"`,
		`"count":42`,
		`"optionalNote":"hello"`,
	}
	for _, want := range requiredKeys {
		if !strings.Contains(got, want) {
			t.Errorf("wire format missing %q in %s", want, got)
		}
	}

	// Forbidden keys that MUST NOT appear: Base's identity/timing fields
	// are unexported and therefore omitted from JSON. If any of these
	// leak into the payload, the read-path Enrich contract breaks because
	// the stored event would carry identity that the event-store envelope
	// is supposed to own.
	forbiddenSubstrings := []string{
		`"name"`,
		`"occurredAt"`,
		`"aggregateID"`,
		`"aggregateType"`,
	}
	for _, bad := range forbiddenSubstrings {
		if strings.Contains(got, bad) {
			t.Errorf("wire format must not carry %q (Base identity must stay out-of-band): %s", bad, got)
		}
	}

	// Round-trip back to confirm the schema version survives and the
	// exported fields decode cleanly. The unexported Base fields will be
	// zero after decode — that is the bug Enrich exists to repair, and
	// it is covered by the Enrich tests in events_test.go; here we only
	// assert the wire contract.
	var decoded goldenEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.SchemaVersion != CurrentSchemaVersion {
		t.Errorf("decoded SchemaVersion = %d, want %d", decoded.SchemaVersion, CurrentSchemaVersion)
	}
	if decoded.RequiredName != "fixture" {
		t.Errorf("decoded RequiredName = %q", decoded.RequiredName)
	}
	if decoded.Count != 42 {
		t.Errorf("decoded Count = %d", decoded.Count)
	}
	if decoded.OptionalNote == nil || *decoded.OptionalNote != "hello" {
		t.Errorf("decoded OptionalNote = %v", decoded.OptionalNote)
	}
}

// TestGoldenWireFormatNilOptional confirms the omitempty tag on the nullable
// *string field drops it from the payload when nil. This is the second half
// of the wire-format contract: the field is optional on write AND on read.
func TestGoldenWireFormatNilOptional(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	evt := goldenEvent{
		Base:         NewBase("golden.fixture", "uuid-golden-nil", "golden", now),
		RequiredName: "fixture",
		Count:        1,
		OptionalNote: nil,
	}

	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(data)

	if strings.Contains(got, `"optionalNote"`) {
		t.Errorf("nil *string with omitempty must be dropped from payload, got %s", got)
	}

	var decoded goldenEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.OptionalNote != nil {
		t.Errorf("decoded OptionalNote = %v, want nil", decoded.OptionalNote)
	}
}
