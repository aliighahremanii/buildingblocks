package registry

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	domainevents "github.com/aliighahremanii/buildingblocks/domain/events"
	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/upcaster"
)

// stubEvent is a minimal concrete event satisfying domainevents.Event purely
// for registry tests — it does not belong to any module and lives only here.
// The Value2 field models a v2 rename: v2 payloads carry "value2" instead of
// "value", and the v1->v2 upcaster below rewrites the key.
type stubEvent struct {
	domainevents.Base
	Value  string `json:"value"`
	Value2 string `json:"value2"`
}

func newStubEvent() domainevents.Event { return &stubEvent{} }

func TestRegistryUnmarshalRegistered(t *testing.T) {
	r := New()
	r.Register("stub.occurred", newStubEvent)

	ev, err := r.Unmarshal("stub.occurred", []byte(`{"value":"hello"}`))
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	s, ok := ev.(*stubEvent)
	if !ok {
		t.Fatalf("got %T, want *stubEvent", ev)
	}
	if s.Value != "hello" {
		t.Fatalf("Value mismatch: got %q, want %q", s.Value, "hello")
	}
}

func TestRegistryUnknownEventType(t *testing.T) {
	r := New()
	_, err := r.Unmarshal("does.not.exist", []byte(`{}`))
	if err == nil {
		t.Fatal("want error for unknown event type, got nil")
	}
	// Unmarshal of an unregistered type must surface a recognizable message
	// rather than a generic json failure.
	var syntax *json.SyntaxError
	if errors.As(err, &syntax) {
		t.Fatalf("unknown-type error should not be a json.SyntaxError: %v", err)
	}
}

func TestRegistryUnmarshalBadJSON(t *testing.T) {
	r := New()
	r.Register("stub.occurred", newStubEvent)

	if _, err := r.Unmarshal("stub.occurred", []byte(`{not-json`)); err == nil {
		t.Fatal("want error for malformed json, got nil")
	}
}

func TestRegistryNewReturnsNonNil(t *testing.T) {
	if New() == nil {
		t.Fatal("New() returned nil")
	}
}

// --- Upcaster integration ---

// stringUpcaster synthesizes a v(N+1) shape by renaming the "value" field to
// "value2". Used by the upcaster-integration tests below.
func stringUpcaster(oldField, newField string) upcaster.Upcaster {
	return func(data []byte) ([]byte, error) {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		if v, ok := m[oldField]; ok {
			m[newField] = v
			delete(m, oldField)
		}
		return json.Marshal(m)
	}
}

// TestRegistryUnmarshalWithUpcasters verifies that a v1 payload (no
// schemaVersion field, "value" key) is upgraded to v2 (schemaVersion=2,
// "value2" key) before being decoded into the fresh concrete struct.
func TestRegistryUnmarshalWithUpcasters(t *testing.T) {
	t.Parallel()

	u := upcaster.New()
	u.Register("stub.occurred", 1, stringUpcaster("value", "value2"))

	r := NewWithUpcasters(u)
	r.Register("stub.occurred", newStubEvent)

	// v1 payload mimicking an event persisted before v2 introduced the rename.
	v1 := []byte(`{"value":"hello"}`)
	ev, err := r.Unmarshal("stub.occurred", v1)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	s, ok := ev.(*stubEvent)
	if !ok {
		t.Fatalf("got %T, want *stubEvent", ev)
	}
	if s.Value2 != "hello" {
		t.Fatalf("Value2 (mapped from value): got %q, want %q", s.Value2, "hello")
	}
	if s.SchemaVersion != 2 {
		t.Fatalf("SchemaVersion on decoded event = %d, want 2 (target after upcast)",
			s.SchemaVersion)
	}
}

// TestRegistryUnmarshalUpcasterFailurePropagates confirms a chain error
// (missing intermediate) surfaces from Unmarshal wrapped with the event
// type, rather than silently passing through.
func TestRegistryUnmarshalUpcasterFailurePropagates(t *testing.T) {
	t.Parallel()

	u := upcaster.New()
	u.DeclareTarget("stub.occurred", 3) // target v3 but only v1->v2 registered
	u.Register("stub.occurred", 1, stringUpcaster("value", "value2"))

	r := NewWithUpcasters(u)
	r.Register("stub.occurred", newStubEvent)

	_, err := r.Unmarshal("stub.occurred", []byte(`{"schemaVersion":1,"value":"x"}`))
	if err == nil {
		t.Fatal("want upcaster-chain error, got nil")
	}
	if !strings.Contains(err.Error(), "stub.occurred") {
		t.Fatalf("error should name the event type: %v", err)
	}
}

// TestRegistryUnmarshalWithoutUpcastersPreservesLegacyBehavior ensures that
// Registries created via New() (no upcasters) decode payloads as before —
// the schemaVersion field round-trips opportunistically but no transform runs.
func TestRegistryUnmarshalWithoutUpcastersPreservesLegacyBehavior(t *testing.T) {
	t.Parallel()

	r := New()
	r.Register("stub.occurred", newStubEvent)

	payload := []byte(`{"schemaVersion":2,"value":"hello"}`)
	ev, err := r.Unmarshal("stub.occurred", payload)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	s := ev.(*stubEvent)
	if s.Value != "hello" {
		t.Fatalf("Value = %q, want %q", s.Value, "hello")
	}
	if s.SchemaVersion != 2 {
		t.Fatalf("SchemaVersion = %d, want 2 (round-tripped from payload)", s.SchemaVersion)
	}
}

// TestRegistrySetUpcasters tests that attaching upcasters post-construction
// flips Unmarshal into version-aware mode.
func TestRegistrySetUpcasters(t *testing.T) {
	t.Parallel()

	r := New()
	r.Register("stub.occurred", newStubEvent)
	if r.Upcasters() != nil {
		t.Fatal("expected nil upcasters initially")
	}

	u := upcaster.New()
	u.Register("stub.occurred", 1, stringUpcaster("value", "value2"))
	r.SetUpcasters(u)
	if r.Upcasters() == nil {
		t.Fatal("expected non-nil upcasters after SetUpcasters")
	}

	// Now the v1 payload should be upgraded before decode: v1's `value` key
	// gets renamed to `value2`, populating Value2 on the decoded struct.
	ev, err := r.Unmarshal("stub.occurred", []byte(`{"value":"v1"}`))
	if err != nil {
		t.Fatalf("unmarshal after SetUpcasters: %v", err)
	}
	if ev.(*stubEvent).Value2 != "v1" {
		t.Fatalf("expected upgrade to map value->value2: got %q", ev.(*stubEvent).Value2)
	}
}

// TestDetectSchemaVersion covers the peek helper directly: the field is
// honored when present, absent/malformed payloads default to 1.
func TestDetectSchemaVersion(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		data []byte
		want int
	}{
		{"present v3", []byte(`{"schemaVersion":3,"x":1}`), 3},
		{"present v1", []byte(`{"schemaVersion":1}`), 1},
		{"absent (legacy)", []byte(`{"value":"old"}`), 1},
		{"explicit zero defaults to 1", []byte(`{"schemaVersion":0}`), 1},
		{"negative defaults to 1", []byte(`{"schemaVersion":-2}`), 1},
		{"malformed defaults to 1", []byte(`{not-json`), 1},
		{"non-object defaults to 1", []byte(`[1,2,3]`), 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := detectSchemaVersion(tc.data); got != tc.want {
				t.Fatalf("detectSchemaVersion = %d, want %d", got, tc.want)
			}
		})
	}
}

// Ensure errors.As is still referenced to keep imports honest if the test
// suite grows new error-mapping cases.
var _ = errors.As
