package snapshot

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// fakeState is a minimal state value used to exercise Marshal/Unmarshal. It
// mirrors the shape of a module's SnapshotState struct (id + a payload field).
type fakeState struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
}

func TestMarshalRoundTrip(t *testing.T) {
	t.Parallel()

	orig := fakeState{ID: "agg-1", Payload: "hello"}

	data, err := Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// The envelope must carry the current schema version.
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.SchemaVersion != CurrentSchemaVersion {
		t.Fatalf("envelope SchemaVersion = %d, want %d", env.SchemaVersion, CurrentSchemaVersion)
	}
	if len(env.State) == 0 {
		t.Fatal("envelope State should be populated")
	}

	var got fakeState
	if err := Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got != orig {
		t.Fatalf("round-trip mismatch: got %+v, want %+v", got, orig)
	}
}

func TestUnmarshalBareStatePreEnvelope(t *testing.T) {
	t.Parallel()

	// A payload produced before the envelope existed — just the state struct
	// directly, no schemaVersion / state wrapper. Must decode transparently.
	bare := []byte(`{"id":"agg-1","payload":"old"}`)

	var got fakeState
	if err := Unmarshal(bare, &got); err != nil {
		t.Fatalf("Unmarshal bare: %v", err)
	}
	if got.ID != "agg-1" || got.Payload != "old" {
		t.Fatalf("bare decode mismatch: got %+v", got)
	}
}

func TestUnmarshalRejectsForwardVersion(t *testing.T) {
	t.Parallel()

	// An envelope claiming a schema version newer than CurrentSchemaVersion
	// must surface a clear error rather than silently mis-decoding.
	forward := []byte(`{"schemaVersion":` + itoa(CurrentSchemaVersion+1) + `,"state":{"id":"x"}}`)

	err := Unmarshal(forward, new(fakeState))
	if err == nil {
		t.Fatal("want error for forward-incompatible version, got nil")
	}
	if !strings.Contains(err.Error(), "newer than current") {
		t.Fatalf("error should mention forward-incompatibility: %v", err)
	}
}

func TestUnmarshalMalformedJSON(t *testing.T) {
	t.Parallel()

	err := Unmarshal([]byte(`{not-json`), new(fakeState))
	if err == nil {
		t.Fatal("want error for malformed JSON, got nil")
	}
}

func TestUnmarshalMalformedState(t *testing.T) {
	t.Parallel()

	// Envelope well-formed (state is valid JSON), but state's shape does not
	// match the target struct — a JSON string cannot decode into fakeState.
	// This exercises the inner-state decode-error branch.
	bad := []byte(`{"schemaVersion":1,"state":"not-an-object"}`)
	err := Unmarshal(bad, new(fakeState))
	if err == nil {
		t.Fatal("want error for malformed state, got nil")
	}
	if !strings.Contains(err.Error(), "snapshot unmarshal state") {
		t.Fatalf("error should be wrapped by Unmarshal's state-decode branch: %v", err)
	}
	_ = errors.Is // keep errors import honest
}

// TestUnmarshalStateWithMissingFields confirms that fields absent in the
// envelope's state are left at their zero value on the target, and decoding
// does not error on extra unknown fields (json's default leniency).
func TestUnmarshalStateWithMissingFields(t *testing.T) {
	t.Parallel()

	// State carries only id; payload omitted.
	data, err := Marshal(fakeState{ID: "agg-2"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got fakeState
	if err := Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.ID != "agg-2" {
		t.Fatalf("ID = %q, want %q", got.ID, "agg-2")
	}
	if got.Payload != "" {
		t.Fatalf("Payload = %q, want empty (field absent in state)", got.Payload)
	}
}

// TestMarshalRejectsUnencodableState ensures Marshal surfaces a state value
// that encoding/json cannot handle (channels, funcs, cyclic refs) rather than
// silently producing an empty envelope. We use a func value, which json.Marshal
// refuses.
func TestMarshalRejectsUnencodableState(t *testing.T) {
	t.Parallel()

	_, err := Marshal(func() {})
	if err == nil {
		t.Fatal("want error for unencodable state, got nil")
	}
	if !strings.Contains(err.Error(), "snapshot marshal state") {
		t.Fatalf("error should be wrapped by Marshal: %v", err)
	}
}

// TestUnmarshalRejectsNonPointerTarget verifies the target must be a pointer;
// passing a non-pointer is caught by json.Unmarshal and surfaced cleanly.
func TestUnmarshalRejectsNonPointerTarget(t *testing.T) {
	t.Parallel()

	// A bare struct (not a pointer) — json.Unmarshal will return an error
	// because it needs to write into the value.
	if err := Unmarshal([]byte(`{"id":"x"}`), fakeState{}); err == nil {
		t.Fatal("want error when target is not a pointer, got nil")
	}
}

// TestUnmarshalEnvelopeAtCurrentVersion exercises the version-equals-current
// path through Unmarshal (env.SchemaVersion == CurrentSchemaVersion): no
// error, state is decoded cleanly.
func TestUnmarshalEnvelopeAtCurrentVersion(t *testing.T) {
	t.Parallel()

	data, err := Marshal(fakeState{ID: "agg-current", Payload: "p"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got fakeState
	if err := Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.ID != "agg-current" || got.Payload != "p" {
		t.Fatalf("decode mismatch: got %+v", got)
	}
}

// TestUnmarshalEnvelopeWithEmptyState covers the (unusual but valid) case of
// an envelope carrying an empty state object. Decoding should succeed and
// leave the target at its zero value.
func TestUnmarshalEnvelopeWithEmptyState(t *testing.T) {
	t.Parallel()

	// Envelope with empty object state.
	env := []byte(`{"schemaVersion":1,"state":{}}`)
	var got fakeState
	if err := Unmarshal(env, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got != (fakeState{}) {
		t.Fatalf("expected zero-value fakeState, got %+v", got)
	}
}

// TestCurrentSchemaVersionIsPositive is a guardrail: a wrong constant (0 or
// negative) would silently trigger the "bare state" branch for every
// freshly-Marshaled envelope.
func TestCurrentSchemaVersionIsPositive(t *testing.T) {
	t.Parallel()

	if CurrentSchemaVersion <= 0 {
		t.Fatalf("CurrentSchemaVersion = %d, must be >= 1", CurrentSchemaVersion)
	}
}

// itoa formats v as a decimal string without pulling in strconv at the test
// level — keeping the test imports minimal.
func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
