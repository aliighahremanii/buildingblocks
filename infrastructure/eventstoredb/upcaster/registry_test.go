package upcaster

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// TestApplyNoUpcastersNeeded verifies the happy path for freshly-written
// events: detectedVersion equals target, no upcasters run, the schemaVersion
// is stamped to confirm the current version.
func TestApplyNoUpcastersNeeded(t *testing.T) {
	t.Parallel()

	r := New()
	r.DeclareTarget("evt", 2)
	// Payload already at v2 including the schemaVersion field — Apply stamps
	// (idempotently) and returns re-marshaled bytes carrying v2.
	payload := []byte(`{"schemaVersion":2,"v":"latest"}`)
	out, ver, err := r.Apply("evt", payload, 2)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if ver != 2 {
		t.Fatalf("version = %d, want 2", ver)
	}
	var peek struct {
		SchemaVersion int    `json:"schemaVersion"`
		V             string `json:"v"`
	}
	if err := json.Unmarshal(out, &peek); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if peek.SchemaVersion != 2 {
		t.Fatalf("stamped schemaVersion = %d, want 2", peek.SchemaVersion)
	}
	if peek.V != "latest" {
		t.Fatalf("non-version fields should survive stamp: v = %q", peek.V)
	}
}

// TestApplyStampsVersionOnUpgradedPayload verifies the post-chain stamping:
// a v1 payload (with no schemaVersion field) ends up carrying schemaVersion=2
// after the v1->v2 step, so the decoded struct sees the target version.
func TestApplyStampsVersionOnUpgradedPayload(t *testing.T) {
	t.Parallel()

	r := New()
	r.Register("evt", 1, func(data []byte) ([]byte, error) {
		return UpsertField(data, "v2field", "from-v1")
	})

	out, ver, err := r.Apply("evt", []byte(`{"original":"x"}`), 1)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if ver != 2 {
		t.Fatalf("version = %d, want 2", ver)
	}
	var peek struct {
		SchemaVersion int `json:"schemaVersion"`
	}
	if err := json.Unmarshal(out, &peek); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if peek.SchemaVersion != 2 {
		t.Fatalf("stamped schemaVersion = %d, want 2", peek.SchemaVersion)
	}
}

// TestApplyChainsMultipleSteps exercises a v1->v2->v3 chain where each
// upcaster adds a field, proving the chain runs in order and each step's
// output feeds the next. Final schema version is stamped onto the payload.
func TestApplyChainsMultipleSteps(t *testing.T) {
	t.Parallel()

	r := New()
	r.Register("evt", 1, func(data []byte) ([]byte, error) {
		return UpsertField(data, "v2field", "from-v1")
	})
	r.Register("evt", 2, func(data []byte) ([]byte, error) {
		return UpsertField(data, "v3field", "from-v2")
	})

	// v1 payload (no schemaVersion, mimicking a pre-versioning event).
	out, ver, err := r.Apply("evt", []byte(`{"original":"x"}`), 1)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if ver != 3 {
		t.Fatalf("version = %d, want 3", ver)
	}

	// Decode into a map[string]any so the stamped int schemaVersion coexists
	// with the string fields added by each upcaster.
	var decoded map[string]any
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if got, _ := decoded["schemaVersion"].(float64); int(got) != 3 {
		t.Fatalf("stamped schemaVersion = %v, want 3", decoded["schemaVersion"])
	}
	if got, _ := decoded["original"].(string); got != "x" {
		t.Errorf("original field lost: got %q", got)
	}
	if got, _ := decoded["v2field"].(string); got != "from-v1" {
		t.Errorf("v2field: got %q, want %q", got, "from-v1")
	}
	if got, _ := decoded["v3field"].(string); got != "from-v2" {
		t.Errorf("v3field: got %q, want %q", got, "from-v2")
	}
}

// TestApplyMissingIntermediateStep verifies the hard-error policy: if a v1->v2
// upcaster is registered but not v2->v3, applying a v1 event against a
// Declared target 3 must fail with a message identifying the missing step.
func TestApplyMissingIntermediateStep(t *testing.T) {
	t.Parallel()

	r := New()
	r.DeclareTarget("evt", 3)
	r.Register("evt", 1, func(data []byte) ([]byte, error) {
		return UpsertField(data, "v2field", "ok")
	})
	// v2->v3 deliberately NOT registered despite the declared target of 3.

	_, _, err := r.Apply("evt", []byte(`{}`), 1)
	if err == nil {
		t.Fatal("want error for missing intermediate upcaster, got nil")
	}
	if !strings.Contains(err.Error(), "v2->v3") {
		t.Fatalf("error should identify the missing step v2->v3: %v", err)
	}
}

// TestApplyVersionExceedsTarget verifies the forward-incompatibility guard:
// a stored event claiming version 4 against a target of 2 must fail rather
// than silently truncate.
func TestApplyVersionExceedsTarget(t *testing.T) {
	t.Parallel()

	r := New()
	r.DeclareTarget("evt", 2)
	_, _, err := r.Apply("evt", []byte(`{"schemaVersion":4}`), 4)
	if err == nil {
		t.Fatal("want error when detected > target, got nil")
	}
	if !strings.Contains(err.Error(), "newer than target") {
		t.Fatalf("error should mention forward-incompatibility: %v", err)
	}
}

// TestApplyZeroVersionDefaultsToOne verifies that detectedVersion <= 0 (the
// value seen when an old event has no schemaVersion field) is treated as 1.
func TestApplyZeroVersionDefaultsToOne(t *testing.T) {
	t.Parallel()

	r := New()
	r.Register("evt", 1, func(data []byte) ([]byte, error) {
		return UpsertField(data, "upgraded", "yes")
	})

	out, ver, err := r.Apply("evt", []byte(`{"x":1}`), 0)
	if err != nil {
		t.Fatalf("Apply with detectedVersion=0: %v", err)
	}
	if ver != 2 {
		t.Fatalf("version = %d, want 2", ver)
	}
	if !strings.Contains(string(out), `"upgraded":"yes"`) {
		t.Fatalf("expected upcaster to run when detectedVersion=0 (treated as 1): %s", out)
	}
}

// TestApplyUpcasterErrorPropagates verifies that a failing Upcaster surfaces
// its error, wrapped with the event type and version step for diagnosis.
func TestApplyUpcasterErrorPropagates(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("synthetic transform failure")
	r := New()
	r.Register("evt", 1, func(data []byte) ([]byte, error) {
		return nil, sentinel
	})

	_, _, err := r.Apply("evt", []byte(`{}`), 1)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error to propagate, got: %v", err)
	}
	if !strings.Contains(err.Error(), "evt") || !strings.Contains(err.Error(), "v1->v2") {
		t.Fatalf("error should name the event type and step: %v", err)
	}
}

// TestRegisterPanicsOnInvalidFromVersion documents the startup-time invariant
// that fromVersion must be >= 1; wiring mistakes should crash loudly during
// composition rather than silently misbehaving at read time.
func TestRegisterPanicsOnInvalidFromVersion(t *testing.T) {
	t.Parallel()

	r := New()
	for _, bad := range []int{0, -1, -5} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic for fromVersion=%d", bad)
				}
			}()
			r.Register("evt", bad, func([]byte) ([]byte, error) { return nil, nil })
		}()
	}
}

// TestRegisterPanicsOnNilUpcaster catches the wiring mistake of registering a
// nil transform.
func TestRegisterPanicsOnNilUpcaster(t *testing.T) {
	t.Parallel()

	r := New()
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic for nil upcaster")
			}
		}()
		r.Register("evt", 1, nil)
	}()
}

// TestTargetForDefaultsToOne verifies the per-event-type default: an event
// type with no upcasters and no DeclareTarget returns target 1, so Apply is a
// passthrough (no schema bump forced on unchanged events).
func TestTargetForDefaultsToOne(t *testing.T) {
	t.Parallel()

	r := New()
	if got := r.TargetFor("never.versioned"); got != 1 {
		t.Fatalf("TargetFor unknown type = %d, want 1", got)
	}
}

// TestTargetForDerivedFromRegisteredUpcasters verifies the per-event-type
// target is implicitly set by Register: registering v1->v2 bumps target to
// 2, and registering v2->v3 further bumps target to 3.
func TestTargetForDerivedFromRegisteredUpcasters(t *testing.T) {
	t.Parallel()

	r := New()
	r.Register("evt", 1, func([]byte) ([]byte, error) { return nil, nil })
	if got := r.TargetFor("evt"); got != 2 {
		t.Fatalf("after v1->v2 registration, TargetFor = %d, want 2", got)
	}
	r.Register("evt", 2, func([]byte) ([]byte, error) { return nil, nil })
	if got := r.TargetFor("evt"); got != 3 {
		t.Fatalf("after v2->v3 registration, TargetFor = %d, want 3", got)
	}
}

// TestDeclareTargetIsMonotonic ensures DeclareTarget cannot lower a target
// (which would defeat backfill-style upcaster chains).
func TestDeclareTargetIsMonotonic(t *testing.T) {
	t.Parallel()

	r := New()
	r.DeclareTarget("evt", 5)
	r.DeclareTarget("evt", 2) // lower — must be ignored
	if got := r.TargetFor("evt"); got != 5 {
		t.Fatalf("DeclareTarget must be monotonic; got %d, want 5", got)
	}
}

// TestApplyPassthroughForUnversionedEventType is the keystone test for
// per-event-type versioning: an event type that has NEVER had an upcaster
// registered is at target 1. Apply on a v1 (or v0/absent) payload is a
// passthrough that only stamps schemaVersion=1. This keeps unchanged events
// readable after global version bumps for OTHER event types.
func TestApplyPassthroughForUnversionedEventType(t *testing.T) {
	t.Parallel()

	r := New()
	// No upcasters or DeclareTarget for "untouched.event".

	out, ver, err := r.Apply("untouched.event", []byte(`{"x":"y"}`), 1)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if ver != 1 {
		t.Fatalf("version = %d, want 1 (no upcasters for this type)", ver)
	}

	var peek struct {
		SchemaVersion int    `json:"schemaVersion"`
		X             string `json:"x"`
	}
	if err := json.Unmarshal(out, &peek); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if peek.SchemaVersion != 1 {
		t.Fatalf("stamped schemaVersion = %d, want 1", peek.SchemaVersion)
	}
	if peek.X != "y" {
		t.Fatalf("non-version fields should survive: x = %q", peek.X)
	}
}

// TestUpsertFieldAddModifyRemove covers the three modes of the UpsertField
// helper: adding a new field, overwriting an existing one, and removing a
// field via empty string.
func TestUpsertFieldAddModifyRemove(t *testing.T) {
	t.Parallel()

	// decode returns a fresh map each call so json.Unmarshal's "absent keys
	// are left untouched in an existing map" semantics can't leak across
	// sub-tests.
	decode := func(t *testing.T, data []byte) map[string]string {
		t.Helper()
		var m map[string]string
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return m
	}

	// Add.
	out, err := UpsertField([]byte(`{"a":"1"}`), "b", "2")
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	m := decode(t, out)
	if m["a"] != "1" || m["b"] != "2" {
		t.Fatalf("add: expected a=1 b=2, got %v", m)
	}

	// Modify.
	out, err = UpsertField([]byte(`{"a":"1"}`), "a", "changed")
	if err != nil {
		t.Fatalf("modify: %v", err)
	}
	m = decode(t, out)
	if m["a"] != "changed" {
		t.Fatalf("modify: expected a=changed, got %q", m["a"])
	}

	// Remove via empty value.
	out, err = UpsertField([]byte(`{"a":"1","b":"2"}`), "b", "")
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	m = decode(t, out)
	if _, ok := m["b"]; ok {
		t.Fatalf("remove: expected b to be absent, got %q", m["b"])
	}
	if m["a"] != "1" {
		t.Fatalf("remove: expected a to survive, got %q", m["a"])
	}
}

// TestUpsertFieldRejectsBadJSON verifies the helper surfaces malformed input
// rather than silently producing an empty or corrupt payload.
func TestUpsertFieldRejectsBadJSON(t *testing.T) {
	t.Parallel()

	if _, err := UpsertField([]byte(`{not-json`), "x", "y"); err == nil {
		t.Fatal("expected error for malformed JSON input")
	}
}

// TestDeclareTargetIgnoresNonPositive ensures DeclareTarget(version<=0) is a
// no-op so wiring mistakes don't accidentally regress a type to "unversioned".
func TestDeclareTargetIgnoresNonPositive(t *testing.T) {
	t.Parallel()

	r := New()
	r.DeclareTarget("evt", 0)
	r.DeclareTarget("evt", -3)
	if got := r.TargetFor("evt"); got != 1 {
		t.Fatalf("TargetFor after non-positive DeclareTarget = %d, want 1", got)
	}
}

// TestStampSchemaVersionOnMalformedReturnsOriginal documents the safety net:
// if the payload cannot be parsed as a JSON object, stamping is skipped and
// the original bytes are returned so the downstream json.Unmarshal surfaces
// the malformed input as its own error rather than Apply hiding it.
func TestStampSchemaVersionOnMalformedReturnsOriginal(t *testing.T) {
	t.Parallel()

	malformed := []byte(`{not-json`)
	out := stampSchemaVersion(malformed, 9)
	if string(out) != string(malformed) {
		t.Fatalf("expected malformed input returned unchanged, got %s", out)
	}

	// Non-object JSON (e.g. an array) is also NOT a valid envelope target.
	nonObject := []byte(`[1,2,3]`)
	out = stampSchemaVersion(nonObject, 9)
	if string(out) != string(nonObject) {
		t.Fatalf("expected non-object input returned unchanged, got %s", out)
	}
}

// TestUpsertFieldRejectsNonObjectInput verifies the helper enforces map-types
// input rather than silently dropping the field on array/scalar payloads.
func TestUpsertFieldRejectsNonObjectInput(t *testing.T) {
	t.Parallel()

	for _, bad := range [][]byte{[]byte(`[1,2]`), []byte(`"string"`), []byte(`42`)} {
		if _, err := UpsertField(bad, "x", "y"); err == nil {
			t.Fatalf("expected error for non-object input %s", bad)
		}
	}
}

// TestNewReturnsRegistryWithInitializedMaps confirms the constructor returns a
// non-nil registry with initialized internal maps ready for Register calls.
func TestNewReturnsRegistryWithInitializedMaps(t *testing.T) {
	t.Parallel()

	r := New()
	if r == nil {
		t.Fatal("New() returned nil")
	}
	if r.upcasters == nil {
		t.Fatal("upcasters map should be initialized")
	}
	if r.targets == nil {
		t.Fatal("targets map should be initialized")
	}
	if r.TargetFor("anything") != 1 {
		t.Fatal("default target should be 1")
	}
}
