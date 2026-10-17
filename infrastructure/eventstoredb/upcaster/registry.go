// Package upcaster provides a registry of JSON-to-JSON transform functions
// that upgrade persisted domain-event payloads from one schema version to the
// next.
//
// Upcasters run on READ only — existing events in a KurrentDB stream are never
// rewritten. When a breaking change is made to an event struct's shape (field
// rename, retype, removal, or semantic shift), the module author:
//
//  1. Widens the event struct to the new shape.
//  2. Registers an Upcaster for (eventType, fromVersion = oldVersion) via
//     [Registry.Register]. The act of registering implicitly declares the
//     event type's new target version (fromVersion+1).
//
// The [Registry.Apply] method walks the chain v1->v2->...->vN for the given
// event type, returning the upgraded JSON bytes ready to be unmarshaled into
// the latest struct. If any intermediate step lacks a registered Upcaster,
// Apply returns a hard error rather than silently skipping — this forces
// authors to write every migration step explicitly and prevents partial
// upgrades that could corrupt aggregate state.
//
// # Per-event-type versioning
//
// Versioning is PER EVENT TYPE, not global. An event type's "target version"
// is derived from the upcasters registered for it (the highest
// fromVersion+1). Event types that have never had an upcaster registered are
// treated as perpetually at version 1 — their payloads pass through Apply
// unchanged regardless of what [events.CurrentSchemaVersion] says. This means
// bumping the global constant to introduce a v2 for ONE event type does not
// affect the 20 other event types that did not change shape.
//
// This package is pure Go (only the standard library) and carries no
// dependency on KurrentDB, Postgres, or any module's domain types.
package upcaster

import (
	"encoding/json"
	"fmt"
)

// Upcaster transforms a JSON event payload from version N to version N+1. The
// input is the raw JSON bytes of the N payload; the output is the raw JSON
// bytes of the N+1 payload. Implementations typically unmarshal into a
// version-N-specific struct, translate, and re-marshal as the version-N+1
// shape — but a byte-level edit (e.g. via map[string]json.RawMessage) is also
// valid when only a field rename is needed.
//
// Upcasters MUST be deterministic and side-effect-free: the same input always
// produces the same output, and no global state is mutated. They run inside
// read paths (stream replay, projection workers) and may be called many times
// for the same persisted event.
//
// Upcasters do NOT need to touch the "schemaVersion" field — [Apply] stamps
// the final version into the payload automatically after the chain completes.
type Upcaster func(data []byte) ([]byte, error)

// Key identifies one step in an upcaster chain: transforming events of the
// given EventType from FromVersion to FromVersion+1.
type Key struct {
	EventType   string
	FromVersion int
}

// Registry holds the set of registered upcasters and per-event-type target
// versions. A zero-value Registry (or one constructed via [New]) applies no
// upgrades: every event type defaults to target 1 and Apply passes payloads
// through unchanged.
//
// A Registry is safe for concurrent reads after registration is complete.
// Registration is expected to happen at startup before any goroutine reads.
type Registry struct {
	upcasters map[Key]Upcaster
	targets   map[string]int
}

// New constructs an empty Registry. Per-event-type target versions are
// derived from registered upcasters (fromVersion+1) or [Registry.DeclareTarget]
// calls. Event types with no upcasters and no DeclareTarget default to
// version 1 and pass through Apply unchanged.
func New() *Registry {
	return &Registry{
		upcasters: make(map[Key]Upcaster),
		targets:   make(map[string]int),
	}
}

// TargetFor returns the current schema version that Apply will upgrade
// eventType toward. Returns 1 for event types that have never been versioned
// (no upcasters registered, no [Registry.DeclareTarget] call).
func (r *Registry) TargetFor(eventType string) int {
	if t, ok := r.targets[eventType]; ok && t > 0 {
		return t
	}
	return 1
}

// DeclareTarget records that eventType's current schema version is version.
// This is needed only for event types introduced at a version >1 without any
// upcasters (i.e., brand-new event types added after a global version bump).
// Event types that have upcasters registered automatically get their target
// bumped via [Register], so DeclareTarget is rarely called directly.
//
// version must be >= 1; calling with version <= 0 is a no-op.
func (r *Registry) DeclareTarget(eventType string, version int) {
	if version <= 0 {
		return
	}
	if version > r.targets[eventType] {
		r.targets[eventType] = version
	}
}

// Register associates an Upcaster with the step from fromVersion to
// fromVersion+1 for the given EventType. Also implicitly declares the event
// type's target as at least fromVersion+1 (see [DeclareTarget]).
//
// Panics if fromVersion is less than 1 (versions are 1-indexed) to catch
// wiring mistakes at startup. Registering the same (EventType, FromVersion)
// key twice overwrites the previous Upcaster.
func (r *Registry) Register(eventType string, fromVersion int, u Upcaster) {
	if fromVersion < 1 {
		panic(fmt.Sprintf("upcaster: fromVersion must be >= 1, got %d for %q", fromVersion, eventType))
	}
	if u == nil {
		panic(fmt.Sprintf("upcaster: nil upcaster for %q v%d", eventType, fromVersion))
	}
	r.upcasters[Key{EventType: eventType, FromVersion: fromVersion}] = u
	r.DeclareTarget(eventType, fromVersion+1)
}

// Apply upgrades data from detectedVersion up to the event type's target
// version (as reported by [TargetFor]), chaining registered upcasters
// v(N)->v(N+1)->...->v(target).
//
// Behavior:
//
//   - detectedVersion <= 0 is treated as 1 (events written before versioning
//     existed carry no schemaVersion field).
//   - If the event type has no upcasters registered (target == 1), Apply is a
//     passthrough: it stamps the detected version (defaulting to 1) and
//     returns. This is the normal path for the vast majority of event types
//     that have never changed shape.
//   - If detectedVersion == target, no upcasters run; the target version is
//     stamped. This is the normal path for freshly-written events of versioned
//     types.
//   - If detectedVersion > target, Apply returns a hard error: the stored
//     event is newer than the code can handle (forward-incompatibility).
//   - If detectedVersion < target and any intermediate step lacks a
//     registered Upcaster, Apply returns a hard error identifying the missing
//     step. This prevents silent partial upgrades.
//
// After the chain completes (or when no chain is needed), Apply stamps the
// final schema version into the "schemaVersion" field of the JSON payload.
// This frees upcaster authors from having to bump the version field
// themselves and guarantees the decoded struct always reflects the version it
// was upgraded to.
//
// The returned int is the final version reached (always == target on success).
func (r *Registry) Apply(eventType string, data []byte, detectedVersion int) ([]byte, int, error) {
	if detectedVersion <= 0 {
		detectedVersion = 1
	}
	target := r.TargetFor(eventType)

	if detectedVersion > target {
		return nil, 0, fmt.Errorf(
			"upcaster: event %q at version %d is newer than target %d (code upgrade required)",
			eventType, detectedVersion, target,
		)
	}

	currentData := data
	currentVersion := detectedVersion
	for currentVersion < target {
		u, ok := r.upcasters[Key{EventType: eventType, FromVersion: currentVersion}]
		if !ok {
			return nil, 0, fmt.Errorf(
				"upcaster: missing %s v%d->v%d step (register an upcaster to bridge the gap)",
				eventType, currentVersion, currentVersion+1,
			)
		}
		next, err := u(currentData)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"upcaster: %s v%d->v%d transform failed: %w",
				eventType, currentVersion, currentVersion+1, err,
			)
		}
		currentData = next
		currentVersion++
	}
	// Stamp the final schema version so the decoded struct reflects the
	// upgrade. This is idempotent when the payload already carries it (e.g.
	// freshly-written events at the current version) and normalizes legacy
	// payloads that never had the field.
	stamped := stampSchemaVersion(currentData, currentVersion)
	return stamped, currentVersion, nil
}

// stampSchemaVersion sets the top-level "schemaVersion" field in the JSON
// payload to version. If the payload is malformed or not a JSON object, the
// original bytes are returned unchanged — the caller's json.Unmarshal will
// surface the malformed input as its own error.
//
// The json.Marshal step cannot fail on the map produced here: every value in
// the map came from a successful json.Unmarshal (so it is encodable), and the
// only insertion is an int (also always encodable). No error branch is needed.
func stampSchemaVersion(data []byte, version int) []byte {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return data
	}
	m["schemaVersion"] = version
	out, err := json.Marshal(m)
	if err != nil {
		// Unreachable for valid maps built from JSON input; keep the
		// defensive return so a future Go stdlib regression cannot
		// silently corrupt the payload.
		return data
	}
	return out
}

// UpsertField is a convenience helper for upcasters that need to add or
// overwrite a single top-level JSON field while leaving all other fields
// intact. It is useful when a new field is synthesized from an existing one
// (e.g. duplicating an Intl-only name into a Local slot).
//
// A nil value removes the field. This helper is intentionally limited to
// top-level string fields; more complex transforms should marshal/unmarshal
// into typed structs within the caller's Upcaster.
func UpsertField(data []byte, field, value string) ([]byte, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("upsert field %q: unmarshal: %w", field, err)
	}
	if value == "" {
		delete(m, field)
	} else {
		m[field] = value
	}
	out, err := json.Marshal(m)
	if err != nil {
		// Unreachable for maps built from valid JSON input; defensive guard
		// against future stdlib regressions.
		return nil, fmt.Errorf("upsert field %q: marshal: %w", field, err)
	}
	return out, nil
}
