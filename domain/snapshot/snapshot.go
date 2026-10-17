// Package snapshot provides a versioned JSON envelope for serializing
// event-sourced aggregate snapshots.
//
// A snapshot payload is structured as:
//
//	{
//	  "schemaVersion": N,
//	  "state": { ... module-specific aggregate state ... }
//	}
//
// The schemaVersion field evolves independently of the event schemaVersion
// (defined in [events]). When a module changes its aggregate's snapshot shape
// in a breaking way, callers bump [CurrentSchemaVersion] and (in the future)
// register a snapshot upcaster analogous to the event upcasters.
//
// Pre-envelope payloads (bare state structs without the wrapping object) are
// handled transparently by [Unmarshal]: when the schemaVersion field is
// absent, the entire payload is treated as the bare state. This keeps the
// envelope adoption non-breaking for existing snapshot rows.
//
// This package is pure Go (only the standard library) and may be imported by
// any layer, including domain aggregates' MarshalState / UnmarshalState
// methods.
package snapshot

import (
	"encoding/json"
	"fmt"
)

// CurrentSchemaVersion is the snapshot-envelope schema version stamped by
// [Marshal]. When a module's snapshot shape changes in a breaking way, this
// constant is bumped and a snapshot upcaster is registered (once that
// mechanism is wired).
//
// History:
//   - v1: initial versioned envelope.
const CurrentSchemaVersion = 1

// Envelope is the JSON structure wrapping every versioned snapshot.
type Envelope struct {
	// SchemaVersion is the snapshot-envelope version. Absent on read for
	// pre-envelope payloads; Unmarshal treats absence as "bare state".
	SchemaVersion int             `json:"schemaVersion,omitempty"`
	State         json.RawMessage `json:"state,omitempty"`
}

// Marshal wraps state in a versioned envelope at [CurrentSchemaVersion] and
// returns the serialized bytes.
//
// state is the module-specific aggregate state value (typically a
// SnapshotState struct). It is JSON-encoded once into Envelope.State so the
// outer wrapper carries clean key separation.
func Marshal(state any) ([]byte, error) {
	raw, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("snapshot marshal state: %w", err)
	}
	return json.Marshal(Envelope{
		SchemaVersion: CurrentSchemaVersion,
		State:         raw,
	})
}

// Unmarshal decodes a (possibly enveloped) snapshot payload into target.
//
// Behavior:
//
//   - If the payload carries a non-zero schemaVersion field, it is treated as
//     a versioned envelope. A schemaVersion newer than [CurrentSchemaVersion]
//     returns an error (forward-incompatible).
//   - If the payload has no schemaVersion field (pre-envelope bare state),
//     the entire payload is decoded directly into target.
//
// Future snapshot upcasters would be applied between envelope detection and
// state decoding; for now, only the version guard runs.
//
// target must be a pointer to a struct (typically a module's SnapshotState).
func Unmarshal(data []byte, target any) error {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return fmt.Errorf("snapshot unmarshal envelope: %w", err)
	}

	// No schemaVersion field means pre-envelope bare state. Decode the whole
	// payload directly. This keeps envelope adoption non-breaking.
	if env.SchemaVersion == 0 {
		if err := json.Unmarshal(data, target); err != nil {
			return fmt.Errorf("snapshot unmarshal bare state: %w", err)
		}
		return nil
	}

	if env.SchemaVersion > CurrentSchemaVersion {
		return fmt.Errorf(
			"snapshot schema version %d is newer than current %d (code upgrade required)",
			env.SchemaVersion, CurrentSchemaVersion,
		)
	}

	// Future: apply snapshot upcasters for env.SchemaVersion < CurrentSchemaVersion.

	if err := json.Unmarshal(env.State, target); err != nil {
		return fmt.Errorf("snapshot unmarshal state: %w", err)
	}
	return nil
}
