// Package registry provides a generic event-type registry that replaces the
// per-module hardcoded `switch eventType` blocks used to unmarshal domain
// events stored in KurrentDB.
//
// Each event-sourced module constructs a *Registry once (usually in its
// composition root) and registers a factory func for every concrete event
// struct it produces. The event store and projection workers share the same
// registry so the wiring lives in exactly one place per module.
//
// # Schema-version-aware unmarshaling
//
// When an optional [*upcaster.Registry] is attached via [Registry.SetUpcasters]
// or [NewWithUpcasters], Unmarshal detects the schemaVersion of each stored
// payload, runs the upcaster chain to bring it up to the current version, and
// only then decodes it into the fresh concrete struct. A nil upcaster
// registry preserves the legacy behavior (decode directly), which keeps
// modules that have not yet adopted versioning unaffected.
//
// The registry is safe for concurrent use after registration is complete —
// registration is expected to happen at startup before any goroutine reads.
package registry

import (
	"encoding/json"
	"fmt"
	"sync"

	domainevents "github.com/aliighahremanii/buildingblocks/domain/events"
	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/upcaster"
)

// Registry maps KurrentDB event-type strings to a factory that returns a
// pointer to a zero-value concrete event struct. Unmarshal then JSON-decodes
// the stored payload into that fresh struct.
//
// When upcasters is non-nil, Unmarshal transparently upgrades stored payloads
// from their detected schema version to the current version before decoding.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]func() domainevents.Event
	upcasters *upcaster.Registry
}

// New constructs an empty Registry ready for Register calls. No upcasters are
// attached; Unmarshal decodes stored payloads directly into the concrete
// struct. Use [NewWithUpcasters] or [Registry.SetUpcasters] to enable
// schema-version-aware unmarshaling.
func New() *Registry {
	return &Registry{factories: make(map[string]func() domainevents.Event)}
}

// NewWithUpcasters constructs an empty Registry whose Unmarshal will run the
// given upcaster chain before decoding payloads. Modules that have adopted
// event schema versioning should use this constructor (or call SetUpcasters)
// in their composition root.
func NewWithUpcasters(u *upcaster.Registry) *Registry {
	return &Registry{
		factories: make(map[string]func() domainevents.Event),
		upcasters: u,
	}
}

// SetUpcasters attaches (or replaces) the upcaster chain used by Unmarshal.
// Passing nil disables version-aware unmarshaling. This is intended for use
// at composition time; callers must not call it concurrently with Unmarshal.
func (r *Registry) SetUpcasters(u *upcaster.Registry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.upcasters = u
}

// Upcasters returns the currently-attached upcaster registry, or nil if none.
// Useful for modules that want to assert at startup that upcasters were wired.
func (r *Registry) Upcasters() *upcaster.Registry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.upcasters
}

// Register associates eventType with a factory that returns a fresh pointer
// to the concrete event struct. The factory MUST return a pointer so
// json.Unmarshal can decode into it; a value type would silently fail to
// populate.
//
// Register is safe to call concurrently but in practice all registrations
// happen at startup before reads.
func (r *Registry) Register(eventType string, factory func() domainevents.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[eventType] = factory
}

// Unmarshal looks up the factory for eventType, creates a fresh event value,
// and JSON-decodes data into it. An unknown event type returns an error
// formatted with the offending type so callers can identify the gap.
//
// When an upcaster registry is attached, the data bytes are first upgraded
// from their detected schema version (read from the "schemaVersion" JSON
// field; absent = version 1) to the upcaster target before decoding. An
// upcaster failure (missing step, forward-incompatible version, transform
// error) is returned wrapped with the event type for diagnosis.
func (r *Registry) Unmarshal(eventType string, data []byte) (domainevents.Event, error) {
	r.mu.RLock()
	factory, ok := r.factories[eventType]
	u := r.upcasters
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("registry: unknown event type %q", eventType)
	}

	// When an upcaster chain is attached, detect the payload's schema
	// version and upgrade to the current version before decoding. A nil
	// chain preserves the legacy direct-decode behavior.
	if u != nil {
		detected := detectSchemaVersion(data)
		upgraded, _, err := u.Apply(eventType, data, detected)
		if err != nil {
			return nil, fmt.Errorf("registry: upcast %s: %w", eventType, err)
		}
		data = upgraded
	}

	e := factory()
	if err := json.Unmarshal(data, e); err != nil {
		return nil, fmt.Errorf("registry: unmarshal %s: %w", eventType, err)
	}
	return e, nil
}

// detectSchemaVersion peeks at the "schemaVersion" field in the JSON payload
// without decoding the whole struct. Returns 1 when the field is absent
// (events written before versioning existed) so callers can route old events
// through the v1->... upcaster chain.
func detectSchemaVersion(data []byte) int {
	var peek struct {
		SchemaVersion int `json:"schemaVersion"`
	}
	// Best-effort: malformed JSON or non-object payloads surface as 1, which
	// is correct for the only unversioned format that ever existed.
	if err := json.Unmarshal(data, &peek); err != nil {
		return 1
	}
	if peek.SchemaVersion <= 0 {
		return 1
	}
	return peek.SchemaVersion
}
