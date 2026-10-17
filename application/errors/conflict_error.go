package errors

// ConflictError represents a 409 Conflict response.
type ConflictError struct {
	Code    string
	Message string
}

func NewConflictError(code, message string) ConflictError {
	return ConflictError{Code: code, Message: message}
}

func (e ConflictError) Error() string { return e.Message }

// PermanentlyDeletedConflictMessage is the stable, actionable, non-leaking
// client message for operations rejected because the target aggregate's
// stream was hard-deleted (tombstoned) in the event store. A tombstoned
// KurrentDB stream can never be re-created in place — not even with a
// NoStream expectation — so the only remedy is registering a fresh identity.
//
// The sentence is shared verbatim by the building-blocks event-store append
// mapping (eventstore.mapAppendError) and by module-level permanent-deletion
// pre-checks, so every boundary that rejects the operation surfaces one
// identical contract (D127). Constructing errors with it:
//
//	NewConflictError(aggregateType, PermanentlyDeletedConflictMessage)
const PermanentlyDeletedConflictMessage = "permanently deleted identity cannot be recreated; register under fresh identity"
