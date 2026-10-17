package errors

// ConcurrencyError is returned when a Save's expected stream revision does not
// match the actual revision in the event store. It signals an optimistic
// concurrency conflict: another command committed changes to the same
// aggregate between the time the caller loaded it and the time they tried to
// save.
//
// Callers should NOT blindly retry: the business state has changed, so the
// load-mutate-save cycle must restart from a fresh load. At the API layer,
// this surfaces as HTTP 409 Conflict so the client can decide whether to
// re-fetch and re-submit.
//
// ConcurrencyError implements the same shape as [ConflictError] (Code +
// Message) so the existing transport mapping (WriteProblemError → HTTP 409)
// handles it via errors.As through ConflictError. Embedding ConflictError
// would require pointer-receiver semantics; instead we expose Code/Message
// fields directly and provide the same access pattern. A future
// type-assertion helper can unify them if needed.
type ConcurrencyError struct {
	// Code is the problem-response "code" field, identifying the conflict
	// category (e.g. "OptimisticConcurrencyConflict").
	Code string
	// Message is the human-readable problem-response "message" field.
	Message string
	// AggregateType is the type of the conflicting aggregate (e.g. "country").
	// Included for logging/metrics; not part of the HTTP response body.
	AggregateType string
	// AggregateID is the UUID of the conflicting aggregate. Included for
	// logging/metrics; not part of the HTTP response body.
	AggregateID string
	// ExpectedRevision is the stream revision the caller expected (the
	// revision they loaded at). -1 means "no stream" (Register conflict).
	// Included for logging/metrics; not part of the HTTP response body.
	ExpectedRevision int64
	// ActualRevision is the stream revision the event store reported at
	// append time. -1 means the server did not report a value. Included for
	// logging/metrics; not part of the HTTP response body.
	ActualRevision int64
}

// NewConcurrencyError constructs a ConcurrencyError with the standard code
// and message for OCC conflicts.
func NewConcurrencyError(aggregateType, aggregateID string, expected, actual int64) ConcurrencyError {
	return ConcurrencyError{
		Code:             ConcurrencyConflictCode,
		Message:          ConcurrencyConflictMessage,
		AggregateType:    aggregateType,
		AggregateID:      aggregateID,
		ExpectedRevision: expected,
		ActualRevision:   actual,
	}
}

func (e ConcurrencyError) Error() string { return e.Message }

// ConcurrencyConflictCode and ConcurrencyConflictMessage are the
// problem-response defaults for optimistic-concurrency conflicts.
const (
	ConcurrencyConflictCode    = "ConcurrencyConflict"
	ConcurrencyConflictMessage = "The resource was modified by another command. Reload and retry."
)
