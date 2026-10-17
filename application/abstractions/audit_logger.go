package abstractions

import (
	"context"
	"time"
)

// AuditLogger writes immutable audit trail entries.
type AuditLogger interface {
	Log(ctx context.Context, event string, subjectID string, occurredAt time.Time, metadata map[string]string) error
}

type OperationalAuditLogger interface {
	LogOperationalEvent(ctx context.Context, event string, resource string, resourceID string, metadata map[string]string) error
}
