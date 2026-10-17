package commands

import (
	"context"
	"errors"
	"testing"

	"github.com/aliighahremanii/buildingblocks/application/abstractions"
)

func TestRequireOperationalAuditLogsAfterSuccessfulCommand(t *testing.T) {
	t.Parallel()

	logger := &stubOperationalAuditLogger{}
	handler := RequireOperationalAudit[testAuditCommand, string](logger, func(command testAuditCommand) (string, string, string, map[string]string) {
		return "UpdateOrder", "order", command.ID, map[string]string{"source": "test"}
	})(HandlerFunc[testAuditCommand, string](func(ctx context.Context, command testAuditCommand) (string, error) {
		return "ok", nil
	}))

	result, err := handler.Handle(context.Background(), testAuditCommand{ID: "O-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result != "ok" {
		t.Fatalf("result = %q, want ok", result)
	}
	if logger.event != "UpdateOrder" || logger.resource != "order" || logger.resourceID != "O-1" || logger.metadata["source"] != "test" {
		t.Fatalf("audit log = %#v", logger)
	}
}

func TestRequireOperationalAuditDoesNotLogFailedCommand(t *testing.T) {
	t.Parallel()

	logger := &stubOperationalAuditLogger{}
	expectedErr := errors.New("command failed")
	handler := RequireOperationalAudit[testAuditCommand, string](logger, func(command testAuditCommand) (string, string, string, map[string]string) {
		return "UpdateOrder", "order", command.ID, nil
	})(HandlerFunc[testAuditCommand, string](func(ctx context.Context, command testAuditCommand) (string, error) {
		return "", expectedErr
	}))

	_, err := handler.Handle(context.Background(), testAuditCommand{ID: "O-1"})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("err = %v, want command failure", err)
	}
	if logger.called {
		t.Fatal("audit should not be logged for failed command")
	}
}

func TestRequireOperationalAuditFailsCommandWhenAuditFails(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("audit failed")
	logger := &stubOperationalAuditLogger{err: expectedErr}
	handler := RequireOperationalAudit[testAuditCommand, string](logger, func(command testAuditCommand) (string, string, string, map[string]string) {
		return "UpdateOrder", "order", command.ID, nil
	})(HandlerFunc[testAuditCommand, string](func(ctx context.Context, command testAuditCommand) (string, error) {
		return "ok", nil
	}))

	result, err := handler.Handle(context.Background(), testAuditCommand{ID: "O-1"})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("err = %v, want audit failure", err)
	}
	if result != "" {
		t.Fatalf("result = %q, want zero value", result)
	}
}

type testAuditCommand struct {
	ID string
}

type stubOperationalAuditLogger struct {
	called     bool
	event      string
	resource   string
	resourceID string
	metadata   map[string]string
	err        error
}

func (l *stubOperationalAuditLogger) LogOperationalEvent(ctx context.Context, event string, resource string, resourceID string, metadata map[string]string) error {
	l.called = true
	l.event = event
	l.resource = resource
	l.resourceID = resourceID
	l.metadata = metadata
	return l.err
}

var _ abstractions.OperationalAuditLogger = (*stubOperationalAuditLogger)(nil)
