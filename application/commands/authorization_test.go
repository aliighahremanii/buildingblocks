package commands

import (
	"context"
	"errors"
	"testing"

	appabstractions "github.com/aliighahremanii/buildingblocks/application/abstractions"
	apperrors "github.com/aliighahremanii/buildingblocks/application/errors"
)

type stubCurrentUser struct {
	authenticated bool
	permissionOK  bool
}

func (s stubCurrentUser) UserID(context.Context) (string, bool) { return "", false }

func (s stubCurrentUser) IsAuthenticated(context.Context) bool { return s.authenticated }

func (s stubCurrentUser) HasPermission(context.Context, string) bool { return s.permissionOK }

var _ appabstractions.CurrentUser = stubCurrentUser{}

func TestRequireAuthenticatedReturnsUnauthorizedError(t *testing.T) {
	t.Parallel()

	handler := RequireAuthenticated[struct{}, struct{}](stubCurrentUser{})(
		HandlerFunc[struct{}, struct{}](func(context.Context, struct{}) (struct{}, error) {
			t.Fatal("expected middleware to stop the handler")
			return struct{}{}, nil
		}),
	)

	_, err := handler.Handle(context.Background(), struct{}{})
	if err == nil {
		t.Fatal("expected unauthorized error")
	}

	var unauthorized apperrors.UnauthorizedError
	if !errors.As(err, &unauthorized) {
		t.Fatalf("expected UnauthorizedError, got %T", err)
	}
	if unauthorized.Code != apperrors.AuthenticationRequiredCode {
		t.Fatalf("expected code %q, got %q", apperrors.AuthenticationRequiredCode, unauthorized.Code)
	}
	if unauthorized.Message != apperrors.AuthenticationRequiredMessage {
		t.Fatalf("expected message %q, got %q", apperrors.AuthenticationRequiredMessage, unauthorized.Message)
	}
}

func TestRequireAuthenticatedAllowsAuthenticatedUser(t *testing.T) {
	t.Parallel()

	called := false
	handler := RequireAuthenticated[struct{}, struct{}](stubCurrentUser{authenticated: true})(
		HandlerFunc[struct{}, struct{}](func(context.Context, struct{}) (struct{}, error) {
			called = true
			return struct{}{}, nil
		}),
	)

	_, err := handler.Handle(context.Background(), struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("expected handler to run")
	}
}
