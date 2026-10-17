package auth

import "context"

type contextKey int

const (
	ctxKeyAuthenticated contextKey = 1
	ctxKeyUserID        contextKey = 2
	ctxKeyRoles         contextKey = 3
)

func WithAuthenticatedPrincipal(ctx context.Context, userID string, roles []string) context.Context {
	ctx = context.WithValue(ctx, ctxKeyAuthenticated, true)
	if userID != "" {
		ctx = context.WithValue(ctx, ctxKeyUserID, userID)
	}
	if len(roles) > 0 {
		copiedRoles := append([]string(nil), roles...)
		ctx = context.WithValue(ctx, ctxKeyRoles, copiedRoles)
	}
	return ctx
}

func WithAuthenticatedUser(ctx context.Context, userID string) context.Context {
	return WithAuthenticatedPrincipal(ctx, userID, nil)
}

func IsAuthenticated(ctx context.Context) bool {
	authenticated, _ := ctx.Value(ctxKeyAuthenticated).(bool)
	return authenticated
}

func UserID(ctx context.Context) (string, bool) {
	userID, ok := ctx.Value(ctxKeyUserID).(string)
	return userID, ok && userID != ""
}

func Roles(ctx context.Context) []string {
	roles, _ := ctx.Value(ctxKeyRoles).([]string)
	if len(roles) == 0 {
		return nil
	}
	return append([]string(nil), roles...)
}

func HasRole(ctx context.Context, role string) bool {
	for _, assignedRole := range Roles(ctx) {
		if assignedRole == role {
			return true
		}
	}
	return false
}
