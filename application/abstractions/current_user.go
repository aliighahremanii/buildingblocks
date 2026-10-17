package abstractions

import "context"

type CurrentUser interface {
	UserID(ctx context.Context) (string, bool)
	IsAuthenticated(ctx context.Context) bool
	HasPermission(ctx context.Context, permission string) bool
}
