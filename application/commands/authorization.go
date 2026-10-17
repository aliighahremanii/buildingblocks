package commands

import (
	"context"

	"github.com/aliighahremanii/buildingblocks/application/abstractions"
	apperrors "github.com/aliighahremanii/buildingblocks/application/errors"
)

func RequireAuthenticated[Command any, Result any](
	currentUser abstractions.CurrentUser,
) Middleware[Command, Result] {
	return func(next Handler[Command, Result]) Handler[Command, Result] {
		return HandlerFunc[Command, Result](func(ctx context.Context, command Command) (Result, error) {
			if !currentUser.IsAuthenticated(ctx) {
				var zero Result
				return zero, apperrors.NewAuthenticationRequiredError()
			}

			return next.Handle(ctx, command)
		})
	}
}

func RequirePermission[Command any, Result any](
	currentUser abstractions.CurrentUser,
	permission string,
) Middleware[Command, Result] {
	return func(next Handler[Command, Result]) Handler[Command, Result] {
		return HandlerFunc[Command, Result](func(ctx context.Context, command Command) (Result, error) {
			if !currentUser.HasPermission(ctx, permission) {
				var zero Result
				return zero, apperrors.NewForbiddenError("permission is required: " + permission)
			}

			return next.Handle(ctx, command)
		})
	}
}
