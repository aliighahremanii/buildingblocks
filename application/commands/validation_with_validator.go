package commands

import (
	"context"

	"github.com/aliighahremanii/buildingblocks/application/abstractions"
)

func ValidationWith[Command any, Result any](
	validator abstractions.Validator[Command],
) Middleware[Command, Result] {
	return func(next Handler[Command, Result]) Handler[Command, Result] {
		return HandlerFunc[Command, Result](func(ctx context.Context, command Command) (Result, error) {
			if err := validator.Validate(ctx, command); err != nil {
				var zero Result
				return zero, err
			}

			return next.Handle(ctx, command)
		})
	}
}
