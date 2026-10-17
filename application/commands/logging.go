package commands

import (
	"context"
	"time"

	"github.com/aliighahremanii/buildingblocks/application/abstractions"
)

func Logging[Command any, Result any](
	logger abstractions.Logger,
	name string,
) Middleware[Command, Result] {
	return func(next Handler[Command, Result]) Handler[Command, Result] {
		return HandlerFunc[Command, Result](func(ctx context.Context, command Command) (Result, error) {
			startedAt := time.Now()

			result, err := next.Handle(ctx, command)

			duration := time.Since(startedAt)

			if err != nil {
				logger.Error(
					ctx,
					"command failed",
					abstractions.String("command", name),
					abstractions.String("duration", duration.String()),
					abstractions.Error(err),
				)

				return result, err
			}

			logger.Info(
				ctx,
				"command succeeded",
				abstractions.String("command", name),
				abstractions.String("duration", duration.String()),
			)

			return result, nil
		})
	}
}
