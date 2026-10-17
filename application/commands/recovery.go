package commands

import (
	"context"
	"fmt"

	"github.com/aliighahremanii/buildingblocks/application/abstractions"
)

func Recovery[Command any, Result any](
	logger abstractions.Logger,
	commandName string,
) Middleware[Command, Result] {
	return func(next Handler[Command, Result]) Handler[Command, Result] {
		return HandlerFunc[Command, Result](func(ctx context.Context, command Command) (result Result, err error) {
			defer func() {
				if recovered := recover(); recovered != nil {
					err = fmt.Errorf("panic while handling command %s: %v", commandName, recovered)

					logger.Error(
						ctx,
						"command panic recovered",
						abstractions.String("command", commandName),
						abstractions.Any("panic", recovered),
						abstractions.Error(err),
					)
				}
			}()

			return next.Handle(ctx, command)
		})
	}
}
