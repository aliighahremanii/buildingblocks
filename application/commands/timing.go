package commands

import (
	"context"
	"time"
)

type TimingObserver interface {
	ObserveCommandDuration(ctx context.Context, commandName string, duration time.Duration, failed bool)
}

func Timing[Command any, Result any](
	observer TimingObserver,
	commandName string,
) Middleware[Command, Result] {
	return func(next Handler[Command, Result]) Handler[Command, Result] {
		return HandlerFunc[Command, Result](func(ctx context.Context, command Command) (Result, error) {
			startedAt := time.Now()

			result, err := next.Handle(ctx, command)

			observer.ObserveCommandDuration(
				ctx,
				commandName,
				time.Since(startedAt),
				err != nil,
			)

			return result, err
		})
	}
}
