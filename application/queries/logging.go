package queries

import (
	"context"
	"time"

	"github.com/aliighahremanii/buildingblocks/application/abstractions"
)

// Logging returns a middleware that logs query execution time and errors.
func Logging[Query any, Result any](
	logger abstractions.Logger,
	name string,
) Middleware[Query, Result] {
	return func(next Handler[Query, Result]) Handler[Query, Result] {
		return HandlerFunc[Query, Result](func(ctx context.Context, query Query) (Result, error) {
			startedAt := time.Now()

			result, err := next.Handle(ctx, query)

			duration := time.Since(startedAt)

			if err != nil {
				logger.Error(
					ctx,
					"query failed",
					abstractions.String("query", name),
					abstractions.String("duration", duration.String()),
					abstractions.Error(err),
				)
				return result, err
			}

			logger.Info(
				ctx,
				"query succeeded",
				abstractions.String("query", name),
				abstractions.String("duration", duration.String()),
			)

			return result, nil
		})
	}
}
