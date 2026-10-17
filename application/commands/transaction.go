package commands

import (
	"context"

	"github.com/aliighahremanii/buildingblocks/application/abstractions"
)

func Transaction[Command any, Result any](
	uow abstractions.UnitOfWork,
) Middleware[Command, Result] {
	return func(next Handler[Command, Result]) Handler[Command, Result] {
		return HandlerFunc[Command, Result](func(ctx context.Context, command Command) (Result, error) {
			var result Result

			err := uow.WithinTransaction(ctx, func(txCtx context.Context) error {
				var handleErr error

				result, handleErr = next.Handle(txCtx, command)

				return handleErr
			})

			if err != nil {
				var zero Result
				return zero, err
			}

			return result, nil
		})
	}
}
