package commands

import "context"

type Validatable interface {
	Validate() error
}

func Validation[Command Validatable, Result any]() Middleware[Command, Result] {
	return func(next Handler[Command, Result]) Handler[Command, Result] {
		return HandlerFunc[Command, Result](func(ctx context.Context, command Command) (Result, error) {
			if err := command.Validate(); err != nil {
				var zero Result
				return zero, err
			}

			return next.Handle(ctx, command)
		})
	}
}
