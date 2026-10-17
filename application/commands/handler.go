package commands

import "context"

type Handler[Command any, Result any] interface {
	Handle(ctx context.Context, command Command) (Result, error)
}

type HandlerFunc[Command any, Result any] func(ctx context.Context, command Command) (Result, error)

func (function HandlerFunc[Command, Result]) Handle(ctx context.Context, command Command) (Result, error) {
	return function(ctx, command)
}

type Middleware[Command any, Result any] func(next Handler[Command, Result]) Handler[Command, Result]
