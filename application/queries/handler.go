package queries

import "context"

type Handler[Query any, Result any] interface {
	Handle(ctx context.Context, query Query) (Result, error)
}

type HandlerFunc[Query any, Result any] func(ctx context.Context, query Query) (Result, error)

func (f HandlerFunc[Query, Result]) Handle(ctx context.Context, query Query) (Result, error) {
	return f(ctx, query)
}

type Middleware[Query any, Result any] func(next Handler[Query, Result]) Handler[Query, Result]
