package queries

func Chain[Query any, Result any](
	handler Handler[Query, Result],
	middlewares ...Middleware[Query, Result],
) Handler[Query, Result] {
	wrapped := handler

	for i := len(middlewares) - 1; i >= 0; i-- {
		wrapped = middlewares[i](wrapped)
	}

	return wrapped
}
