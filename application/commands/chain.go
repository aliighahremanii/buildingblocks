package commands

func Chain[Command any, Result any](
	handler Handler[Command, Result],
	middlewares ...Middleware[Command, Result],
) Handler[Command, Result] {
	wrapped := handler

	for i := len(middlewares) - 1; i >= 0; i-- {
		wrapped = middlewares[i](wrapped)
	}

	return wrapped
}
