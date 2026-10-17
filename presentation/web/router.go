package web

type Router interface {
	Get(path string, handler HandlerFunc, middlewares ...Middleware)
	Post(path string, handler HandlerFunc, middlewares ...Middleware)
	Put(path string, handler HandlerFunc, middlewares ...Middleware)
	Patch(path string, handler HandlerFunc, middlewares ...Middleware)
	Delete(path string, handler HandlerFunc, middlewares ...Middleware)
}
