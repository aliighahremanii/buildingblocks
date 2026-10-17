package webhttp

import (
	"net/http"

	"github.com/aliighahremanii/buildingblocks/presentation/web"
)

type Router struct {
	mux *http.ServeMux
}

func NewRouter(mux *http.ServeMux) *Router {
	return &Router{
		mux: mux,
	}
}

func (router *Router) Get(path string, handler web.HandlerFunc, middlewares ...web.Middleware) {
	router.handle(http.MethodGet, path, handler, middlewares...)
}

func (router *Router) Post(path string, handler web.HandlerFunc, middlewares ...web.Middleware) {
	router.handle(http.MethodPost, path, handler, middlewares...)
}

func (router *Router) Put(path string, handler web.HandlerFunc, middlewares ...web.Middleware) {
	router.handle(http.MethodPut, path, handler, middlewares...)
}

func (router *Router) Patch(path string, handler web.HandlerFunc, middlewares ...web.Middleware) {
	router.handle(http.MethodPatch, path, handler, middlewares...)
}

func (router *Router) Delete(path string, handler web.HandlerFunc, middlewares ...web.Middleware) {
	router.handle(http.MethodDelete, path, handler, middlewares...)
}

func (router *Router) handle(
	method string,
	path string,
	handler web.HandlerFunc,
	middlewares ...web.Middleware,
) {
	finalHandler := applyMiddlewares(handler, middlewares...)

	pattern := method + " " + path

	router.mux.HandleFunc(pattern, func(responseWriter http.ResponseWriter, request *http.Request) {
		ctx := newContext(responseWriter, request)

		if err := finalHandler(ctx); err != nil {
			http.Error(responseWriter, err.Error(), http.StatusInternalServerError)
		}
	})
}

func applyMiddlewares(handler web.HandlerFunc, middlewares ...web.Middleware) web.HandlerFunc {
	finalHandler := handler

	for i := len(middlewares) - 1; i >= 0; i-- {
		finalHandler = middlewares[i](finalHandler)
	}

	return finalHandler
}
