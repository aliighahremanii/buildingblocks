package webhttp

import (
	"github.com/aliighahremanii/buildingblocks/presentation/web"
)

type PrefixedRouter struct {
	prefix string
	inner  web.Router
}

func NewPrefixedRouter(inner web.Router, prefix string) *PrefixedRouter {
	return &PrefixedRouter{
		prefix: NormalizePathPrefix(prefix),
		inner:  inner,
	}
}

func (router *PrefixedRouter) Get(path string, handler web.HandlerFunc, middlewares ...web.Middleware) {
	router.inner.Get(JoinPath(router.prefix, path), handler, middlewares...)
}

func (router *PrefixedRouter) Post(path string, handler web.HandlerFunc, middlewares ...web.Middleware) {
	router.inner.Post(JoinPath(router.prefix, path), handler, middlewares...)
}

func (router *PrefixedRouter) Put(path string, handler web.HandlerFunc, middlewares ...web.Middleware) {
	router.inner.Put(JoinPath(router.prefix, path), handler, middlewares...)
}

func (router *PrefixedRouter) Patch(path string, handler web.HandlerFunc, middlewares ...web.Middleware) {
	router.inner.Patch(JoinPath(router.prefix, path), handler, middlewares...)
}

func (router *PrefixedRouter) Delete(path string, handler web.HandlerFunc, middlewares ...web.Middleware) {
	router.inner.Delete(JoinPath(router.prefix, path), handler, middlewares...)
}
