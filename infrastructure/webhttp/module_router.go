package webhttp

import (
	"net/http"

	"github.com/aliighahremanii/buildingblocks/presentation/web"
)

func NewModuleRouter(mux *http.ServeMux) web.Router {
	return NewModuleRouterForSegment(mux, "")
}

func NewModuleRouterForSegment(mux *http.ServeMux, moduleSegment string) web.Router {
	router := NewRouter(mux)
	prefix := PathPrefixForModule(moduleSegment)
	if prefix == "/" {
		return router
	}
	return NewPrefixedRouter(router, prefix)
}
