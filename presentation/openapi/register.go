package openapi

import (
	"github.com/aliighahremanii/buildingblocks/infrastructure/webhttp"
	"github.com/aliighahremanii/buildingblocks/presentation/web"
)

func RegisterDocument(router web.Router, spec func() map[string]any, middlewares ...web.Middleware) {
	RegisterDocumentForModule(router, "", spec, middlewares...)
}

func RegisterDocumentForModule(router web.Router, moduleSegment string, spec func() map[string]any, middlewares ...web.Middleware) {
	router.Get("/openapi.json", func(ctx web.Context) error {
		document := spec()
		prefix := webhttp.PathPrefixForModule(moduleSegment)
		if prefix != "/" {
			document["servers"] = []map[string]any{{"url": prefix}}
		}
		return ctx.JSON(web.StatusOK, document)
	}, middlewares...)
}
