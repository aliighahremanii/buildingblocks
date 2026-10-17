package webhttp

import (
	"net/http"

	"github.com/aliighahremanii/buildingblocks/presentation/web"
)

func healthCheckHandler(ctx web.Context) error {
	return ctx.JSON(web.StatusOK, map[string]string{"status": "pong"})
}

// RegisterHealthCheck registers GET /health on router. The router must already
// include the module ingress prefix, typically from NewModuleRouterForSegment.
func RegisterHealthCheck(router web.Router) {
	router.Get("/health", healthCheckHandler)
}

// RegisterHealthCheckForModule registers GET /health at the module ingress path,
// for example /api/orders/v1/health when using the default prefix shape.
func RegisterHealthCheckForModule(mux *http.ServeMux, moduleSegment string) {
	RegisterHealthCheck(NewModuleRouterForSegment(mux, moduleSegment))
}

// HealthCheckPathForModule returns the full HTTP path for a module health probe.
func HealthCheckPathForModule(moduleSegment string) string {
	return JoinPath(PathPrefixForModule(moduleSegment), "/health")
}
