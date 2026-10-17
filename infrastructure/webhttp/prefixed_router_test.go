package webhttp

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aliighahremanii/buildingblocks/presentation/web"
)

func TestPrefixedRouterRegistersRoutes(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	router := NewPrefixedRouter(NewRouter(mux), "/api/v1")

	router.Get("/health", func(ctx web.Context) error {
		return ctx.JSON(web.StatusOK, map[string]string{"status": "ok"})
	})

	request := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}
}

func TestNewModuleRouterUsesEnvPrefix(t *testing.T) {
	t.Setenv(httpAPIBasePathEnv, "/api")
	t.Setenv(httpAPIVersionEnv, "v1")

	mux := http.NewServeMux()
	router := NewModuleRouterForSegment(mux, "orders")
	router.Get("/ready", func(ctx web.Context) error {
		return ctx.NoContent(web.StatusNoContent)
	})

	request := httptest.NewRequest(http.MethodGet, "/api/orders/v1/ready", nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d", http.StatusNoContent, response.Code)
	}
}
