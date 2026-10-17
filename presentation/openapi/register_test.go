package openapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aliighahremanii/buildingblocks/infrastructure/webhttp"
)

func TestRegisterDocumentAddsServersFromEnvPrefix(t *testing.T) {
	t.Setenv("HTTP_API_BASE_PATH", "/api")
	t.Setenv("HTTP_API_VERSION", "v1")

	mux := http.NewServeMux()
	router := webhttp.NewPrefixedRouter(webhttp.NewRouter(mux), "/api/orders/v1")

	RegisterDocumentForModule(router, "orders", func() map[string]any {
		return map[string]any{
			"openapi": "3.0.3",
			"paths":   map[string]any{"/health": map[string]any{}},
		}
	})

	request := httptest.NewRequest(http.MethodGet, "/api/orders/v1/openapi.json", nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}

	var document struct {
		Servers []map[string]any `json:"servers"`
	}
	if err := json.NewDecoder(response.Body).Decode(&document); err != nil {
		t.Fatalf("decode openapi json: %v", err)
	}
	if len(document.Servers) != 1 {
		t.Fatalf("expected one server, got %d", len(document.Servers))
	}
	if document.Servers[0]["url"] != "/api/orders/v1" {
		t.Fatalf("expected server url /api/orders/v1, got %#v", document.Servers[0]["url"])
	}
}
