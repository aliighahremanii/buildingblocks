package webhttp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aliighahremanii/buildingblocks/presentation/web"
)

func TestRegisterHealthCheck(t *testing.T) {
	t.Parallel()

	t.Run("prefixed router", func(t *testing.T) {
		t.Parallel()

		mux := http.NewServeMux()
		router := NewPrefixedRouter(NewRouter(mux), "/api/v1")
		RegisterHealthCheck(router)

		request := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)

		assertHealthResponse(t, response)
	})
}

func TestRegisterHealthCheckForModule(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	RegisterHealthCheckForModule(mux, "orders")

	t.Run("ingress path", func(t *testing.T) {
		t.Parallel()

		request := httptest.NewRequest(http.MethodGet, HealthCheckPathForModule("orders"), nil)
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)

		assertHealthResponse(t, response)
	})

	t.Run("bare health path is not registered", func(t *testing.T) {
		t.Parallel()

		request := httptest.NewRequest(http.MethodGet, "/health", nil)
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)

		if response.Code != http.StatusNotFound {
			t.Fatalf("expected status %d, got %d", http.StatusNotFound, response.Code)
		}
	})
}

func assertHealthResponse(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()

	if response.Code != web.StatusOK {
		t.Fatalf("expected status %d, got %d", web.StatusOK, response.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	if body["status"] != "pong" {
		t.Fatalf("expected status pong, got %q", body["status"])
	}
}
