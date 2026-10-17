package webhttp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestInterModuleClientUsesHTTP2(t *testing.T) {
	var gotProto string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotProto = r.Proto
		w.WriteHeader(http.StatusOK)
	})

	server := httptest.NewUnstartedServer(handler)
	server.Config = NewServer("", handler)
	server.Start()
	t.Cleanup(server.Close)

	client := InterModuleClient(time.Second)
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)

	if gotProto != "HTTP/2.0" {
		t.Fatalf("expected HTTP/2.0 request, got %q", gotProto)
	}
	if response.Proto != "HTTP/2.0" {
		t.Fatalf("expected HTTP/2.0 response, got %q", response.Proto)
	}
}

func TestInterModuleClientFromEnvHTTP1(t *testing.T) {
	t.Setenv(interModuleHTTPProtocolEnv, "http1")

	var gotProto string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotProto = r.Proto
		w.WriteHeader(http.StatusOK)
	})

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	client := InterModuleClientFromEnv(time.Second)
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)

	if gotProto != "HTTP/1.1" {
		t.Fatalf("expected HTTP/1.1 request, got %q", gotProto)
	}
}

func TestInterModuleClientFromEnvH2C(t *testing.T) {
	t.Setenv(interModuleHTTPProtocolEnv, "h2c")

	var gotProto string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotProto = r.Proto
		w.WriteHeader(http.StatusOK)
	})

	server := httptest.NewUnstartedServer(handler)
	server.Config = NewServer("", handler)
	server.Start()
	t.Cleanup(server.Close)

	client := InterModuleClientFromEnv(time.Second)
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)

	if gotProto != "HTTP/2.0" {
		t.Fatalf("expected HTTP/2.0 request, got %q", gotProto)
	}
}
