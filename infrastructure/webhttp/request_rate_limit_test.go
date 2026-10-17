package webhttp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeCache struct {
	counts map[string]int64
	ttls   map[string]time.Duration
}

func (c *fakeCache) Get(context.Context, string) (string, bool, error)        { return "", false, nil }
func (c *fakeCache) Set(context.Context, string, string, time.Duration) error { return nil }
func (c *fakeCache) SetIfAbsent(context.Context, string, string, time.Duration) (bool, error) {
	return false, nil
}
func (c *fakeCache) Delete(context.Context, string) error { return nil }

func (c *fakeCache) Increment(_ context.Context, key string, ttl time.Duration) (int64, error) {
	if c.counts == nil {
		c.counts = map[string]int64{}
	}
	if c.ttls == nil {
		c.ttls = map[string]time.Duration{}
	}
	c.counts[key]++
	if _, ok := c.ttls[key]; !ok {
		c.ttls[key] = ttl
	}
	return c.counts[key], nil
}

func (c *fakeCache) TTL(_ context.Context, key string) (time.Duration, bool, error) {
	ttl, ok := c.ttls[key]
	return ttl, ok, nil
}

func TestRequestRateLimitRejectsAfterConfiguredLimit(t *testing.T) {
	t.Parallel()

	cache := &fakeCache{}
	rejected := false
	handler := RequestRateLimit(cache, RequestRateLimitConfig{
		Prefix:      "otp:ip:",
		Window:      time.Minute,
		MaxRequests: 2,
		Match: func(request *http.Request) bool {
			return request.Method == http.MethodPost && request.URL.Path == "/verification/otp/send"
		},
		Key: func(request *http.Request) (string, bool) {
			return ClientIP(request), true
		},
		OnRejected: func(responseWriter http.ResponseWriter, _ *http.Request, _ time.Duration) {
			rejected = true
			responseWriter.WriteHeader(http.StatusTooManyRequests)
		},
	})(http.HandlerFunc(func(responseWriter http.ResponseWriter, _ *http.Request) {
		responseWriter.WriteHeader(http.StatusOK)
	}))

	for i := 0; i < 2; i++ {
		request := httptest.NewRequest(http.MethodPost, "/verification/otp/send", nil)
		request.RemoteAddr = "10.0.0.1:1234"
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", response.Code)
		}
	}

	request := httptest.NewRequest(http.MethodPost, "/verification/otp/send", nil)
	request.RemoteAddr = "10.0.0.1:1234"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("expected status 429, got %d", response.Code)
	}
	if !rejected {
		t.Fatal("expected rejection callback to be invoked")
	}
}

func TestClientIPPrefersForwardedHeaders(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodGet, "/verification/otp/send", nil)
	request.RemoteAddr = "10.0.0.1:1234"
	request.Header.Set("X-Forwarded-For", "203.0.113.5, 10.0.0.1")

	if ip := ClientIP(request); ip != "203.0.113.5" {
		t.Fatalf("expected forwarded ip, got %q", ip)
	}
}
