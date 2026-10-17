package meilisearch

// client_test.go — regression for D118: every SDK client opened by this
// package MUST carry an overall per-request timeout. The meilisearch-go
// default http.Client has NO such bound: its transport bounds only dial
// (30s), TLS handshake (10s), and idle connections (90s), so a request that
// is accepted by the server but never answered blocks the calling goroutine
// FOREVER. In production this stalled the operations search projection at a
// fixed $all position for days (the D109 watchdog deliberately skips while a
// projector is in flight), and only a process restart — temporarily —
// recovered it.
//
// The test drives the REAL meilisearch-go client constructed through the
// PRODUCTION helper (openClientWithTimeout, OpenClient's own construction
// path) with a small injected timeout: an httptest server answers /health,
// accepts the exact AddDocuments endpoint, then never responds until cleanup
// releases it. It proves the request reached the handler, the call returns a
// timeout-class error (errors.As to net.Error with Timeout() == true, via the
// SDK error's OriginError — *meilisearch.Error has no Unwrap) within
// tolerant timing bounds WHILE THE HANDLER IS STILL PARKED (so the error can
// only come from the client-side timeout, never from a server response), and
// that cleanup (release + server close) completes. No exact error strings or
// exact timings are asserted. Run repeatedly (go test -count=20).

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/meilisearch/meilisearch-go"
)

// TestOpenClientBoundedRequestReturnsTimeoutError pins the D118 contract:
// a hung Meilisearch response terminates with a timeout-class error instead
// of blocking the caller forever.
func TestOpenClientBoundedRequestReturnsTimeoutError(t *testing.T) {
	const (
		indexUID = "regression-index"
		timeout  = 250 * time.Millisecond // small so the test is fast
		// slack absorbs CI scheduling/jitter around the client timer; the
		// bound is tolerant, never exact.
		slack = 5 * time.Second
	)

	var addReached atomic.Bool
	release := make(chan struct{})

	// The fake server: healthy /health (the production construction path
	// probes it), the exact AddDocuments endpoint (POST
	// /indexes/{uid}/documents) is accepted then NEVER responds until
	// cleanup releases it, anything else 404s so a drifting client call
	// fails loudly instead of passing vacuously.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"available"}`))
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/indexes/"+indexUID+"/documents" {
			addReached.Store(true)
			<-release
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	// Cleanup order proves it cannot hang: release the parked handler
	// FIRST, then close the server (httptest.Server.Close blocks until
	// every outstanding handler returns). If the bounded call had never
	// returned, or release/Close deadlocked, the package test timeout
	// would fail loudly here.
	t.Cleanup(func() {
		close(release)
		srv.Close()
	})

	client, err := openClientWithTimeout(context.Background(), Config{
		URL:    srv.URL,
		APIKey: "regression-test-key",
	}, timeout)
	if err != nil {
		t.Fatalf("openClientWithTimeout against a healthy /health: %v", err)
	}

	start := time.Now()
	_, err = client.Index(indexUID).AddDocumentsWithContext(
		context.Background(), []map[string]string{{"id": "regression-doc-1"}}, nil)
	elapsed := time.Since(start)

	// The request must have REACHED the handler — otherwise the test
	// proves nothing about a hung response (e.g. it failed at dial).
	if !addReached.Load() {
		t.Fatal("AddDocuments request never reached the test server handler; test premise broken")
	}

	if err == nil {
		t.Fatal("AddDocuments against a never-responding server unexpectedly succeeded; the per-request timeout bound is missing (D118 regression)")
	}

	// Timeout-class assertion, typed — never by error string: extract the
	// SDK's typed error, then errors.As its OriginError to net.Error with
	// Timeout() true. (*meilisearch.Error does not implement Unwrap, so
	// errors.As must go through the OriginError field.)
	var meiliErr *meilisearch.Error
	if !errors.As(err, &meiliErr) {
		t.Fatalf("error type = %T (%v), want *meilisearch.Error", err, err)
	}
	var netErr net.Error
	if !errors.As(meiliErr.OriginError, &netErr) {
		t.Fatalf("OriginError type = %T (%v), want a net.Error", meiliErr.OriginError, meiliErr.OriginError)
	}
	if !netErr.Timeout() {
		t.Fatalf("net.Error.Timeout() = false for a never-responding server; want the client-side request timeout to fire (D118)")
	}

	// Tolerant timing bounds: the call cannot return before the client
	// timeout fires (the handler is still parked — release is unclosed at
	// this point, proving no server response contributed), and it must
	// return well within timeout+slack rather than hanging forever.
	if elapsed < timeout {
		t.Fatalf("call returned after %s, before the %s client timeout — a server response raced the parked handler; test premise broken", elapsed, timeout)
	}
	if elapsed > timeout+slack {
		t.Fatalf("call returned after %s, exceeding the tolerant bound %s (timeout+slack); the request hung past the client timeout (D118 regression)", elapsed, timeout+slack)
	}
}

// TestDefaultRequestTimeoutIsProductionBound pins the shared production
// bound itself: OpenClient must construct with exactly 30s so every process
// (API, worker, admin) gets one identical, non-configurable request budget.
// Changing the constant is a production contract change and must update
// docs/decisions.md D118 and the AGENTS.md rule in the same change.
func TestDefaultRequestTimeoutIsProductionBound(t *testing.T) {
	if defaultRequestTimeout != 30*time.Second {
		t.Fatalf("defaultRequestTimeout = %s, want 30s (the D118 shared production bound)", defaultRequestTimeout)
	}
}
