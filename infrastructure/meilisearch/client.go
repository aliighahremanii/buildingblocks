package meilisearch

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/meilisearch/meilisearch-go"

	"github.com/aliighahremanii/buildingblocks/infrastructure/envconfig"
)

const (
	urlEnv     = "MEILISEARCH_URL"
	apiKeyEnv  = "MEILISEARCH_API_KEY"
	defaultURL = "http://localhost:7700"
	defaultKey = "dev-meili-master-key"

	// defaultRequestTimeout bounds EVERY HTTP round trip made by SDK
	// clients opened by this package: one overall per-request timeout on
	// the custom http.Client, covering connect, body write, and the wait
	// for response headers AND body. The SDK's own default client has NO
	// overall timeout — its transport bounds only dial (30s), TLS
	// handshake (10s), and idle connections (90s), so a server that
	// accepts the connection and the request but never responds hangs the
	// calling goroutine FOREVER (D118: the operations search projection
	// stalled at a fixed $all position for days because the D109 watchdog
	// deliberately skips while a projector is in flight). 30s is far
	// above every legitimate Meilisearch round trip (enqueue, task poll,
	// document read) yet small enough that a wedged request fails, the
	// projection retry/DLQ machinery can run, and the checkpoint keeps
	// advancing. See docs/decisions.md D118.
	defaultRequestTimeout = 30 * time.Second
)

type Config struct {
	URL    string
	APIKey string
	Err    error
}

func ConfigFromEnv() Config {
	url, err := envconfig.Value(urlEnv)
	if err != nil {
		return Config{Err: err}
	}
	if url == "" {
		url = defaultURL
	}
	key, err := envconfig.Value(apiKeyEnv)
	if err != nil {
		return Config{Err: err}
	}
	if key == "" {
		key = defaultKey
	}
	return Config{URL: url, APIKey: key}
}

func (c Config) Enabled() bool {
	return c.Err != nil || c.URL != ""
}

// OpenClient creates a new Meilisearch ServiceManager from the given config.
// Every client it creates carries the production per-request timeout
// (defaultRequestTimeout) — the single shared bound for the API, worker, and
// admin processes (D118).
func OpenClient(ctx context.Context, cfg Config) (meilisearch.ServiceManager, error) {
	return openClientWithTimeout(ctx, cfg, defaultRequestTimeout)
}

// openClientWithTimeout is OpenClient's construction path with the
// per-request timeout injectable. It exists ONLY so the regression test can
// prove the bound in milliseconds instead of waiting 30 seconds; production
// callers always go through OpenClient and never choose a timeout of their
// own (one shared bound — no caller-specific config, D118).
func openClientWithTimeout(_ context.Context, cfg Config, timeout time.Duration) (meilisearch.ServiceManager, error) {
	if cfg.Err != nil {
		return nil, cfg.Err
	}
	// WithCustomClient replaces the SDK's default http.Client (which has
	// no overall timeout) with one bounded per request. The Transport is
	// deliberately left nil — net/http's DefaultTransport — no transport
	// tuning, no retry changes; only the overall request bound is added
	// (D118). Health semantics are unchanged (still the context-less
	// Health probe, now itself bounded by the same client timeout).
	client := meilisearch.New(cfg.URL,
		meilisearch.WithAPIKey(cfg.APIKey),
		meilisearch.WithCustomClient(&http.Client{Timeout: timeout}),
	)
	resp, err := client.Health()
	if err != nil {
		return nil, fmt.Errorf("meilisearch health check: %w", err)
	}
	if resp.Status != "available" {
		return nil, fmt.Errorf("meilisearch not available: %s", resp.Status)
	}
	return client, nil
}
