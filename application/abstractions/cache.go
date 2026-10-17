package abstractions

import (
	"context"
	"time"
)

// Cache is a generic string-based cache abstraction for infrastructure-backed caching.
type Cache interface {
	Get(ctx context.Context, key string) (string, bool, error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	SetIfAbsent(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	Increment(ctx context.Context, key string, ttl time.Duration) (int64, error)
	TTL(ctx context.Context, key string) (time.Duration, bool, error)
	Delete(ctx context.Context, key string) error
}
