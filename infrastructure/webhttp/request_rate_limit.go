package webhttp

import (
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	appabstractions "github.com/aliighahremanii/buildingblocks/application/abstractions"
)

type RequestRateLimitConfig struct {
	Prefix      string
	Window      time.Duration
	MaxRequests int
	Match       func(*http.Request) bool
	Key         func(*http.Request) (string, bool)
	OnRejected  func(http.ResponseWriter, *http.Request, time.Duration)
}

func RequestRateLimit(cache appabstractions.Cache, cfg RequestRateLimitConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if cache == nil || cfg.Window <= 0 || cfg.MaxRequests <= 0 {
			return next
		}

		return http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
			if cfg.Match != nil && !cfg.Match(request) {
				next.ServeHTTP(responseWriter, request)
				return
			}

			if cfg.Key == nil {
				next.ServeHTTP(responseWriter, request)
				return
			}

			keyPart, ok := cfg.Key(request)
			if !ok || keyPart == "" {
				next.ServeHTTP(responseWriter, request)
				return
			}

			key := cfg.Prefix + keyPart
			count, err := cache.Increment(request.Context(), key, cfg.Window)
			if err != nil {
				next.ServeHTTP(responseWriter, request)
				return
			}
			if int(count) <= cfg.MaxRequests {
				next.ServeHTTP(responseWriter, request)
				return
			}

			retryAfter, _, err := cache.TTL(request.Context(), key)
			if err != nil {
				retryAfter = cfg.Window
			}
			if retryAfter > 0 {
				responseWriter.Header().Set("Retry-After", formatRetryAfter(retryAfter))
			}
			if cfg.OnRejected != nil {
				cfg.OnRejected(responseWriter, request, retryAfter)
				return
			}

			http.Error(responseWriter, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
		})
	}
}

func ClientIP(request *http.Request) string {
	if forwarded := request.Header.Get("X-Forwarded-For"); forwarded != "" {
		parts := strings.Split(forwarded, ",")
		if ip := strings.TrimSpace(parts[0]); ip != "" {
			return ip
		}
	}

	if realIP := strings.TrimSpace(request.Header.Get("X-Real-IP")); realIP != "" {
		return realIP
	}

	host, _, err := net.SplitHostPort(strings.TrimSpace(request.RemoteAddr))
	if err == nil && host != "" {
		return host
	}

	return strings.TrimSpace(request.RemoteAddr)
}

func formatRetryAfter(retryAfter time.Duration) string {
	seconds := int(retryAfter.Round(time.Second).Seconds())
	if seconds < 1 {
		seconds = 1
	}
	return strconv.Itoa(seconds)
}
