package graphql

import (
	"fmt"
	"net/http"
)

// defaultMaxBodyBytes is the request-body cap applied when a handler is built
// without an explicit limit (1 MiB). GraphQL query bodies are small; anything
// larger is abusive or a client bug.
const defaultMaxBodyBytes int64 = 1 << 20

// WithBodyLimit wraps next with a request-body size guard. Bodies whose
// declared Content-Length already exceeds maxBytes are rejected with 413
// before any downstream handler runs. Bodies without a declared length are
// capped by an http.MaxBytesReader so an oversized chunked body fails during
// decode instead of being buffered whole.
//
// maxBytes <= 0 falls back to defaultMaxBodyBytes.
func WithBodyLimit(next http.Handler, maxBytes int64) http.Handler {
	if maxBytes <= 0 {
		maxBytes = defaultMaxBodyBytes
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > maxBytes {
			writeBodyLimitError(w, maxBytes)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
		next.ServeHTTP(w, r)
	})
}

func writeBodyLimitError(w http.ResponseWriter, maxBytes int64) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusRequestEntityTooLarge)
	_, _ = w.Write([]byte(`{"errors":[{"message":"` +
		sanitizeMessage(fmt.Sprintf("request body exceeds maximum allowed size of %d bytes", maxBytes)) +
		`"}]}`))
}
