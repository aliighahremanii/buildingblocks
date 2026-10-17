package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aliighahremanii/buildingblocks/application/correlation"
	"github.com/aliighahremanii/buildingblocks/presentation/web"
)

// --- CorrelationID() web.Middleware ---

// headerWebContext is a minimal web.Context for exercising the middleware.
// Only Header() and Context() are exercised by the code under test; the
// remaining methods return zero values. It implements the interface
// explicitly rather than embedding web.Context because that interface
// declares a Context() method which collides with the embedded field name.
type headerWebContext struct {
	ctx    context.Context
	header map[string]string
}

func newHeaderWebContext(header string) *headerWebContext {
	return &headerWebContext{
		ctx:    context.Background(),
		header: map[string]string{CorrelationHeaderName: header},
	}
}

func (h *headerWebContext) Context() context.Context            { return h.ctx }
func (h *headerWebContext) Method() string                      { return "GET" }
func (h *headerWebContext) Path() string                        { return "/" }
func (h *headerWebContext) Query(string) string                 { return "" }
func (h *headerWebContext) Param(string) string                 { return "" }
func (h *headerWebContext) Header(name string) string           { return h.header[name] }
func (h *headerWebContext) SetHeader(string, string)            {}
func (h *headerWebContext) BindJSON(any) error                  { return nil }
func (h *headerWebContext) JSON(int, any) error                 { return nil }
func (h *headerWebContext) Text(int, string) error              { return nil }
func (h *headerWebContext) NoContent(int) error                 { return nil }
func (h *headerWebContext) ResponseWriter() http.ResponseWriter { return nil }
func (h *headerWebContext) Request() *http.Request              { return nil }

func TestCorrelationIDMiddlewareHeaderPresent(t *testing.T) {
	t.Parallel()

	var got correlation.Context
	var ok bool
	inner := web.HandlerFunc(func(ctx web.Context) error {
		got, ok = correlation.FromContext(ctx.Context())
		return nil
	})

	mw := CorrelationID()
	h := mw(inner)

	if err := h(newHeaderWebContext("client-corr-1")); err != nil {
		t.Fatalf("handler: %v", err)
	}

	if !ok {
		t.Fatal("correlation Context not propagated to inner handler")
	}
	if got.CorrelationID != "client-corr-1" {
		t.Fatalf("CorrelationID = %q, want %q", got.CorrelationID, "client-corr-1")
	}
	if got.CommandID == "" {
		t.Fatal("CommandID empty; should be minted")
	}
	if got.CausationID != got.CommandID {
		t.Fatalf("CausationID = %q, want == CommandID %q", got.CausationID, got.CommandID)
	}
}

func TestCorrelationIDMiddlewareHeaderAbsentMintsID(t *testing.T) {
	t.Parallel()

	var got correlation.Context
	inner := web.HandlerFunc(func(ctx web.Context) error {
		got, _ = correlation.FromContext(ctx.Context())
		return nil
	})

	mw := CorrelationID()
	h := mw(inner)

	if err := h(newHeaderWebContext("")); err != nil {
		t.Fatalf("handler: %v", err)
	}

	if got.CorrelationID == "" {
		t.Fatal("CorrelationID empty; should be minted when header absent")
	}
}

func TestCorrelationIDMiddlewareCommandIDAlwaysFresh(t *testing.T) {
	t.Parallel()

	var ids []string
	inner := web.HandlerFunc(func(ctx web.Context) error {
		c, _ := correlation.FromContext(ctx.Context())
		ids = append(ids, c.CommandID)
		return nil
	})

	mw := CorrelationID()
	h := mw(inner)

	_ = h(newHeaderWebContext("same-corr"))
	_ = h(newHeaderWebContext("same-corr"))

	if len(ids) != 2 {
		t.Fatalf("want 2 command IDs, got %d", len(ids))
	}
	if ids[0] == "" || ids[1] == "" {
		t.Fatal("command IDs should be non-empty")
	}
	if ids[0] == ids[1] {
		t.Fatal("two commands must get distinct command IDs")
	}
}

// --- WithCorrelationID http.Handler ---

func TestWithCorrelationIDHeaderPresent(t *testing.T) {
	t.Parallel()

	var got correlation.Context
	var ok bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok = correlation.FromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(CorrelationHeaderName, "from-client")
	rec := httptest.NewRecorder()
	WithCorrelationID(next).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !ok {
		t.Fatal("correlation not in context")
	}
	if got.CorrelationID != "from-client" {
		t.Fatalf("CorrelationID = %q, want %q", got.CorrelationID, "from-client")
	}
	if got.CommandID == "" {
		t.Fatal("CommandID empty; should be minted")
	}
	if got.CausationID != got.CommandID {
		t.Fatalf("CausationID = %q, want == CommandID %q", got.CausationID, got.CommandID)
	}
}

func TestWithCorrelationIDHeaderAbsentMintsID(t *testing.T) {
	t.Parallel()

	var got correlation.Context
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got, _ = correlation.FromContext(r.Context())
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	WithCorrelationID(next).ServeHTTP(rec, req)

	if got.CorrelationID == "" {
		t.Fatal("CorrelationID empty; should be minted when header absent")
	}
}
