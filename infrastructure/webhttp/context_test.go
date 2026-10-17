package webhttp

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aliighahremanii/buildingblocks/presentation/web"
)

func TestBindJSONWrapsDecoderErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want error
	}{
		{name: "empty", body: "", want: io.EOF},
		{name: "truncated", body: `{"name":`, want: io.ErrUnexpectedEOF},
		{name: "trailing garbage", body: `{"name":"X"}garbage`, want: nil},
		{name: "trailing second object", body: `{"name":"X"}{"name":"Y"}`, want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			ctx := newContext(httptest.NewRecorder(), req)
			var target struct {
				Name string `json:"name"`
			}
			err := ctx.BindJSON(&target)
			var bindErr *web.BindJSONError
			if !errors.As(err, &bindErr) {
				t.Fatalf("expected BindJSONError, got %T: %v", err, err)
			}
			if tt.want != nil {
				if !errors.Is(err, tt.want) {
					t.Fatalf("error = %v, want wrapped %v", err, tt.want)
				}
			}
		})
	}
}

func TestBindJSONRejectsTrailingData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "trailing garbage bytes", body: `{"name":"Iran"}garbage`},
		{name: "trailing second JSON value", body: `{"name":"Iran"}{"name":"Other"}`},
		{name: "trailing comma", body: `{"name":"Iran"},`},
		{name: "trailing closing bracket", body: `{"name":"Iran"}]`},
		{name: "trailing closing brace", body: `{"name":"Iran"}}`},
		{name: "trailing scalar", body: `{"name":"Iran"}123`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			ctx := newContext(httptest.NewRecorder(), req)
			var target struct {
				Name string `json:"name"`
			}
			err := ctx.BindJSON(&target)
			var bindErr *web.BindJSONError
			if !errors.As(err, &bindErr) {
				t.Fatalf("expected BindJSONError for trailing data, got %T: %v", err, err)
			}
			if target.Name != "Iran" {
				t.Fatalf("first value should have been decoded: got %q", target.Name)
			}
		})
	}
}

func TestBindJSONAcceptsValidSingleObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "no trailing whitespace", body: `{"name":"Iran"}`},
		{name: "trailing newline", body: "{\"name\":\"Iran\"}\n"},
		{name: "trailing spaces", body: `{"name":"Iran"}   `},
		{name: "trailing tab", body: "{\"name\":\"Iran\"}\t"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			ctx := newContext(httptest.NewRecorder(), req)
			var target struct {
				Name string `json:"name"`
			}
			if err := ctx.BindJSON(&target); err != nil {
				t.Fatalf("expected nil error for valid body with trailing whitespace, got: %v", err)
			}
			if target.Name != "Iran" {
				t.Fatalf("Name = %q, want %q", target.Name, "Iran")
			}
		})
	}
}
