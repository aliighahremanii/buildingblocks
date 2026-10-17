package webhttp

import "testing"

func TestNormalizePathPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "empty becomes root", raw: "", want: "/"},
		{name: "trim spaces", raw: " /api/v2/ ", want: "/api/v2"},
		{name: "add leading slash", raw: "api/v2", want: "/api/v2"},
		{name: "root prefix", raw: "/", want: "/"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := NormalizePathPrefix(tt.raw); got != tt.want {
				t.Fatalf("NormalizePathPrefix(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestPathPrefixFromEnv(t *testing.T) {
	t.Setenv(httpPathPrefixEnv, "/custom")
	if got := PathPrefixForModule("orders"); got != "/custom" {
		t.Fatalf("PathPrefixForModule() = %q, want /custom", got)
	}
}

func TestPathPrefixForModuleUsesDefaultShape(t *testing.T) {
	if got := PathPrefixForModule("orders"); got != "/api/orders/v1" {
		t.Fatalf("PathPrefixForModule(default) = %q, want /api/orders/v1", got)
	}

	t.Setenv(httpAPIBasePathEnv, "/rest")
	t.Setenv(httpAPIVersionEnv, "v2")
	if got := PathPrefixForModule("orders"); got != "/rest/orders/v2" {
		t.Fatalf("PathPrefixForModule(custom base/version) = %q, want /rest/orders/v2", got)
	}

	t.Setenv("ORDERS_HTTP_PATH_PREFIX", "/edge/orders")
	if got := PathPrefixForModule("orders"); got != "/edge/orders" {
		t.Fatalf("PathPrefixForModule(module override) = %q, want /edge/orders", got)
	}
}

func TestJoinPath(t *testing.T) {
	t.Parallel()

	if got := JoinPath("/api/v1", "/auth/sign-in"); got != "/api/v1/auth/sign-in" {
		t.Fatalf("JoinPath() = %q", got)
	}
	if got := JoinPath("/", "/auth/sign-in"); got != "/auth/sign-in" {
		t.Fatalf("JoinPath(root) = %q", got)
	}
}

func TestJoinURLPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		baseURL    string
		pathPrefix string
		want       string
	}{
		{
			name:       "append prefix to host",
			baseURL:    "http://127.0.0.1:8082",
			pathPrefix: "/api/orders/v1",
			want:       "http://127.0.0.1:8082/api/orders/v1",
		},
		{
			name:       "preserve existing path",
			baseURL:    "http://orders:8082/internal",
			pathPrefix: "/api/orders/v1",
			want:       "http://orders:8082/internal/api/orders/v1",
		},
		{
			name:       "root prefix unchanged",
			baseURL:    "http://127.0.0.1:8082",
			pathPrefix: "/",
			want:       "http://127.0.0.1:8082",
		},
		{
			name:       "skip duplicate prefix",
			baseURL:    "http://127.0.0.1:8082/api/orders/v1",
			pathPrefix: "/api/orders/v1",
			want:       "http://127.0.0.1:8082/api/orders/v1",
		},
		{
			name:       "do not skip partial suffix match",
			baseURL:    "http://127.0.0.1:8082/internal-api/v1",
			pathPrefix: "/api/orders/v1",
			want:       "http://127.0.0.1:8082/internal-api/v1/api/orders/v1",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := JoinURLPath(tt.baseURL, tt.pathPrefix); got != tt.want {
				t.Fatalf("JoinURLPath() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestServiceBaseURLFromEnv(t *testing.T) {
	t.Setenv("ORDERS_BASE_URL", "http://127.0.0.1:8082")

	if got := ServiceBaseURLFromEnvForModule("ORDERS_BASE_URL", "http://fallback:8082", "orders"); got != "http://127.0.0.1:8082/api/orders/v1" {
		t.Fatalf("ServiceBaseURLFromEnvForModule() = %q", got)
	}

	t.Setenv("ORDERS_BASE_URL", "")
	if got := ServiceBaseURLFromEnvForModule("ORDERS_BASE_URL", "http://fallback:8082", "orders"); got != "http://fallback:8082/api/orders/v1" {
		t.Fatalf("ServiceBaseURLFromEnvForModule(default) = %q", got)
	}
}
