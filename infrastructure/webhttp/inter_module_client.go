package webhttp

import (
	"net/http"
	"os"
	"strings"
	"time"
)

const interModuleHTTPProtocolEnv = "INTER_MODULE_HTTP_PROTOCOL"

// InterModuleClient returns an HTTP client configured for unencrypted HTTP/2 (h2c)
// module-to-module calls. Use only for synchronous inter-module communication, not
// external integrations such as payment gateways or SMS providers.
func InterModuleClient(timeout time.Duration) *http.Client {
	protocols := &http.Protocols{}
	protocols.SetUnencryptedHTTP2(true)

	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Protocols: protocols,
		},
	}
}

// InterModuleClientFromEnv returns an inter-module HTTP client. When
// INTER_MODULE_HTTP_PROTOCOL=http1, a standard HTTP/1.1 client is returned instead.
func InterModuleClientFromEnv(timeout time.Duration) *http.Client {
	if interModuleHTTPProtocolFromEnv() == "http1" {
		return &http.Client{Timeout: timeout}
	}
	return InterModuleClient(timeout)
}

func interModuleHTTPProtocolFromEnv() string {
	return strings.ToLower(strings.TrimSpace(os.Getenv(interModuleHTTPProtocolEnv)))
}
