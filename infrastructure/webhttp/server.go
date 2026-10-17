package webhttp

import "net/http"

// NewServer creates an http.Server that accepts both HTTP/1.1 and unencrypted
// HTTP/2 (h2c). Go 1.24+ handles h2c natively through http.Server.Protocols
// without any additional package.
func NewServer(addr string, handler http.Handler) *http.Server {
	protocols := &http.Protocols{}
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	return &http.Server{
		Addr:      addr,
		Handler:   handler,
		Protocols: protocols,
	}
}
