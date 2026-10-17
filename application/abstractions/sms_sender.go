package abstractions

import "context"

// SMSMessage is a provider-agnostic SMS payload.
type SMSMessage struct {
	To       string
	From     string
	Body     string
	Metadata map[string]string
	Source   string
	TraceID  string
}

// SMSSender delivers SMS messages through a concrete transport.
type SMSSender interface {
	Send(ctx context.Context, message SMSMessage) error
}
