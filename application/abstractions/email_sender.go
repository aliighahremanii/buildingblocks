package abstractions

import "context"

// EmailMessage is a provider-agnostic email payload.
type EmailMessage struct {
	FromAddress string
	FromName    string
	ToAddress   string
	Subject     string
	TextBody    string
	Headers     map[string]string
}

// EmailSender delivers email messages through a concrete transport.
type EmailSender interface {
	Send(ctx context.Context, message EmailMessage) error
}
