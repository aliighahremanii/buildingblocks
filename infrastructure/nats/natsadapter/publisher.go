// Package natsadapter wraps github.com/nats-io/nats.go's JetStream
// context to satisfy the natspub.Publisher and natspub.Validator
// interfaces. Adapters live here (rather than in natspub) so the natspub
// package has no dependency on nats.go and can be unit-tested with fake
// publishers/validators.
package natsadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	publisher "github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/natspub"
)

// asyncPublisher is the narrow JetStream surface the publisher needs;
// *nats.JetStreamContext satisfies it. Tests substitute a fake so the
// error classification is unit-testable without a server.
type asyncPublisher interface {
	PublishAsync(subj string, data []byte, opts ...nats.PubOpt) (nats.PubAckFuture, error)
}

// JetStreamPublisher adapts a nats.JetStreamContext to the
// natspub.Publisher interface. It uses PublishAsync with the
// Nats-Msg-Id header (set from the KurrentDB EventID) so JetStream
// deduplicates replays within the stream's duplicate window.
type JetStreamPublisher struct {
	js      asyncPublisher
	timeout time.Duration
}

// New constructs a JetStreamPublisher. timeout is the max wait for a
// PublishAsync ack; zero falls back to the JetStream context's default
// (typically 5s).
func New(js nats.JetStreamContext, timeout time.Duration) *JetStreamPublisher {
	return &JetStreamPublisher{js: js, timeout: timeout}
}

// PublishAsync publishes data to subject with msgId as the
// Nats-Msg-Id header. It waits for the JetStream ack before returning
// so the worker can advance its position with confidence the event is
// durable in JetStream. Failures are classified (D114): a publish that
// no stream is interested in surfaces as nats.ErrNoResponders and is
// returned as a *publisher.Error with actionable guidance, because the
// usual cause is the stream's subject filter not covering the subject
// family (e.g. configured "<prefix>.*" instead of "<prefix>.>").
func (p *JetStreamPublisher) PublishAsync(ctx context.Context, subject string, data []byte, msgID string) error {
	// PublishAsync returns a PubAckFuture; we wait for either its Ok()
	// (ack received — message durable in JetStream) or its Err()
	// (publish failed) channel so a successful return means the event
	// is durably stored.
	fut, err := p.js.PublishAsync(subject, data, nats.MsgId(msgID))
	if err != nil {
		return classifyPublishError(subject, err)
	}
	// Honor the caller's context deadline; PublishAsync itself does not
	// take a context.
	wait := p.timeout
	if wait == 0 {
		wait = 5 * time.Second
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()

	select {
	case <-fut.Ok():
		return nil
	case err := <-fut.Err():
		if err == nil {
			err = fmt.Errorf("nats publish %s: ack channel closed without error", subject)
		}
		return classifyPublishError(subject, err)
	case <-ctx.Done():
		return ctx.Err()
	case <-deadline.C:
		return &publisher.Error{
			Op:      "publish",
			Kind:    publisher.ErrKindUnavailable,
			Subject: subject,
			Detail:  fmt.Sprintf("timed out waiting for ack after %s", wait),
			Guidance: "retrying with backoff; if it persists, check JetStream server health " +
				"(GET http://<nats-host>:8222/jsz) — the ack wait is not evidence of loss, the position is not advanced on failure",
		}
	}
}

// classifyPublishError maps a JetStream publish failure onto the natspub
// error taxonomy. nats.ErrNoResponders on publish means no stream is
// interested in the subject: the stream is missing/renamed or its subject
// filter does not cover the publisher's subject family.
func classifyPublishError(subject string, err error) error {
	var ne *publisher.Error
	switch {
	case errors.Is(err, nats.ErrNoResponders):
		ne = &publisher.Error{
			Op:      "publish",
			Kind:    publisher.ErrKindNoResponders,
			Subject: subject,
			Detail:  "no JetStream stream is interested in this subject (no responders)",
			Guidance: "no JetStream stream is interested in this subject — verify the configured stream's subjects cover " +
				"the published family, expected '<prefix>.>' ('*' matches exactly one token and never matches published " +
				"subjects); check `nats stream info $NATS_STREAM_NAME`. " +
				"If the stream was recently corrected, the worker's next validation cycle picks it up after reconnect",
		}
	case errors.Is(err, nats.ErrTimeout),
		errors.Is(err, nats.ErrConnectionClosed),
		errors.Is(err, nats.ErrNoServers):
		ne = &publisher.Error{
			Op:       "publish",
			Kind:     publisher.ErrKindUnavailable,
			Subject:  subject,
			Detail:   "transient JetStream publish failure",
			Guidance: "retrying with backoff; the position is not advanced on failure so no event is skipped",
		}
	default:
		ne = &publisher.Error{
			Op:       "publish",
			Kind:     publisher.ErrKindUnavailable,
			Subject:  subject,
			Detail:   err.Error(),
			Guidance: "retrying with backoff; the position is not advanced on failure so no event is skipped",
		}
	}
	ne.Err = err
	return ne
}

// Compile-time interface check.
var _ publisher.Publisher = (*JetStreamPublisher)(nil)
