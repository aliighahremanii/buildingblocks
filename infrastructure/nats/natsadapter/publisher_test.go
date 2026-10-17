package natsadapter

// publisher_test.go — unit coverage for the JetStreamPublisher error
// classification (D114): the nats.ErrNoResponders publish failure (no
// stream interested in the subject — the production outbox stall) is
// surfaced as a classified *natspub.Error with actionable guidance, and
// the ack/timeout/cancel paths keep their contracts.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	natspub "github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/natspub"
)

// fakePubAckFuture fakes the JetStream ack future.
type fakePubAckFuture struct {
	okCh  chan *nats.PubAck
	errCh chan error
}

func (f *fakePubAckFuture) Ok() <-chan *nats.PubAck { return f.okCh }
func (f *fakePubAckFuture) Err() <-chan error       { return f.errCh }
func (f *fakePubAckFuture) Msg() *nats.Msg          { return nil }

// fakeAsyncPublisher fakes the narrow JetStream publish surface.
type fakeAsyncPublisher struct {
	calls   int
	subject string
	data    []byte
	pubOpts []nats.PubOpt
	initErr error
	future  *fakePubAckFuture
}

func (f *fakeAsyncPublisher) PublishAsync(subject string, data []byte, opts ...nats.PubOpt) (nats.PubAckFuture, error) {
	f.calls++
	f.subject = subject
	f.data = data
	f.pubOpts = opts
	if f.initErr != nil {
		return nil, f.initErr
	}
	return f.future, nil
}

func newTestPublisher(js *fakeAsyncPublisher, timeout time.Duration) *JetStreamPublisher {
	p := New(nil, timeout)
	p.js = js
	return p
}

func TestPublishAsyncSuccessWaitsForAck(t *testing.T) {
	t.Parallel()

	future := &fakePubAckFuture{okCh: make(chan *nats.PubAck, 1), errCh: make(chan error, 1)}
	future.okCh <- &nats.PubAck{Stream: "yardflow-events", Sequence: 1}
	js := &fakeAsyncPublisher{future: future}
	p := newTestPublisher(js, time.Second)

	if err := p.PublishAsync(context.Background(), "yardflow.bol.bol.registered", []byte("data"), "msg-1"); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if js.calls != 1 || js.subject != "yardflow.bol.bol.registered" {
		t.Fatalf("publish call = %d subject = %q", js.calls, js.subject)
	}
	if string(js.data) != "data" {
		t.Fatalf("data = %q", js.data)
	}
	// The MsgId option must carry the KurrentDB EventID (dedup contract).
	if len(js.pubOpts) != 1 {
		t.Fatalf("pub opts = %v, want exactly MsgId", js.pubOpts)
	}
}

// TestPublishAsyncClassifiesNoRespondersOnInit is the unit pin of the
// production failure mode: publishing to a subject no stream covers.
func TestPublishAsyncClassifiesNoRespondersOnInit(t *testing.T) {
	t.Parallel()

	js := &fakeAsyncPublisher{initErr: nats.ErrNoResponders}
	p := newTestPublisher(js, time.Second)

	err := p.PublishAsync(context.Background(), "yardflow.bol.bol.registered", []byte("d"), "m")
	var ne *natspub.Error
	if !errors.As(err, &ne) {
		t.Fatalf("expected *natspub.Error, got %T: %v", err, err)
	}
	if ne.Kind != natspub.ErrKindNoResponders {
		t.Fatalf("Kind = %q, want no_responders", ne.Kind)
	}
	if ne.Subject != "yardflow.bol.bol.registered" {
		t.Fatalf("Subject = %q", ne.Subject)
	}
	for _, frag := range []string{"no JetStream stream is interested", "<prefix>.>'", "nats stream info"} {
		if !strings.Contains(ne.Guidance, frag) {
			t.Fatalf("Guidance must contain %q: %q", frag, ne.Guidance)
		}
	}
	if !errors.Is(err, nats.ErrNoResponders) {
		t.Fatal("errors.Is must reach nats.ErrNoResponders")
	}
}

// TestPublishAsyncClassifiesNoRespondersOnAck covers the same error
// arriving on the future's error channel (async publish path).
func TestPublishAsyncClassifiesNoRespondersOnAck(t *testing.T) {
	t.Parallel()

	future := &fakePubAckFuture{okCh: make(chan *nats.PubAck), errCh: make(chan error, 1)}
	future.errCh <- nats.ErrNoResponders
	js := &fakeAsyncPublisher{future: future}
	p := newTestPublisher(js, time.Second)

	err := p.PublishAsync(context.Background(), "s", []byte("d"), "m")
	var ne *natspub.Error
	if !errors.As(err, &ne) || ne.Kind != natspub.ErrKindNoResponders {
		t.Fatalf("expected no_responders classification, got %v", err)
	}
}

func TestPublishAsyncClassifiesTransientErrors(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"timeout", nats.ErrTimeout},
		{"connection closed", nats.ErrConnectionClosed},
		{"no servers", nats.ErrNoServers},
		{"other", errors.New("boom")},
	} {
		tc := tc
		t.Run(tc.name+" (init)", func(t *testing.T) {
			t.Parallel()
			js := &fakeAsyncPublisher{initErr: tc.err}
			p := newTestPublisher(js, time.Second)
			err := p.PublishAsync(context.Background(), "s", []byte("d"), "m")
			var ne *natspub.Error
			if !errors.As(err, &ne) || ne.Kind != natspub.ErrKindUnavailable {
				t.Fatalf("expected unavailable, got %v", err)
			}
			if !ne.Retryable() {
				t.Fatal("unavailable must be retryable")
			}
			if !errors.Is(err, tc.err) {
				t.Fatalf("errors.Is must reach %v", tc.err)
			}
		})
		t.Run(tc.name+" (ack)", func(t *testing.T) {
			t.Parallel()
			future := &fakePubAckFuture{okCh: make(chan *nats.PubAck), errCh: make(chan error, 1)}
			future.errCh <- tc.err
			js := &fakeAsyncPublisher{future: future}
			p := newTestPublisher(js, time.Second)
			err := p.PublishAsync(context.Background(), "s", []byte("d"), "m")
			var ne *natspub.Error
			if !errors.As(err, &ne) || ne.Kind != natspub.ErrKindUnavailable {
				t.Fatalf("expected unavailable, got %v", err)
			}
		})
	}
}

// TestPublishAsyncAckChannelClosedWithoutError pins the defensive
// contract: an ack channel closing with a nil error is an error, not
// success.
func TestPublishAsyncAckChannelClosedWithoutError(t *testing.T) {
	t.Parallel()

	future := &fakePubAckFuture{okCh: make(chan *nats.PubAck), errCh: make(chan error, 1)}
	future.errCh <- nil
	js := &fakeAsyncPublisher{future: future}
	p := newTestPublisher(js, time.Second)

	err := p.PublishAsync(context.Background(), "s", []byte("d"), "m")
	var ne *natspub.Error
	if !errors.As(err, &ne) {
		t.Fatalf("expected classified error, got %v", err)
	}
	if !strings.Contains(ne.Detail, "ack channel closed without error") {
		t.Fatalf("Detail must describe the closed ack channel: %q", ne.Detail)
	}
}

func TestPublishAsyncHonorsCallerContextCancel(t *testing.T) {
	t.Parallel()

	future := &fakePubAckFuture{okCh: make(chan *nats.PubAck), errCh: make(chan error)}
	js := &fakeAsyncPublisher{future: future}
	p := newTestPublisher(js, time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.PublishAsync(ctx, "s", []byte("d"), "m"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestPublishAsyncAckTimeout(t *testing.T) {
	t.Parallel()

	future := &fakePubAckFuture{okCh: make(chan *nats.PubAck), errCh: make(chan error)}
	js := &fakeAsyncPublisher{future: future}
	p := newTestPublisher(js, 10*time.Millisecond)

	err := p.PublishAsync(context.Background(), "s", []byte("d"), "m")
	var ne *natspub.Error
	if !errors.As(err, &ne) || ne.Kind != natspub.ErrKindUnavailable {
		t.Fatalf("expected unavailable on ack timeout, got %v", err)
	}
	if !strings.Contains(ne.Detail, "timed out waiting for ack") {
		t.Fatalf("Detail must describe the timeout: %q", ne.Detail)
	}
}
