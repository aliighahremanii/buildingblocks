package natspub

// transform_test.go — focused deterministic coverage for the 0..N
// batch-emission extension (R3-F1, AR-510 Step B STOP-check lane):
//
//   - batch determinism: a replayed event yields the IDENTICAL message set
//     with the same message ids (cursor re-drive correctness);
//   - whole-source-event redrive: a publish failure MID-BATCH stops the
//     attempt with the durable position NOT advanced past the source
//     event — redrive republishes the whole batch (the JetStream
//     Nats-Msg-Id dedupe absorbs the overlap);
//   - a zero-message event still advances the position (an irrelevant
//     event is not a stall);
//   - a transform error ends the attempt with the position preserved (the
//     event is never skipped);
//   - the duplicate (subject, msgID) guard rejects an ambiguous batch;
//   - the raw mode is byte-identical to the pre-extension behavior (one
//     message, subjectFor subject, EventID as Nats-Msg-Id).

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

// scriptedSub yields a fixed sequence of subscription events then ends.
type scriptedSub struct {
	events []*kurrentdb.SubscriptionEvent
	i      int
}

func (s *scriptedSub) Recv() *kurrentdb.SubscriptionEvent {
	if s.i >= len(s.events) {
		return nil
	}
	ev := s.events[s.i]
	s.i++
	return ev
}

func (s *scriptedSub) Close() error { return nil }

func appeared(ev *kurrentdb.RecordedEvent) *kurrentdb.SubscriptionEvent {
	return &kurrentdb.SubscriptionEvent{EventAppeared: &kurrentdb.ResolvedEvent{Event: ev}}
}

func recorded(id string, commit uint64, eventType string) *kurrentdb.RecordedEvent {
	return &kurrentdb.RecordedEvent{
		EventID:     uuid.MustParse(id),
		EventType:   eventType,
		StreamID:    "order-1",
		EventNumber: commit, // revision within the stream (1-based here)
		Position:    kurrentdb.Position{Commit: commit, Prepare: commit},
		CreatedDate: time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC),
		Data:        []byte(`{"OrderNumber":"ORD-1"}`),
	}
}

// newTransformTestWorker builds a worker whose runOnce attempt is driven
// through the scripted subscription (subscribeF seam), a transform-mode
// config, and the given publisher/store.
func newTransformTestWorker(pub *fakePublisher, store *fakePositionStore, transform EventTransform) *Worker {
	w := New(nil, store, pub, nil, WorkerConfig{
		Name:      "contracts-relay-test",
		Prefixes:  []string{"order-"},
		BatchSize: 1,
		Transform: transform,
	})
	w.subscribeF = func(_ *kurrentdb.Client, _ context.Context, _ kurrentdb.SubscribeToAllOptions) (subscription, error) {
		return nil, errors.New("unused — runOnce is invoked directly")
	}
	return w
}

// TestDeriveBatchRawModeIsUnchanged — with no transform, one event yields
// exactly the legacy single message (subjectFor subject, raw data, the
// KurrentDB EventID as Nats-Msg-Id).
func TestDeriveBatchRawModeIsUnchanged(t *testing.T) {
	w := New(nil, &fakePositionStore{}, &fakePublisher{}, nil, WorkerConfig{Name: "raw", Prefixes: []string{"order-"}})
	ev := recorded("9f2b0000-0000-4000-8000-000000000001", 1, "order.registered")
	batch, err := w.deriveBatch(ev)
	if err != nil {
		t.Fatalf("deriveBatch: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("raw batch = %d messages, want 1", len(batch))
	}
	if batch[0].Subject != "yardflow.order.order.registered" {
		t.Fatalf("subject = %q", batch[0].Subject)
	}
	if batch[0].MsgID != ev.EventID.String() {
		t.Fatalf("msgID = %q, want the KurrentDB EventID", batch[0].MsgID)
	}
	if string(batch[0].Data) != string(ev.Data) {
		t.Fatal("raw data must pass through unchanged")
	}
}

// TestDeriveBatchRejectsAmbiguousBatches — a batch may never carry a
// duplicate (subject, msgID) pair, nor a message without subject or id:
// either would make the batch identity ambiguous under replay.
func TestDeriveBatchRejectsAmbiguousBatches(t *testing.T) {
	w := New(nil, &fakePositionStore{}, &fakePublisher{}, nil, WorkerConfig{Name: "bad", Prefixes: []string{"order-"}})
	ev := recorded("9f2b0000-0000-4000-8000-000000000001", 1, "order.registered")
	dup := EventTransform(func(*kurrentdb.RecordedEvent) ([]BatchMessage, error) {
		m := BatchMessage{Subject: "s", Data: nil, MsgID: "m"}
		return []BatchMessage{m, m}, nil
	})
	w.config.Transform = dup
	if _, err := w.deriveBatch(ev); err == nil {
		t.Fatal("duplicate (subject,msgID) must be rejected")
	}
	missing := EventTransform(func(*kurrentdb.RecordedEvent) ([]BatchMessage, error) {
		return []BatchMessage{{Subject: "s"}}, nil
	})
	w.config.Transform = missing
	if _, err := w.deriveBatch(ev); err == nil {
		t.Fatal("a message without msgID must be rejected")
	}
	boom := EventTransform(func(*kurrentdb.RecordedEvent) ([]BatchMessage, error) {
		return nil, errors.New("undecodable")
	})
	w.config.Transform = boom
	if _, err := w.deriveBatch(ev); err == nil || err.Error() == "undecodable" {
		t.Fatalf("transform errors must be wrapped with the relay name: %v", err)
	}
}

// countingTransform derives 0..N messages deterministically from the event
// type: "batch" events yield 3 messages, "silent" events yield none.
func countingTransform() EventTransform {
	return EventTransform(func(ev *kurrentdb.RecordedEvent) ([]BatchMessage, error) {
		switch ev.EventType {
		case "order.batch":
			out := make([]BatchMessage, 0, 3)
			for i := 0; i < 3; i++ {
				out = append(out, BatchMessage{
					Subject: fmt.Sprintf("yardflow.reporting.contracts.test.%d", i),
					Data:    []byte(fmt.Sprintf(`{"i":%d}`, i)),
					MsgID:   ev.EventID.String() + ":test:" + fmt.Sprint(i),
				})
			}
			return out, nil
		case "order.silent":
			return nil, nil
		default:
			return nil, fmt.Errorf("unknown event type %q", ev.EventType)
		}
	})
}

// TestRunOnceBatchModePublishesWholeBatchAndAdvances — a batch event
// publishes all 3 messages and the position store delta accounts 3; a
// zero-message event still advances the commit position past the event.
func TestRunOnceBatchModePublishesWholeBatchAndAdvances(t *testing.T) {
	store := &fakePositionStore{}
	pub := &fakePublisher{}
	w := newTransformTestWorker(pub, store, countingTransform())

	// BatchSize 1 → a save after every event; positions 1 (batch) and 2
	// (silent). runOnce is invoked directly (the subscribeF seam is not
	// used — see newTransformTestWorker).
	w.runAttempt = func(ctx context.Context) (uint64, error) {
		return w.runOnce(ctx)
	}
	// Drive runOnce through the scripted subscription by rebinding
	// subscribeF to return it.
	sub := &scriptedSub{events: []*kurrentdb.SubscriptionEvent{
		appeared(recorded("9f2b0000-0000-4000-8000-000000000001", 1, "order.batch")),
		appeared(recorded("9f2b0000-0000-4000-8000-000000000002", 2, "order.silent")),
	}}
	w.subscribeF = func(_ *kurrentdb.Client, _ context.Context, _ kurrentdb.SubscribeToAllOptions) (subscription, error) {
		return sub, nil
	}

	published, err := w.runOnce(context.Background())
	if err != nil {
		t.Fatalf("runOnce: %v", err)
	}
	if published != 3 {
		t.Fatalf("published = %d, want 3 (only the batch event's messages)", published)
	}
	msgs := pub.snapshot()
	if len(msgs) != 3 {
		t.Fatalf("published messages = %d, want 3", len(msgs))
	}
	// Deterministic identity: subject/msgID pairs derive from the event.
	for i, m := range msgs {
		wantSubj := fmt.Sprintf("yardflow.reporting.contracts.test.%d", i)
		if m.subject != wantSubj {
			t.Fatalf("msg[%d].subject = %q, want %q", i, m.subject, wantSubj)
		}
		if m.msgID != "9f2b0000-0000-4000-8000-000000000001:test:"+fmt.Sprint(i) {
			t.Fatalf("msg[%d].msgID = %q", i, m.msgID)
		}
	}
	// The terminal flush persisted the position past BOTH events (the
	// silent event advanced the cursor without publishing anything) and
	// the counter accounted exactly 3 published messages.
	if store.pos != 2 {
		t.Fatalf("durable position = %d, want 2 (silent event advanced the cursor)", store.pos)
	}
	if store.published != 3 {
		t.Fatalf("events_published counter = %d, want 3", store.published)
	}
}

// TestRunOnceMidBatchPublishFailureKeepsWholeEventRedriveable — a publish
// failure on the SECOND message of a 3-message batch ends the attempt with
// the durable position still at the previous boundary: the whole source
// event (all 3 messages) replays on redrive and JetStream dedups the
// overlap. This is the STOP-check invariant of the whole-source-event
// redrive.
func TestRunOnceMidBatchPublishFailureKeepsWholeEventRedriveable(t *testing.T) {
	store := &fakePositionStore{pos: 0}
	pub := &fakePublisher{}
	pub.failOnMsgID = "9f2b0000-0000-4000-8000-000000000001:test:1" // fail the 2nd message
	w := newTransformTestWorker(pub, store, countingTransform())
	sub := &scriptedSub{events: []*kurrentdb.SubscriptionEvent{
		appeared(recorded("9f2b0000-0000-4000-8000-000000000001", 1, "order.batch")),
	}}
	w.subscribeF = func(_ *kurrentdb.Client, _ context.Context, _ kurrentdb.SubscribeToAllOptions) (subscription, error) {
		return sub, nil
	}
	w.runAttempt = func(ctx context.Context) (uint64, error) { return w.runOnce(ctx) }

	if _, err := w.runOnce(context.Background()); err == nil {
		t.Fatal("a mid-batch publish failure must end the attempt with an error")
	}
	// The durable position is UNCHANGED: the source event is fully
	// redriveable (its whole batch replays).
	if store.pos != 0 {
		t.Fatalf("durable position = %d, want 0 (whole source event must stay redriveable)", store.pos)
	}
	if store.published != 0 {
		t.Fatalf("events_published = %d, want 0 (no delta accounted past the failed batch)", store.published)
	}
	// The first message WAS published (the ack was received) but its delta
	// is NOT accounted — the retry replays the whole batch and JetStream
	// dedups the overlap. Only the position boundary matters.
	msgs := pub.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("published before failure = %d, want 1 (the batch aborted on the 2nd message)", len(msgs))
	}

	// RECOVERY PROOF: a retry from the SAME durable position re-derives
	// the identical batch (determinism) and, with the failure cleared,
	// publishes ALL 3 messages and advances the position past the event.
	pub2 := &fakePublisher{}
	w2 := newTransformTestWorker(pub2, store, countingTransform())
	sub2 := &scriptedSub{events: []*kurrentdb.SubscriptionEvent{
		appeared(recorded("9f2b0000-0000-4000-8000-000000000001", 1, "order.batch")),
	}}
	w2.subscribeF = func(_ *kurrentdb.Client, _ context.Context, _ kurrentdb.SubscribeToAllOptions) (subscription, error) {
		return sub2, nil
	}
	if _, err := w2.runOnce(context.Background()); err != nil {
		t.Fatalf("recovery runOnce: %v", err)
	}
	if got := len(pub2.snapshot()); got != 3 {
		t.Fatalf("recovery published = %d messages, want the whole 3-message batch", got)
	}
	if store.pos != 1 {
		t.Fatalf("recovered position = %d, want 1", store.pos)
	}
	// Determinism: the replayed batch is byte-identical (same subjects,
	// same ids, same order) to the original derivation.
	first := pub.snapshot()[0]
	replay := pub2.snapshot()[0]
	if first.subject != replay.subject || first.msgID != replay.msgID || string(first.data) != string(replay.data) {
		t.Fatal("the replayed batch is not identical to the original derivation")
	}
}

// TestRunOnceTransformErrorPreservesPosition — an undecodable source event
// (a relay defect) ends the attempt with the position preserved: the
// event is never skipped.
func TestRunOnceTransformErrorPreservesPosition(t *testing.T) {
	store := &fakePositionStore{}
	pub := &fakePublisher{}
	w := newTransformTestWorker(pub, store, countingTransform())
	sub := &scriptedSub{events: []*kurrentdb.SubscriptionEvent{
		appeared(recorded("9f2b0000-0000-4000-8000-000000000003", 1, "order.unknown")),
	}}
	w.subscribeF = func(_ *kurrentdb.Client, _ context.Context, _ kurrentdb.SubscribeToAllOptions) (subscription, error) {
		return sub, nil
	}
	if _, err := w.runOnce(context.Background()); err == nil {
		t.Fatal("an unknown event type must end the attempt with a transform error")
	}
	if store.pos != 0 || len(pub.snapshot()) != 0 {
		t.Fatalf("transform error must preserve position and publish nothing: pos=%d msgs=%d", store.pos, len(pub.snapshot()))
	}
}
