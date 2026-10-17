// transform.go — the 0..N batch-emission extension of the natspub
// publisher (AR-510 / R02 Step B, R3-F1).
//
// The raw-republish mode (WorkerConfig.Transform == nil) is UNCHANGED:
// one source event → one NATS message at subjectFor(...) with
// Nats-Msg-Id = the KurrentDB EventID. The transform mode generalizes the
// cardinality WITHOUT touching the raw path: one committed source event
// derives a BATCH of 0..N contract messages — each with its own subject,
// payload, and Nats-Msg-Id — published through the same Publisher
// interface, the same validation gate, the same position store, and the
// same terminal-flush discipline.
//
// Whole-source-event redrive (the invariant the STOP-check demanded):
// the durable position advances past a source event ONLY after EVERY
// message of its batch has been published (ack received). A publish
// failure anywhere in the batch ends the subscription attempt with the
// position still at the last fully-published batch boundary — redrive
// replays the WHOLE source event (all messages re-published) and JetStream
// deduplicates the overlap by Nats-Msg-Id. A batch is therefore never
// half-published durably: either all of an event's messages are past the
// cursor, or none are.
//
// Determinism (R3-F1): the same replayed event must always yield the same
// message set with the same message ids, so cursor re-drive, JetStream
// dedupe, and consumer-side inbox dedupe converge. Transforms are pure
// functions of the recorded event; they must not consult external state.
package natspub

import (
	"fmt"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

// BatchMessage is ONE derived contract message of a source event's 0..N
// batch. Subject is the full NATS subject; MsgID is the Nats-Msg-Id
// dedupe header (identical to the contract envelope's messageId for the
// Reporting contracts — R3-F1).
type BatchMessage struct {
	Subject string
	Data    []byte
	MsgID   string
}

// EventTransform derives the batch of 0..N contract messages from one
// committed source event. It must be DETERMINISTIC (the same event always
// yields the same message set and message ids — a replayed event must
// re-derive byte-identical messages so JetStream's Nats-Msg-Id dedupe and
// the consumer's inbox dedupe absorb re-drive duplicates) and PURE (no
// external state: the transform is the relay's re-drive correctness).
//
// An error is a relay defect (an undecodable source event): the worker
// ends the subscription attempt with the position preserved — the event
// is never skipped — and the failure classifies as an unproductive
// attempt (bounded backoff, retry, operator visibility).
type EventTransform func(ev *kurrentdb.RecordedEvent) ([]BatchMessage, error)

// DeriveBatch resolves the batch for one event from a worker config: the
// configured transform, or the single raw-republish message (the
// untransformed mode — one message at subjectFor with the EventID as
// Nats-Msg-Id). EXPORTED so the as-of exporter (kurrentexport, R02 Step E)
// reuses the relay's EXACT batch derivation and determinism guards — the
// live relay and the bootstrap export must derive byte-identical batches
// from the same event or the sources' overlap is not dedupable. The
// worker's deriveBatch delegates here (one implementation, two drivers).
//
// Pass a config normalized through New (as the worker does) when using
// raw mode; transform mode reads only Name and Transform.
func DeriveBatch(cfg WorkerConfig, ev *kurrentdb.RecordedEvent) ([]BatchMessage, error) {
	if cfg.Transform == nil {
		return []BatchMessage{{
			Subject: subjectFor(cfg.SubjectPrefix, ev.StreamID, ev.EventType),
			Data:    ev.Data,
			MsgID:   ev.EventID.String(),
		}}, nil
	}
	msgs, err := cfg.Transform(ev)
	if err != nil {
		return nil, fmt.Errorf("natspub transform %s: %w", cfg.Name, err)
	}
	// Determinism guard: a batch may never carry two messages with the
	// same (subject, msgID) — that would make the batch's identity
	// ambiguous under replay and silently drop a contribution.
	seen := make(map[string]struct{}, len(msgs))
	for i, m := range msgs {
		if m.Subject == "" || m.MsgID == "" {
			return nil, fmt.Errorf("natspub transform %s: batch[%d] lacks subject or msg id", cfg.Name, i)
		}
		key := m.Subject + "\x00" + m.MsgID
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("natspub transform %s: batch carries duplicate (subject,msgID) %q/%q", cfg.Name, m.Subject, m.MsgID)
		}
		seen[key] = struct{}{}
	}
	return msgs, nil
}

// deriveBatch resolves the batch for one event through the shared
// DeriveBatch over the worker's (normalized) config.
func (w *Worker) deriveBatch(ev *kurrentdb.RecordedEvent) ([]BatchMessage, error) {
	return DeriveBatch(w.config, ev)
}
