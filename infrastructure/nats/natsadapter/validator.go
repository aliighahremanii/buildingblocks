// validator.go — read-only JetStream stream/subject-family validation for
// the natspub worker (D114).
//
// The validator performs exactly one JetStream API read (StreamInfo) and
// classifies every failure into the natspub error taxonomy so the worker
// can block its KurrentDB subscription with ONE actionable log line per
// condition. It never creates, updates, or deletes the stream — stream
// configuration (retention, max-age, duplicate window) is an operational
// concern owned out-of-band.
package natsadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	natspub "github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/natspub"
)

// streamInfoGetter is the narrow JetStream surface the validator needs;
// *nats.JetStreamContext satisfies it. Tests substitute a fake so the
// classification is unit-testable without a server.
type streamInfoGetter interface {
	StreamInfo(stream string, opts ...nats.JSOpt) (*nats.StreamInfo, error)
}

// StreamValidatorConfig configures the read-only validation.
type StreamValidatorConfig struct {
	// StreamName is the configured JetStream stream (NATS_STREAM_NAME).
	StreamName string
	// SubjectPrefix is the publisher's NATS subject prefix
	// (NATS_SUBJECT_PREFIX, default "yardflow").
	SubjectPrefix string
	// StreamPrefixes are the KurrentDB stream-id prefixes the publisher
	// is filtered to (the host's allDomainStreamPrefixes union). Each
	// prefix implies one emittable subject family
	// "<SubjectPrefix>.<aggregateType>.>".
	StreamPrefixes []string
	// URL is the NATS server URL, used only to make guidance text
	// copy-pasteable. Optional.
	URL string
	// Timeout bounds each StreamInfo attempt. Zero falls back to 5s. The
	// caller's context is honored as well, so the worker's shutdown
	// cancels an in-flight validation.
	Timeout time.Duration
}

const defaultStreamInfoTimeout = 5 * time.Second

// StreamValidator validates (read-only) that the configured JetStream
// stream exists and covers the publisher's subject family. It implements
// natspub.Validator.
type StreamValidator struct {
	js  streamInfoGetter
	cfg StreamValidatorConfig
}

// NewStreamValidator constructs a StreamValidator over the given JetStream
// context. The context is only ever read (StreamInfo).
func NewStreamValidator(js nats.JetStreamContext, cfg StreamValidatorConfig) *StreamValidator {
	return &StreamValidator{js: js, cfg: cfg}
}

// Validate looks the stream up read-only and checks subject-family
// coverage. Failures are returned as *natspub.Error with kind and
// guidance; transient failures carry ErrKindUnavailable (retryable).
//
// The lookup is bounded and context-cancellable: a deadline context is
// derived from the caller's ctx and passed via nats.Context. (nats.go
// v1.38 rejects combining nats.Context with nats.MaxWait —
// ErrContextAndTimeout — so the bound comes from the derived deadline,
// which also inherits the caller's cancellation.)
func (v *StreamValidator) Validate(ctx context.Context) error {
	timeout := v.cfg.Timeout
	if timeout <= 0 {
		timeout = defaultStreamInfoTimeout
	}
	vctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	info, err := v.js.StreamInfo(v.cfg.StreamName, nats.Context(vctx))
	if err != nil {
		return classifyStreamInfoError(err, v.cfg)
	}
	if info == nil {
		return &natspub.Error{
			Op:       "validate",
			Kind:     natspub.ErrKindUnavailable,
			Stream:   v.cfg.StreamName,
			Detail:   "StreamInfo returned no configuration",
			Guidance: "retry; if it persists, inspect the server with `nats stream info " + v.cfg.StreamName + "`",
		}
	}
	if verr := natspub.ValidateSubjectCoverage(
		v.cfg.StreamName, info.Config.Subjects, v.cfg.SubjectPrefix, v.cfg.StreamPrefixes); verr != nil {
		return verr
	}
	return nil
}

// Compile-time interface check.
var _ natspub.Validator = (*StreamValidator)(nil)

// classifyStreamInfoError maps a StreamInfo failure onto the natspub error
// taxonomy. nats.ErrNoResponders is the subtle one: the JetStream API
// request ($JS.API.STREAM.INFO.<stream>) had no subscriber, which on a
// reachable server means JetStream is not enabled (server or account) —
// it does NOT mean the stream is missing (a missing stream on a
// JetStream-enabled server answers ErrStreamNotFound).
func classifyStreamInfoError(err error, cfg StreamValidatorConfig) error {
	switch {
	case errors.Is(err, nats.ErrNoResponders):
		return &natspub.Error{
			Op:     "validate",
			Kind:   natspub.ErrKindNoResponders,
			Stream: cfg.StreamName,
			Detail: "JetStream API had no responder for the stream-info request",
			Guidance: "JetStream appears unavailable: verify the server was started with JetStream enabled " +
				"(`-js` / `js_enabled`, with a storage dir) — check `nats -s " + cfgURL(cfg) + " server check jetstream` " +
				"or GET http://<nats-host>:8222/jsz; also verify the URL does not point at a plain core-NATS server " +
				"and the account has JetStream enabled",
			Err: err,
		}
	case errors.Is(err, nats.ErrStreamNotFound):
		return &natspub.Error{
			Op:     "validate",
			Kind:   natspub.ErrKindStreamNotFound,
			Stream: cfg.StreamName,
			Detail: "configured JetStream stream does not exist",
			Guidance: fmt.Sprintf(
				"create the stream out-of-band (the publisher never auto-creates it): "+
					"`nats -s %s stream add %s --subjects '%s.>' --storage file --retention limits --dupe-window 2m` "+
					"(the 2m duplicate window is required for Nats-Msg-Id replay dedup)",
				cfgURL(cfg), cfg.StreamName, cfg.SubjectPrefix),
			Err: err,
		}
	case errors.Is(err, nats.ErrJetStreamNotEnabled),
		errors.Is(err, nats.ErrJetStreamNotEnabledForAccount):
		return &natspub.Error{
			Op:     "validate",
			Kind:   natspub.ErrKindNoResponders,
			Stream: cfg.StreamName,
			Detail: "server/account reported JetStream not enabled",
			Guidance: "enable JetStream on the server (start with `-js` and a storage dir) or for the account, " +
				"then restart the worker; verify with GET http://<nats-host>:8222/jsz",
			Err: err,
		}
	case errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, nats.ErrTimeout),
		errors.Is(err, nats.ErrConnectionClosed),
		errors.Is(err, nats.ErrNoServers):
		return &natspub.Error{
			Op:       "validate",
			Kind:     natspub.ErrKindUnavailable,
			Stream:   cfg.StreamName,
			Detail:   "transient failure while contacting the JetStream API",
			Guidance: "retrying with backoff; if it persists, verify NATS reachability (" + cfgURL(cfg) + ")",
			Err:      err,
		}
	default:
		return &natspub.Error{
			Op:       "validate",
			Kind:     natspub.ErrKindUnavailable,
			Stream:   cfg.StreamName,
			Detail:   fmt.Sprintf("unexpected stream-info failure: %v", err),
			Guidance: "retrying with backoff; if it persists, inspect the server with `nats stream info " + cfg.StreamName + "`",
			Err:      err,
		}
	}
}

// cfgURL renders the configured URL for guidance text (empty when unknown —
// the adapter does not hold the connection config).
func cfgURL(cfg StreamValidatorConfig) string {
	if cfg.URL != "" {
		return cfg.URL
	}
	return "$NATS_URL"
}
