// contract_validator.go — read-only JetStream validation for the
// Reporting contract stream (AR-510 / R02 Step B, precondition T2).
//
// The producer-owned Reporting contract relays publish ONLY to the
// dedicated contract subject namespace
// `yardflow.reporting.contracts.<family>.v<major>` (reportingcontracts).
// Before ANY relay subscribes to KurrentDB (the D114 discipline, reused
// verbatim), the contract JetStream stream is validated read-only: the
// stream must exist and its subject filters must cover EVERY emittable
// contract subject. A missing or misfiltered stream otherwise makes every
// publish fail with nats.ErrNoResponders and the relays crash-loop
// through filtered `$all` subscriptions. The validator never creates or
// mutates the stream — provisioning (including the retention policy that
// MUST cover the consumer-before-export bootstrap window, R3-F2) is an
// operational concern owned out-of-band.
package natsadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	natspub "github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/natspub"
)

// ContractStreamValidatorConfig configures the read-only contract stream
// validation.
type ContractStreamValidatorConfig struct {
	// StreamName is the configured JetStream stream carrying the
	// reporting contract events (NATS_REPORTING_STREAM_NAME).
	StreamName string
	// Subjects is the EXHAUSTIVE list of concrete subjects the relays can
	// emit — reportingcontracts.EmittableSubjects(major) at construction.
	// The validator proves every one of them is matched by some stream
	// subject filter, so a stream configured with the namespace filter
	// `yardflow.reporting.contracts.>` passes AND a stream listing every
	// concrete subject passes, while any narrower filter fails.
	Subjects []string
	// URL is the NATS server URL, used only to make guidance text
	// copy-pasteable. Optional.
	URL string
	// Timeout bounds each StreamInfo attempt. Zero falls back to 5s.
	Timeout time.Duration
}

// ContractStreamValidator validates (read-only) that the configured
// contract JetStream stream exists and covers every emittable contract
// subject. It implements natspub.Validator.
type ContractStreamValidator struct {
	js  streamInfoGetter
	cfg ContractStreamValidatorConfig
}

// NewContractStreamValidator constructs a ContractStreamValidator over the
// given JetStream context. The context is only ever read (StreamInfo).
func NewContractStreamValidator(js nats.JetStreamContext, cfg ContractStreamValidatorConfig) *ContractStreamValidator {
	return &ContractStreamValidator{js: js, cfg: cfg}
}

// Validate looks the contract stream up read-only and checks concrete
// subject coverage. Failures are returned as *natspub.Error with kind and
// guidance; transient failures carry ErrKindUnavailable (retryable), so
// the natspub worker parks in its validation gate with one actionable log
// line instead of crash-looping KurrentDB subscriptions.
func (v *ContractStreamValidator) Validate(ctx context.Context) error {
	timeout := v.cfg.Timeout
	if timeout <= 0 {
		timeout = defaultStreamInfoTimeout
	}
	vctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	info, err := v.js.StreamInfo(v.cfg.StreamName, nats.Context(vctx))
	if err != nil {
		return classifyContractStreamInfoError(err, v.cfg)
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
	var uncovered []string
	for _, subject := range v.cfg.Subjects {
		covered := false
		for _, filter := range info.Config.Subjects {
			if natspub.FilterMatchesSubject(filter, subject) {
				covered = true
				break
			}
		}
		if !covered {
			uncovered = append(uncovered, subject)
		}
	}
	if len(uncovered) == 0 {
		return nil
	}
	return &natspub.Error{
		Op:     "validate",
		Kind:   natspub.ErrKindSubjectMismatch,
		Stream: v.cfg.StreamName,
		Detail: fmt.Sprintf("stream subjects %v do not cover %d of %d emittable contract subjects (uncovered: %v)",
			info.Config.Subjects, len(uncovered), len(v.cfg.Subjects), uncovered),
		Guidance: fmt.Sprintf(
			"re-create or update the contract stream out-of-band with subjects 'yardflow.reporting.contracts.>' "+
				"(e.g. `nats -s %s stream update %s --subjects 'yardflow.reporting.contracts.>'`); the retention policy MUST cover "+
				"the longest tolerated consumer-before-export bootstrap window (R3-F2) — verify with `nats stream info %s`",
			contractCfgURL(v.cfg), v.cfg.StreamName, v.cfg.StreamName),
	}
}

// Compile-time interface check.
var _ natspub.Validator = (*ContractStreamValidator)(nil)

// classifyContractStreamInfoError maps a contract StreamInfo failure onto
// the natspub error taxonomy with CONTRACT-specific guidance (the correct
// subjects are the reporting contract namespace, not the raw event
// prefix).
func classifyContractStreamInfoError(err error, cfg ContractStreamValidatorConfig) error {
	switch {
	case errors.Is(err, nats.ErrNoResponders):
		return &natspub.Error{
			Op:     "validate",
			Kind:   natspub.ErrKindNoResponders,
			Stream: cfg.StreamName,
			Detail: "JetStream API had no responder for the stream-info request",
			Guidance: "JetStream appears unavailable: verify the server was started with JetStream enabled " +
				"(`-js` / `js_enabled`, with a storage dir) — check `nats -s " + contractCfgURL(cfg) + " server check jetstream` " +
				"or GET http://<nats-host>:8222/jsz",
			Err: err,
		}
	case errors.Is(err, nats.ErrStreamNotFound):
		return &natspub.Error{
			Op:     "validate",
			Kind:   natspub.ErrKindStreamNotFound,
			Stream: cfg.StreamName,
			Detail: "configured reporting contract JetStream stream does not exist",
			Guidance: fmt.Sprintf(
				"provision the contract stream out-of-band BEFORE the relays publish (T2; the relays never auto-create it): "+
					"`nats -s %s stream add %s --subjects 'yardflow.reporting.contracts.>' --storage file --retention limits --dupe-window 2m` "+
					"(retention MUST cover the consumer-before-export bootstrap window)",
				contractCfgURL(cfg), cfg.StreamName),
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
			Guidance: "retrying with backoff; if it persists, verify NATS reachability (" + contractCfgURL(cfg) + ")",
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

// contractCfgURL renders the configured URL for guidance text (empty when
// unknown — the adapter does not hold the connection config).
func contractCfgURL(cfg ContractStreamValidatorConfig) string {
	if cfg.URL != "" {
		return cfg.URL
	}
	return "$NATS_URL"
}
