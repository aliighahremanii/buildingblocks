package natspub

// validation.go — read-only JetStream stream/subject-family validation and
// the shared error classification used by the publisher worker (D114).
//
// The worker must not tail KurrentDB's $all stream into a sink that cannot
// durably accept events: if the configured JetStream stream does not exist,
// JetStream is not enabled, or the stream's subjects do not cover the
// publisher's subject family, every PublishAsync fails (typically with
// nats.ErrNoResponders) and the worker crash-loops through KurrentDB
// subscriptions. Validation runs BEFORE the KurrentDB subscription is
// opened, is strictly read-only (a StreamInfo lookup — streams are never
// created or mutated by this process), and produces classified, actionable
// errors the worker logs without duplication.
//
// Subject-family contract: the publisher derives subjects as
// SubjectPrefix.aggregateType.eventType (see subjectFor). eventType is a
// dotted name (e.g. "country.registered"), so every publishable subject has
// at least TWO tokens after the subject prefix. The emittable family for one
// aggregate type A is therefore the wildcard set "<prefix>.A.>" (one or more
// tokens after the aggregate token). A stream subject filter of
// "<prefix>.*" matches exactly ONE token after the prefix — it matches none
// of the published subjects and must be rejected with guidance to use
// "<prefix>.>" instead.

import (
	"context"
	"fmt"
	"strings"
)

// ErrKind classifies a NATS publication/validation failure. The kind drives
// worker behavior (retry vs blocked-with-guidance) and log deduplication.
type ErrKind string

const (
	// ErrKindStreamNotFound: the configured JetStream stream does not
	// exist. The stream is created out-of-band (deliberately — retention,
	// max-age, and duplicate window are operational concerns); the
	// publisher never auto-creates it.
	ErrKindStreamNotFound ErrKind = "stream_not_found"
	// ErrKindNoResponders: the JetStream API request had no responder —
	// most commonly JetStream is not enabled on the server/account, or
	// the URL points at a plain core-NATS server. On the publish path the
	// same error means no stream is interested in the subject.
	ErrKindNoResponders ErrKind = "no_responders"
	// ErrKindSubjectMismatch: the stream exists but its subject filters
	// do not cover every subject family the publisher emits.
	ErrKindSubjectMismatch ErrKind = "subject_mismatch"
	// ErrKindUnavailable: transient infrastructure failure (timeout,
	// closed connection, cancelled context). Retrying is safe.
	ErrKindUnavailable ErrKind = "unavailable"
	// ErrKindInvalidConfig: the publisher's own configuration is invalid
	// (e.g. no stream prefixes) and no retry can fix it.
	ErrKindInvalidConfig ErrKind = "invalid_config"
)

// Error is the classified NATS publication/validation error surfaced by the
// natsadapter implementations of Validator and Publisher. The worker uses
// Kind for retry/dedup decisions and logs Guidance verbatim so operators get
// one actionable message instead of a repeated raw transport error.
type Error struct {
	// Op is the failing operation: "validate" or "publish".
	Op string
	// Kind classifies the failure.
	Kind ErrKind
	// Stream is the configured JetStream stream name (validate path).
	Stream string
	// Subject is the NATS subject involved (publish path).
	Subject string
	// Detail is the kind-specific machine context (configured subjects,
	// uncovered families, API error text).
	Detail string
	// Guidance is the actionable operator remediation.
	Guidance string
	// Err is the wrapped cause (may be nil for pure config errors).
	Err error
}

// Error renders a single-line, actionable message. Guidance is included so
// even a plain "%v" log of this error stays self-contained.
func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString("natspub ")
	if e.Op != "" {
		b.WriteString(e.Op)
		b.WriteString(": ")
	}
	b.WriteString(string(e.Kind))
	switch {
	case e.Stream != "" && e.Subject != "":
		fmt.Fprintf(&b, " (stream=%s subject=%s)", e.Stream, e.Subject)
	case e.Stream != "":
		fmt.Fprintf(&b, " (stream=%s)", e.Stream)
	case e.Subject != "":
		fmt.Fprintf(&b, " (subject=%s)", e.Subject)
	}
	if e.Detail != "" {
		b.WriteString(": ")
		b.WriteString(e.Detail)
	}
	if e.Err != nil {
		b.WriteString(": ")
		b.WriteString(e.Err.Error())
	}
	if e.Guidance != "" {
		b.WriteString(" — ")
		b.WriteString(e.Guidance)
	}
	return b.String()
}

// Unwrap exposes the underlying cause for errors.Is/errors.As chains.
func (e *Error) Unwrap() error { return e.Err }

// Retryable reports whether the failure is transient and the attempt may be
// repeated without operator action.
func (e *Error) Retryable() bool { return e.Kind == ErrKindUnavailable }

// Validator performs the read-only pre-subscription check that the
// configured JetStream stream exists and its subject filters cover every
// subject family this publisher emits. The concrete JetStream
// implementation lives in infrastructure/nats/natsadapter.
type Validator interface {
	// Validate MUST NOT create or mutate the stream; a failure is
	// returned as a *Error describing the blocking condition.
	Validate(ctx context.Context) error
}

// aggregateToken derives the NATS subject token for one KurrentDB stream
// prefix: the stream id is "<aggregateType>-<uuid>", the subject token is
// everything before the first '-' (mirroring subjectFor for the
// well-formed prefixes this validation receives — module declarations like
// "country-"). A leading dash yields an empty token, which is treated as
// invalid (skipped); well-formed prefixes never produce one.
func aggregateToken(streamPrefix string) string {
	if i := strings.Index(streamPrefix, "-"); i >= 0 {
		return streamPrefix[:i]
	}
	return streamPrefix
}

// familyPattern returns the emittable subject family for one aggregate
// token under subjectPrefix: "<prefix>.<agg>.>" — every subject with the
// prefix token, the aggregate token, and one or more trailing tokens.
func familyPattern(subjectPrefix, agg string) string {
	return subjectPrefix + "." + agg + ".>"
}

// FilterMatchesSubject reports whether a JetStream stream subject filter
// (possibly carrying NATS wildcards) matches one CONCRETE subject (no
// wildcards). NATS wildcard semantics: '*' matches exactly one token,
// '>' matches one or more trailing tokens and is only legal as the final
// filter token. A filter that ends before the subject (without a trailing
// '>') or that leaves the subject short does not match.
//
// This is the concrete-subject counterpart of filterCoversFamily (which
// computes wildcard-containment between two PATTERNS); the reporting
// contract relays use it to prove the contract stream covers every
// concrete emittable contract subject
// (yardflow.reporting.contracts.<family>.v<major>).
func FilterMatchesSubject(filter, subject string) bool {
	ft := strings.Split(filter, ".")
	st := strings.Split(subject, ".")
	for i := 0; ; i++ {
		fDone := i >= len(ft)
		sDone := i >= len(st)
		if fDone || sDone {
			// Only a filter that ends EXACTLY with the subject (both
			// consumed) matches; a filter that ended on '>' never
			// reaches here (it returns inside the loop).
			return fDone && sDone
		}
		switch f := ft[i]; f {
		case ">":
			// '>' must be the final filter token and consumes one or
			// more subject tokens: require at least one remaining.
			return i == len(ft)-1 && len(st)-i >= 1
		case "*":
			// Exactly one token.
			continue
		default:
			if f != st[i] {
				return false
			}
		}
	}
}

// filterCoversFamily reports whether every concrete subject matched by the
// family pattern is also matched by the stream subject filter. Both use
// NATS wildcard semantics: '*' matches exactly one token, '>' matches one
// or more trailing tokens. This is wildcard-pattern containment, not
// equality: "pfx.>" covers "pfx.country.>" while "pfx.*" does not (it
// matches only the single token "pfx.<x>", which the publisher never
// emits).
func filterCoversFamily(filter, family string) bool {
	ft := strings.Split(filter, ".")
	pt := strings.Split(family, ".")
	for i := 0; ; i++ {
		fDone := i >= len(ft)
		pDone := i >= len(pt)
		switch {
		case fDone && pDone:
			return true
		case fDone || pDone:
			// One pattern ran out of tokens: the longer one matches
			// subjects the shorter one cannot (no '>' involved — a
			// '>' would have returned below).
			return false
		}
		f, p := ft[i], pt[i]
		if f == ">" {
			// The filter's '>' absorbs every remaining family token,
			// including an unbounded family tail.
			return true
		}
		if p == ">" {
			// The family is unbounded from here; only a filter '>'
			// (handled above) covers arbitrarily deep subjects.
			return false
		}
		if f != "*" && f != p {
			return false
		}
	}
}

// ValidateSubjectCoverage checks that at least one configured stream
// subject filter covers EVERY emittable subject family implied by the
// KurrentDB stream prefixes (one family per aggregate token). It returns
// nil when fully covered, or a *Error of kind ErrKindSubjectMismatch whose
// Guidance names the correct wildcard form. An empty prefix list is an
// ErrKindInvalidConfig error (the publisher would have no events to
// publish, which is always a wiring mistake).
func ValidateSubjectCoverage(streamName string, streamSubjects []string, subjectPrefix string, streamPrefixes []string) *Error {
	aggregates := make([]string, 0, len(streamPrefixes))
	seen := make(map[string]struct{}, len(streamPrefixes))
	for _, p := range streamPrefixes {
		agg := aggregateToken(p)
		if agg == "" {
			continue
		}
		if _, dup := seen[agg]; dup {
			continue
		}
		seen[agg] = struct{}{}
		aggregates = append(aggregates, agg)
	}
	if len(aggregates) == 0 {
		return &Error{
			Op:       "validate",
			Kind:     ErrKindInvalidConfig,
			Stream:   streamName,
			Detail:   fmt.Sprintf("no non-empty KurrentDB stream prefixes configured (got %v)", streamPrefixes),
			Guidance: "configure the publisher with the module stream-prefix union (hosts allDomainStreamPrefixes)",
		}
	}

	var uncovered []string
	for _, agg := range aggregates {
		family := familyPattern(subjectPrefix, agg)
		covered := false
		for _, filter := range streamSubjects {
			if filterCoversFamily(filter, family) {
				covered = true
				break
			}
		}
		if !covered {
			uncovered = append(uncovered, family)
		}
	}
	if len(uncovered) == 0 {
		return nil
	}

	exampleSubject := subjectPrefix + "." + aggregates[0] + "." + aggregates[0] + ".registered"
	return &Error{
		Op:     "validate",
		Kind:   ErrKindSubjectMismatch,
		Stream: streamName,
		Detail: fmt.Sprintf("stream subjects %v do not cover %d of %d subject families (uncovered: %s)",
			streamSubjects, len(uncovered), len(aggregates), strings.Join(uncovered, ", ")),
		Guidance: fmt.Sprintf(
			"re-create or update the stream out-of-band with subjects '%s.>' (e.g. `nats stream update %s --subjects '%s.>'`); "+
				"'%s.*' matches exactly ONE token and publishes are '%s' (multiple tokens), so a '.*' filter matches none of them — verify with `nats stream info %s` (expected subjects like %s)",
			subjectPrefix, streamName, subjectPrefix,
			subjectPrefix, exampleSubject, streamName, exampleSubject),
	}
}
