package natspub

// validation_test.go — unit coverage for the pure validation logic (D114):
// wildcard-pattern containment, subject-family coverage, and the classified
// error taxonomy. This is domain-style pure logic, so coverage must stay at
// 100%.

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestAggregateToken(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in, want string
	}{
		{"country-", "country"},
		{"bol-", "bol"},
		{"externalordernumber-", "externalordernumber"},
		{"country-uuid-suffix", "country"},
		{"nouuid", "nouuid"}, // no dash — used as-is (subjectFor parity)
		{"-leading", ""},     // leading dash → empty → invalid, skipped
		{"", ""},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			if got := aggregateToken(tc.in); got != tc.want {
				t.Fatalf("aggregateToken(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestFamilyPattern(t *testing.T) {
	t.Parallel()

	if got := familyPattern("yardflow", "country"); got != "yardflow.country.>" {
		t.Fatalf("familyPattern = %q, want %q", got, "yardflow.country.>")
	}
}

// TestFilterCoversFamily pins the wildcard-containment semantics — the
// exact predicate that turns a "<prefix>.*" stream into a validation
// failure while "<prefix.>" passes.
func TestFilterCoversFamily(t *testing.T) {
	t.Parallel()

	cases := []struct {
		filter, family string
		want           bool
	}{
		// The production-hazard forms (D114).
		{"yardflow.*", "yardflow.country.>", false}, // '*' = ONE token; publisher emits deeper subjects
		{"yardflow.*", "yardflow.country.>", false},
		{"yardflow.>", "yardflow.country.>", true}, // documented correct form
		{"yardflow.*.>", "yardflow.country.>", true},
		{">", "yardflow.country.>", true},
		{"*.country.>", "yardflow.country.>", true},

		// Family-scoped filters cover exactly their family.
		{"yardflow.country.>", "yardflow.country.>", true},
		{"yardflow.bol.>", "yardflow.country.>", false},
		{"yardflow.*.registered", "yardflow.country.>", false}, // literal tail cannot cover open tail
		{"yardflow.country.registered", "yardflow.country.>", false},

		// Strictness: a bounded-depth filter does not cover an unbounded
		// family, even when every current subject would match.
		{"yardflow.*.*.>", "yardflow.country.>", false},
		{"yardflow.*.*", "yardflow.country.>", false},

		// Literal mismatches at any token.
		{"other.>", "yardflow.country.>", false},
		{"yardflow.", "yardflow.country.>", false},
		{"yardflow", "yardflow.country.>", false},

		// Equality without wildcards.
		{"a.b", "a.b", true},
		{"a.*", "a.b", true},
		{"a.b.c", "a.b", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.filter+" ⊇ "+tc.family, func(t *testing.T) {
			t.Parallel()
			if got := filterCoversFamily(tc.filter, tc.family); got != tc.want {
				t.Fatalf("filterCoversFamily(%q, %q) = %v, want %v", tc.filter, tc.family, got, tc.want)
			}
		})
	}
}

func TestValidateSubjectCoverage(t *testing.T) {
	t.Parallel()

	prefixes := []string{"country-", "bol-", "truck-", "country-"} // duplicate entry must not double-count

	t.Run("covers everything with prefix family wildcard", func(t *testing.T) {
		t.Parallel()
		if err := ValidateSubjectCoverage("yardflow-events", []string{"yardflow.>"}, "yardflow", prefixes); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("covers with per-family wildcards", func(t *testing.T) {
		t.Parallel()
		if err := ValidateSubjectCoverage("yardflow-events",
			[]string{"yardflow.country.>", "yardflow.bol.>", "yardflow.truck.>"},
			"yardflow", prefixes); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("star filter fails with actionable guidance", func(t *testing.T) {
		t.Parallel()
		err := ValidateSubjectCoverage("yardflow-events", []string{"yardflow.*"}, "yardflow", prefixes)
		if err == nil {
			t.Fatal("expected subject_mismatch error, got nil")
		}
		if err.Kind != ErrKindSubjectMismatch {
			t.Fatalf("Kind = %q, want %q", err.Kind, ErrKindSubjectMismatch)
		}
		if err.Stream != "yardflow-events" {
			t.Fatalf("Stream = %q, want yardflow-events", err.Stream)
		}
		if !strings.Contains(err.Detail, "3 of 3") {
			t.Fatalf("Detail should count all families uncovered: %q", err.Detail)
		}
		if !strings.Contains(err.Detail, "yardflow.country.>") || !strings.Contains(err.Detail, "yardflow.bol.>") {
			t.Fatalf("Detail should name the uncovered families: %q", err.Detail)
		}
		for _, frag := range []string{"yardflow.>", "nats stream update", "exactly ONE token", "nats stream info"} {
			if !strings.Contains(err.Guidance, frag) {
				t.Fatalf("Guidance should contain %q: %q", frag, err.Guidance)
			}
		}
	})

	t.Run("partial coverage names only uncovered families", func(t *testing.T) {
		t.Parallel()
		err := ValidateSubjectCoverage("yardflow-events", []string{"yardflow.country.>"}, "yardflow", prefixes)
		if err == nil {
			t.Fatal("expected subject_mismatch error, got nil")
		}
		if !strings.Contains(err.Detail, "2 of 3") {
			t.Fatalf("Detail should count the uncovered families: %q", err.Detail)
		}
		// The configured subjects legitimately appear in the Detail; only
		// the UNCOVERED segment must exclude covered families.
		if idx := strings.Index(err.Detail, "(uncovered: "); idx >= 0 && strings.Contains(err.Detail[idx:], "yardflow.country.>") {
			t.Fatalf("covered family must not be listed as uncovered: %q", err.Detail)
		}
		if !strings.Contains(err.Detail, "yardflow.bol.>") || !strings.Contains(err.Detail, "yardflow.truck.>") {
			t.Fatalf("Detail should name the uncovered families: %q", err.Detail)
		}
	})

	t.Run("empty subject list fails", func(t *testing.T) {
		t.Parallel()
		err := ValidateSubjectCoverage("yardflow-events", nil, "yardflow", prefixes)
		if err == nil || err.Kind != ErrKindSubjectMismatch {
			t.Fatalf("expected subject_mismatch, got %v", err)
		}
	})

	t.Run("no stream prefixes is invalid config", func(t *testing.T) {
		t.Parallel()
		err := ValidateSubjectCoverage("yardflow-events", []string{"yardflow.>"}, "yardflow", nil)
		if err == nil || err.Kind != ErrKindInvalidConfig {
			t.Fatalf("expected invalid_config, got %v", err)
		}
		err = ValidateSubjectCoverage("yardflow-events", []string{"yardflow.>"}, "yardflow", []string{"", "-"})
		if err == nil || err.Kind != ErrKindInvalidConfig {
			t.Fatalf("expected invalid_config for empty aggregates, got %v", err)
		}
	})
}

// TestFilterMatchesSubject pins the concrete-subject matching semantics —
// the predicate the reporting contract relays' validation uses to prove a
// stream filter covers every concrete emittable contract subject.
func TestFilterMatchesSubject(t *testing.T) {
	t.Parallel()

	cases := []struct {
		filter, subject string
		want            bool
	}{
		// The contract namespace filter covers every family subject.
		{"yardflow.reporting.contracts.>", "yardflow.reporting.contracts.operations.order.v1", true},
		{"yardflow.reporting.contracts.>", "yardflow.reporting.contracts.fleet.display.v1", true},
		{"yardflow.reporting.contracts.>", "yardflow.reporting.contracts", false}, // '>' needs at least ONE token

		// Exact subjects listed explicitly on the stream also cover.
		{"yardflow.reporting.contracts.operations.order.v1", "yardflow.reporting.contracts.operations.order.v1", true},
		{"yardflow.reporting.contracts.operations.order.v1", "yardflow.reporting.contracts.fleet.display.v1", false},

		// Single-token wildcards.
		{"yardflow.reporting.contracts.*.v1", "yardflow.reporting.contracts.operations.order.v1", false}, // family spans TWO tokens
		{"yardflow.reporting.contracts.*.*.v1", "yardflow.reporting.contracts.operations.order.v1", true},
		{"yardflow.reporting.contracts.*.*.v1", "yardflow.reporting.contracts.fleet.assignment.v1", true},

		// A shorter wildcard namespace that stops one token early.
		{"yardflow.reporting.>", "yardflow.reporting.contracts.operations.order.v1", true},
		{"yardflow.contracts.>", "yardflow.reporting.contracts.operations.order.v1", false},

		// The classic '.*' hazard: exactly one token.
		{"yardflow.*", "yardflow.reporting.contracts.operations.order.v1", false},

		// Filter longer than subject / subject longer than filter.
		{"a.b.c.d.e", "a.b.c", false},
		{"a.b", "a.b.c", false},
		{"a.b", "a.b", true},

		// A non-final '>' token is not a legal NATS filter; treat as no
		// match rather than silently accepting.
		{"a.>.b", "a.x.b", false},

		// '>' at the end with nothing following does not match.
		{"a.b.>", "a.b", false},
		{"a.b.>", "a.b.c", true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.filter+" ⊇ "+tc.subject, func(t *testing.T) {
			t.Parallel()
			if got := FilterMatchesSubject(tc.filter, tc.subject); got != tc.want {
				t.Fatalf("FilterMatchesSubject(%q, %q) = %v, want %v", tc.filter, tc.subject, got, tc.want)
			}
		})
	}
}

// TestErrorRenderingAndClassification pins the Error contract: message
// completeness, unwrapping, and retryability per kind.
func TestErrorRenderingAndClassification(t *testing.T) {
	t.Parallel()

	cause := errors.New("nats: no responders available for request")
	e := &Error{
		Op:       "validate",
		Kind:     ErrKindNoResponders,
		Stream:   "yardflow-events",
		Detail:   "JetStream API had no responder",
		Guidance: "check -js",
		Err:      cause,
	}
	msg := e.Error()
	for _, frag := range []string{"natspub validate", "no_responders", "stream=yardflow-events", "JetStream API had no responder", cause.Error(), "check -js"} {
		if !strings.Contains(msg, frag) {
			t.Fatalf("Error() should contain %q: %q", frag, msg)
		}
	}
	if !errors.Is(e, cause) {
		t.Fatal("errors.Is should reach the wrapped cause")
	}
	if e.Retryable() {
		t.Fatal("no_responders is not retryable-without-action")
	}

	minimal := &Error{Op: "publish", Kind: ErrKindUnavailable, Subject: "yardflow.bol.bol.registered"}
	if minimal.Retryable() != true {
		t.Fatal("unavailable must be retryable")
	}
	if got := minimal.Error(); !strings.Contains(got, "subject=yardflow.bol.bol.registered") {
		t.Fatalf("Error() should carry the subject: %q", got)
	}

	bare := &Error{Kind: ErrKindInvalidConfig}
	if got := bare.Error(); got != "natspub invalid_config" {
		t.Fatalf("bare Error() = %q", got)
	}
}

// TestValidatorInterfaceConcurrency is a compile/behavior smoke test that
// the Validator interface is implementable by an external fake and that a
// nil validator is distinguishable (worker skips validation).
func TestValidatorInterface(t *testing.T) {
	t.Parallel()

	var v Validator = &fakeValidator{err: nil}
	if err := v.Validate(context.Background()); err != nil {
		t.Fatalf("nil-error validator returned %v", err)
	}
	v = &fakeValidator{err: &Error{Kind: ErrKindStreamNotFound}}
	if err := v.Validate(context.Background()); err == nil {
		t.Fatal("erroring validator returned nil")
	}
}

type fakeValidator struct {
	err   error
	calls int
}

func (f *fakeValidator) Validate(_ context.Context) error {
	f.calls++
	return f.err
}
