package natsadapter

// validator_test.go — unit coverage for the read-only JetStream stream
// validator's error classification (D114). The narrow streamInfoGetter
// interface lets every classification path run without a server; the
// live behavior (including the no-responder publish failure mode) is
// covered by validator_live_test.go against a real NATS.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	natspub "github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/natspub"
)

// fakeStreamInfoGetter fakes the single JetStream call the validator
// makes. It also proves the validator performs no other JetStream
// operation: any additional method would have to be added to the fake and
// would fail to compile here without a matching justification.
type fakeStreamInfoGetter struct {
	calls int
	info  *nats.StreamInfo
	err   error
}

func (f *fakeStreamInfoGetter) StreamInfo(_ string, _ ...nats.JSOpt) (*nats.StreamInfo, error) {
	f.calls++
	return f.info, f.err
}

func testValidatorConfig() StreamValidatorConfig {
	return StreamValidatorConfig{
		StreamName:     "yardflow-events",
		SubjectPrefix:  "yardflow",
		StreamPrefixes: []string{"country-", "bol-"},
		URL:            "nats://test:4222",
		Timeout:        50 * time.Millisecond,
	}
}

func streamInfoWithSubjects(subjects ...string) *nats.StreamInfo {
	return &nats.StreamInfo{
		Config: nats.StreamConfig{
			Name:     "yardflow-events",
			Subjects: subjects,
		},
	}
}

func TestStreamValidatorPassesCoveringStream(t *testing.T) {
	t.Parallel()

	getter := &fakeStreamInfoGetter{info: streamInfoWithSubjects("yardflow.>")}
	v := NewStreamValidator(nil, testValidatorConfig())
	v.js = getter

	if err := v.Validate(context.Background()); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if getter.calls != 1 {
		t.Fatalf("StreamInfo calls = %d, want exactly 1 (read-only single lookup)", getter.calls)
	}
}

func TestStreamValidatorPassesPerFamilySubjects(t *testing.T) {
	t.Parallel()

	getter := &fakeStreamInfoGetter{info: streamInfoWithSubjects("yardflow.country.>", "yardflow.bol.>")}
	v := NewStreamValidator(nil, testValidatorConfig())
	v.js = getter

	if err := v.Validate(context.Background()); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

// TestStreamValidatorRejectsStarSubjects is the unit pin of the
// production-cause hypothesis (D114): a stream configured with the
// formerly documented "yardflow.*" covers NONE of the published subjects
// and must block the KurrentDB subscription with actionable guidance.
func TestStreamValidatorRejectsStarSubjects(t *testing.T) {
	t.Parallel()

	getter := &fakeStreamInfoGetter{info: streamInfoWithSubjects("yardflow.*")}
	v := NewStreamValidator(nil, testValidatorConfig())
	v.js = getter

	err := v.Validate(context.Background())
	var ne *natspub.Error
	if !errors.As(err, &ne) {
		t.Fatalf("expected *natspub.Error, got %T: %v", err, err)
	}
	if ne.Kind != natspub.ErrKindSubjectMismatch {
		t.Fatalf("Kind = %q, want subject_mismatch", ne.Kind)
	}
	if ne.Stream != "yardflow-events" {
		t.Fatalf("Stream = %q, want yardflow-events", ne.Stream)
	}
	for _, frag := range []string{"yardflow.>", "nats stream update", "exactly ONE token"} {
		if !strings.Contains(ne.Guidance, frag) {
			t.Fatalf("Guidance must contain %q: %q", frag, ne.Guidance)
		}
	}
	if ne.Retryable() {
		t.Fatal("subject_mismatch must not be classified retryable")
	}
}

func TestStreamValidatorClassifiesErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		err      error
		wantKind natspub.ErrKind
		guidance string
	}{
		{
			name:     "no responders means JetStream unavailable",
			err:      nats.ErrNoResponders,
			wantKind: natspub.ErrKindNoResponders,
			guidance: "JetStream appears unavailable",
		},
		{
			name:     "stream not found means out-of-band creation needed",
			err:      nats.ErrStreamNotFound,
			wantKind: natspub.ErrKindStreamNotFound,
			guidance: "nats -s nats://test:4222 stream add yardflow-events",
		},
		{
			name:     "server jetstream not enabled",
			err:      nats.ErrJetStreamNotEnabled,
			wantKind: natspub.ErrKindNoResponders,
			guidance: "enable JetStream on the server",
		},
		{
			name:     "account jetstream not enabled",
			err:      nats.ErrJetStreamNotEnabledForAccount,
			wantKind: natspub.ErrKindNoResponders,
			guidance: "enable JetStream",
		},
		{
			name:     "request timeout is transient",
			err:      nats.ErrTimeout,
			wantKind: natspub.ErrKindUnavailable,
			guidance: "retrying with backoff",
		},
		{
			name:     "context cancelled is transient",
			err:      context.Canceled,
			wantKind: natspub.ErrKindUnavailable,
			guidance: "retrying with backoff",
		},
		{
			name:     "context deadline is transient",
			err:      context.DeadlineExceeded,
			wantKind: natspub.ErrKindUnavailable,
			guidance: "retrying with backoff",
		},
		{
			name:     "connection closed is transient",
			err:      nats.ErrConnectionClosed,
			wantKind: natspub.ErrKindUnavailable,
			guidance: "retrying with backoff",
		},
		{
			name:     "no servers is transient",
			err:      nats.ErrNoServers,
			wantKind: natspub.ErrKindUnavailable,
			guidance: "verify NATS reachability",
		},
		{
			name:     "unknown errors classify as retryable unavailability",
			err:      errors.New("mystery failure"),
			wantKind: natspub.ErrKindUnavailable,
			guidance: "nats stream info",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			v := NewStreamValidator(nil, testValidatorConfig())
			v.js = &fakeStreamInfoGetter{err: tc.err}

			err := v.Validate(context.Background())
			var ne *natspub.Error
			if !errors.As(err, &ne) {
				t.Fatalf("expected *natspub.Error, got %T: %v", err, err)
			}
			if ne.Kind != tc.wantKind {
				t.Fatalf("Kind = %q, want %q", ne.Kind, tc.wantKind)
			}
			if !strings.Contains(ne.Guidance, tc.guidance) {
				t.Fatalf("Guidance must contain %q: %q", tc.guidance, ne.Guidance)
			}
			if ne.Retryable() != (tc.wantKind == natspub.ErrKindUnavailable) {
				t.Fatalf("Retryable = %v for kind %q", ne.Retryable(), ne.Kind)
			}
			if !errors.Is(err, tc.err) {
				t.Fatalf("errors.Is must reach the wrapped cause %v", tc.err)
			}
		})
	}
}

// TestStreamValidatorNilInfoIsUnavailable guards the degenerate server
// response path.
func TestStreamValidatorNilInfoIsUnavailable(t *testing.T) {
	t.Parallel()

	v := NewStreamValidator(nil, testValidatorConfig())
	v.js = &fakeStreamInfoGetter{info: nil}

	err := v.Validate(context.Background())
	var ne *natspub.Error
	if !errors.As(err, &ne) || ne.Kind != natspub.ErrKindUnavailable {
		t.Fatalf("expected unavailable classification, got %v", err)
	}
}

// TestStreamValidatorDefaultTimeout pins the fallback used when Timeout
// is unset (the host passes an explicit 5s; zero must still behave).
func TestStreamValidatorDefaultTimeout(t *testing.T) {
	t.Parallel()

	cfg := testValidatorConfig()
	cfg.Timeout = 0
	v := NewStreamValidator(nil, cfg)
	v.js = &fakeStreamInfoGetter{info: streamInfoWithSubjects("yardflow.>")}

	if err := v.Validate(context.Background()); err != nil {
		t.Fatalf("expected nil with default timeout, got %v", err)
	}
	if v.cfg.Timeout != 0 || defaultStreamInfoTimeout != 5*time.Second {
		t.Fatalf("default timeout contract changed: %s", defaultStreamInfoTimeout)
	}
}

// TestStreamValidatorInvalidPrefixUnion guards the host wiring contract:
// an empty prefix union is a wiring mistake, surfaced as invalid_config
// rather than a silent pass.
func TestStreamValidatorInvalidPrefixUnion(t *testing.T) {
	t.Parallel()

	cfg := testValidatorConfig()
	cfg.StreamPrefixes = nil
	v := NewStreamValidator(nil, cfg)
	v.js = &fakeStreamInfoGetter{info: streamInfoWithSubjects("yardflow.>")}

	err := v.Validate(context.Background())
	var ne *natspub.Error
	if !errors.As(err, &ne) || ne.Kind != natspub.ErrKindInvalidConfig {
		t.Fatalf("expected invalid_config, got %v", err)
	}
}

// TestStreamValidatorReadsConfiguredStreamName ensures the lookup targets
// the configured stream (a wrong-name validation would silently bless a
// different stream).
func TestStreamValidatorReadsConfiguredStreamName(t *testing.T) {
	t.Parallel()

	getter := &fakeStreamInfoGetter{info: streamInfoWithSubjects("yardflow.>")}
	cfg := testValidatorConfig()
	cfg.StreamName = "other-stream"
	v := NewStreamValidator(nil, cfg)
	v.js = getter

	if err := v.Validate(context.Background()); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if getter.info.Config.Name != "yardflow-events" {
		// The fake ignores the name argument; assert via a name-capturing
		// fake instead.
		t.Log("fake does not capture the requested name; covered by nameCapturingGettter")
	}

	nameGetter := &nameCapturingGetter{info: streamInfoWithSubjects("yardflow.>")}
	v.js = nameGetter
	if err := v.Validate(context.Background()); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if nameGetter.stream != "other-stream" {
		t.Fatalf("StreamInfo requested %q, want other-stream", nameGetter.stream)
	}
}

type nameCapturingGetter struct {
	stream string
	info   *nats.StreamInfo
}

func (n *nameCapturingGetter) StreamInfo(stream string, _ ...nats.JSOpt) (*nats.StreamInfo, error) {
	n.stream = stream
	return n.info, nil
}
