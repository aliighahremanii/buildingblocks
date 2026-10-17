package natsadapter

// validator_live_test.go — live coverage against a real NATS JetStream
// server (D114), gated on NATS_URL (auto-skip otherwise, per repo test
// policy; FAILS instead when the run declares integration tests required —
// BUILDINGBLOCKS_REQUIRE_INTEGRATION). Every test owns its fixture end to
// end: a uniquely named
// test-owned stream (created and deleted by the test — the PRODUCTION
// validator never mutates streams; only the test harness does) and
// unique subjects. The shared `yardflow-events` stream, if present, is
// never touched.
//
// Live evidence pinned here mirrors the production failure hypothesis:
// a stream configured with "prefix.*" subjects covers none of the
// published subjects (validation must reject it), a missing stream is
// stream_not_found, and publishing to an uncovered subject fails with
// the same ErrNoResponders class the production publisher would see —
// classified with actionable guidance.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	natspub "github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/natspub"
	"github.com/aliighahremanii/buildingblocks/infrastructure/testenv"
)

// liveEnvLoaded ensures .env is read only once per process (mirrors
// kurrenttest).
var liveEnvLoaded bool

func loadLiveDotEnv(t *testing.T) {
	t.Helper()
	if liveEnvLoaded {
		return
	}
	liveEnvLoaded = true
	dir, err := os.Getwd()
	if err != nil {
		return
	}
	for i := 0; i < 12; i++ {
		envPath := filepath.Join(dir, ".env")
		if data, err := os.ReadFile(envPath); err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				line = strings.TrimSpace(line)
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}
				key, value, ok := strings.Cut(line, "=")
				if !ok {
					continue
				}
				key = strings.TrimSpace(key)
				if os.Getenv(key) == "" {
					_ = os.Setenv(key, strings.TrimSpace(strings.Trim(strings.TrimSpace(value), `"'`)))
				}
			}
			return
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return
		}
		dir = parent
	}
}

// openLiveNATS connects to NATS_URL with a bounded retry dial (the CI
// compose starts NATS with service_started semantics — no healthcheck on
// the scratch image — so the first dial may race server boot). Skips
// (never fails) when NATS_URL is unset or the server stays unreachable;
// when integration tests are REQUIRED (BUILDINGBLOCKS_REQUIRE_INTEGRATION)
// both conditions FAIL instead, so a job that provisioned NATS cannot pass
// on a skip.
func openLiveNATS(t *testing.T) nats.JetStreamContext {
	t.Helper()

	loadLiveDotEnv(t)
	url := os.Getenv("NATS_URL")
	if url == "" {
		testenv.SkipOrFail(t, "NATS integration test skipped: NATS_URL not set")
	}

	var js nats.JetStreamContext
	deadline := time.Now().Add(15 * time.Second)
	for {
		nc, err := nats.Connect(url,
			nats.Name("yardflow-natsadapter-live-test"),
			nats.Timeout(2*time.Second),
		)
		if err == nil {
			t.Cleanup(func() { nc.Close() })
			var jsErr error
			js, jsErr = nc.JetStream()
			if jsErr != nil {
				t.Fatalf("jetstream context: %v", jsErr)
			}
			return js
		}
		if time.Now().After(deadline) {
			testenv.SkipOrFail(t, "NATS integration test skipped: server at %s unreachable: %v", url, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// ensureLiveStream creates a uniquely named test-owned stream and
// registers ownership-aware cleanup (delete) — the ONLY mutation in this
// file, performed by the test harness, never by production code.
func ensureLiveStream(t *testing.T, js nats.JetStreamContext, name string, subjects ...string) {
	t.Helper()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     name,
		Subjects: subjects,
		Storage:  nats.MemoryStorage,
	}, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("add test stream %s: %v", name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = js.DeleteStream(name, nats.Context(ctx))
	})
}

func liveValidatorConfig(prefix, stream string) StreamValidatorConfig {
	return StreamValidatorConfig{
		StreamName:     stream,
		SubjectPrefix:  prefix,
		StreamPrefixes: []string{"country-", "bol-"},
		URL:            os.Getenv("NATS_URL"),
		Timeout:        3 * time.Second,
	}
}

// TestLiveStreamValidatorAcceptsFamilyWildcard proves the documented
// correct configuration validates green against a real server.
func TestLiveStreamValidatorAcceptsFamilyWildcard(t *testing.T) {
	js := openLiveNATS(t)

	prefix := "yftest-" + uuid.NewString()[:8]
	stream := prefix + "-events"
	ensureLiveStream(t, js, stream, prefix+".>")

	v := NewStreamValidator(js, liveValidatorConfig(prefix, stream))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := v.Validate(ctx); err != nil {
		t.Fatalf("expected validation to pass on stream with subjects %s.>: %v", prefix, err)
	}
}

// TestLiveStreamValidatorRejectsStarSubjects reproduces the production
// failure hypothesis live: a stream created with the formerly documented
// "prefix.*" subjects must be rejected with subject_mismatch and
// prefix.> guidance.
func TestLiveStreamValidatorRejectsStarSubjects(t *testing.T) {
	js := openLiveNATS(t)

	prefix := "yftest-" + uuid.NewString()[:8]
	stream := prefix + "-events"
	ensureLiveStream(t, js, stream, prefix+".*")

	v := NewStreamValidator(js, liveValidatorConfig(prefix, stream))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := v.Validate(ctx)
	var ne *natspub.Error
	if !errors.As(err, &ne) {
		t.Fatalf("expected *natspub.Error, got %T: %v", err, err)
	}
	if ne.Kind != natspub.ErrKindSubjectMismatch {
		t.Fatalf("Kind = %q, want subject_mismatch", ne.Kind)
	}
	if !strings.Contains(ne.Guidance, prefix+".>") {
		t.Fatalf("Guidance must name %s.>: %q", prefix, ne.Guidance)
	}
	if !strings.Contains(ne.Detail, prefix+".*") {
		t.Fatalf("Detail must name the configured star subjects: %q", ne.Detail)
	}
}

// TestLiveStreamValidatorClassifiesMissingStream proves the missing-stream
// classification against a real server answering the API request.
func TestLiveStreamValidatorClassifiesMissingStream(t *testing.T) {
	js := openLiveNATS(t)

	prefix := "yftest-" + uuid.NewString()[:8]
	stream := prefix + "-missing" // never created

	v := NewStreamValidator(js, liveValidatorConfig(prefix, stream))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := v.Validate(ctx)
	var ne *natspub.Error
	if !errors.As(err, &ne) {
		t.Fatalf("expected *natspub.Error, got %T: %v", err, err)
	}
	if ne.Kind != natspub.ErrKindStreamNotFound {
		t.Fatalf("Kind = %q, want stream_not_found", ne.Kind)
	}
	if !strings.Contains(ne.Guidance, "stream add "+stream) {
		t.Fatalf("Guidance must name the create command: %q", ne.Guidance)
	}
}

// TestLivePublishNoRespondersIsClassified proves the production publish
// failure mode end to end: publishing to a subject no stream covers
// returns nats.ErrNoResponders from the real server, and the adapter
// classifies it as no_responders with subject-family guidance.
func TestLivePublishNoRespondersIsClassified(t *testing.T) {
	js := openLiveNATS(t)

	prefix := "yftest-" + uuid.NewString()[:8]
	// NO stream covers this family — the uncovered-subject condition.
	p := New(js, 3*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	subject := fmt.Sprintf("%s.bol.bol.registered", prefix)
	err := p.PublishAsync(ctx, subject, []byte(`{"live":true}`), uuid.NewString())
	var ne *natspub.Error
	if !errors.As(err, &ne) {
		t.Fatalf("expected *natspub.Error, got %T: %v", err, err)
	}
	if ne.Kind != natspub.ErrKindNoResponders {
		t.Fatalf("Kind = %q, want no_responders (live server must reject the uncovered subject)", ne.Kind)
	}
	if !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("errors.Is must reach nats.ErrNoResponders, got %v", err)
	}
	if !strings.Contains(ne.Guidance, "<prefix>.>") {
		t.Fatalf("Guidance must carry the prefix.> remediation: %q", ne.Guidance)
	}
}

// TestLivePublishCoveredSubjectIsDurable proves the happy path against the
// real server: the publish waits for its ack, the message is stored on
// the stream, and dedup by Nats-Msg-Id does not double-store on replay.
func TestLivePublishCoveredSubjectIsDurable(t *testing.T) {
	js := openLiveNATS(t)

	prefix := "yftest-" + uuid.NewString()[:8]
	stream := prefix + "-events"
	ensureLiveStream(t, js, stream, prefix+".>")

	p := New(js, 3*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	subject := fmt.Sprintf("%s.country.country.registered", prefix)
	msgID := uuid.NewString()
	payload := []byte(fmt.Sprintf(`{"id":%q}`, msgID))

	if err := p.PublishAsync(ctx, subject, payload, msgID); err != nil {
		t.Fatalf("publish to covered subject failed: %v", err)
	}
	// Replay with the same Nats-Msg-Id within the duplicate window must
	// be accepted (deduped) — the crash-replay no-loss contract.
	if err := p.PublishAsync(ctx, subject, payload, msgID); err != nil {
		t.Fatalf("replayed publish failed: %v", err)
	}

	ictx, icancel := context.WithTimeout(ctx, 3*time.Second)
	defer icancel()
	info, err := js.StreamInfo(stream, nats.Context(ictx))
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("stream messages = %d, want 1 (deduped replay must not double-store)", info.State.Msgs)
	}
}
