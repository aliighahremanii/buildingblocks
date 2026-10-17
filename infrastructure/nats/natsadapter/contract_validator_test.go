package natsadapter

// contract_validator_test.go — unit coverage for the contract-stream
// validator: every classification path and the concrete-subject coverage
// check run without a server via the narrow streamInfoGetter fake.
//
// The subjects here are SYNTHETIC on purpose. This module must not depend on
// any product's reporting catalog, so the generic behaviour (namespace filter
// passes, exhaustive concrete subjects pass, narrower filters fail with the
// uncovered counts, transport failures classify) is pinned with a made-up
// contract namespace. A consumer that owns a real catalog asserts its own
// subjects against the same validator from its own test suite.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	natspub "github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/natspub"
)

// syntheticSubjects is a small stand-in for a product's emittable contract
// subjects: five concrete subjects across three families, two of them under
// the same family prefix so a narrower family filter covers only a subset.
var syntheticSubjects = []string{
	"acme.contracts.orders.v1",
	"acme.contracts.orders.item.v1",
	"acme.contracts.fleet.display.v1",
	"acme.contracts.masterdata.customer.v1",
	"acme.contracts.masterdata.customer-display.v1",
}

func testContractValidatorConfig() ContractStreamValidatorConfig {
	return ContractStreamValidatorConfig{
		StreamName: "acme-contracts",
		Subjects:   syntheticSubjects,
		URL:        "nats://test:4222",
		Timeout:    50 * time.Millisecond,
	}
}

func TestContractValidatorPassesNamespaceFilter(t *testing.T) {
	t.Parallel()

	getter := &fakeStreamInfoGetter{info: streamInfoWithSubjects("acme.contracts.>")}
	v := NewContractStreamValidator(nil, testContractValidatorConfig())
	v.js = getter

	if err := v.Validate(context.Background()); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if getter.calls != 1 {
		t.Fatalf("StreamInfo calls = %d, want exactly 1 (read-only single lookup)", getter.calls)
	}
}

func TestContractValidatorPassesExhaustiveConcreteSubjects(t *testing.T) {
	t.Parallel()

	// A stream listing every concrete contract subject explicitly also
	// covers everything the relays can emit.
	getter := &fakeStreamInfoGetter{info: streamInfoWithSubjects(syntheticSubjects...)}
	v := NewContractStreamValidator(nil, testContractValidatorConfig())
	v.js = getter

	if err := v.Validate(context.Background()); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestContractValidatorRejectsNarrowerFilter(t *testing.T) {
	t.Parallel()

	getter := &fakeStreamInfoGetter{info: streamInfoWithSubjects("acme.contracts.orders.>")}
	v := NewContractStreamValidator(nil, testContractValidatorConfig())
	v.js = getter

	err := v.Validate(context.Background())
	if err == nil {
		t.Fatal("expected subject_mismatch, got nil")
	}
	var ne *natspub.Error
	if !errors.As(err, &ne) {
		t.Fatalf("error must be a *natspub.Error, got %T", err)
	}
	if ne.Kind != natspub.ErrKindSubjectMismatch {
		t.Fatalf("Kind = %q, want %q", ne.Kind, natspub.ErrKindSubjectMismatch)
	}
	if ne.Stream != "acme-contracts" {
		t.Fatalf("Stream = %q", ne.Stream)
	}
	// Detail names the uncovered subjects and the counts: the orders-only
	// filter covers the two orders subjects, so the other three are
	// uncovered.
	for _, frag := range []string{"3 of 5", "acme.contracts.fleet.display.v1", "acme.contracts.masterdata.customer-display.v1"} {
		if !strings.Contains(ne.Detail, frag) {
			t.Fatalf("Detail should contain %q: %q", frag, ne.Detail)
		}
	}
	// Guidance names the actionable stream update command and the bootstrap
	// retention obligation.
	for _, frag := range []string{"stream update", "bootstrap window"} {
		if !strings.Contains(ne.Guidance, frag) {
			t.Fatalf("Guidance should contain %q: %q", frag, ne.Guidance)
		}
	}
}

func TestContractValidatorRejectsSingleTokenWildcard(t *testing.T) {
	t.Parallel()

	// The classic '.*' hazard: matches exactly ONE token and none of the
	// multi-token contract subjects.
	getter := &fakeStreamInfoGetter{info: streamInfoWithSubjects("acme.contracts.*")}
	v := NewContractStreamValidator(nil, testContractValidatorConfig())
	v.js = getter

	err := v.Validate(context.Background())
	if err == nil {
		t.Fatal("expected subject_mismatch, got nil")
	}
	var ne *natspub.Error
	if !errors.As(err, &ne) || ne.Kind != natspub.ErrKindSubjectMismatch {
		t.Fatalf("expected subject_mismatch, got %v", err)
	}
	if !strings.Contains(ne.Detail, "5 of 5") {
		t.Fatalf("all subjects uncovered: %q", ne.Detail)
	}
}

func TestContractValidatorClassifiesStreamNotFound(t *testing.T) {
	t.Parallel()

	getter := &fakeStreamInfoGetter{err: nats.ErrStreamNotFound}
	v := NewContractStreamValidator(nil, testContractValidatorConfig())
	v.js = getter

	err := v.Validate(context.Background())
	var ne *natspub.Error
	if !errors.As(err, &ne) || ne.Kind != natspub.ErrKindStreamNotFound {
		t.Fatalf("expected stream_not_found, got %v", err)
	}
	// Guidance: provision BEFORE relays publish + the retention window.
	for _, frag := range []string{"provision the contract stream out-of-band", "bootstrap window"} {
		if !strings.Contains(ne.Guidance, frag) {
			t.Fatalf("Guidance should contain %q: %q", frag, ne.Guidance)
		}
	}
}

func TestContractValidatorClassifiesNoResponders(t *testing.T) {
	t.Parallel()

	getter := &fakeStreamInfoGetter{err: nats.ErrNoResponders}
	v := NewContractStreamValidator(nil, testContractValidatorConfig())
	v.js = getter

	err := v.Validate(context.Background())
	var ne *natspub.Error
	if !errors.As(err, &ne) || ne.Kind != natspub.ErrKindNoResponders {
		t.Fatalf("expected no_responders, got %v", err)
	}
}

func TestContractValidatorClassifiesJetStreamNotEnabled(t *testing.T) {
	t.Parallel()

	getter := &fakeStreamInfoGetter{err: nats.ErrJetStreamNotEnabled}
	v := NewContractStreamValidator(nil, testContractValidatorConfig())
	v.js = getter

	err := v.Validate(context.Background())
	var ne *natspub.Error
	if !errors.As(err, &ne) || ne.Kind != natspub.ErrKindNoResponders {
		t.Fatalf("expected no_responders (jetstream not enabled), got %v", err)
	}
}

func TestContractValidatorClassifiesTransientFailures(t *testing.T) {
	t.Parallel()

	for _, cause := range []error{
		context.Canceled,
		context.DeadlineExceeded,
		nats.ErrTimeout,
		nats.ErrConnectionClosed,
		nats.ErrNoServers,
		errors.New("boom"),
	} {
		getter := &fakeStreamInfoGetter{err: cause}
		v := NewContractStreamValidator(nil, testContractValidatorConfig())
		v.js = getter
		err := v.Validate(context.Background())
		var ne *natspub.Error
		if !errors.As(err, &ne) || ne.Kind != natspub.ErrKindUnavailable || !ne.Retryable() {
			t.Fatalf("expected retryable unavailable for %v, got %v", cause, err)
		}
	}
}

func TestContractValidatorNilStreamInfoIsUnavailable(t *testing.T) {
	t.Parallel()

	getter := &fakeStreamInfoGetter{}
	v := NewContractStreamValidator(nil, testContractValidatorConfig())
	v.js = getter

	err := v.Validate(context.Background())
	var ne *natspub.Error
	if !errors.As(err, &ne) || ne.Kind != natspub.ErrKindUnavailable {
		t.Fatalf("expected unavailable for nil StreamInfo, got %v", err)
	}
}
