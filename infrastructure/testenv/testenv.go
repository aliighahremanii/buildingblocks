// Package testenv centralizes this module's live-integration test policy.
//
// The datastore-backed suites here auto-SKIP when their environment variables
// are unset, so a bare `go test ./...` stays green on a machine with no
// Postgres, KurrentDB, Meilisearch or NATS. That default is convenient
// locally, and WRONG in a job that provisions those services: a skip there is
// a silent pass, and the gate would be green while the live suites never ran.
//
// Setting BUILDINGBLOCKS_REQUIRE_INTEGRATION turns every environment-gated
// skip in this module into a FAILURE, so the integration job that provisions
// the services cannot pass without actually exercising them. The switch is
// deliberately opt-in — it is set by the workflow that owns the services, not
// by default — because a developer running `go test ./...` without a stack
// must keep getting skips instead of failures.
//
// Only environment-gated skips consult this package. A deliberate
// coverage-delegation skip (a test whose live sibling owns the behavior) does
// not: that is a statement about where the coverage lives, not a missing
// dependency, and it must keep skipping.
package testenv

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// RequireIntegrationEnv names the environment variable that turns
// datastore-gated skips into failures. It is enabled by any value other than
// the empty string and the explicit off-values ("0", "false", "no").
const RequireIntegrationEnv = "BUILDINGBLOCKS_REQUIRE_INTEGRATION"

// Required reports whether this process must run the live integration suites.
func Required() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(RequireIntegrationEnv))) {
	case "", "0", "false", "no":
		return false
	default:
		return true
	}
}

// SkipOrFail skips t with the given reason when integration tests are
// optional, and FAILS t with the same reason when Required() is true. The
// reason names the missing dependency, which is exactly what a job that
// provisioned that dependency must not tolerate.
//
// Both branches terminate the test (t.Skip and t.Fatalf both call
// runtime.Goexit), so a caller may treat the call as non-returning.
func SkipOrFail(t testing.TB, format string, args ...any) {
	t.Helper()

	reason := fmt.Sprintf(format, args...)
	if Required() {
		t.Fatalf("%s — integration tests are REQUIRED in this run (%s is set), so a missing datastore is a failure, not a skip", reason, RequireIntegrationEnv)
	}
	t.Skip(reason)
}
