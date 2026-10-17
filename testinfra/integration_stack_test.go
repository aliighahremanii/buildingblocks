package testinfra

import (
	"regexp"
	"strings"
	"testing"
)

// composeServiceNames lists the services defined in a Compose file.
func composeServiceNames(compose string) []string {
	var names []string
	inServices := false
	for _, raw := range strings.Split(compose, "\n") {
		trimmed := strings.TrimSpace(raw)
		if !strings.HasPrefix(raw, " ") && trimmed != "" {
			inServices = trimmed == "services:"
			continue
		}
		if !inServices {
			continue
		}
		if m := regexp.MustCompile(`^  ([a-zA-Z0-9_-]+):$`).FindStringSubmatch(raw); m != nil {
			names = append(names, m[1])
		}
	}
	return names
}

func composeServiceBlock(t *testing.T, compose, name string) string {
	t.Helper()
	var captured []string
	capturing := false
	for _, raw := range strings.Split(compose, "\n") {
		switch {
		case raw == "  "+name+":":
			capturing = true
			captured = append(captured, raw)
		case capturing && (strings.HasPrefix(raw, "    ") || strings.HasPrefix(raw, "\t") || strings.TrimSpace(raw) == ""):
			captured = append(captured, raw)
		case capturing:
			return strings.Join(captured, "\n")
		}
	}
	if !capturing {
		t.Fatalf("compose file does not define service %q", name)
	}
	return strings.Join(captured, "\n")
}

// TestIntegrationStackProvidesEveryRequiredDatastore pins the stack the live
// suites need. Each datastore is a PUBLIC image pinned to a concrete tag (the
// CI runner has no private-registry access), publishes its standard port, and
// keeps its data on tmpfs so a repeat run cannot inherit a previous run's
// streams, indexes or rows. The `migrate` service exists because the suites
// read tables created by the module's own shared migration SQL — without it
// every Postgres-backed test fails on "relation does not exist".
func TestIntegrationStackProvidesEveryRequiredDatastore(t *testing.T) {
	t.Parallel()

	compose := readRepoFile(t, "docker-compose.integration.yml")

	services := composeServiceNames(compose)
	want := []string{"postgres", "kurrentdb", "meilisearch", "nats", "migrate"}
	for _, name := range want {
		if !containsString(services, name) {
			t.Errorf("docker-compose.integration.yml must define service %q; got %v", name, services)
		}
	}

	// Public, pinned images: no private registry host and no floating tag.
	for _, name := range want {
		block := composeServiceBlock(t, compose, name)
		image := regexp.MustCompile(`(?m)^    image:\s*(\S+)\s*$`).FindStringSubmatch(block)
		if image == nil {
			t.Errorf("service %q must pin an image explicitly:\n%s", name, block)
			continue
		}
		ref := image[1]
		if strings.Contains(ref, "git.sabasystem.app") {
			t.Errorf("service %q must use a PUBLIC image (CI runs on GitHub-hosted runners): %s", name, ref)
		}
		if !strings.Contains(ref, ":") || strings.HasSuffix(ref, ":latest") {
			t.Errorf("service %q must pin a concrete tag, not a floating one: %s", name, ref)
		}
	}

	// Host-published ports, because the suite runs on the host.
	for _, want := range []string{`"5432:5432"`, `"2113:2113"`, `"7700:7700"`, `"4222:4222"`, `"8222:8222"`} {
		if !strings.Contains(compose, want) {
			t.Errorf("docker-compose.integration.yml must publish %s for the host-run suite", want)
		}
	}

	// Ephemeral data dirs: no cross-run state.
	for _, name := range []string{"postgres", "kurrentdb", "nats"} {
		block := composeServiceBlock(t, compose, name)
		if !strings.Contains(block, "tmpfs:") {
			t.Errorf("service %q must keep its data on tmpfs so repeat runs start clean:\n%s", name, block)
		}
	}

	// Readiness must be observable for the services that can report it.
	for _, name := range []string{"postgres", "kurrentdb", "meilisearch"} {
		block := composeServiceBlock(t, compose, name)
		if !strings.Contains(block, "healthcheck:") {
			t.Errorf("service %q must define a healthcheck (readiness is a gate, not a sleep):\n%s", name, block)
		}
	}

	// The migration source is the module's real SQL, applied by the real
	// runner, and a failed migration fails the stack.
	migrate := composeServiceBlock(t, compose, "migrate")
	for _, want := range []string{
		"migrate/migrate:v4.19.1",
		"./infrastructure/postgres/shared/migrations:/migrations:ro",
		`"-path=/migrations"`,
		`"up"`,
		"condition: service_healthy",
	} {
		if !strings.Contains(migrate, want) {
			t.Errorf("migrate service must contain %q:\n%s", want, migrate)
		}
	}
}

// TestIntegrationScriptIsTheSingleGateEntryPoint pins the script's contract:
// it brings the stack up, waits for REAL readiness, applies migrations, runs
// the whole suite with the require-integration switch, and tears down — and it
// leaves Go's package parallelism at its default, because the suite's
// shared-Postgres concurrency contract (a cross-process advisory lock) is
// exactly what `-p 1 -parallel 1` would hide.
func TestIntegrationScriptIsTheSingleGateEntryPoint(t *testing.T) {
	t.Parallel()

	script := readRepoFile(t, "script/integration-test.sh")

	for _, want := range []struct{ needle, why string }{
		{"set -euo pipefail", "must fail fast"},
		{"trap cleanup EXIT", "must tear the stack down even when the suite fails"},
		{"compose up -d postgres kurrentdb meilisearch nats", "must start every datastore"},
		{"pg_isready", "must probe Postgres readiness"},
		{"http://127.0.0.1:2113/health/live", "must probe KurrentDB readiness"},
		{"http://127.0.0.1:7700/health", "must probe Meilisearch readiness"},
		{"http://127.0.0.1:8222/healthz", "must probe NATS readiness"},
		{"compose run --rm --no-deps migrate", "must apply the shared migrations"},
		{"go test ./... -count=1", "must run the whole suite"},
		{"down --remove-orphans --volumes", "must remove its own containers and volumes"},
	} {
		if !strings.Contains(script, want.needle) {
			t.Errorf("script/integration-test.sh %s (%q missing)", want.why, want.needle)
		}
	}

	// The switch is what makes a skip a failure; it must be the SAME literal
	// the module's testenv package reads.
	testenv := readRepoFile(t, "infrastructure/testenv/testenv.go")
	if !strings.Contains(testenv, `RequireIntegrationEnv = "BUILDINGBLOCKS_REQUIRE_INTEGRATION"`) {
		t.Fatalf("infrastructure/testenv must define the switch name; source:\n%s", testenv)
	}
	if !strings.Contains(script, "BUILDINGBLOCKS_REQUIRE_INTEGRATION=1") {
		t.Errorf("script/integration-test.sh must export BUILDINGBLOCKS_REQUIRE_INTEGRATION=1 so a datastore-gated skip FAILS")
	}

	// The datastore env var names must be exactly what the module reads.
	for _, variable := range []struct{ name, source string }{
		{"POSTGRES_DSN", "infrastructure/postgres/config.go"},
		{"KURRENTDB_URL", "infrastructure/eventstoredb/config.go"},
		{"MEILISEARCH_URL", "infrastructure/meilisearch/client.go"},
		{"NATS_URL", "infrastructure/nats/natsadapter/validator.go"},
	} {
		if !strings.Contains(readRepoFile(t, variable.source), variable.name) {
			t.Errorf("%s no longer reads %s — update the integration stack and this pin together", variable.source, variable.name)
		}
		if !strings.Contains(script, variable.name) {
			t.Errorf("script/integration-test.sh must export %s (the name %s reads)", variable.name, variable.source)
		}
	}

	// Default parallelism is part of the contract. Comments are stripped
	// first: the header explains WHY `-p 1 -parallel 1` is forbidden, and that
	// prose must not trip the check.
	scriptCode := stripComments(script)
	for _, forbidden := range []string{"-p 1", "-parallel 1", "-p=1", "-parallel=1"} {
		if strings.Contains(scriptCode, forbidden) {
			t.Errorf("script/integration-test.sh must not pass %s — the suite is written for Go's default package parallelism against one shared Postgres", forbidden)
		}
	}

	// A stack that never becomes ready must fail, not proceed.
	if !strings.Contains(script, "did not become ready") || !strings.Contains(script, "return 1") {
		t.Errorf("script/integration-test.sh must FAIL when a datastore never becomes ready")
	}
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
