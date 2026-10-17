package testinfra

import (
	"strings"
	"testing"
)

// TestLibraryCIDoesNotConsumeRepositorySecrets pins the property that makes
// this repository's CI safe to run on any revision: it needs NO repository
// secret at all. The module's dependencies are public, every integration
// image is a public Docker Hub image, and the datastores run locally in the
// job — so there is no registry credential, no private-module credential, and
// nothing to exfiltrate through a pull request.
func TestLibraryCIDoesNotConsumeRepositorySecrets(t *testing.T) {
	t.Parallel()

	wf := stripComments(readRepoFile(t, ".github/workflows/ci.yml"))
	if strings.Contains(wf, "secrets.") {
		t.Errorf("ci.yml must not reference `secrets.` — this repository's CI needs no credential (public module, public images, local datastores)")
	}
	if env := topLevelBlock(wf, "env"); strings.Contains(env, "secrets.") {
		t.Errorf("ci.yml must not declare secrets in a workflow-level env block:\n%s", env)
	}
}

// TestLibraryCIHasARealIntegrationJob pins the distinction the pipeline is
// built around: the `verify` job is unit coverage (the datastore suites
// auto-skip there), and the `integration` job runs them for real through the
// same script the release gate uses. Without this job a green pipeline would
// say nothing about Postgres, KurrentDB, Meilisearch or NATS.
func TestLibraryCIHasARealIntegrationJob(t *testing.T) {
	t.Parallel()

	wf := readRepoFile(t, ".github/workflows/ci.yml")

	integration := jobBlock(t, wf, "integration")
	if !strings.Contains(integration, "bash script/integration-test.sh") {
		t.Errorf("ci.yml integration job must run script/integration-test.sh:\n%s", integration)
	}
	if strings.Contains(integration, "BUILDINGBLOCKS_REQUIRE_INTEGRATION") {
		t.Errorf("ci.yml must not set the switch inline — it belongs to the script (one entry point for CI and release):\n%s", integration)
	}

	verify := jobBlock(t, wf, "verify")
	if !strings.Contains(verify, "go test ./... -count=1 -timeout 300s") {
		t.Errorf("ci.yml verify job must keep the unit-only suite:\n%s", verify)
	}
	if strings.Contains(verify, "BUILDINGBLOCKS_REQUIRE_INTEGRATION") {
		t.Errorf("ci.yml verify job must NOT require integration — it has no datastores, so the live suites must keep skipping there:\n%s", verify)
	}
}

// TestLibraryCICheckoutsNeverPersistCredentials is the same pin as the release
// workflow's: no job here pushes, so no job may leave a token in
// `.git/config`.
func TestLibraryCICheckoutsNeverPersistCredentials(t *testing.T) {
	t.Parallel()

	wf := readRepoFile(t, ".github/workflows/ci.yml")
	checkouts := 0
	for _, step := range stepBlocks(wf) {
		if !strings.Contains(step, "uses: actions/checkout@") {
			continue
		}
		checkouts++
		if !strings.Contains(step, "persist-credentials: false") {
			t.Errorf("ci.yml checkout step must set `persist-credentials: false`:\n%s", step)
		}
	}
	if checkouts < 2 {
		t.Errorf("ci.yml should check out in both jobs; found %d checkouts — parser drift?", checkouts)
	}
}
