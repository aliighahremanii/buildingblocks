package testenv_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/aliighahremanii/buildingblocks/infrastructure/testenv"
)

// TestRequired pins the switch semantics: only an explicit on-value turns the
// live suites into a hard requirement, and the off-values (including the
// empty string a developer's shell produces) keep the module's default
// skip-when-unconfigured behavior.
func TestRequired(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  bool
	}{
		{"", false},
		{"0", false},
		{"false", false},
		{"FALSE", false},
		{"no", false},
		{"1", true},
		{"true", true},
		{"YES", true},
		{"on", true},
		{" 1 ", true},
	} {
		t.Run("value="+tc.value, func(t *testing.T) {
			t.Setenv(testenv.RequireIntegrationEnv, tc.value)
			if got := testenv.Required(); got != tc.want {
				t.Fatalf("Required() with %s=%q = %v, want %v", testenv.RequireIntegrationEnv, tc.value, got, tc.want)
			}
		})
	}
}

// TestSkipOrFailSkipsWhenOptional proves the default path still skips (a
// developer without a datastore stack must not get failures).
func TestSkipOrFailSkipsWhenOptional(t *testing.T) {
	t.Setenv(testenv.RequireIntegrationEnv, "")

	var skipped bool
	ok := t.Run("subtest", func(st *testing.T) {
		// The deferred read runs while t.Skip unwinds the goroutine.
		defer func() { skipped = st.Skipped() }()
		testenv.SkipOrFail(st, "Postgres integration test skipped: no datastore")
	})

	if !ok {
		t.Fatal("the subtest must not FAIL when integration tests are optional")
	}
	if !skipped {
		t.Fatal("SkipOrFail must SKIP the test when integration tests are optional")
	}
}

// requireSubprocessEnv switches this test into its own child process, where
// the required-mode failure can be observed without failing the parent test
// (a failing subtest marks its parent failed, so the failure path must be
// exercised out of process).
const requireSubprocessEnv = "TESTENV_REQUIRED_SUBPROCESS"

// TestSkipOrFailFailsWhenRequired proves the gate is load-bearing: with
// BUILDINGBLOCKS_REQUIRE_INTEGRATION set, a missing datastore FAILS the test
// instead of skipping it, so a provisioning job cannot report green while the
// live suite never ran.
func TestSkipOrFailFailsWhenRequired(t *testing.T) {
	if os.Getenv(requireSubprocessEnv) == "1" {
		// Child process: this call must fail the test binary.
		testenv.SkipOrFail(t, "Postgres integration test skipped: no datastore")
		t.Fatal("SkipOrFail returned in required mode — it must fail the test")
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestSkipOrFailFailsWhenRequired$", "-test.v")
	cmd.Env = append(os.Environ(),
		requireSubprocessEnv+"=1",
		testenv.RequireIntegrationEnv+"=1",
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("the child process must FAIL when %s is set; it succeeded instead. Output:\n%s", testenv.RequireIntegrationEnv, out)
	}
	if !strings.Contains(string(out), "integration tests are REQUIRED") {
		t.Fatalf("the child failure must name the required-integration policy; output:\n%s", out)
	}
	if !strings.Contains(string(out), "no datastore") {
		t.Fatalf("the child failure must carry the original skip reason; output:\n%s", out)
	}
}
