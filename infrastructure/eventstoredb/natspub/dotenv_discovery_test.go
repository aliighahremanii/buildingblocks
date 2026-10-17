package natspub_test

// dotenv_discovery_test.go — root-safe, testable .env discovery for the
// live natspub tests. CI run #3058 regression: the previous inline walker
// in worker_live_test.go derived the parent directory with
// dir[:strings.LastIndex(dir, "/")]. Walking up from the CI integration
// image's /src working directory (no .env anywhere — .dockerignore excludes
// it), the walk reached "/" (LastIndex = 0 → parent ""), then recomputed
// ""[:LastIndex("", "/")] = ""[:-1] and PANicked the test binary before any
// service gate could skip or pass. The helpers below are parameterized
// (explicit start directory / explicit file path) so the discovery and
// parsing contracts are unit-testable without changing the process working
// directory and without depending on where the repository happens to live.
//
// Parsing semantics intentionally mirror kurrenttest.loadDotEnv / hosts
// LoadDevEnvironment: unset-only variables, comments and blank lines
// skipped, surrounding quotes trimmed.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// maxDotEnvWalk bounds the upward walk. The root check below is the actual
// terminator (parent == dir at "/"); the bound is defensive only.
const maxDotEnvWalk = 12

// findDotEnvFrom walks up from startDir looking for the nearest .env file,
// returning its path or "". It is root-safe: reaching the filesystem root
// (or being handed an empty path) terminates the walk instead of panicking.
func findDotEnvFrom(startDir string) string {
	dir := startDir
	for i := 0; i < maxDotEnvWalk; i++ {
		if dir == "" {
			return ""
		}
		candidate := filepath.Join(dir, ".env")
		if fi, err := os.Stat(candidate); err == nil && !fi.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			// Filesystem root reached — never slice past it (CI #3058).
			return ""
		}
		dir = parent
	}
	return ""
}

// applyDotEnvFile reads key=value pairs from path into the process
// environment. Variables already set (non-empty) are never overwritten.
func applyDotEnvFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
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
		if key == "" {
			continue
		}
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if os.Getenv(key) == "" {
			_ = os.Setenv(key, value)
		}
	}
	return nil
}

// TestFindDotEnvFrom_WalksUpToNearest pins discovery order: the NEAREST
// ancestor .env wins, and a nested start directory finds an ancestor's file.
func TestFindDotEnvFrom_WalksUpToNearest(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".env"), []byte("A=1\n"), 0o644); err != nil {
		t.Fatalf("write root .env: %v", err)
	}

	deep := filepath.Join(root, "a", "b", "c")
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatalf("mkdir deep: %v", err)
	}
	if got := findDotEnvFrom(deep); got != filepath.Join(root, ".env") {
		t.Fatalf("findDotEnvFrom(deep) = %q, want %q", got, filepath.Join(root, ".env"))
	}

	// A nearer .env shadows the ancestor.
	nearer := filepath.Join(root, "a", "b")
	if err := os.WriteFile(filepath.Join(nearer, ".env"), []byte("A=2\n"), 0o644); err != nil {
		t.Fatalf("write nearer .env: %v", err)
	}
	if got := findDotEnvFrom(deep); got != filepath.Join(nearer, ".env") {
		t.Fatalf("findDotEnvFrom(deep) = %q, want the nearer %q", got, filepath.Join(nearer, ".env"))
	}
}

// TestFindDotEnvFrom_RootSafeIsThe3058Regression is the CI run #3058
// regression pin: walking up from a shallow directory with no .env above it
// (the CI image's /src), from the filesystem root itself, and from an empty
// path must all terminate WITHOUT panicking. The old walker died with
// `slice bounds out of range [:-1]` on exactly this input shape.
func TestFindDotEnvFrom_RootSafeIsThe3058Regression(t *testing.T) {
	// A directory guaranteed to have no .env of ours: a fresh temp dir with
	// nothing planted (its ancestors are the test runner's tmp tree).
	shallow := t.TempDir()

	cases := map[string]string{
		"empty start":  "",
		"root start":   "/",
		"shallow walk": shallow,
	}
	for name, start := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("%s: findDotEnvFrom panicked (%v) — the walk must be root-safe (CI #3058)", name, r)
				}
			}()
			got := findDotEnvFrom(start)
			// Deterministic contract: either nothing found, or an actual
			// .env file path (an operator machine may legitimately carry a
			// stray /.env or /tmp/.env — never a bogus path).
			if got != "" {
				if !strings.HasSuffix(got, ".env") {
					t.Fatalf("%s: bogus discovery result %q", name, got)
				}
				if _, err := os.Stat(got); err != nil {
					t.Fatalf("%s: discovery returned missing file %q: %v", name, got, err)
				}
			}
		}()
	}
}

// TestApplyDotEnvFile pins the parsing contract: unset variables are set,
// already-set variables are never overwritten, comments/blank/malformed
// lines are skipped, and surrounding quotes are trimmed.
func TestApplyDotEnvFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")

	t.Setenv("NATSPUB_DOTENV_TEST_UNSET_QUOTED", "")
	t.Setenv("NATSPUB_DOTENV_TEST_PRESET", "from-process")

	content := strings.Join([]string{
		"# comment line",
		"",
		"NATSPUB_DOTENV_TEST_UNSET_PLAIN=hello",
		`NATSPUB_DOTENV_TEST_UNSET_QUOTED="quoted value"`,
		"NATSPUB_DOTENV_TEST_PRESET=from-file",
		"not-a-pair",
		"   = empty key",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	if err := applyDotEnvFile(path); err != nil {
		t.Fatalf("applyDotEnvFile: %v", err)
	}
	if got := os.Getenv("NATSPUB_DOTENV_TEST_UNSET_PLAIN"); got != "hello" {
		t.Fatalf("unset plain variable = %q, want %q", got, "hello")
	}
	if got := os.Getenv("NATSPUB_DOTENV_TEST_UNSET_QUOTED"); got != "quoted value" {
		t.Fatalf("unset quoted variable = %q, want %q (quotes trimmed)", got, "quoted value")
	}
	if got := os.Getenv("NATSPUB_DOTENV_TEST_PRESET"); got != "from-process" {
		t.Fatalf("preset variable must never be overwritten, got %q", got)
	}

	if err := applyDotEnvFile(filepath.Join(dir, "missing.env")); err == nil {
		t.Fatal("a missing file must surface its error")
	}
}

// TestApplyDotEnvFile_UnsetEmptyStaysUnset pins the subtle pre-existing
// semantic: a KEY= line with an empty value sets an empty value, which the
// unset-only guard treats as still-unset for later loads (idempotent).
func TestApplyDotEnvFile_UnsetEmptyStaysUnset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("NATSPUB_DOTENV_TEST_EMPTYVAL=\n"), 0o644); err != nil {
		t.Fatalf("write .env: %v", err)
	}
	if err := applyDotEnvFile(path); err != nil {
		t.Fatalf("applyDotEnvFile: %v", err)
	}
	if got := os.Getenv("NATSPUB_DOTENV_TEST_EMPTYVAL"); got != "" {
		t.Fatalf("empty value must remain empty, got %q", got)
	}
	// A later file providing the real value still applies (the variable is
	// "unset" for the guard).
	if err := os.WriteFile(path, []byte("NATSPUB_DOTENV_TEST_EMPTYVAL=now-set\n"), 0o644); err != nil {
		t.Fatalf("rewrite .env: %v", err)
	}
	if err := applyDotEnvFile(path); err != nil {
		t.Fatalf("applyDotEnvFile again: %v", err)
	}
	if got := os.Getenv("NATSPUB_DOTENV_TEST_EMPTYVAL"); got != "now-set" {
		t.Fatalf("empty-before value must be settable later, got %q", got)
	}
}
