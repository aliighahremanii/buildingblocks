package testinfra

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

const modulePath = "github.com/aliighahremanii/buildingblocks"

// topLevelBlock returns the block of a top-level YAML key (a key at column 0
// plus its more-indented lines), or "" when the key is absent.
func topLevelBlock(workflow, key string) string {
	var captured []string
	capturing := false
	for _, raw := range strings.Split(workflow, "\n") {
		switch {
		case raw == key+":":
			capturing = true
			captured = append(captured, raw)
		case capturing && (strings.HasPrefix(raw, "  ") || strings.HasPrefix(raw, "\t") || strings.TrimSpace(raw) == ""):
			captured = append(captured, raw)
		case capturing:
			return strings.Join(captured, "\n")
		}
	}
	return strings.Join(captured, "\n")
}

// jobNames lists the workflow's job keys (two-space `  name:` at job level).
func jobNames(workflow string) []string {
	var names []string
	for _, raw := range strings.Split(workflow, "\n") {
		if m := regexp.MustCompile(`^  ([a-zA-Z0-9_-]+):$`).FindStringSubmatch(raw); m != nil {
			names = append(names, m[1])
		}
	}
	return names
}

// TestReleaseWorkflowIsReadOnlyExceptWhenPublishing pins the least-privilege
// permission model: the workflow default is `contents: read`, and the ONLY
// job that may write is the publish job. A verification job that can push to
// the repository is an unnecessary credential surface — and the fetchability
// proof consumes no token.
func TestReleaseWorkflowIsReadOnlyExceptWhenPublishing(t *testing.T) {
	t.Parallel()

	wf := readRepoFile(t, ".github/workflows/release.yml")

	defaults := topLevelBlock(wf, "permissions")
	if !strings.Contains(defaults, "contents: read") {
		t.Errorf("release.yml must declare a workflow-level `permissions: contents: read` (least privilege default); block:\n%s", defaults)
	}
	if strings.Contains(defaults, "contents: write") {
		t.Errorf("release.yml must NOT grant `contents: write` at workflow level — only the publish job may write; block:\n%s", defaults)
	}

	names := jobNames(wf)
	if len(names) == 0 {
		t.Fatal("release.yml: no jobs parsed — parser drift")
	}
	writers := 0
	for _, name := range names {
		block := jobBlock(t, wf, name)
		hasWrite := strings.Contains(block, "contents: write")
		if name == "release" {
			if !hasWrite {
				t.Errorf("release.yml job %q must grant `permissions: contents: write` (it creates the release)", name)
			}
			writers++
			continue
		}
		if hasWrite {
			t.Errorf("release.yml job %q must not grant `contents: write` — only the publish job may write", name)
		}
	}
	if writers != 1 {
		t.Errorf("release.yml must have exactly one writing job, found %d", writers)
	}
}

// TestReleaseWorkflowCheckoutsNeverPersistCredentials pins
// `persist-credentials: false` on EVERY checkout. The default leaves the job
// token in the checkout's `.git/config`, where any later step (or a
// compromised dependency in a later step) can read it — none of these jobs
// needs to push.
func TestReleaseWorkflowCheckoutsNeverPersistCredentials(t *testing.T) {
	t.Parallel()

	wf := readRepoFile(t, ".github/workflows/release.yml")
	steps := stepBlocks(wf)

	checkouts := 0
	for _, step := range steps {
		if !strings.Contains(step, "uses: actions/checkout@") {
			continue
		}
		checkouts++
		if !strings.Contains(step, "persist-credentials: false") {
			t.Errorf("release.yml checkout step must set `persist-credentials: false`:\n%s", step)
		}
	}
	if checkouts < 4 {
		t.Errorf("release.yml should check out in tag verify, module verify, integration, and publish; found %d checkouts — parser drift?", checkouts)
	}
}

// TestReleaseWorkflowProvesAnonymousPublicFetchWithChecksumVerification pins
// the public-consumption path the release proves before publishing:
//
//   - the module is fetched ANONYMOUSLY through the public module proxy
//     (GOPROXY names the proxy and NOTHING else — no `,direct` fallback), so
//     Go never performs a Git fetch of it and no credential, Git auth header,
//     or credential helper in the job's environment can satisfy the download;
//   - checksum verification is ON: GOSUMDB is the public default, the private
//     scope is asserted EMPTY (no GOPRIVATE/GONOPROXY/GONOSUMDB exemption for
//     any module), go.sum must carry both h1 hashes for the exact version, and
//     `go mod verify` re-hashes every downloaded module;
//   - the module cache, build cache, GOPATH and HOME are fresh temporary
//     directories, so nothing can be served from a pre-populated cache, a
//     `~/.netrc`, or a `~/.gitconfig` credential helper;
//   - the step consumes NO secret at all, and no credential-bearing transport
//     (`GIT_CONFIG_*` extraheader, base64 basic auth, a token URL, a netrc
//     write, an env file) exists anywhere in it;
//   - the resolved version is asserted to be exactly the tag, from the fresh
//     module cache, with no replace directive.
func TestReleaseWorkflowProvesAnonymousPublicFetchWithChecksumVerification(t *testing.T) {
	t.Parallel()

	wf := readRepoFile(t, ".github/workflows/release.yml")
	stripped := stripComments(wf)

	// 1. No global verification weakening anywhere in the workflow. An EMPTY
	//    assignment is the opposite (an explicit assertion of the public
	//    default), so only non-empty weakening forms are forbidden.
	for _, forbidden := range []string{"GOSUMDB=off", "GONOSUMDB=*", "GOPRIVATE=*", "GONOSUMCHECK=1", "GOFLAGS=-insecure"} {
		if strings.Contains(stripped, forbidden) {
			t.Errorf("release.yml must not contain %q — public modules must keep proxy + checksum verification", forbidden)
		}
	}
	if regexp.MustCompile(`GOSUMDB\s*[:=]\s*"?off`).MatchString(stripped) {
		t.Errorf("release.yml must never set GOSUMDB=off")
	}
	if regexp.MustCompile(`GOPROXY\s*[:=]\s*"?[^\n"]*,direct`).MatchString(stripped) {
		t.Errorf("release.yml must not add a `,direct` GOPROXY fallback: a direct VCS fetch could be satisfied by a Git credential, which is exactly what an anonymous public-consumption proof must rule out")
	}

	// 2. The fetch step itself.
	var fetchStep string
	for _, step := range stepBlocks(jobBlock(t, wf, "verify-public-fetch")) {
		if strings.Contains(step, "fetchable") {
			fetchStep = step
			break
		}
	}
	if fetchStep == "" {
		t.Fatal("release.yml has no step that proves the module is fetchable — parser drift")
	}
	// Prose explaining the avoided private scope must not satisfy (or trip) a
	// code assertion, so every check below runs on the comment-stripped step.
	step := stripComments(fetchStep)

	if !strings.Contains(step, `module="`+modulePath+`"`) {
		t.Errorf("fetch step must resolve the exact module path %q:\n%s", modulePath, step)
	}

	// Public resolution, explicitly stated.
	for _, want := range []string{
		`export GOPROXY="https://proxy.golang.org"`,
		`export GOSUMDB="sum.golang.org"`,
		"export GOWORK=off",
	} {
		if !strings.Contains(step, want) {
			t.Errorf("fetch step must set %s (public proxy, public checksum database, no workspace substitution):\n%s", want, step)
		}
	}

	// Empty private scope: no module may be exempted from the proxy or the
	// checksum database.
	for _, want := range []string{
		"export GOPRIVATE=",
		"export GONOPROXY=",
		"export GONOSUMDB=",
	} {
		if !strings.Contains(step, want) {
			t.Errorf("fetch step must clear the private scope with %q so the fetch is genuinely anonymous and checksum-verified:\n%s", want, step)
		}
	}

	// Fresh caches and HOME: nothing may be served from a pre-populated cache,
	// a netrc, or a git credential helper.
	for _, want := range []string{
		"export GOMODCACHE=",
		"export GOCACHE=",
		"export GOPATH=",
		"export HOME=",
	} {
		if !strings.Contains(step, want) {
			t.Errorf("fetch step must isolate %q to a fresh temporary directory — otherwise a warm cache or a credential file in the runner's home could satisfy the fetch:\n%s", want, step)
		}
	}

	// No interactive credential path, and no Git credential can help.
	for _, want := range []string{
		"GIT_TERMINAL_PROMPT=0",
		"GIT_ASKPASS=/bin/false",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
	} {
		if !strings.Contains(step, want) {
			t.Errorf("fetch step must set %s so a credential prompt fails actionably and no global Git credential is reachable:\n%s", want, step)
		}
	}

	// 3. NO credential of any kind in the step, and no credential transport.
	//    The step's own defensive `unset GITHUB_TOKEN GH_TOKEN ...` is the one
	//    legitimate mention: it guarantees no inherited credential exists, so
	//    it is asserted present and then removed before the negative scan.
	if !strings.Contains(step, "unset GITHUB_TOKEN GH_TOKEN") {
		t.Errorf("fetch step must explicitly unset any inherited token variables (GITHUB_TOKEN/GH_TOKEN) — the anonymous public-consumption proof must not be able to reach a credential that happens to be in the job environment:\n%s", step)
	}
	scanned := step
	for _, line := range strings.Split(step, "\n") {
		if strings.Contains(line, "unset GITHUB_TOKEN GH_TOKEN") {
			scanned = strings.Replace(scanned, line, "", 1)
		}
	}
	for _, forbidden := range []string{
		"secrets.",
		"GITHUB_TOKEN",
		"GH_TOKEN",
		"GIT_CONFIG_VALUE_0",
		"GIT_CONFIG_KEY_0",
		"http.extraheader",
		"base64",
		"x-access-token",
		"@github.com",
		"NETRC",
		"netrc",
		"GITHUB_ENV",
		"--password",
	} {
		if strings.Contains(scanned, forbidden) {
			t.Errorf("fetch step must not reference %q — the anonymous public-consumption proof must have no credential available and no credential transport (a Git auth header or token URL would let a credential satisfy a fetch that the public proxy is supposed to serve):\n%s", forbidden, step)
		}
	}

	// 4. The proof must assert the EXACT version, the absence of a replace
	//    directive, and that the module really came from the proxy-filled
	//    module cache (a local directory is what a replace/workspace would
	//    substitute).
	if !strings.Contains(step, `go list -m -f '{{.Version}}'`) {
		t.Errorf("fetch step must assert the resolved version equals the tag:\n%s", step)
	}
	if !strings.Contains(step, "REPLACED") {
		t.Errorf("fetch step must prove the module did not resolve through a replace directive:\n%s", step)
	}
	if !strings.Contains(step, `go list -m -f '{{.Dir}}'`) || !strings.Contains(step, "${GOMODCACHE}") {
		t.Errorf("fetch step must prove the module resolved inside the fresh module cache (not a local directory):\n%s", step)
	}

	// 5. Checksum verification is the integrity record: both h1 entries must be
	//    present in go.sum and every downloaded module must re-hash.
	if !strings.Contains(step, "go.sum") || !strings.Contains(step, "h1:") {
		t.Errorf("fetch step must assert go.sum carries the module's h1 checksum entries:\n%s", step)
	}
	if !strings.Contains(step, "go mod verify") {
		t.Errorf("fetch step must run `go mod verify` so every downloaded module is re-hashed against go.sum:\n%s", step)
	}

	// 6. The step consumes no secret, and no secret is declared at workflow
	//    level (inherited by every step).
	if env := topLevelBlock(wf, "env"); strings.Contains(env, "secrets.") {
		t.Errorf("release.yml must not declare secrets in a workflow-level env block (inherited by every step):\n%s", env)
	}
}

// privateScopeAssignments returns every line that ASSIGNS a non-empty value to
// a private-module scope variable — in shell (`export GOPRIVATE=value`) or in a
// YAML `env:` block (`GOPRIVATE: value`). An EMPTY assignment is deliberately
// excluded: the fetch step CLEARS the scope as an assertion of the public
// default, which is the opposite of an exemption.
func privateScopeAssignments(workflow string) []string {
	var found []string
	assign := regexp.MustCompile(`^\s*(?:export\s+)?(GOPRIVATE|GONOPROXY|GONOSUMDB|GOINSECURE|GONOSUMCHECK|GOFLAGS)\s*[:=]\s*(.*)$`)
	for _, raw := range strings.Split(workflow, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		m := assign.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		value := strings.Trim(strings.TrimSpace(m[2]), `"'`)
		if value == "" {
			continue // an explicit empty assignment: the public default, not an exemption
		}
		if m[1] == "GOFLAGS" && !strings.Contains(value, "-insecure") {
			continue // ordinary build flags are not a verification weakening
		}
		found = append(found, line)
	}
	return found
}

// TestReleaseWorkflowHasNoPrivateModuleCredential pins the negative space for
// the whole workflow: no private-module exemption carries a VALUE, and no
// credential transport survives anywhere in release.yml now that the module is
// public. (The fetch step legitimately CLEARS the private scope — an empty
// assignment is an assertion of the public default, not an exemption — so only
// a non-empty value is forbidden.)
//
// The workflow's `secrets.GITHUB_TOKEN` uses are NOT in scope: they are the
// retained GitHub release-creation permission on the single publish job, which
// is a separate, legitimate capability from a module-download credential.
func TestReleaseWorkflowHasNoPrivateModuleCredential(t *testing.T) {
	t.Parallel()

	wf := readRepoFile(t, ".github/workflows/release.yml")
	stripped := stripComments(wf)

	for _, line := range privateScopeAssignments(stripped) {
		t.Errorf("release.yml assigns a private-module scope value: %q — this module is PUBLIC (it is consumed anonymously through the public proxy with checksum verification), so a private-module exemption is dead configuration that would silently weaken the consumer contract", line)
	}
	for _, forbidden := range []string{
		"GIT_CONFIG_VALUE_0",
		"http.extraheader",
		"x-access-token",
		"netrc",
		"NETRC",
	} {
		if strings.Contains(stripped, forbidden) {
			t.Errorf("release.yml must not contain %q — the module is fetched anonymously, so no credential transport should exist at all", forbidden)
		}
	}
	// The release-creation permission is a separate capability and must survive
	// the credential removal.
	if !strings.Contains(wf, "gh release create") {
		t.Error("release.yml no longer creates a GitHub release — the module-download credential removal must not remove the publication capability")
	}
}

// TestReleaseWorkflowWaitsForLiveIntegration pins the release gate: a version
// may not be published while the datastore-backed suites have never actually
// run. The `integration` job calls the same entry point as CI (which sets
// BUILDINGBLOCKS_REQUIRE_INTEGRATION=1, so a skip is a failure), and the
// publish job needs it.
func TestReleaseWorkflowWaitsForLiveIntegration(t *testing.T) {
	t.Parallel()

	wf := readRepoFile(t, ".github/workflows/release.yml")

	integration := jobBlock(t, wf, "integration")
	if !strings.Contains(integration, "bash script/integration-test.sh") {
		t.Errorf("release.yml integration job must run script/integration-test.sh (the live gate):\n%s", integration)
	}

	release := jobBlock(t, wf, "release")
	needs := regexp.MustCompile(`(?m)^    needs:\s*\[(.+)\]`).FindStringSubmatch(release)
	if needs == nil {
		t.Fatalf("release.yml publish job must declare `needs:` as an inline list; block:\n%s", release)
	}
	dependencies := strings.Split(strings.ReplaceAll(needs[1], " ", ""), ",")
	for _, required := range []string{"verify-tag", "verify-module", "integration", "verify-public-fetch"} {
		if !slices.Contains(dependencies, required) {
			t.Errorf("release.yml publish job must need %q before publishing; got needs: [%s]", required, needs[1])
		}
	}
}

func TestReleaseWorkflowPublicFetchWaitsForAllVerification(t *testing.T) {
	t.Parallel()
	wf := readRepoFile(t, ".github/workflows/release.yml")
	fetch := stripComments(jobBlock(t, wf, "verify-public-fetch"))
	needs := regexp.MustCompile(`(?m)^    needs:\s*\[(.+)\]`).FindStringSubmatch(fetch)
	if needs == nil {
		t.Fatalf("public fetch must declare its verification dependencies:\n%s", fetch)
	}
	dependencies := strings.Split(strings.ReplaceAll(needs[1], " ", ""), ",")
	for _, required := range []string{"verify-tag", "verify-module", "integration"} {
		if !slices.Contains(dependencies, required) {
			t.Errorf("public fetch must wait for %s; got %v", required, dependencies)
		}
	}
	if !strings.Contains(fetch, "cache: false") {
		t.Error("public fetch must disable setup-go cache restoration")
	}
	for _, forbidden := range []string{"actions/checkout@", "secrets.", "continue-on-error:", "if:"} {
		if strings.Contains(fetch, forbidden) {
			t.Errorf("public fetch must not contain %q", forbidden)
		}
	}
	for _, name := range []string{"verify-tag", "verify-module", "integration", "release"} {
		job := stripComments(jobBlock(t, wf, name))
		for _, forbidden := range []string{"go get", "proxy.golang.org", "continue-on-error:", "if:"} {
			if strings.Contains(job, forbidden) {
				t.Errorf("job %s must not bypass gates or perform the public fetch: %q", name, forbidden)
			}
		}
	}
}

// releaseStepScript extracts a named step's literal shell body so failure-path
// tests exercise the actual workflow rather than a copy of its logic.
func releaseStepScript(t *testing.T, wf, job, name string) string {
	t.Helper()
	for _, step := range stepBlocks(jobBlock(t, wf, job)) {
		if !strings.HasPrefix(step, "      - name: "+name+"\n") {
			continue
		}
		_, body, ok := strings.Cut(step, "        run: |\n")
		if !ok {
			t.Fatalf("step %q has no literal run body", name)
		}
		var lines []string
		for _, line := range strings.Split(body, "\n") {
			if strings.TrimSpace(line) != "" && !strings.HasPrefix(line, "          ") {
				t.Fatalf("unexpected run indentation in %q: %q", name, line)
			}
			lines = append(lines, strings.TrimPrefix(line, "          "))
		}
		return strings.Join(lines, "\n")
	}
	t.Fatalf("missing step %q in job %q", name, job)
	return ""
}

func TestReleaseWorkflowUsesValidatedCuratedNotesFromTag(t *testing.T) {
	t.Parallel()
	wf := readRepoFile(t, ".github/workflows/release.yml")
	release := stripComments(jobBlock(t, wf, "release"))
	for _, required := range []string{
		"ref: ${{ needs.verify-tag.outputs.tag }}",
		`--notes-file "docs/releases/${TAG}.md"`,
		"--verify-tag",
	} {
		if !strings.Contains(release, required) {
			t.Errorf("publish job must contain %q", required)
		}
	}
	if strings.Contains(stripComments(wf), "--generate-notes") {
		t.Error("release must use curated notes, not generated notes")
	}
	module := jobBlock(t, wf, "verify-module")
	if !strings.Contains(module, "TAG: ${{ needs.verify-tag.outputs.tag }}") {
		t.Error("notes validation must use the validated tag")
	}
	script := releaseStepScript(t, wf, "verify-module", "Validate curated release notes")
	for _, tc := range []struct {
		name, notes string
		wantOK      bool
	}{
		{name: "missing"},
		{name: "empty", notes: ""},
		{name: "wrong version", notes: "# v0.2.0\n\nOther notes.\n"},
		{name: "partial heading", notes: "# v0.1.0-rc.1\n\nOther notes.\n"},
		{name: "curated v0.1.0", notes: readRepoFile(t, "docs/releases/v0.1.0.md"), wantOK: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.name != "missing" {
				notesDir := filepath.Join(dir, "docs", "releases")
				if err := os.MkdirAll(notesDir, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(notesDir, "v0.1.0.md"), []byte(tc.notes), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command("bash", "-e", "-o", "pipefail", "-c", script)
			cmd.Dir = dir
			cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "TAG=v0.1.0"}
			output, err := cmd.CombinedOutput()
			if (err == nil) != tc.wantOK {
				t.Fatalf("notes validation error = %v, want success %t:\n%s", err, tc.wantOK, output)
			}
		})
	}
}

func TestReleaseWorkflowExistenceChecksFailClosed(t *testing.T) {
	t.Parallel()
	wf := readRepoFile(t, ".github/workflows/release.yml")
	for _, gate := range []struct{ job, step string }{
		{"verify-tag", "Refuse to republish an existing release"},
		{"release", "Create the release"},
	} {
		script := releaseStepScript(t, wf, gate.job, gate.step)
		for _, tc := range []struct {
			name, response, status string
			wantOK                 bool
		}{
			{name: "empty repository", status: "0", wantOK: true},
			{name: "other versions", response: "v0.1.0-rc.1\nv0.2.0", status: "0", wantOK: true},
			{name: "already published or concurrent release", response: "v0.0.1\nv0.1.0\nv0.2.0", status: "0"},
			{name: "large listing with early match", response: "v0.1.0\n" + strings.Repeat("v0.0.1\n", 10000), status: "0"},
			{name: "API failure", status: "1"},
			{name: "later page failure", response: "v0.0.1", status: "1"},
		} {
			t.Run(gate.job+"/"+tc.name, func(t *testing.T) {
				dir := t.TempDir()
				// Never invoke the real gh CLI: this shell function is the only
				// API surface available to the workflow script in these tests.
				stub := `
gh() {
  if [ "$1" = api ]; then
    if [ "$#" -ne 5 ] || [ "$2" != --paginate ] || [ "$3" != repos/example/repo/releases ] || [ "$4" != --jq ] || [ "$5" != '.[].tag_name' ]; then
      echo "unexpected API arguments: $*" >&2
      return 2
    fi
    printf '%s\n' "${RESPONSE}"
    return "${STATUS}"
  fi
  if [ "$1" = release ] && [ "$2" = create ]; then
    printf '%s\n' "$@" > created
    return 0
  fi
  echo "unexpected gh invocation: $*" >&2
  return 2
}
`
				cmd := exec.Command("bash", "-e", "-o", "pipefail", "-c", stub+script)
				cmd.Dir = dir
				cmd.Env = []string{
					"PATH=" + os.Getenv("PATH"), "TAG=v0.1.0", "GITHUB_REPOSITORY=example/repo",
					"RESPONSE=" + tc.response, "STATUS=" + tc.status,
				}
				output, err := cmd.CombinedOutput()
				if (err == nil) != tc.wantOK {
					t.Fatalf("existence check error = %v, want success %t:\n%s", err, tc.wantOK, output)
				}
				_, err = os.Stat(filepath.Join(dir, "created"))
				wantCreate := tc.wantOK && gate.job == "release"
				if (err == nil) != wantCreate {
					t.Fatalf("release creation = %t, want %t", err == nil, wantCreate)
				}
			})
		}
	}
}
