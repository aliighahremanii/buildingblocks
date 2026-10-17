// Package testinfra pins this repository's OWN test/CI infrastructure by
// reading the checked-in workflow, Compose and script files. It is a
// test-only package (no non-test Go files): it exists so that a silent
// regression in the integration gate, the release permission model or the
// anonymous public-module fetch fails the build instead of surviving as prose.
package testinfra

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// repoRoot returns the module root, derived from THIS file's location, so the
// tests read the real checked-in artifacts regardless of the working
// directory.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(filepath.Dir(file))
}

func readRepoFile(t *testing.T, rel string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(data)
}

// jobBlock returns the YAML block of one job (its `  <name>:` key plus every
// more-indented line) up to the next job key or the end of the document.
func jobBlock(t *testing.T, workflow, name string) string {
	t.Helper()

	lines := strings.Split(workflow, "\n")
	header := "  " + name + ":"
	var captured []string
	capturing := false
	for _, raw := range lines {
		switch {
		case raw == header:
			capturing = true
			captured = append(captured, raw)
		case capturing && (strings.HasPrefix(raw, "    ") || strings.HasPrefix(raw, "\t") || strings.TrimSpace(raw) == ""):
			captured = append(captured, raw)
		case capturing:
			return strings.Join(captured, "\n")
		}
	}
	if !capturing {
		t.Fatalf("workflow does not define job %q", name)
	}
	return strings.Join(captured, "\n")
}

// stepBlocks splits a workflow into its step bodies (each starting at the
// six-space `- ` list item) so an assertion can be scoped to one step.
func stepBlocks(workflow string) []string {
	var blocks []string
	var current []string
	for _, raw := range strings.Split(workflow, "\n") {
		if strings.HasPrefix(raw, "      - ") && len(current) > 0 {
			blocks = append(blocks, strings.Join(current, "\n"))
			current = nil
		}
		if strings.HasPrefix(raw, "      - ") || len(current) > 0 {
			current = append(current, raw)
		}
	}
	if len(current) > 0 {
		blocks = append(blocks, strings.Join(current, "\n"))
	}
	return blocks
}

// stripComments removes full-line YAML/shell comments so prose explaining a
// deliberately-avoided construct does not trip a negative assertion.
func stripComments(text string) string {
	return regexp.MustCompile(`(?m)^[[:space:]]*#.*$`).ReplaceAllString(text, "")
}
