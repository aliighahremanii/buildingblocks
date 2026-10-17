package envconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValueReadsEnvironment(t *testing.T) {
	t.Setenv("TEST_SECRET", "from-env")
	value, err := Value("TEST_SECRET")
	if err != nil || value != "from-env" {
		t.Fatalf("value=%q err=%v", value, err)
	}
}

func TestValuePrefersSecretFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(file, []byte("from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_SECRET", "from-env")
	t.Setenv("TEST_SECRET_FILE", file)
	value, err := Value("TEST_SECRET")
	if err != nil || value != "from-file" {
		t.Fatalf("value=%q err=%v", value, err)
	}
}

func TestValueReturnsFileError(t *testing.T) {
	t.Setenv("TEST_SECRET_FILE", filepath.Join(t.TempDir(), "missing"))
	if _, err := Value("TEST_SECRET"); err == nil {
		t.Fatal("expected missing secret-file error")
	}
}
