package i18n

import (
	"errors"
	"testing"
)

func TestNewTrimsAndRequiresOneValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		local     string
		intl      string
		wantLocal string
		wantIntl  string
		wantErr   error
	}{
		{name: "both empty", local: "", intl: "", wantErr: ErrNameRequired},
		{name: "both blank", local: "   ", intl: " \t ", wantErr: ErrNameRequired},
		{name: "only local", local: "  ایران  ", intl: "", wantLocal: "ایران", wantIntl: ""},
		{name: "only intl", local: "", intl: " Iran ", wantLocal: "", wantIntl: "Iran"},
		{name: "both", local: "ایران", intl: "Iran", wantLocal: "ایران", wantIntl: "Iran"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := New(tt.local, tt.intl)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("New() error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if got.Local() != tt.wantLocal {
				t.Fatalf("Local() = %q, want %q", got.Local(), tt.wantLocal)
			}
			if got.Intl() != tt.wantIntl {
				t.Fatalf("Intl() = %q, want %q", got.Intl(), tt.wantIntl)
			}
		})
	}
}

func TestRehydrateKeepsValuesAsIs(t *testing.T) {
	t.Parallel()

	got := Rehydrate("  ", "")
	if got.Local() != "  " {
		t.Fatalf("Rehydrate should not trim, Local() = %q", got.Local())
	}
	if got.IsEmpty() {
		t.Fatalf("Rehydrate value with non-empty Local should not be IsEmpty")
	}

	empty := Rehydrate("", "")
	if !empty.IsEmpty() {
		t.Fatalf("both-empty Rehydrate should be IsEmpty")
	}

	full := Rehydrate("ایران", "Iran")
	if full.IsEmpty() {
		t.Fatalf("populated Rehydrate should not be IsEmpty")
	}
}

func TestEqual(t *testing.T) {
	t.Parallel()

	a := Rehydrate("a", "b")
	b := Rehydrate("a", "b")
	c := Rehydrate("a", "c")
	if !a.Equal(b) {
		t.Fatalf("identical values should be Equal")
	}
	if a.Equal(c) {
		t.Fatalf("differing intl should not be Equal")
	}
}
