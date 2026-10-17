package uid

import (
	"testing"
)

func TestNewGeneratesValidUUID(t *testing.T) {
	t.Parallel()
	for i := 0; i < 100; i++ {
		got := New()
		if !IsValid(got) {
			t.Fatalf("New() produced invalid UUID: %q", got)
		}
	}
}

func TestNewIsUnique(t *testing.T) {
	t.Parallel()
	seen := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		id := New()
		if seen[id] {
			t.Fatalf("duplicate UUID generated: %q", id)
		}
		seen[id] = true
	}
}

func TestIsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		s    string
		want bool
	}{
		{"canonical lowercase", "550e8400-e29b-41d4-a716-446655440000", true},
		{"canonical uppercase", "550E8400-E29B-41D4-A716-446655440000", true},
		{"mixed case", "550e8400-E29b-41D4-a716-446655440000", true},
		{"empty", "", false},
		{"too short", "550e8400-e29b-41d4", false},
		{"too long", "550e8400-e29b-41d4-a716-446655440000-extra", false},
		{"missing dashes", "550e8400e29b41d4a716446655440000", false},
		{"wrong dash positions", "550e8400-e29b41-d4a716-446655-440000", false},
		{"non-hex chars", "550e8400-e29b-41d4-a716-44665544gggg", false},
		{"extra dashes", "550e8400-e29b-41d4-a716-44665544000-", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsValid(tt.s); got != tt.want {
				t.Fatalf("IsValid(%q) = %v, want %v", tt.s, got, tt.want)
			}
		})
	}
}

func TestIsValidAllHyphenPositionsChecked(t *testing.T) {
	t.Parallel()

	base := "550e8400e29b41d4a716446655440000"
	for dashPos := 0; dashPos < 36; dashPos++ {
		switch dashPos {
		case 8, 13, 18, 23:
			continue
		}
		s := base
		if dashPos < len(s) {
			s = s[:dashPos] + "-" + s[dashPos:]
			if len(s) == 36 && IsValid(s) {
				t.Fatalf("UUID with dash at wrong position %d should be invalid: %q", dashPos, s)
			}
		}
	}
}

func TestFromKeyDeterministic(t *testing.T) {
	t.Parallel()
	id1 := FromKey("my-idempotency-key")
	id2 := FromKey("my-idempotency-key")
	if id1 != id2 {
		t.Fatalf("FromKey not deterministic: %q != %q", id1, id2)
	}
	if !IsValid(id1) {
		t.Fatalf("FromKey produced invalid UUID: %q", id1)
	}
}

func TestFromKeyDifferentKeys(t *testing.T) {
	t.Parallel()
	id1 := FromKey("key-a")
	id2 := FromKey("key-b")
	if id1 == id2 {
		t.Fatalf("different keys produced same UUID: %q", id1)
	}
}

func TestFromKeyDiffersFromRandom(t *testing.T) {
	t.Parallel()
	// Deterministic keys should not collide with random UUIDs in practice.
	det := FromKey("test")
	rnd := New()
	if det == rnd {
		t.Fatalf("deterministic UUID collided with random: %q", det)
	}
}
