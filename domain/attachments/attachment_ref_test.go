package attachments

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

// validSHA computes a valid lowercase-hex sha256 from arbitrary input.
func validSHA(t *testing.T, seed string) string {
	t.Helper()
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])
}

func TestNewAttachmentRef_Success(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	r, err := NewAttachmentRef("ref-1", validSHA(t, "blob"), "invoice.pdf", "user-1", now)
	if err != nil {
		t.Fatalf("NewAttachmentRef: %v", err)
	}
	if r.ID() != "ref-1" || r.Filename() != "invoice.pdf" || r.UploadedBy() != "user-1" {
		t.Errorf("fields = %+v", r)
	}
	if r.BlobSHA256() != validSHA(t, "blob") {
		t.Errorf("BlobSHA256 mismatch")
	}
	if !r.UploadedAt().Equal(now) {
		t.Errorf("UploadedAt = %v, want %v", r.UploadedAt(), now)
	}
}

func TestNewAttachmentRef_NormalizesSHAAndTrimsFields(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	// Upper-case sha + surrounding whitespace on every text field.
	r, err := NewAttachmentRef("  ref-1  ", strings.ToUpper(validSHA(t, "blob")), "  invoice.pdf  ", "  user  ", now)
	if err != nil {
		t.Fatalf("NewAttachmentRef: %v", err)
	}
	if r.BlobSHA256() != validSHA(t, "blob") {
		t.Errorf("BlobSHA256 not lower-cased: %q", r.BlobSHA256())
	}
	if r.ID() != "ref-1" || r.Filename() != "invoice.pdf" || r.UploadedBy() != "user" {
		t.Errorf("trim failed: %+v", r)
	}
}

func TestNewAttachmentRef_Validation(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	valid := validSHA(t, "blob")
	cases := []struct {
		name, id, sha, filename, by string
		ts                          time.Time
		want                        error
	}{
		{"empty id", "", valid, "f.pdf", "u", now, ErrIDRequired},
		{"whitespace id", "   ", valid, "f.pdf", "u", now, ErrIDRequired},
		{"invalid sha (short)", "id", "abc", "f.pdf", "u", now, ErrInvalidBlobSHA256},
		{"invalid sha (upper hex still accepted after lower)", "id", strings.Repeat("A", 64), "f.pdf", "u", now, nil},
		{"invalid sha (non-hex)", "id", strings.Repeat("z", 64), "f.pdf", "u", now, ErrInvalidBlobSHA256},
		{"empty filename", "id", valid, "", "u", now, ErrFilenameRequired},
		{"zero uploadedAt", "id", valid, "f.pdf", "u", time.Time{}, ErrUploadedAtRequired},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewAttachmentRef(c.id, c.sha, c.filename, c.by, c.ts)
			// The "upper hex" case is valid (we accept then lowercase); expect nil.
			if c.want == nil {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
				return
			}
			if err != c.want {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
		})
	}
}

func TestIsValidSHA256(t *testing.T) {
	t.Parallel()
	if !IsValidSHA256(strings.Repeat("0", 64)) {
		t.Error("64 zeros should be valid")
	}
	if IsValidSHA256("") {
		t.Error("empty should be invalid")
	}
	if IsValidSHA256(strings.Repeat("0", 63)) {
		t.Error("63 chars should be invalid")
	}
	if IsValidSHA256(strings.Repeat("g", 64)) {
		t.Error("non-hex 64 chars should be invalid")
	}
	if IsValidSHA256(strings.Repeat("G", 64)) {
		t.Error("uppercase 64 chars should be invalid")
	}
}

func TestHashBytes_MatchesSHA256(t *testing.T) {
	t.Parallel()
	got := HashBytes([]byte("hello"))
	sum := sha256.Sum256([]byte("hello"))
	want := hex.EncodeToString(sum[:])
	if got != want {
		t.Errorf("HashBytes = %q, want %q", got, want)
	}
	if !IsValidSHA256(got) {
		t.Errorf("HashBytes output not valid: %q", got)
	}
}

func TestAttachmentRef_Equal(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sha := validSHA(t, "blob")
	a, _ := NewAttachmentRef("ref-1", sha, "a.pdf", "u", now)
	b, _ := NewAttachmentRef("ref-1", sha, "different.pdf", "u", now) // same id, different filename
	c, _ := NewAttachmentRef("ref-2", sha, "a.pdf", "u", now)         // different id
	if !a.Equal(b) {
		t.Error("same id should be Equal")
	}
	if a.Equal(c) {
		t.Error("different ids should not be Equal")
	}
}

func TestMustAttachmentRef_PanicsOnError(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on invalid ref")
		}
	}()
	_ = MustAttachmentRef("", validSHA(t, "x"), "f", "u", time.Now())
}

func TestMustAttachmentRef_Success(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	r := MustAttachmentRef("ref-1", validSHA(t, "x"), "f", "u", now)
	if r.ID() != "ref-1" {
		t.Fatalf("id = %q", r.ID())
	}
}

func TestValidateBatch(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sha := validSHA(t, "blob")
	// Empty batch is valid.
	if err := ValidateBatch(nil); err != nil {
		t.Fatalf("empty batch err = %v", err)
	}
	// All valid.
	good, _ := NewAttachmentRef("r1", sha, "a.pdf", "u", now)
	good2, _ := NewAttachmentRef("r2", sha, "b.pdf", "u", now)
	if err := ValidateBatch([]AttachmentRef{good, good2}); err != nil {
		t.Fatalf("all valid err = %v", err)
	}
	// Each error branch: missing id, invalid sha, missing filename, zero time.
	cases := []struct {
		name string
		ref  AttachmentRef
	}{
		{"missing id", AttachmentRef{id: "", blobSHA256: sha, filename: "f", uploadedAt: now}},
		{"invalid sha", AttachmentRef{id: "r", blobSHA256: "bad", filename: "f", uploadedAt: now}},
		{"missing filename", AttachmentRef{id: "r", blobSHA256: sha, filename: "", uploadedAt: now}},
		{"zero time", AttachmentRef{id: "r", blobSHA256: sha, filename: "f", uploadedAt: time.Time{}}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if err := ValidateBatch([]AttachmentRef{good, c.ref}); err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
		})
	}
}
