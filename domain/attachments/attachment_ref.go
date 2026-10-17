// Package attachments provides the AttachmentRef value object shared by
// every operational workflow that accepts optional cancellation/rejection
// attachments (BOL cancellation, Release cancellation, Bijak cancellation,
// Gate rejection, Recovery). Master plan §5.3.
//
// Attachment bytes live in an external object store (RustFS / S3-compatible,
// see buildingblocks/infrastructure/objectstorage/rustfs); the
// attachment_blobs Postgres table records blob metadata (sha256 PK, size,
// mime, object key, bucket) and attachment_refs records the per-upload
// reference (id PK, blob sha256 FK, filename, uploaded_by, uploaded_at).
//
// Identical bytes uploaded under different filenames create ONE blob row
// and SEPARATE attachment_refs rows. Events carry immutable AttachmentRef
// snapshots, never file bytes.
package attachments

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// SHA256HexLen is the length of a lowercase-hex sha256 digest string.
const SHA256HexLen = 64

// AttachmentRef is an immutable reference to an uploaded attachment blob.
// It is the value object embedded in cancellation/rejection domain events.
type AttachmentRef struct {
	id         string
	blobSHA256 string
	filename   string
	uploadedBy string
	uploadedAt time.Time
}

// NewAttachmentRef constructs an AttachmentRef after validating its fields.
// id is the per-reference primary key (a UUID); blobSHA256 is the
// lowercase-hex sha256 of the blob bytes; filename is the original filename
// supplied by the uploader (non-empty); uploadedBy is the actor id (may be
// empty for system-initiated operations); uploadedAt is the upload
// timestamp.
func NewAttachmentRef(id, blobSHA256, filename, uploadedBy string, uploadedAt time.Time) (AttachmentRef, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return AttachmentRef{}, ErrIDRequired
	}
	blobSHA256 = strings.ToLower(strings.TrimSpace(blobSHA256))
	if !IsValidSHA256(blobSHA256) {
		return AttachmentRef{}, ErrInvalidBlobSHA256
	}
	filename = strings.TrimSpace(filename)
	if filename == "" {
		return AttachmentRef{}, ErrFilenameRequired
	}
	if uploadedAt.IsZero() {
		return AttachmentRef{}, ErrUploadedAtRequired
	}
	return AttachmentRef{
		id:         id,
		blobSHA256: blobSHA256,
		filename:   filename,
		uploadedBy: strings.TrimSpace(uploadedBy),
		uploadedAt: uploadedAt.UTC(),
	}, nil
}

// MustAttachmentRef is a test helper that panics on error.
func MustAttachmentRef(id, blobSHA256, filename, uploadedBy string, uploadedAt time.Time) AttachmentRef {
	r, err := NewAttachmentRef(id, blobSHA256, filename, uploadedBy, uploadedAt)
	if err != nil {
		panic(err)
	}
	return r
}

// Accessors.
func (a AttachmentRef) ID() string            { return a.id }
func (a AttachmentRef) BlobSHA256() string    { return a.blobSHA256 }
func (a AttachmentRef) Filename() string      { return a.filename }
func (a AttachmentRef) UploadedBy() string    { return a.uploadedBy }
func (a AttachmentRef) UploadedAt() time.Time { return a.uploadedAt }

// Equal reports whether two AttachmentRefs refer to the same reference id.
func (a AttachmentRef) Equal(other AttachmentRef) bool { return a.id == other.id }

// IsValidSHA256 reports whether s is a lowercase-hex sha256 digest string
// (64 hex chars). Empty strings are NOT valid.
func IsValidSHA256(s string) bool {
	if len(s) != SHA256HexLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// HashBytes returns the lowercase-hex sha256 of the supplied bytes. Useful
// at the upload boundary to compute the blob digest before storing the ref.
func HashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ValidateBatch validates every ref in a slice and reports the index of the
// first invalid ref. Returns nil if the slice is empty or all refs are
// valid. This is used by command handlers that accept a list of attachment
// references (e.g. BOL cancellation).
func ValidateBatch(refs []AttachmentRef) error {
	for i, r := range refs {
		if r.id == "" {
			return fmt.Errorf("attachmentRefs[%d]: %w", i, ErrIDRequired)
		}
		if !IsValidSHA256(r.blobSHA256) {
			return fmt.Errorf("attachmentRefs[%d]: %w", i, ErrInvalidBlobSHA256)
		}
		if r.filename == "" {
			return fmt.Errorf("attachmentRefs[%d]: %w", i, ErrFilenameRequired)
		}
		if r.uploadedAt.IsZero() {
			return fmt.Errorf("attachmentRefs[%d]: %w", i, ErrUploadedAtRequired)
		}
	}
	return nil
}

// Errors.
var (
	ErrIDRequired         = errors.New("attachment ref id is required")
	ErrInvalidBlobSHA256  = errors.New("attachment blob sha256 must be a 64-char lowercase hex digest")
	ErrFilenameRequired   = errors.New("attachment filename is required")
	ErrUploadedAtRequired = errors.New("attachment uploadedAt is required")
)
