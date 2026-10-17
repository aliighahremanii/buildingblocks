// Package attachments defines the application ports for the shared
// attachment subsystem (master plan §5.3): BlobStore (external object store)
// and RefStore (Postgres metadata). Domain types live in
// buildingblocks/domain/attachments.
package attachments

import (
	"context"
	"io"
	"time"

	"github.com/aliighahremanii/buildingblocks/domain/attachments"
)

// BlobStore is the port for an external object store (RustFS / S3-compatible)
// that holds attachment blob bytes.
type BlobStore interface {
	// Put stores blob bytes under the object key = sha256 hex. Idempotent: a
	// second Put with the same sha256 is a no-op (or overwrites, which is
	// safe since the content is content-addressed).
	Put(ctx context.Context, sha256 string, contentType string, size int64, reader io.Reader) error
	// Get retrieves blob bytes by sha256.
	Get(ctx context.Context, sha256 string) (io.ReadCloser, error)
	// Exists reports whether a blob with the given sha256 is already stored.
	Exists(ctx context.Context, sha256 string) (bool, error)
}

// RefRecord is the stored form of an AttachmentRef (with size + mime from the
// blob row).
type RefRecord struct {
	attachments.AttachmentRef
	Size int64
	Mime string
}

// RefStore is the Postgres-backed store for attachment reference metadata
// (attachment_blobs + attachment_refs tables).
type RefStore interface {
	// InsertBlob inserts a blob metadata row (dedup by sha256; idempotent).
	InsertBlob(ctx context.Context, sha256 string, size int64, mime, objectKey, bucket string) error
	// InsertRef inserts a per-upload reference row.
	InsertRef(ctx context.Context, ref attachments.AttachmentRef) error
	// GetRef returns a single reference by its id.
	GetRef(ctx context.Context, id string) (RefRecord, error)
	// ListRefs returns the references for a list of ids (used to populate
	// cancellation attachment context in detail views).
	ListRefs(ctx context.Context, ids []string) ([]RefRecord, error)
}

// UploadedBy is a convenience type for the actor identity carried through the
// upload pipeline.
type UploadedBy struct {
	UserID string
	Now    time.Time
}
