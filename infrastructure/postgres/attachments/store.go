// Package attachments is the Postgres-backed implementation of the
// attachments.RefStore port over the attachment_blobs + attachment_refs
// tables (master plan §5.3).
package attachments

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	bbattachments "github.com/aliighahremanii/buildingblocks/application/attachments"
	apperrors "github.com/aliighahremanii/buildingblocks/application/errors"
	domainattach "github.com/aliighahremanii/buildingblocks/domain/attachments"
)

// Store implements bbattachments.RefStore against Postgres.
type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// InsertBlob inserts a blob metadata row. Idempotent on sha256 (ON CONFLICT
// DO NOTHING — the first uploader wins; subsequent identical-sha uploads reuse
// the same blob).
func (s *Store) InsertBlob(ctx context.Context, sha256 string, size int64, mime, objectKey, bucket string) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO attachment_blobs (sha256, size, mime, object_key, bucket)
		VALUES ($1,$2,$3,$4,$5) ON CONFLICT (sha256) DO NOTHING`,
		sha256, size, mime, objectKey, bucket)
	if err != nil {
		return fmt.Errorf("insert attachment blob: %w", err)
	}
	return nil
}

// InsertRef inserts a per-upload reference row.
func (s *Store) InsertRef(ctx context.Context, ref domainattach.AttachmentRef) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO attachment_refs (id, blob_sha256, filename, uploaded_by, uploaded_at)
		VALUES ($1,$2,$3,$4,$5) ON CONFLICT (id) DO NOTHING`,
		ref.ID(), ref.BlobSHA256(), ref.Filename(), ref.UploadedBy(), ref.UploadedAt())
	if err != nil {
		return fmt.Errorf("insert attachment ref: %w", err)
	}
	return nil
}

// GetRef returns a single reference by its id, joined with blob metadata.
func (s *Store) GetRef(ctx context.Context, id string) (bbattachments.RefRecord, error) {
	var rid, sha, filename, uploadedBy string
	var uploadedAt time.Time
	var size int64
	var mime string
	err := s.pool.QueryRow(ctx, `SELECT r.id, r.blob_sha256, r.filename, r.uploaded_by, r.uploaded_at,
		b.size, b.mime
		FROM attachment_refs r
		JOIN attachment_blobs b ON b.sha256 = r.blob_sha256
		WHERE r.id = $1`, id).Scan(&rid, &sha, &filename, &uploadedBy, &uploadedAt, &size, &mime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return bbattachments.RefRecord{}, apperrors.NewNotFoundError("attachment", id)
		}
		return bbattachments.RefRecord{}, fmt.Errorf("get attachment ref: %w", err)
	}
	att, _ := domainattach.NewAttachmentRef(rid, sha, filename, uploadedBy, uploadedAt)
	return bbattachments.RefRecord{AttachmentRef: att, Size: size, Mime: mime}, nil
}

// ListRefs returns the references for a list of ids.
func (s *Store) ListRefs(ctx context.Context, ids []string) ([]bbattachments.RefRecord, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `SELECT r.id, r.blob_sha256, r.filename, r.uploaded_by, r.uploaded_at,
		b.size, b.mime
		FROM attachment_refs r
		JOIN attachment_blobs b ON b.sha256 = r.blob_sha256
		WHERE r.id = ANY($1)`, ids)
	if err != nil {
		return nil, fmt.Errorf("list attachment refs: %w", err)
	}
	defer rows.Close()
	var out []bbattachments.RefRecord
	for rows.Next() {
		var rid, sha, filename, uploadedBy string
		var uploadedAt time.Time
		var size int64
		var mime string
		if err := rows.Scan(&rid, &sha, &filename, &uploadedBy, &uploadedAt, &size, &mime); err != nil {
			return nil, fmt.Errorf("list attachment refs scan: %w", err)
		}
		att, _ := domainattach.NewAttachmentRef(rid, sha, filename, uploadedBy, uploadedAt)
		out = append(out, bbattachments.RefRecord{AttachmentRef: att, Size: size, Mime: mime})
	}
	return out, rows.Err()
}
