// Package rustfs implements the attachments.BlobStore port against a
// RustFS (S3-compatible) object store via the minio-go/v7 client.
//
// Configuration is environment-driven:
//
//	RUSTFS_ENDPOINT      — e.g. "rustfs:9000" (no scheme)
//	RUSTFS_ACCESS_KEY    — access key
//	RUSTFS_SECRET_KEY    — secret key
//	RUSTFS_USE_SSL       — "true" or "false" (default false)
//	ATTACHMENTS_BUCKET   — bucket name (auto-created on first use)
//
// When RUSTFS_ENDPOINT is empty, Store is nil and all attachment operations
// auto-skip (matching the DSN auto-skip pattern used by Postgres/KurrentDB).
package rustfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	bbattachments "github.com/aliighahremanii/buildingblocks/application/attachments"
	"github.com/aliighahremanii/buildingblocks/infrastructure/envconfig"
)

// Config holds the RustFS/S3 connection parameters.
type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	UseSSL    bool
	Bucket    string
}

// ConfigFromEnv builds a Config from the environment. Returns ok=false when
// RUSTFS_ENDPOINT is unset (the store should auto-skip).
func ConfigFromEnv() (Config, bool, error) {
	endpointValue, err := envconfig.Value("RUSTFS_ENDPOINT")
	if err != nil {
		return Config{}, false, err
	}
	endpoint := strings.TrimSpace(endpointValue)
	if endpoint == "" {
		return Config{}, false, nil
	}
	accessKey, err := envconfig.Value("RUSTFS_ACCESS_KEY")
	if err != nil {
		return Config{}, false, err
	}
	secretKey, err := envconfig.Value("RUSTFS_SECRET_KEY")
	if err != nil {
		return Config{}, false, err
	}
	useSSL, err := envconfig.Value("RUSTFS_USE_SSL")
	if err != nil {
		return Config{}, false, err
	}
	return Config{
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
		UseSSL:    strings.EqualFold(useSSL, "true"),
		Bucket:    envOr("ATTACHMENTS_BUCKET", "yardflow-attachments"),
	}, true, nil
}

func envOr(key, def string) string {
	value, _ := envconfig.Value(key)
	if v := strings.TrimSpace(value); v != "" {
		return v
	}
	return def
}

// Store implements bbattachments.BlobStore against RustFS/S3.
type Store struct {
	client *minio.Client
	bucket string
}

// New constructs a Store from the given Config, ensuring the bucket exists.
// Returns (nil, nil) when the endpoint is empty — callers should auto-skip.
func New(cfg Config) (*Store, error) {
	if cfg.Endpoint == "" {
		return nil, nil
	}
	cli, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("rustfs client: %w", err)
	}
	ctx := context.Background()
	exists, err := cli.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("rustfs bucket exists check: %w", err)
	}
	if !exists {
		if err := cli.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("rustfs create bucket: %w", err)
		}
		slog.Info("rustfs: created attachment bucket", "bucket", cfg.Bucket)
	}
	return &Store{client: cli, bucket: cfg.Bucket}, nil
}

// Put stores blob bytes content-addressed by sha256. Idempotent.
func (s *Store) Put(ctx context.Context, sha256, contentType string, size int64, reader io.Reader) error {
	if s == nil {
		return errors.New("rustfs: store is nil (RUSTFS_ENDPOINT unset)")
	}
	_, err := s.client.PutObject(ctx, s.bucket, sha256, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("rustfs put %s: %w", sha256, err)
	}
	return nil
}

// Get retrieves blob bytes by sha256.
func (s *Store) Get(ctx context.Context, sha256 string) (io.ReadCloser, error) {
	if s == nil {
		return nil, errors.New("rustfs: store is nil (RUSTFS_ENDPOINT unset)")
	}
	obj, err := s.client.GetObject(ctx, s.bucket, sha256, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("rustfs get %s: %w", sha256, err)
	}
	// Probe to surface not-found immediately.
	if _, err := obj.Stat(); err != nil {
		obj.Close()
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("rustfs blob %s not found", sha256)
		}
		return nil, fmt.Errorf("rustfs stat %s: %w", sha256, err)
	}
	return obj, nil
}

// Exists reports whether a blob is already stored.
func (s *Store) Exists(ctx context.Context, sha256 string) (bool, error) {
	if s == nil {
		return false, errors.New("rustfs: store is nil (RUSTFS_ENDPOINT unset)")
	}
	_, err := s.client.StatObject(ctx, s.bucket, sha256, minio.StatObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode == http.StatusNotFound {
			return false, nil
		}
		return false, fmt.Errorf("rustfs stat %s: %w", sha256, err)
	}
	return true, nil
}

// DeleteObject removes one blob object by its EXACT bucket and object key.
// This is NOT part of the application BlobStore port (the product runtime
// never deletes blobs); it exists for ownership-proven operational cleanup —
// the one-time purge (cmd/admin production-purge-execute, D095), which
// deletes exclusively purge-owned objects by their PLANNED bucket/object key
// (never a re-derived key). A missing object is a no-op (idempotent).
func (s *Store) DeleteObject(ctx context.Context, bucket, objectKey string) error {
	if s == nil {
		return errors.New("rustfs: store is nil (RUSTFS_ENDPOINT unset)")
	}
	err := s.client.RemoveObject(ctx, bucket, objectKey, minio.RemoveObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode == http.StatusNotFound {
			return nil // already gone — idempotent
		}
		return fmt.Errorf("rustfs delete %s/%s: %w", bucket, objectKey, err)
	}
	return nil
}

// Compile-time assertion.
var _ bbattachments.BlobStore = (*Store)(nil)
