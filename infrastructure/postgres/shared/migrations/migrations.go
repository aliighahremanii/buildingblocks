// Package migrations embeds the REUSABLE BuildingBlocks Postgres migration
// SQL — the shared infrastructure tables every event-sourced deployment
// needs (es_snapshots, command idempotency, the projection DLQ, NATS
// published positions, sagas, attachments).
//
// The SQL is embedded rather than read from disk so a consuming binary
// (YardFlow's cmd/migrator) can compose these migrations WITHOUT shipping
// this module's source tree inside its runtime image: the bytes travel in
// the compiled binary.
//
// This package is a SOURCE, not a runner. Consumers compose FS() with their
// own module migration directories and hand the combined set to
// golang-migrate; the single schema_migrations ledger, filename ordering and
// duplicate-version semantics stay the consumer's responsibility (YardFlow:
// internal/hosts/db-migrator.go).
//
// Product-owned SQL (for example YardFlow's admin_purge_journal pair) must
// NOT be added here — it belongs to the product that owns the table.
package migrations

import (
	"embed"
	"io/fs"
)

// embedded holds every *.sql file in this directory. Migration file names
// follow golang-migrate's `<version>_<name>.<up|down>.sql` convention and are
// preserved byte-for-byte, including their version prefixes.
//
//go:embed *.sql
var embedded embed.FS

// FS returns the embedded migration SQL as a read-only filesystem rooted at
// this package's directory, so `fs.ReadDir(FS(), ".")` yields the migration
// file names in lexical order exactly as `os.ReadDir` would for the original
// on-disk directory.
func FS() fs.FS {
	return embedded
}
