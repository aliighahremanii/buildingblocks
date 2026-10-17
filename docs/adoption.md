# Adoption guide

How a service adopts this toolkit. The reference consumer is the YardFlow
backend, from which the module was extracted; this guide describes the shape
that worked there without depending on any YardFlow-specific detail.

For a consumer-side architecture recommendation, see
[Vertical slices and DDD](vertical-slice-ddd.md). For the toolkit's package
boundaries and replay invariants, see the [architecture guide](architecture.md).

## 1. Add the dependency

```bash
go get github.com/aliighahremanii/buildingblocks@v0.1.0
```

The module is **public**, so the fetch is anonymous: the standard public module
proxy serves it and the public checksum database verifies it — see
[Fetching](../README.md#fetching). No credential, no `.netrc`, no BuildKit
secret and no `GOPRIVATE`-style exemption is needed or wanted. Pin an exact
version and do not add a `replace` to the committed `go.mod`; the integrity
record for this module is your committed `go.sum`.

## 2. Keep the Go module scope at its public default

```bash
export GOWORK=off   # optional: never let a stray workspace substitute a local directory
```

Do **not** add this module to `GOPRIVATE`, `GONOPROXY`, `GONOSUMDB`, or
`GOINSECURE`. An exemption forces a direct VCS fetch and skips the public
checksum database for a module that needs neither, and it turns an ordinary
anonymous `go mod download` into one that depends on a Git credential your
build does not otherwise have. Keep `GOSUMDB` at its default
(`sum.golang.org`); never work around a checksum mismatch with
`GOSUMDB=off`, `GONOSUMCHECK`, or `GOFLAGS=-insecure` — a mismatch means the
version is not the published one.

## 3. Compose migrations

This module ships the shared infrastructure SQL as an embedded filesystem:

```go
import bbmigrations "github.com/aliighahremanii/buildingblocks/infrastructure/postgres/shared/migrations"
```

Compose it with your own product SQL and your module migration directories into
**one** combined source, and hand that to `golang-migrate`. Requirements:

- **One ledger.** All sources share a single `schema_migrations` table and a
  single global version namespace. Migration versions must be globally unique
  across every source; a duplicate `(version, direction)` is a hard error.
- **Lexical filename order** is the apply order. Use the
  `<version>_<name>.<up|down>.sql` convention.
- **Fail on an empty source.** Do not treat "no files found" as success. That
  is the silent-failure class where a migrator exits 0 having created nothing.
  Validate at build time that every configured source actually exists and
  contains files.
- **Embed what must not be lost.** Anything that has to reach production — the
  reusable shared set, your product SQL — should travel inside the binary
  rather than being read from a path that an image build might omit.
- **Never scan the module cache.** Read the embedded `FS()`; do not go looking
  for the module's source directory on disk.

Sketch:

```go
type source struct {
    name string
    fsys fs.FS   // embedded
    dir  string  // on-disk module tree
}

// 1. collect sources (embedded shared set first, then your product set, then module dirs)
// 2. list files from each; error if any source yields none
// 3. sort all file names lexically
// 4. reject duplicate (version, direction)
// 5. copy every file into one temp dir and point golang-migrate at it
```

## 4. Wire the event store

The write model is KurrentDB. Per aggregate:

- embed `events.Recorder` and record domain events from behavior methods;
- implement `Apply(events.Event)` for replay — state restoration **without**
  validation;
- implement `NewFromEvents(id, []events.Event)` for stream reconstruction;
- implement `MarshalState()`/`UnmarshalState()` for snapshots.

Write-side stores expose exactly two methods — `Save(ctx, *Aggregate)` and
`ByID(ctx, id) (*Aggregate, error)`. Do not add `Update`, `Delete`, or `List`:
reads are read-side concerns served by denormalized queries.

### Event schema versioning

A breaking event-shape change (rename, retype, removal, semantic shift)
requires all of:

1. widen the event struct to the new shape;
2. register a `v(N)->v(N+1)` upcaster in your module's event-store registry;
3. bump `events.CurrentSchemaVersion`;
4. declare the upcaster's target for **every** event type your module
   registers — not only the widened one. A type left at the default target is
   rejected on read by the forward-incompatibility guard.

Existing streams need no migration: upcasters transform old payloads on read.

## 5. Run projections

Every module runs projection workers that subscribe to `$all` and project to
Postgres (mirror tables) and Meilisearch (search indexes).

- Call the upcaster before any inline `json.Unmarshal`, so v1 events project
  cleanly.
- Write all Postgres effects of one event in a **single transaction**; a
  mid-event failure must leave no partial read model.
- Bind empty optional UUID strings as SQL `NULL` (`NULLIF($n,'')::uuid`) and
  coalesce on read — an empty string bound into a `uuid` column is a
  dead-letter-on-first-event defect.
- **Nil-safety:** create each sink worker only when its client is non-nil. A
  nil client must log and skip, never construct a worker that panics on its
  first event. The same rule applies to optional HTTP, admin, poller and
  background dependencies.

## 6. Test against real infrastructure

```bash
go build ./...
go vet ./...
go test ./... -count=1
```

Datastore-backed suites auto-skip without their env vars. **A skip is not a
pass.**

For integration evidence, run the module's own gate, which provisions real
services and makes a skip fatal:

```bash
bash script/integration-test.sh
```

It starts `docker-compose.integration.yml` (public pinned images), waits for
readiness, applies the shared migrations with the golang-migrate runner, runs
the suite with `BUILDINGBLOCKS_REQUIRE_INTEGRATION=1`, and tears the stack down
again. Reproduce the same shape in your own pipeline: provision the datastores,
prove they are ready, apply migrations, then run the suite with the switch set
so a missing datastore is a failure rather than a silent green.

### Shared-database concurrency contract

If your suite runs at Go's default package parallelism against one shared
Postgres — and it should, because that is how CI runs — then every test that
opens a pool and mutates shared read-model tables must hold a cross-process
advisory lock across its mutation window:

```go
import "github.com/aliighahremanii/buildingblocks/infrastructure/postgres/postgrestest"

pool := postgrestest.OpenPool(t)
postgrestest.Lock(t, pool) // in-process mutex + advisory lock, held until the test ends
```

Ownership-scoped writes to rows no other suite truncates, read-only usage, and
suites behind an opt-in build tag are the legitimate lock-free exceptions —
record each one with a reason in a reviewed allowlist so a new lock-free caller
cannot land silently.

`postgrestest` is a **reusable helper**. The *policy* that decides which files
must take the lock is product-owned: its allowlist names your test files, so
keep that scanner in your own repository.

## 7. Optional: search and messaging

- **Meilisearch.** Create each index explicitly before the first document
  mutation, verifying the primary key on adopt. Index **settings** belong to an
  operator reconcile command, not to a worker. Every client must carry an
  overall per-request timeout — a request that is accepted but never answered
  otherwise blocks a projection worker's goroutine forever.
- **NATS JetStream.** Treat stream provisioning as deployment-owned and
  fail-closed: never mutate or delete an existing stream, and create a missing
  stream only when its durable state is demonstrably pristine. Validate
  read-only that a stream exists and covers every subject you can emit before
  the first subscription; a missing or misfiltered stream otherwise
  crash-loops the publisher.

## 8. Deployment checklist

- [ ] Exact module version pinned; no `replace`, no `go.work`.
- [ ] Module scope at its PUBLIC default: no `GOPRIVATE`/`GONOPROXY`/
      `GONOSUMDB`/`GOINSECURE` entry for this module, `GOSUMDB` untouched, and
      `go mod download` runs anonymously through the public proxy — no
      credential, no BuildKit secret, no `ARG`/`ENV` token, no `COPY`ed
      credential file, no credential-bearing URL.
- [ ] Migrations composed from the embedded shared set + your product set +
      module trees, with the duplicate-version and empty-source checks active.
- [ ] A source that yields no migration files fails the build.
- [ ] Datastore-backed tests actually ran (not skipped) before release.

## Known residual coupling

A few operator-facing and telemetry strings still carry the original product's
name (notably the default OTel tracer/service name and the contract-namespace
example in the contract-stream validator's guidance text). They were preserved
verbatim so behavior did not change; see the note in the
[README](../README.md#known-residual-coupling).
