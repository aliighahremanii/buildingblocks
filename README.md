# buildingblocks

Reusable Go toolkit for event-sourced, CQRS, vertical-slice services.

Extracted from the YardFlow backend (a private product repository, not
published here), where it is the canonical toolkit for every module. This
repository is the source of truth for the toolkit; YardFlow consumes it as a
pinned module dependency.

- **Module path:** `github.com/aliighahremanii/buildingblocks`
- **Go:** `1.26.3`
- **License:** [Apache-2.0](LICENSE)
- **Visibility:** public — consumed anonymously through the standard Go module
  proxy (see [Fetching](#fetching))

**Versioned consumption:** the `v0.1.0` commands below apply once that version
has completed the [release procedure](docs/release.md). Check its
[GitHub Release](https://github.com/aliighahremanii/buildingblocks/releases/tag/v0.1.0)
and successful Release workflow before adopting it; these examples and local
candidate verification do not establish publication or public availability.

## What this is

A toolkit, not a framework. It contains small, explicit building blocks that a
host application composes itself:

- typed command/query handlers and middleware — **no** command bus, no handler
  registry, no reflection, no `Dispatch(ctx, any)`;
- an event-sourcing core over KurrentDB (EventStoreDB): aggregate stores,
  snapshots, upcasters, projection workers, checkpoints, a dead-letter queue;
- Postgres infrastructure: shared migration SQL, idempotency, DLQ, saga
  coordinator, read helpers, test helpers;
- NATS JetStream adapters and the required-streams catalog;
- Meilisearch client + settings reconciliation;
- presentation helpers: web router/context, problem-error mapping, OpenAPI
  builders, GraphQL protections.

Everything is dependency-injected through explicit constructors. Packages
define interfaces where they are consumed; infrastructure implements them.

The toolkit supplies reusable capabilities, not an application architecture
framework. For a new service, we recommend organizing business behavior as
vertical slices around use cases, using DDD concepts such as bounded contexts
and an explicit domain model where they fit the problem. This is guidance for
the consuming project: this module does not create your modules, aggregates,
business contracts, endpoints, or composition roots. See
[`docs/vertical-slice-ddd.md`](docs/vertical-slice-ddd.md) for the recommended
consumer-side structure and [`docs/architecture.md`](docs/architecture.md) for
the toolkit's boundaries and invariants.

## Packages

| Area | Packages |
|---|---|
| Application | `application/abstractions`, `application/attachments`, `application/commands`, `application/concurrency`, `application/correlation`, `application/errors`, `application/events`, `application/idempotency`, `application/queries` |
| Domain | `domain/attachments`, `domain/events`, `domain/i18n`, `domain/snapshot`, `domain/uid` |
| Event store | `infrastructure/eventstoredb`, `.../checkpoint`, `.../dlq`, `.../eventstore`, `.../kurrentexport`, `.../kurrenttest`, `.../natspub`, `.../projection`, `.../registry`, `.../replay`, `.../snapshot`, `.../upcaster` |
| Postgres | `infrastructure/postgres`, `.../attachments`, `.../dlq`, `.../idempotency`, `.../natspub`, `.../postgrestest`, `.../reads`, `.../saga`, `.../shared/migrations` |
| NATS | `infrastructure/nats/natsadapter` |
| Meilisearch | `infrastructure/meilisearch`, `.../settingsreconcile` |
| Other infra | `infrastructure/clock`, `infrastructure/envconfig`, `infrastructure/logging`, `infrastructure/metrics`, `infrastructure/objectstorage/rustfs`, `infrastructure/saga`, `infrastructure/tracing`, `infrastructure/webhttp` |
| Presentation | `presentation/auth`, `presentation/contracts`, `presentation/graphql`, `presentation/middleware`, `presentation/openapi`, `presentation/web` |

## Quick start

```bash
go get github.com/aliighahremanii/buildingblocks@v0.1.0
```

Run the compiling, dependency-free example with `go run ./examples/quickstart`.
Its source is the single source of truth for the typed command and middleware
API; it demonstrates explicit pipeline composition, validation, and typed
application errors without requiring a datastore. See
[`docs/adoption.md`](docs/adoption.md) for the integration path (migrations,
projections, search, deployment).

## Migrations

`infrastructure/postgres/shared/migrations` embeds the shared infrastructure
SQL (snapshots, command idempotency, projection DLQ, NATS published positions,
sagas, attachments) and exposes it as an `fs.FS`:

```go
import bbmigrations "github.com/aliighahremanii/buildingblocks/infrastructure/postgres/shared/migrations"

source := bbmigrations.FS() // fs.ReadDir(source, ".") -> the migration files
```

The SQL travels **inside your compiled binary** — a consumer's migrator image
does not need this module's source tree, and the migration set cannot silently
disappear from a shipped image. This package is a **source, not a runner**:
compose `FS()` with your own product and module migration directories and hand
the combined set to `golang-migrate`. The single `schema_migrations` ledger,
filename ordering, and duplicate-version semantics stay your responsibility.

Product-owned SQL must **not** be added here. Add it to your own repository.

## Fetching

This module is **public**. Consumers need no credential, no `GOPRIVATE` scope,
and no Git configuration — the standard toolchain fetches it:

```bash
go get github.com/aliighahremanii/buildingblocks@v0.1.0
```

Resolution is the ordinary public path:

- **Proxy.** `proxy.golang.org` serves the pinned version. Do not add a
  `GOPRIVATE`/`GONOPROXY`/`GONOSUMDB` entry for this module: an exemption would
  force a direct VCS fetch and skip the public checksum database for a module
  that needs neither, and it would make a consumer's build depend on a Git
  credential it does not otherwise need.
- **Checksums.** `sum.golang.org` verifies the version's hash and your
  committed `go.sum` pins it. Keep `go.sum` committed; never disable
  verification (`GOFLAGS=-insecure`, `GOSUMDB=off`, `GONOSUMCHECK`) to work
  around a mismatch — a mismatch means the version is not the one that was
  published.
- **No `replace`, no workspace.** Consume the exact published version. A
  `go.work` file or a `replace` directive in the *consumer's* module would
  substitute something else for it and invalidate the checksum guarantee; if
  you need a local override for development, keep it uncommitted and temporary.

After tag, module, and live integration checks pass, the release workflow proves
the same path before creating the GitHub Release: a fresh module
resolves the tag anonymously through the public proxy and the public checksum
database, with an empty private scope and a fresh module cache (see
[`docs/release.md`](docs/release.md)).

## Versioning

Consumers pin **exact** versions; there is no `replace` and no workspace
indirection. Releases are annotated tags plus GitHub releases — see
[`docs/release.md`](docs/release.md). Published versions are immutable: a moved
tag breaks every consumer's `go.sum`.

## Testing

```bash
go build ./...
go vet ./...
go test ./... -count=1
```

The datastore-backed suites (Postgres, KurrentDB, Meilisearch, NATS) auto-skip
when their env vars are unset, so a bare `go test ./...` is unit coverage only.
Skipped is not passed.

To run them for real, use the integration stack and the gate script:

```bash
bash script/integration-test.sh
```

It brings up Postgres, KurrentDB, Meilisearch and NATS
(`docker-compose.integration.yml`, public pinned images), waits for real
readiness, applies the shared migrations with the golang-migrate runner, then
runs the whole suite with `BUILDINGBLOCKS_REQUIRE_INTEGRATION=1` — which turns
every environment-gated skip into a **failure**, so a green run proves the live
suites ran. The same script is the `integration` job in CI and a required gate
in the release workflow. Package parallelism stays at Go's default: the suite
is written for one shared Postgres plus a cross-process advisory lock.

## Architecture and contribution guides

- [Architecture guide](docs/architecture.md): package boundaries, event
  replay, adapter lifecycle, and compatibility expectations.
- [Vertical-slice and DDD adoption guide](docs/vertical-slice-ddd.md): a
  recommended way to organize a consuming application without treating the
  toolkit as a framework.
- [Contributing](CONTRIBUTING.md): contributor verification and change
  guidelines.

## Scope notes

This module is deliberately generic. Two things are **not** in it:

1. **Product business contracts.** A product's analytical-contract catalog
   (family allowlists, payload envelopes, staged identifiers) is business
   domain, not toolkit. YardFlow keeps its own at
   `internal/buildingblocks/domain/reportingcontracts` — a documented
   extraction exception. As a consequence the contract-stream validator here
   (`infrastructure/nats/natsadapter.ContractStreamValidator`) validates a
   caller-supplied subject list and carries no catalog dependency; a consumer
   asserts its own catalog against it from its own test suite.
2. **Product-owned migration SQL and product lock policy.** Product SQL stays
   in the product. The shared-Postgres test lock *policy* scanner is
   product-owned (its allowlist names product test files); only the reusable
   `postgrestest` helper lives here.

### Known residual coupling

A small number of operator-facing and telemetry strings still carry the
original product's name (for example the default OTel tracer/service name
`yardflow` in `infrastructure/tracing`, and the contract-namespace example in
the `ContractStreamValidator` guidance text). These are preserved verbatim from
the extraction so behavior did not change. They are cosmetic for a generic
consumer except where a deployment relies on the telemetry identity; renaming
them would be an observable behavior change and is therefore deferred rather
than done silently.

## License

Apache-2.0. See [LICENSE](LICENSE).
