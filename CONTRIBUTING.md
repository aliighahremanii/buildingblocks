# Contributing

BuildingBlocks is a public Go module. Changes can affect independent consumer
projects, so keep package boundaries, explicit composition, and compatibility
in view. This guide is for maintainers and contributors; it does not grant
permission to publish or release a version.

## Before changing a package

1. Read the package documentation and nearby tests before changing behavior.
   Use the [architecture guide](docs/architecture.md) to understand package
   responsibilities and the [adoption guide](docs/adoption.md) for consumer
   integration contracts.
2. Trace the public API and its consumers. Keep concrete business types and
   product contracts in the consuming application; do not add YardFlow
   business models or product-owned SQL to this generic module.
3. Preserve typed command/query boundaries and explicit wiring. Avoid a global
   bus, arbitrary `any` dispatch, reflection-based handler lookup, or
   implicit runtime discovery.
4. Keep adapters and configuration explicit. Do not silently change defaults,
   timeout behavior, resource ownership, or whether a dependency may be
   absent. Where an adapter supports optional infrastructure, ensure an absent
   dependency cannot lead to a nil dereference and test both configured and
   absent behavior.
5. Update the relevant README or `docs/` page when a public API, behavior,
   adapter contract, or supported workflow changes. Do not document behavior
   that the source and tests do not establish.

## Local verification

The repository uses Go `1.26.3` (see `go.mod`). Run the same core checks used
by CI:

```bash
gofmt -l .
go mod tidy
git diff --exit-code -- go.mod go.sum
go build ./...
go vet ./...
go test ./... -count=1 -timeout 300s
```

`gofmt -l .` should print no files. `go mod tidy` should not leave an
uncommitted change to `go.mod` or `go.sum`. The ordinary test run includes
unit coverage, but datastore-backed tests auto-skip when their service
configuration is absent; record skips as skips, not as integration passes.

For changes that depend on a live service, run the full integration gate:

```bash
bash script/integration-test.sh
```

It requires Docker with the Compose plugin, Go, and `curl`. The script starts
the checked-in integration stack, waits for service readiness, applies the
embedded shared migrations, runs tests with
`BUILDINGBLOCKS_REQUIRE_INTEGRATION=1`, then tears its Compose project down.
That setting makes an environment-gated integration skip fail. Keep Go's
default package parallelism; the shared-Postgres tests use an advisory lock to
coordinate concurrent package test binaries.

## Compatibility and releases

The module path and exported package APIs are part of the consumer contract.
Prefer additive changes and preserve source and data compatibility. If an
event payload or migration contract must change, document the compatibility
and upgrade path and test old data on the read path before release. Avoid
changing a published tag: module versions are immutable. The exact version
scheme, workflow gates, and publishing procedure are in
[Releasing](docs/release.md).

## Documentation and examples

Keep `examples/quickstart` as the source of truth for the compiling command
pipeline example. When integration guidance changes, update
[`docs/adoption.md`](docs/adoption.md); when package responsibilities or
architecture constraints change, update [`docs/architecture.md`](docs/architecture.md).
Use README links to guide consumers to detailed topics rather than duplicating
large code examples. Do not copy secrets, credentials, or environment-specific
secret values into docs or examples.
