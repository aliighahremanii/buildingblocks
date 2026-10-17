# Architecture guide

BuildingBlocks is a library of small Go packages, not a service framework. It
provides reusable handlers, event-sourcing infrastructure, adapters, and
presentation helpers; the consuming application owns its business types,
composition roots, endpoints, deployment, and product contracts. The package
map in the [README](../README.md#packages) is the index of the public areas.

## Package boundaries

The packages are grouped by responsibility:

| Area | Responsibility |
|---|---|
| `domain/` | Domain event recording, snapshot envelopes, identifiers, internationalized strings, and attachment references. `domain/events` and `domain/snapshot` are standard-library-only packages. |
| `application/` | Typed command/query handler contracts and middleware, application error types, concurrency expectations, correlation, and reusable ports. |
| `infrastructure/` | KurrentDB event storage and projections, Postgres adapters and migration source, plus optional integrations such as NATS, Meilisearch, object storage, and telemetry. |
| `presentation/` | HTTP/web primitives, authentication context, problem contracts and mapping, OpenAPI helpers, and GraphQL protections. |

This division describes package responsibilities, not a module layout imposed
on consumers. In a consuming application, keep domain policy independent of
transport and persistence details; put use-case orchestration in application
code; implement application-facing ports in infrastructure; and translate
requests and errors at the presentation boundary. Composition should happen in
the host/application, using explicit constructors and dependencies rather than
package discovery.

## Typed use-case composition

`application/commands.Handler[C, R]` and
`application/queries.Handler[Q, R]` carry concrete input and result types.
Their corresponding `Middleware` types wrap a handler of the same types, and
`Chain` composes those wrappers explicitly. The host chooses the handler and
middleware order and invokes the resulting handler; there is no built-in global
bus or handler-registration mechanism. The compiling
[`examples/quickstart`](../examples/quickstart) demonstrates the command path;
the package source is the authoritative API reference.

Keep each consumer use case's command/query, result, and handler types close to
that use case. Do not route arbitrary values through a generic envelope or
introduce reflection-based handler lookup. The toolkit has no consumer module
registry and does not decide how a host divides its business capabilities; see
the [vertical-slice and DDD guide](vertical-slice-ddd.md).

## Events, replay, and projections

The toolkit's event-sourcing packages provide shared mechanics; concrete
aggregate behavior and event shapes remain owned by the consuming application.
`domain/events.Recorder` records events raised by aggregate behavior methods.
The KurrentDB-backed `infrastructure/eventstoredb/eventstore` package appends
and reads those event values and supplies the generic
`LoadAggregate`/`SaveAggregate` helpers. Callers provide the aggregate-specific
rehydration behavior; the generic store does not use reflection to reconstruct
the consumer's aggregate type.

Event type factories are registered explicitly in
`infrastructure/eventstoredb/registry`. When an upcaster registry is attached,
stored event payloads are upgraded before decoding into the current event
shape. The event-store read helper restores event identity and timing fields
carried in the KurrentDB envelope by calling `domain/events.Enrich`. Preserve
this decode, upcast, and enrich path when adding an adapter or alternate read
path; replay must use the same event interpretation as live writes.

Snapshots are an optimization around replay, not a replacement for the event
stream. `domain/snapshot` defines the JSON envelope, while the event-store
snapshot adapter persists and loads snapshot bytes. A consumer supplies
aggregate-specific state serialization and reconstruction. Event schema
versions and snapshot envelope versions are separate compatibility concerns;
read the package documentation and the consumer's migration policy before
changing either shape.

The generic projection worker subscribes to KurrentDB `$all`, filters by the
configured stream prefixes, and invokes a consumer-supplied `ProjectFunc` for
matching events. The projection's sink-specific writes stay in that callback.
The worker retries failed projection work and, when configured with a DLQ,
records an exhausted event and advances its checkpoint together. Projectors
must be idempotent because an event can be seen again after a restart or
replay. Read `infrastructure/eventstoredb/projection` for its lifecycle,
configuration, and checkpoint details before changing worker behavior.

## Adapters, configuration, and lifecycle

There is no universal client configuration or lifecycle API across all
adapters. A consumer constructs the integrations it uses and passes them to
explicit package constructors. Follow each adapter's own configuration types,
timeouts, and close/ownership contract; do not infer one adapter's defaults
from another. Keep optional infrastructure optional only when the consuming
application has a defined behavior for its absence. If a dependency can be
absent, construction and request/worker paths should fail safely or omit that
dependent surface rather than dereference a nil client. Test both configured
and absent cases where absence is supported.

`infrastructure/envconfig.Value` reads `KEY_FILE` when that setting is
configured, otherwise it reads `KEY`; file contents are trimmed. It is a
single-value helper, not a complete configuration system. Do not put secret
values in documentation, examples, or test output.

The shared Postgres migration package embeds its SQL and exposes it as an
`fs.FS` through `infrastructure/postgres/shared/migrations.FS()`. It is a
migration **source**, not a runner. The consumer composes it with product and
module migration sources and owns the combined migration runner, ledger,
ordering, and globally unique version policy. Keep product-owned tables and
business SQL in the consuming repository.

## Error and presentation boundary

Application errors such as validation, not-found, conflict, concurrency,
authorization, and availability errors are typed values in
`application/errors`. Infrastructure adapters should preserve actionable
application errors and return operational failures as errors; presentation
code decides how application errors become an HTTP or other protocol
response. Use the helpers in `presentation/` where they fit instead of making
domain or application packages depend on HTTP status codes or response types.
The [quickstart](../examples/quickstart) shows typed error inspection without
binding the application handler to HTTP.

## Verification and change expectations

The repository's CI workflow checks formatting, module tidiness, build, vet,
and tests. Its datastore-backed tests skip when the required services are not
configured, so a unit-only pass is not evidence that an adapter works against
its real service. The
[`script/integration-test.sh`](../script/integration-test.sh) gate provisions
Postgres, KurrentDB, Meilisearch, and NATS and makes environment-gated skips
fail. For exact local commands and prerequisites, see
[`CONTRIBUTING.md`](../CONTRIBUTING.md).

Treat exported types and behavior as a public Go module contract. Before
changing an API, event payload, migration source, or adapter lifecycle, check
its consumers and tests, preserve backward compatibility where possible, and
document any externally visible change. Published versions are immutable; use
the versioning and release process in [Releasing](release.md).
