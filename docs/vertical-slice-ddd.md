# Vertical slices and DDD in a consuming application

This guide recommends a way to organize a project that uses BuildingBlocks; it
does not describe packages supplied by the toolkit. BuildingBlocks provides
typed handlers and reusable infrastructure, but it does not create business
modules, domain models, endpoints, or a global application framework.

For a new service, consider organizing code around business capabilities and
use cases rather than around technical CRUD layers. DDD concepts such as
bounded contexts, aggregates, value objects, and domain events are useful when
they make the domain's rules explicit. Apply them to the problem at hand; this
library does not require every consumer to adopt every package or pattern.

## A useful consumer-side shape

One possible layout is:

```text
internal/<context>/
  domain/<aggregate>/
  application/<use-case>/
  infrastructure/<adapter>/
  presentation/<feature>/
  module.go                 # explicit composition for this context
```

Treat this as an organizing example, not a toolkit path convention. Keep a
use-case package cohesive: its typed command or query, result, handler, and
focused tests should be easy to find together. A business intent such as
registering or activating an entity is usually clearer than a generic CRUD
package when the operation has its own rules.

## Keep responsibilities explicit

- **Domain:** own invariants and state transitions in the domain model. Keep
  business rules independent of HTTP, SQL, KurrentDB client types, and other
  infrastructure details.
- **Application:** coordinate a use case, invoke the domain model, and define
  the ports the use case consumes. Keep commands, results, and handlers typed.
- **Infrastructure:** implement those ports with the selected stores, clients,
  and external services. Keep vendor-specific translation at this boundary.
- **Presentation:** bind protocol requests to typed application inputs and map
  results and errors back to protocol responses.
- **Composition:** wire concrete adapters, handlers, and middleware explicitly
  in the application or host. Avoid runtime scanning and implicit
  registration.

This separation is a consumer architecture recommendation. BuildingBlocks
supplies reusable pieces for parts of it—such as `application/commands`,
`application/queries`, `domain/events`, `infrastructure/eventstoredb`, and
`presentation/web`—but consumers own their feature packages and the connections
between them. The toolkit does not provide a global command/query bus,
reflection-driven handler registry, or consumer-specific module topology.

## Typed pipelines

Use `commands.Handler[C, R]` or `queries.Handler[Q, R]` for a concrete use case
and compose compatible middleware with the matching `Chain` function at the
composition root. Keep endpoint code focused on protocol translation rather
than constructing middleware pipelines. The exact compiling API example is
[`examples/quickstart`](../examples/quickstart); use it instead of copying a
second, potentially stale example into this guide.

## If the application uses event sourcing

The toolkit provides event recording, KurrentDB storage and replay helpers,
snapshot support, event upcasting, and a projection worker. The consuming
application still owns its aggregate behavior, event structs, event registry
configuration, rehydration logic, read models, and projector callbacks. Keep
the event stream as the source for aggregate reconstruction, and make
projectors idempotent to tolerate replay. See the [architecture guide](architecture.md)
and [adoption guide](adoption.md) for the relevant integration details.

## Test the boundaries

Test domain rules without infrastructure, exercise use-case branches through
their ports, test protocol mapping at presentation boundaries, and use the
toolkit's integration gate when a change depends on a real datastore or client.
When a use case supports optional infrastructure, cover both the configured
path and the defined absent-dependency behavior. Keep the distinction between
skipped and passed integration tests visible; the repository's
[contributor guide](../CONTRIBUTING.md) describes the available verification
commands.
