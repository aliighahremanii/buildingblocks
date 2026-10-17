# Releasing

Consumers pin this module by **exact version**. A published version is
immutable — treat the steps below as a one-way operation.

The `v0.1.0` commands in this repository are conditional examples, not a claim
that the version is published. Confirm the version's GitHub Release and a
successful Release workflow before adopting it. Local preparation and tests do
not create a tag or release and do not prove public-proxy availability.

## Version scheme

Annotated tags of the form `vMAJOR.MINOR.PATCH`, optionally with a pre-release
suffix (`v0.2.0-rc.1`). Lightweight tags are rejected by the release workflow
because they carry no tagger identity or date.

Because consumers import this module directly, a breaking change means a major
bump. While the module is at `v0.x`, a minor bump is the breaking-change signal
(Go's own convention for `v0`).

## What makes a version immutable

A consumer's first fetch records the version's hash and every later build
verifies it. What caches a `(module, version)` pair for THIS module:

1. **`go.sum` in every consumer** — the recorded `h1:` hash is verified on
   every build, for every consumer, forever.
2. **The public module proxy** (`proxy.golang.org`) — it caches the version's
   `.zip`/`.mod` on first fetch and serves that cached copy to every later
   request.
3. **The public checksum database** (`sum.golang.org`) — it records the hash of
   the first version it is asked about and refuses to corroborate a different
   one.

All three participate because the module is public: it is resolved through the
default proxy with checksum verification, with no `GOPRIVATE`/`GONOPROXY`/
`GONOSUMDB` exemption and no direct VCS fetch. That is why the release workflow
proves fetchability the same way a consumer fetches — anonymously through the
public proxy and the public checksum database, in a fresh module cache — and
why it asserts the private scope is empty before fetching: a proxy-verified
proof is the consumer contract, and a proof taken under a private exemption
would be vacuous.

Moving, re-creating, or deleting a tag after publication therefore produces a
checksum mismatch in every consumer. The remedy is always **a new version**,
never a re-tag.

### Enabling the actual protection

GitHub does not prevent tag moves by itself. The release workflow *refuses* to
republish an existing release, but the mechanism that makes a moved tag
impossible is a repository **ruleset**:

- Target: tags matching `v*`
- Rules: restrict **creation**, **update** (force-push/move), and **deletion**
- Bypass: the release actor only

Configure this before publishing the first version. Without it, a moved tag is
a human-error incident, not a guarded invariant.

## Release procedure

1. Ensure `main` is green — the `CI` workflow runs build, vet, gofmt, tidy and
   unit tests, and a separate `integration` job runs the datastore-backed
   suites against real services. Both must pass; the unit job alone is not
   integration evidence.
2. Confirm `go.mod` declares `module github.com/aliighahremanii/buildingblocks`
   and contains **no** `replace` directive, and that no `go.work` file exists.
3. Review the curated notes in `docs/releases/<tag>.md` (for example,
   [`docs/releases/v0.1.0.md`](releases/v0.1.0.md)) on the release commit. Each
   version requires a non-empty notes file with an exact `# <tag>` heading;
   the workflow does not generate notes. Update version examples in docs as
   appropriate without claiming publication before the workflow completes.
4. Create an **annotated** tag on the release commit:

   ```bash
   git tag -a v0.1.0 -m "v0.1.0"
   git push origin v0.1.0
   ```

5. The `Release` workflow then:
   - validates the tag shape (`vMAJOR.MINOR.PATCH`), annotation, and that the
     tagged commit is reachable from `main`;
    - refuses to proceed if a GitHub release already exists for the tag, or if
      the API cannot establish its absence from a complete release listing;
   - re-verifies the module at the tagged commit (module path, no
      replace/workspace, curated notes, gofmt, tidy, build, vet, unit tests);
   - runs the **datastore-backed suites for real** (`script/integration-test.sh`:
     live Postgres, KurrentDB, Meilisearch and NATS with
     `BUILDINGBLOCKS_REQUIRE_INTEGRATION=1`, so a skip fails) — a version is
     never published on unit coverage alone;
    - only after tag, module, and live integration checks succeed, a separate
      read-only `verify-public-fetch` job proves the module is **fetchable at
      that version** the way a consumer
     fetches it: a fresh module resolves the tag **anonymously through the
     public module proxy** (`https://proxy.golang.org`, no `,direct` fallback)
     with the **public checksum database** (`sum.golang.org`) verifying the
     version's hash — under an explicitly EMPTY private scope, with a fresh
     module cache/GOPATH/HOME, no token, no Git auth header, and a failing
     ASKPASS, so neither a checkout credential nor a Git credential helper can
     satisfy the fetch, and `go.sum` must carry both `h1:` entries before the
     step passes;
    - only after that proof succeeds, re-checks release absence (API failures
      block publication) and creates the GitHub Release using the curated notes
      checked out from the validated tag, not from the dispatch branch.

   Only after this succeeds is the release procedure complete. A tag alone is
   not a completed release, but a **public tag can already be discovered and
   cached** by Go consumers before the workflow finishes. The dependency gates
   delay this workflow's own proxy request; they cannot make tag visibility and
   GitHub Release creation atomic. Treat a pushed version tag as immutable even
   if a later gate fails. Never move, delete, or re-create it to repair a failure.

   **Permissions:** the workflow default is `contents: read`. Only the final
   publish job raises it to `contents: write`, for the single step that creates
   the release, and every checkout sets `persist-credentials: false`.

6. Once the workflow and GitHub Release are confirmed, consumers may pin:

   ```
   require github.com/aliighahremanii/buildingblocks v0.1.0
   ```

## What this repository does NOT do

- **No package registry other than the public Go module proxy.** There is no
  Gitea registry involvement, and no other distribution channel: GitHub tags +
  releases plus the standard public proxy/checksum database are the entire
  publication surface.
- **No credential or private-module scope.** The module is public, so no
  consumer needs read access, a `.netrc`, a BuildKit secret, or a
  `GOPRIVATE`/`GONOPROXY`/`GONOSUMDB` exemption — and none is configured in
  this repository or in the reference consumer.
- **No `replace` in `go.mod`.** A `replace` in a *library* is ignored by
  consumers anyway (only the main module's `replace` directives apply), so it
  would create a false sense that a local path were being used. Consumers that
  need a local override add it in **their own** `go.mod` as a temporary,
  uncommitted convenience — never as the shipped state.
- **No vendoring requirement.** Consumers do not need `vendor/`.

## Re-running a failed publish

The workflow accepts `workflow_dispatch` with a `tag` input. That path still
enforces the "no existing release" rule, so it can only ever complete a publish
that never landed — it cannot overwrite one.

If a transient service or proxy failure interrupted verification, retry the same
unchanged annotated tag. If the tagged source or curated notes need correction,
prepare a new version instead. If creation succeeded but the workflow result is
uncertain, inspect the existing release; a retry refuses to overwrite it. API
errors during either existence check fail closed, rather than authorizing a
retry to publish blindly.
