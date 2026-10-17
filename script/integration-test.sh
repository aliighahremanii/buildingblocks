#!/usr/bin/env bash
#
# Run THIS module's full suite against real datastores.
#
# `go test ./...` on its own is unit coverage only: the datastore-backed
# suites in this module auto-SKIP when their environment variables are unset
# (infrastructure/testenv). A skip in a job that provisioned the datastores is
# a silent pass, so this script:
#
#   1. brings up Postgres, KurrentDB, Meilisearch and NATS from
#      docker-compose.integration.yml (public, pinned images);
#   2. waits for each one to be genuinely READY (bounded retries — a stack
#      that never becomes healthy FAILS, it does not proceed);
#   3. applies the module's shared migration SQL with the real golang-migrate
#      runner, because the suites read es_snapshots, es_projection_dlq,
#      es_projection_checkpoints, command_idempotency, sagas, attachments and
#      nats_published_positions;
#   4. runs `go test ./...` with BUILDINGBLOCKS_REQUIRE_INTEGRATION=1, which
#      turns every environment-gated skip into a FAILURE;
#   5. tears the stack down again (always, including on failure).
#
# Package parallelism is deliberately left at Go's default: the suite is
# written for one shared Postgres with a cross-process advisory lock
# (postgrestest.Lock), and `-p 1 -parallel 1` would hide exactly the
# concurrency behavior that contract exists to protect.
#
# Standard compose environment variables are honored, so a caller can add
# overrides without editing this file:
#   COMPOSE_PROJECT_NAME  project name (default: buildingblocks-integration)
#   COMPOSE_FILE          colon-separated compose files (default:
#                         docker-compose.integration.yml)
#
# Requires: docker (with the compose plugin), Go, curl.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

export COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.integration.yml}"
project="${COMPOSE_PROJECT_NAME:-buildingblocks-integration}"

# Bounded readiness budget: 60 attempts x 2s = 120s per service. Generous for
# a cold image pull on a hosted runner, finite so a broken stack fails the
# job instead of hanging it.
readiness_attempts="${INTEGRATION_READINESS_ATTEMPTS:-60}"

compose() {
  docker compose -p "${project}" "$@"
}

cleanup() {
  # Tear down ONLY this project's resources; --volumes also drops the
  # anonymous volumes a service may have created, and --remove-orphans
  # removes a service that was dropped from the file between runs.
  compose down --remove-orphans --volumes >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_for() {
  local label="$1"
  shift
  local attempt=1
  while [ "${attempt}" -le "${readiness_attempts}" ]; do
    if "$@" >/dev/null 2>&1; then
      echo "ready: ${label} (attempt ${attempt})"
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 2
  done
  echo "ERROR: ${label} did not become ready after ${readiness_attempts} attempts." >&2
  echo "--- stack status:" >&2
  compose ps >&2 || true
  echo "--- recent logs:" >&2
  compose logs --tail 50 >&2 || true
  return 1
}

echo "== starting datastores (project ${project}, files ${COMPOSE_FILE})"
compose up -d postgres kurrentdb meilisearch nats

# Readiness, probed through the REAL client path (not just "container is up"):
# Postgres via pg_isready in-container; the HTTP services via their health
# endpoints on the host-published ports, which is exactly how the suites
# reach them.
wait_for "postgres" compose exec -T postgres pg_isready -U app -d app
wait_for "kurrentdb" curl -sf http://127.0.0.1:2113/health/live
wait_for "meilisearch" curl -sf http://127.0.0.1:7700/health
wait_for "nats" curl -sf http://127.0.0.1:8222/healthz

echo "== applying shared migrations"
# `run --rm --no-deps` returns the one-shot service's exit code, so a failed
# migration fails this script. postgres is already up (readiness above).
compose run --rm --no-deps migrate

echo "== running the suite with integration tests REQUIRED (a skip is a failure)"
# The module's own defaults point at these exact URLs; they are exported
# explicitly so the run is independent of any local .env file and so the
# gated suites see a configured datastore.
export POSTGRES_DSN="postgres://app:app@127.0.0.1:5432/app?sslmode=disable"
export KURRENTDB_URL="kurrentdb://127.0.0.1:2113?tls=false"
export MEILISEARCH_URL="http://127.0.0.1:7700"
export MEILISEARCH_API_KEY="dev-meili-master-key"
export NATS_URL="nats://127.0.0.1:4222"
export BUILDINGBLOCKS_REQUIRE_INTEGRATION=1

# -count=1: no cached results. Default package parallelism (see the header).
go test ./... -count=1 -timeout "${INTEGRATION_TEST_TIMEOUT:-900s}"

echo "== integration suite passed (no datastore-backed test was skipped)"
