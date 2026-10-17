package settingsreconcile

// Live integration coverage for the concurrent-index-creation adoption path
// (review cycle 1, finding 2). These tests run ONLY when MEILISEARCH_URL is
// set (auto-skip otherwise, per the repo integration-test policy) and use a
// disposable index uid that is never part of any manifest inventory — they
// create, verify, and delete it, leaving no state behind.
//
// Ported from the searchops live tests by the settingsreconcile extraction
// (the engine owns the per-index reconciler the searchops adapter now
// delegates to); the disposable manifests are engine Manifests.

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/meilisearch/meilisearch-go"

	"github.com/aliighahremanii/buildingblocks/infrastructure/testenv"
)

// liveTestUID is the disposable index uid for live reconcile tests. A fixed
// name keeps repeat runs and cleanup idempotent; it is deliberately not a
// manifest uid of any inventory.
const liveTestUID = "settingsreconcile_livetest_concurrent"

// liveMeili returns a live client when MEILISEARCH_URL is configured and
// the server answers its health probe, or skips the test. When integration
// tests are REQUIRED (BUILDINGBLOCKS_REQUIRE_INTEGRATION) both conditions
// FAIL instead — the job that provisioned Meilisearch must not pass on a
// skip.
func liveMeili(t *testing.T) meilisearch.ServiceManager {
	t.Helper()
	url := os.Getenv("MEILISEARCH_URL")
	if url == "" {
		testenv.SkipOrFail(t, "MEILISEARCH_URL not set: live Meilisearch reconcile tests skipped")
	}
	key := os.Getenv("MEILISEARCH_API_KEY")
	if key == "" {
		key = "dev-meili-master-key"
	}
	client := meilisearch.New(url, meilisearch.WithAPIKey(key))
	health, err := client.Health()
	if err != nil || health.Status != "available" {
		testenv.SkipOrFail(t, "meilisearch at %s not available (health: %v, %v): skipping live test", url, health, err)
	}
	return client
}

// deleteLiveTestIndex removes the disposable index (best effort) and waits
// for the async delete task so repeat runs start clean.
func deleteLiveTestIndex(t *testing.T, svc meilisearch.ServiceManager) {
	t.Helper()
	task, err := svc.DeleteIndexWithContext(context.Background(), liveTestUID)
	if err != nil {
		return // already gone
	}
	if task != nil {
		_ = waitTask(context.Background(), svc, task.TaskUID, 100*time.Millisecond)
	}
}

// TestReconcileLive_ConcurrentCreateAdoptsWinner exercises the REAL race
// against a live Meilisearch: two apply runs concurrently create the same
// missing index. Meilisearch serializes per-index tasks, so one create
// succeeds and the loser's task fails with the authoritative typed
// index_already_exists code (verified: the create POST itself returns 202).
// The loser must adopt the winner's index (verify uid + primary key) and
// continue settings reconciliation — both runs succeed, exactly one reports
// created, and the final live settings equal the manifest.
func TestReconcileLive_ConcurrentCreateAdoptsWinner(t *testing.T) {
	svc := liveMeili(t)

	manifest := Manifest{
		UID:                  liveTestUID,
		PrimaryKey:           "id",
		SearchableAttributes: []string{"name"},
		FilterableAttributes: []string{"id", "isActive"},
		SortableAttributes:   []string{"name"},
		MaxTotalHits:         testMaxTotalHits,
	}

	// Clean slate + guaranteed cleanup of the disposable index.
	deleteLiveTestIndex(t, svc)
	t.Cleanup(func() { deleteLiveTestIndex(t, svc) })

	interval := 50 * time.Millisecond
	const runs = 2
	reports := make([]IndexReport, runs)
	var wg sync.WaitGroup
	for i := 0; i < runs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			entry, err := reconcileIndex(context.Background(), svc, manifest, ReconcileOptions{Apply: true}, interval)
			if err != nil {
				entry.Err = err
			}
			reports[i] = entry
		}(i)
	}
	wg.Wait()

	created := 0
	for i := 0; i < runs; i++ {
		e := reports[i]
		if e.Err != nil {
			t.Fatalf("run %d must adopt the concurrent winner, got: %v", i, e.Err)
		}
		switch e.Status {
		case StatusCreated, StatusApplied, StatusOK:
		default:
			t.Fatalf("run %d: status %q must be one of created/applied/ok", i, e.Status)
		}
		if e.Status == StatusCreated {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("exactly one run may report created, got %d (statuses: %s/%s)",
			created, reports[0].Status, reports[1].Status)
	}

	// The live index must now exist with the manifest identity and the
	// manifest settings — the winner created it and both runs reconciled
	// settings onto it.
	got, err := svc.GetIndexWithContext(context.Background(), liveTestUID)
	if err != nil {
		t.Fatalf("get live index after race: %v", err)
	}
	if got.PrimaryKey != manifest.PrimaryKey {
		t.Fatalf("live primary key = %q, want %q", got.PrimaryKey, manifest.PrimaryKey)
	}
	settings, err := svc.Index(liveTestUID).GetSettingsWithContext(context.Background())
	if err != nil {
		t.Fatalf("get live settings: %v", err)
	}
	drift := compareSettings(manifest, settings)
	if len(drift) != 0 {
		t.Fatalf("live settings must equal the manifest after both runs, drift: %v", drift)
	}
}

// TestReconcileLive_ClassificationIsTypedAgainstRealServer pins the typed
// classifiers against a real Meilisearch: a GET of a missing index returns
// the authoritative 404 + index_not_found envelope, and creating an index
// that exists fails its task with the typed index_already_exists code. This
// proves the removed string-matching fallback is not needed against the
// real API surface.
func TestReconcileLive_ClassificationIsTypedAgainstRealServer(t *testing.T) {
	svc := liveMeili(t)

	deleteLiveTestIndex(t, svc)
	t.Cleanup(func() { deleteLiveTestIndex(t, svc) })

	// Missing index: authoritative 404 + typed code.
	_, err := svc.GetIndexWithContext(context.Background(), liveTestUID)
	if err == nil {
		t.Fatal("missing index must error")
	}
	if !isIndexNotFound(err) {
		t.Fatalf("live missing-index error must classify typed as not-found, got %v", err)
	}
	var merr *meilisearch.Error
	if !errors.As(err, &merr) || merr.MeilisearchApiError.Code != "index_not_found" {
		t.Fatalf("live not-found must carry the typed index_not_found code, got %v", err)
	}

	// Create, wait for success, then create again: the duplicate task must
	// fail with the typed already-exists code.
	first, err := svc.CreateIndexWithContext(context.Background(), &meilisearch.IndexConfig{Uid: liveTestUID, PrimaryKey: "id"})
	if err != nil {
		t.Fatalf("first create POST: %v", err)
	}
	if err := waitTask(context.Background(), svc, first.TaskUID, 50*time.Millisecond); err != nil {
		t.Fatalf("first create task: %v", err)
	}
	dup, err := svc.CreateIndexWithContext(context.Background(), &meilisearch.IndexConfig{Uid: liveTestUID, PrimaryKey: "id"})
	if err != nil {
		t.Fatalf("duplicate create POST: %v", err)
	}
	if err := waitTask(context.Background(), svc, dup.TaskUID, 50*time.Millisecond); err == nil {
		t.Fatal("duplicate create task must fail")
	} else if !isTaskIndexAlreadyExists(err) {
		t.Fatalf("duplicate create must fail with typed index_already_exists, got %v", err)
	}
}
