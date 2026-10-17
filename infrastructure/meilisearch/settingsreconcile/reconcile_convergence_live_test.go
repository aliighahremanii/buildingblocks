package settingsreconcile

// Live E2E regression proof for the post-apply convergence defect (E2E-1,
// found during the D102/D103 E2E verification against a real Meilisearch
// v1.12):
//
// The runbook's documented closure — "Dry run again after an --apply: every
// row must read ok (or created) and the exit code must be 0" — was UNREACHABLE
// for any manifest whose filterable or sortable attribute list is not already
// alphabetically sorted. Meilisearch canonicalizes filterableAttributes and
// sortableAttributes to alphabetical order regardless of the order submitted;
// the manifest declares a semantic order, so compareSettings reported the
// stored (sorted) order as drift forever — an --apply followed by a dry-run
// never converged.
//
// FIXED: compareSettings compares filterableAttributes and sortableAttributes
// order-insensitively (sets) because Meilisearch canonicalizes them, while
// searchableAttributes stays order-sensitive (rank order is semantic) and
// pagination stays exact.
//
// This test drives the REAL reconcile path (reconcileIndex with Apply, then
// dry-run) against a disposable index whose manifest mirrors the product
// shape (non-alphabetical multi-attribute filterable/sortable lists). It
// asserts the documented invariant: after apply, the dry-run must report no
// drift. It must keep passing against a real Meilisearch — a regression back
// to order-sensitive comparison fails it (verified: it failed against
// Meilisearch v1.12.8 before the fix).
//
// Runs only when MEILISEARCH_URL is set (auto-skip otherwise, per the repo
// integration-test policy). Creates, verifies, and deletes a disposable index
// that is never part of any manifest inventory.
//
// Ported from the searchops live tests by the settingsreconcile extraction.

import (
	"context"
	"testing"
	"time"

	"github.com/meilisearch/meilisearch-go"
)

// convergenceTestUID is the disposable index uid for the convergence test.
// A fixed name keeps repeat runs and cleanup idempotent; it is deliberately
// not a manifest uid of any inventory.
const convergenceTestUID = "settingsreconcile_livetest_convergence"

func TestReconcileLive_ApplyThenDryRunConverges(t *testing.T) {
	svc := liveMeili(t)

	// Mirror the product shape: non-alphabetical multi-attribute
	// filterable and sortable lists (the product contract).
	manifest := Manifest{
		UID:                  convergenceTestUID,
		PrimaryKey:           "id",
		SearchableAttributes: []string{"orderNumber", "externalOrderNumber"},
		FilterableAttributes: []string{"id", "isActive", "status", "customerId"},
		SortableAttributes:   []string{"orderNumber", "externalOrderNumber", "status"},
		MaxTotalHits:         testMaxTotalHits,
	}

	// Clean slate + guaranteed cleanup of the disposable index.
	deleteIndexBestEffort(t, svc, convergenceTestUID)
	t.Cleanup(func() { deleteIndexBestEffort(t, svc, convergenceTestUID) })

	// Apply: create the index and converge settings. reconcileIndex is the
	// production per-index reconciler (driven with a disposable manifest,
	// the same way the concurrent-adoption live test drives it).
	entry, err := reconcileIndex(context.Background(), svc, manifest, ReconcileOptions{
		Apply:            true,
		TaskWaitInterval: 50 * time.Millisecond,
	}, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if entry.Err != nil {
		t.Fatalf("apply must succeed for the single index, got %+v", entry)
	}

	// Post-apply dry-run: the runbook's documented closure requires zero
	// drift (exit 0). Before the E2E-1 fix the applied settings came back
	// alphabetically sorted, which compareSettings reported as
	// filterable/sortable drift; the set comparison must converge instead.
	entry, err = reconcileIndex(context.Background(), svc, manifest, ReconcileOptions{
		TaskWaitInterval: 50 * time.Millisecond,
	}, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("post-apply dry run: %v", err)
	}
	if entry.Err != nil {
		t.Fatalf("post-apply dry run errored: %v", entry.Err)
	}
	if entry.Status != StatusOK {
		t.Fatalf(
			"post-apply dry run must converge to ok (runbook 'Verifying' step 1), got status %q with drift: %v — "+
				"compareSettings treats Meilisearch's alphabetically-canonicalized filterable/sortable lists as order drift; "+
				"the documented post-apply closure is unreachable for non-alphabetical manifests",
			entry.Status, entry.Drift)
	}
}

// deleteIndexBestEffort removes a disposable index (best effort) and waits
// for the async delete task so repeat runs start clean.
func deleteIndexBestEffort(t *testing.T, svc meilisearch.ServiceManager, uid string) {
	t.Helper()
	task, err := svc.DeleteIndexWithContext(context.Background(), uid)
	if err != nil {
		return // already gone
	}
	if task != nil {
		_ = waitTask(context.Background(), svc, task.TaskUID, 50*time.Millisecond)
	}
}
