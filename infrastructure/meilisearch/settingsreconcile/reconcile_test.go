package settingsreconcile

// reconcile_test.go — the engine's unit contract, ported from the
// searchops reconciler tests it replaces (behavior-identical extraction)
// with the inventory manifests replaced by local test manifests of the
// SAME product shapes (order-sensitive searchable, non-alphabetical
// filterable/sortable, pagination bound), PLUS the engine-specific
// contract: explicit non-nil attribute groups (nil rejected, empty
// written as the explicit "none" decision).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/meilisearch/meilisearch-go"
)

// testMaxTotalHits mirrors the product pagination bound (10000).
const testMaxTotalHits int64 = 10000

// testManifests are local manifests mirroring the product shapes the
// searchops tests exercised: multi-attribute NON-ALPHABETICAL
// filterable/sortable lists (the order-insensitivity contract) and an
// order-sensitive searchable list.
var testManifests = []Manifest{
	{
		UID:                  "test_customer",
		PrimaryKey:           "id",
		SearchableAttributes: []string{"name", "code"},
		FilterableAttributes: []string{"id", "isActive", "code"},
		SortableAttributes:   []string{"name", "code"},
		MaxTotalHits:         testMaxTotalHits,
	},
	{
		UID:                  "test_order",
		PrimaryKey:           "id",
		SearchableAttributes: []string{"orderNumber", "externalOrderNumber"},
		FilterableAttributes: []string{"id", "isActive", "status", "customerId", "producerId", "currencyId"},
		SortableAttributes:   []string{"orderNumber", "externalOrderNumber", "status", "totalWeightGrams"},
		MaxTotalHits:         testMaxTotalHits,
	},
	{
		UID:                  "test_truck",
		PrimaryKey:           "id",
		SearchableAttributes: []string{"plateNo", "truckNo"},
		FilterableAttributes: []string{"id", "isActive"},
		SortableAttributes:   []string{"plateNo", "truckNo"},
		MaxTotalHits:         testMaxTotalHits,
	},
	{
		UID:                  "test_driver",
		PrimaryKey:           "id",
		SearchableAttributes: []string{"name"},
		FilterableAttributes: []string{"id", "isActive"},
		SortableAttributes:   []string{"name"},
		MaxTotalHits:         testMaxTotalHits,
	},
}

// emptySearchableManifest mirrors the reportops default-deny shape: a
// manifest whose free-text surface is the EXPLICIT decision "none"
// (non-nil empty array — e.g. a report whose legacy q target is an
// identifier).
var emptySearchableManifest = Manifest{
	UID:                  "test_gate_decision",
	PrimaryKey:           "id",
	SearchableAttributes: []string{},
	FilterableAttributes: []string{"id", "decision", "bijakId"},
	SortableAttributes:   []string{"decision"},
	MaxTotalHits:         testMaxTotalHits,
}

func byTestUID(uid string) (Manifest, bool) {
	for _, m := range append(append([]Manifest(nil), testManifests...), emptySearchableManifest) {
		if m.UID == uid {
			return m, true
		}
	}
	return Manifest{}, false
}

// fakeIndex records settings reads/writes for one index.
type fakeIndex struct {
	meilisearch.IndexManager // embed: only used methods are implemented

	exists     bool
	primaryKey string
	settings   meilisearch.Settings
	updated    []meilisearch.Settings
	// searchableUpdates records the granular searchableAttributes PUTs
	// (the sub-route an explicit "none" group must ride).
	searchableUpdates [][]string
}

func (f *fakeIndex) GetSettingsWithContext(_ context.Context) (*meilisearch.Settings, error) {
	if !f.exists {
		return nil, &meilisearch.Error{StatusCode: 404}
	}
	s := f.settings
	return &s, nil
}

func (f *fakeIndex) UpdateSettingsWithContext(_ context.Context, request *meilisearch.Settings) (*meilisearch.TaskInfo, error) {
	f.updated = append(f.updated, *request)
	f.settings = *request
	return &meilisearch.TaskInfo{TaskUID: 1, Status: meilisearch.TaskStatusEnqueued}, nil
}

func (f *fakeIndex) UpdateSearchableAttributesWithContext(_ context.Context, request *[]string) (*meilisearch.TaskInfo, error) {
	f.searchableUpdates = append(f.searchableUpdates, *request)
	f.settings.SearchableAttributes = *request
	return &meilisearch.TaskInfo{TaskUID: 1, Status: meilisearch.TaskStatusEnqueued}, nil
}

type fakeService struct {
	meilisearch.ServiceManager // embed: only used methods are implemented

	indexes map[string]*fakeIndex
	getErr  error
	// createErr, when set, is returned outright by CreateIndexWithContext
	// (a server variant rejecting the POST with a typed error).
	createErr error
	// raceWinner models the concurrent reconciler's index. When set, the
	// map starts WITHOUT the index (GetIndex 404s), and the first
	// CreateIndexWithContext call completes the winner's creation (the
	// per-index task queue serializes theirs first) instead of ours: with
	// createErr it returns the typed rejection, otherwise a task whose
	// wait fails with failedTask (the live 202 + failed-task shape).
	raceWinner *fakeIndex
	// failedTask is the canned task WaitForTaskWithContext returns for
	// losing-create task uids.
	failedTask *meilisearch.Task
}

func (f *fakeService) Index(uid string) meilisearch.IndexManager {
	return f.indexes[uid]
}

func (f *fakeService) GetIndexWithContext(_ context.Context, uid string) (*meilisearch.IndexResult, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	idx, ok := f.indexes[uid]
	if !ok || !idx.exists {
		return nil, &meilisearch.Error{StatusCode: 404}
	}
	return &meilisearch.IndexResult{UID: uid, PrimaryKey: idx.primaryKey}, nil
}

func (f *fakeService) CreateIndexWithContext(_ context.Context, config *meilisearch.IndexConfig) (*meilisearch.TaskInfo, error) {
	if f.raceWinner != nil {
		// The concurrent winner's index appears; our creation loses.
		f.indexes[config.Uid] = f.raceWinner
		if f.createErr != nil {
			return nil, f.createErr
		}
		return &meilisearch.TaskInfo{TaskUID: 7, Status: meilisearch.TaskStatusEnqueued}, nil
	}
	if f.createErr != nil {
		return nil, f.createErr
	}
	idx := &fakeIndex{exists: true, primaryKey: config.PrimaryKey}
	f.indexes[config.Uid] = idx
	return &meilisearch.TaskInfo{TaskUID: 2, Status: meilisearch.TaskStatusEnqueued}, nil
}

func (f *fakeService) WaitForTaskWithContext(_ context.Context, taskUID int64, _ time.Duration) (*meilisearch.Task, error) {
	if taskUID == 7 && f.failedTask != nil {
		return f.failedTask, nil
	}
	return &meilisearch.Task{Status: meilisearch.TaskStatusSucceeded}, nil
}

// matchedSettings builds the settings a fully reconciled index must carry.
func matchedSettings(t *testing.T, uid string) meilisearch.Settings {
	t.Helper()
	m, ok := byTestUID(uid)
	if !ok {
		t.Fatalf("no test manifest for %s", uid)
	}
	return m.Settings()
}

func TestManifestValidate(t *testing.T) {
	t.Parallel()

	valid := Manifest{
		UID: "u", PrimaryKey: "id",
		SearchableAttributes: []string{"a"},
		FilterableAttributes: []string{"id"},
		SortableAttributes:   []string{},
		MaxTotalHits:         10,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}

	cases := []struct {
		name    string
		mutate  func(m *Manifest)
		wantMsg string
	}{
		{"empty uid", func(m *Manifest) { m.UID = "" }, "empty UID"},
		{"empty primary key", func(m *Manifest) { m.PrimaryKey = "" }, "empty primary key"},
		{"zero maxTotalHits", func(m *Manifest) { m.MaxTotalHits = 0 }, "maxTotalHits must be positive"},
		{"negative maxTotalHits", func(m *Manifest) { m.MaxTotalHits = -1 }, "maxTotalHits must be positive"},
		{"nil searchable", func(m *Manifest) { m.SearchableAttributes = nil }, "nil searchableAttributes"},
		{"nil filterable", func(m *Manifest) { m.FilterableAttributes = nil }, "nil filterableAttributes"},
		{"nil sortable", func(m *Manifest) { m.SortableAttributes = nil }, "nil sortableAttributes"},
	}
	for _, tc := range cases {
		m := valid
		tc.mutate(&m)
		err := m.Validate()
		if err == nil || !strings.Contains(err.Error(), tc.wantMsg) {
			t.Errorf("%s: Validate() = %v, want error containing %q", tc.name, err, tc.wantMsg)
		}
	}
}

// TestSettingsRendersEmptyGroupsAsExplicitNone pins the in-memory half of
// the explicit-groups contract: an EMPTY (non-nil) group renders as a
// non-nil empty slice. (The SDK's Settings struct tags the groups
// `omitempty`, so the wire body omits them — the "none" decision converges
// through the granular per-group sub-routes instead; that transport half
// is pinned by TestReconcile_AppliesExplicitEmptySearchableAsNone.)
func TestSettingsRendersEmptyGroupsAsExplicitNone(t *testing.T) {
	t.Parallel()

	s := emptySearchableManifest.Settings()
	if s.SearchableAttributes == nil {
		t.Fatal("empty searchable group must render as a non-nil empty slice, not nil")
	}
	if len(s.SearchableAttributes) != 0 {
		t.Fatalf("empty searchable group must render empty, got %v", s.SearchableAttributes)
	}
	// Declared-but-empty must never disable the OTHER groups.
	if s.FilterableAttributes == nil || len(s.FilterableAttributes) != 3 {
		t.Fatalf("filterable group unaffected, got %v", s.FilterableAttributes)
	}
}

// TestReconcile_RejectsInvalidManifestsBeforeTouchingServer pins the
// validation boundary: a nil group or a duplicate uid fails the WHOLE run
// with an error before any server interaction — never a half-reconciled
// report.
func TestReconcile_RejectsInvalidManifestsBeforeTouchingServer(t *testing.T) {
	t.Parallel()

	svc := &fakeService{indexes: map[string]*fakeIndex{}}

	nilGroup := testManifests[0]
	nilGroup.SearchableAttributes = nil
	if _, err := Reconcile(context.Background(), svc, ReconcileOptions{}, append([]Manifest{nilGroup}, testManifests[1:]...)); err == nil {
		t.Fatal("nil searchable group must fail the run")
	} else if !strings.Contains(err.Error(), "nil searchableAttributes") {
		t.Fatalf("error must name the defect, got %v", err)
	}

	dupes := []Manifest{testManifests[0], testManifests[0]}
	if _, err := Reconcile(context.Background(), svc, ReconcileOptions{}, dupes); err == nil {
		t.Fatal("duplicate uid must fail the run")
	} else if !strings.Contains(err.Error(), "duplicate manifest uid") {
		t.Fatalf("error must name the duplicate, got %v", err)
	}

	if len(svc.indexes) != 0 {
		t.Fatal("an invalid inventory must not touch the server")
	}
}

func TestReconcile_DryRunReportsDriftWithoutMutation(t *testing.T) {
	t.Parallel()

	svc := &fakeService{indexes: map[string]*fakeIndex{
		"test_customer": {exists: true}, // untouched defaults = drift
	}}
	svc.indexes["test_customer"].settings = meilisearch.Settings{
		SearchableAttributes: []string{"*"},
		FilterableAttributes: []string{"*"},
		SortableAttributes:   []string{"*"},
	}

	report, err := Reconcile(context.Background(), svc, ReconcileOptions{OnlyUIDs: []string{"test_customer"}}, testManifests)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(report.Indexes) != 1 {
		t.Fatalf("report entries = %d", len(report.Indexes))
	}
	e := report.Indexes[0]
	if e.Status != StatusDrift {
		t.Fatalf("status = %q, want drift", e.Status)
	}
	if !report.HasDrift() {
		t.Fatal("HasDrift must be true")
	}
	if len(svc.indexes["test_customer"].updated) != 0 {
		t.Fatal("dry run must not write settings")
	}
	if len(e.Drift) != 4 {
		t.Fatalf("expected 4 drift lines (3 attr groups + pagination), got %v", e.Drift)
	}
	for _, want := range []string{"searchableAttributes", "filterableAttributes", "sortableAttributes", "pagination.maxTotalHits"} {
		found := false
		for _, line := range e.Drift {
			if strings.HasPrefix(line, want) {
				found = true
			}
		}
		if !found {
			t.Errorf("drift lines missing %q: %v", want, e.Drift)
		}
	}
}

func TestReconcile_DryRunReportsMissingIndex(t *testing.T) {
	t.Parallel()

	svc := &fakeService{indexes: map[string]*fakeIndex{}}
	report, err := Reconcile(context.Background(), svc, ReconcileOptions{OnlyUIDs: []string{"test_driver"}}, testManifests)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Indexes[0].Status != StatusMissing || !report.HasDrift() {
		t.Fatalf("expected missing, got %+v", report.Indexes[0])
	}
	if len(svc.indexes) != 0 {
		t.Fatal("dry run must not create indexes")
	}
}

func TestReconcile_OKWhenSettingsMatch(t *testing.T) {
	t.Parallel()

	svc := &fakeService{indexes: map[string]*fakeIndex{
		"test_truck": {exists: true, settings: matchedSettings(t, "test_truck")},
	}}
	report, err := Reconcile(context.Background(), svc, ReconcileOptions{OnlyUIDs: []string{"test_truck"}}, testManifests)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	e := report.Indexes[0]
	if e.Status != StatusOK || e.Err != nil || len(e.Drift) != 0 {
		t.Fatalf("expected ok, got %+v", e)
	}
	if report.HasDrift() || report.HasFailures() {
		t.Fatal("report flags must be clear")
	}
	if len(svc.indexes["test_truck"].updated) != 0 {
		t.Fatal("matched settings must not be rewritten")
	}
}

func TestReconcile_AppliesDriftAndWaits(t *testing.T) {
	t.Parallel()

	svc := &fakeService{indexes: map[string]*fakeIndex{
		"test_order": {exists: true, settings: meilisearch.Settings{
			SearchableAttributes: []string{"orderNumber"},
		}},
	}}
	report, err := Reconcile(context.Background(), svc, ReconcileOptions{OnlyUIDs: []string{"test_order"}, Apply: true}, testManifests)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	e := report.Indexes[0]
	if e.Status != StatusApplied {
		t.Fatalf("status = %q, want applied", e.Status)
	}
	idx := svc.indexes["test_order"]
	if len(idx.updated) != 1 {
		t.Fatalf("settings updates = %d, want 1", len(idx.updated))
	}
	want := matchedSettings(t, "test_order")
	got := idx.updated[0]
	if strings.Join(got.SearchableAttributes, ",") != strings.Join(want.SearchableAttributes, ",") ||
		strings.Join(got.FilterableAttributes, ",") != strings.Join(want.FilterableAttributes, ",") ||
		strings.Join(got.SortableAttributes, ",") != strings.Join(want.SortableAttributes, ",") ||
		got.Pagination == nil || got.Pagination.MaxTotalHits != want.Pagination.MaxTotalHits {
		t.Fatalf("applied settings drift from manifest: %+v", got)
	}
}

// TestReconcile_AppliesExplicitEmptySearchableAsNone pins the default-deny
// convergence for a manifest whose searchable group is the explicit
// decision "none": the untouched index's platform default `["*"]` is drift,
// and the apply must CONVERGE it to none. The SDK's bulk Settings PATCH
// omits empty groups (`omitempty`), so the engine must send the empty
// group through its granular sub-route (PUT of the bare `[]`) — asserting
// only the bulk write would leave `["*"]` in place forever.
func TestReconcile_AppliesExplicitEmptySearchableAsNone(t *testing.T) {
	t.Parallel()

	svc := &fakeService{indexes: map[string]*fakeIndex{
		"test_gate_decision": {exists: true, settings: meilisearch.Settings{
			SearchableAttributes: []string{"*"},
			FilterableAttributes: emptySearchableManifest.FilterableAttributes,
			SortableAttributes:   emptySearchableManifest.SortableAttributes,
			Pagination:           &meilisearch.Pagination{MaxTotalHits: testMaxTotalHits},
		}},
	}}
	manifests := []Manifest{emptySearchableManifest}

	// Dry run: the platform default ["*"] is drift against the explicit none.
	report, err := Reconcile(context.Background(), svc, ReconcileOptions{OnlyUIDs: []string{"test_gate_decision"}}, manifests)
	if err != nil {
		t.Fatal(err)
	}
	if report.Indexes[0].Status != StatusDrift {
		t.Fatalf("platform default searchable must be drift, got %+v", report.Indexes[0])
	}

	// Apply: the empty group must be sent through the GRANULAR sub-route
	// (the bulk PATCH alone cannot express "none").
	report, err = Reconcile(context.Background(), svc, ReconcileOptions{OnlyUIDs: []string{"test_gate_decision"}, Apply: true}, manifests)
	if err != nil {
		t.Fatal(err)
	}
	if report.Indexes[0].Status != StatusApplied {
		t.Fatalf("want applied, got %+v", report.Indexes[0])
	}
	idx := svc.indexes["test_gate_decision"]
	if idx.searchableUpdates == nil || len(idx.searchableUpdates) != 1 || len(idx.searchableUpdates[0]) != 0 {
		t.Fatalf("explicit none must be PUT as a bare empty array via the granular sub-route, got %v", idx.searchableUpdates)
	}
	// The bulk PATCH still rides along for the non-empty groups +
	// pagination (the SDK omits only the empty groups).
	if len(idx.updated) != 1 {
		t.Fatalf("the bulk settings update must still run for the non-empty groups, updates = %d", len(idx.updated))
	}

	// Converged: a stored [] (or nil — same empty set) reports ok.
	svc.indexes["test_gate_decision"].settings = meilisearch.Settings{
		SearchableAttributes: []string{},
		FilterableAttributes: emptySearchableManifest.FilterableAttributes,
		SortableAttributes:   emptySearchableManifest.SortableAttributes,
		Pagination:           &meilisearch.Pagination{MaxTotalHits: testMaxTotalHits},
	}
	report, err = Reconcile(context.Background(), svc, ReconcileOptions{OnlyUIDs: []string{"test_gate_decision"}}, manifests)
	if err != nil {
		t.Fatal(err)
	}
	if report.Indexes[0].Status != StatusOK {
		t.Fatalf("stored empty searchable must converge to ok, got %+v", report.Indexes[0])
	}
}

func TestReconcile_CreatesMissingIndexAndConfigures(t *testing.T) {
	t.Parallel()

	svc := &fakeService{indexes: map[string]*fakeIndex{}}
	report, err := Reconcile(context.Background(), svc, ReconcileOptions{OnlyUIDs: []string{"test_order"}, Apply: true}, testManifests)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	e := report.Indexes[0]
	if e.Status != StatusCreated {
		t.Fatalf("status = %q, want created", e.Status)
	}
	idx, ok := svc.indexes["test_order"]
	if !ok || !idx.exists {
		t.Fatal("index must exist after apply")
	}
	if len(idx.updated) != 1 {
		t.Fatalf("a freshly created index must also receive its settings, updates = %d", len(idx.updated))
	}
}

// TestReconcile_SearchableAttributeOrderIsContract pins the ONE
// order-sensitive attribute group: searchableAttributes rank order is
// semantic (Meilisearch preserves it and uses it for field priority), so a
// same-members reorder is drift. Nothing else may drift in this scenario.
func TestReconcile_SearchableAttributeOrderIsContract(t *testing.T) {
	t.Parallel()

	// test_customer manifest: searchable [name, code]; the stored order is
	// reversed, filterable/sortable carry exactly the manifest members (in
	// a different order), pagination exact.
	matched := matchedSettings(t, "test_customer")
	svc := &fakeService{indexes: map[string]*fakeIndex{
		"test_customer": {exists: true, settings: meilisearch.Settings{
			SearchableAttributes: []string{"code", "name"},
			FilterableAttributes: []string{"isActive", "code", "id"},
			SortableAttributes:   []string{"code", "name"},
			Pagination:           matched.Pagination,
		}},
	}}
	report, err := Reconcile(context.Background(), svc, ReconcileOptions{OnlyUIDs: []string{"test_customer"}}, testManifests)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	e := report.Indexes[0]
	if e.Status != StatusDrift {
		t.Fatalf("reordered searchableAttributes must be drift, got %+v", e)
	}
	if len(e.Drift) != 1 || !strings.HasPrefix(e.Drift[0], "searchableAttributes:") {
		t.Fatalf("only the searchableAttributes group may drift, got %v", e.Drift)
	}
}

// TestReconcile_FilterableSortableReorderIsNotDrift pins the convergence
// semantics fixed for E2E-1: Meilisearch canonicalizes filterableAttributes
// and sortableAttributes to its own (alphabetical) order regardless of
// submission order, so a same-members reorder of those groups is NOT drift
// — only membership differences are. With searchable order and pagination
// exact, a reordered-but-equal filterable/sortable state must report ok
// with no settings rewrite (otherwise the runbook's post-apply dry-run
// closure is unreachable for non-alphabetical manifests).
func TestReconcile_FilterableSortableReorderIsNotDrift(t *testing.T) {
	t.Parallel()

	// test_order manifest: non-alphabetical multi-attribute
	// filterable/sortable lists. Stored order mirrors Meilisearch's
	// canonical alphabetical order; membership is identical.
	matched := matchedSettings(t, "test_order")
	svc := &fakeService{indexes: map[string]*fakeIndex{
		"test_order": {exists: true, settings: meilisearch.Settings{
			SearchableAttributes: matched.SearchableAttributes,
			FilterableAttributes: []string{"currencyId", "customerId", "id", "isActive", "producerId", "status"},
			SortableAttributes:   []string{"externalOrderNumber", "orderNumber", "status", "totalWeightGrams"},
			Pagination:           matched.Pagination,
		}},
	}}
	report, err := Reconcile(context.Background(), svc, ReconcileOptions{OnlyUIDs: []string{"test_order"}}, testManifests)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	e := report.Indexes[0]
	if e.Status != StatusOK || len(e.Drift) != 0 {
		t.Fatalf("filterable/sortable reorder (same members) must converge to ok, got %+v", e)
	}
	if report.HasDrift() || report.HasFailures() {
		t.Fatal("report flags must be clear for a canonicalized-but-equal state")
	}
	if len(svc.indexes["test_order"].updated) != 0 {
		t.Fatal("canonicalized-but-equal settings must not be rewritten")
	}
}

// TestReconcile_FilterableSortableMembershipDifferenceIsDrift guards the
// other side of the set comparison: order-insensitivity must NOT weaken
// into a length check. Same length, one different member (and a duplicate
// in the stored list) must still report drift for the affected group.
func TestReconcile_FilterableSortableMembershipDifferenceIsDrift(t *testing.T) {
	t.Parallel()

	matched := matchedSettings(t, "test_order")
	svc := &fakeService{indexes: map[string]*fakeIndex{
		"test_order": {exists: true, settings: meilisearch.Settings{
			SearchableAttributes: matched.SearchableAttributes,
			// Same length as the manifest, but "producerId" replaced by
			// "truckId" — a genuine membership difference.
			FilterableAttributes: []string{"id", "isActive", "status", "customerId", "truckId", "currencyId"},
			SortableAttributes:   matched.SortableAttributes,
			Pagination:           matched.Pagination,
		}},
	}}
	report, err := Reconcile(context.Background(), svc, ReconcileOptions{OnlyUIDs: []string{"test_order"}}, testManifests)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	e := report.Indexes[0]
	if e.Status != StatusDrift {
		t.Fatalf("membership difference must be drift, got %+v", e)
	}
	if len(e.Drift) != 1 || !strings.HasPrefix(e.Drift[0], "filterableAttributes:") {
		t.Fatalf("only the filterableAttributes group may drift, got %v", e.Drift)
	}
}

func TestReconcile_UnknownUIDReportedNotfound(t *testing.T) {
	t.Parallel()

	svc := &fakeService{indexes: map[string]*fakeIndex{}}
	report, err := Reconcile(context.Background(), svc, ReconcileOptions{OnlyUIDs: []string{"masterdata_nope"}}, testManifests)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Indexes[0].Status != StatusNotFound || report.Indexes[0].Err != nil {
		t.Fatalf("expected notfound, got %+v", report.Indexes[0])
	}
}

func TestReconcile_TransportErrorsAreEntryFailures(t *testing.T) {
	t.Parallel()

	svc := &fakeService{indexes: map[string]*fakeIndex{}, getErr: errors.New("connection refused")}
	report, err := Reconcile(context.Background(), svc, ReconcileOptions{OnlyUIDs: []string{"test_truck"}}, testManifests)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	e := report.Indexes[0]
	if e.Err == nil || e.Status == StatusOK {
		t.Fatalf("expected entry failure, got %+v", e)
	}
	if !report.HasFailures() {
		t.Fatal("HasFailures must be true")
	}
	if !strings.Contains(e.Err.Error(), "connection refused") {
		t.Fatalf("error must wrap transport cause: %v", e.Err)
	}
}

func TestReconcile_NilClientFails(t *testing.T) {
	t.Parallel()
	if _, err := Reconcile(context.Background(), nil, ReconcileOptions{}, testManifests); err == nil {
		t.Fatal("nil client must fail")
	}
}

func TestReconcile_AllManifestsDefaultScope(t *testing.T) {
	t.Parallel()

	svc := &fakeService{indexes: map[string]*fakeIndex{}}
	report, err := Reconcile(context.Background(), svc, ReconcileOptions{Apply: false}, testManifests)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(report.Indexes) != len(testManifests) {
		t.Fatalf("default scope must cover every manifest: %d vs %d", len(report.Indexes), len(testManifests))
	}
}

// typedMeiliError builds a *meilisearch.Error from raw JSON because the
// MeilisearchApiError payload field's type is unexported and cannot be
// named in a composite literal from this package.
func typedMeiliError(t *testing.T, payload string) *meilisearch.Error {
	t.Helper()
	var e meilisearch.Error
	if err := json.Unmarshal([]byte(payload), &e); err != nil {
		t.Fatalf("unmarshal meilisearch error payload: %v", err)
	}
	return &e
}

// failedTask builds a failed async task from raw JSON for the same reason
// as typedMeiliError (Task.Error's type is unexported).
func failedTask(t *testing.T, code string) *meilisearch.Task {
	t.Helper()
	var task meilisearch.Task
	payload := fmt.Sprintf(`{"status":"failed","uid":7,"error":{"code":%q,"message":"boom"}}`, code)
	if err := json.Unmarshal([]byte(payload), &task); err != nil {
		t.Fatalf("unmarshal failed task payload: %v", err)
	}
	return &task
}

// TestIsIndexNotFound_TypedClassificationOnly pins the review-cycle-1
// contract: not-found classification reads ONLY the authoritative typed
// *meilisearch.Error envelope. An error whose rendered text happens to
// contain "404" or "not found" (transport noise, wrapped messages) must NOT
// classify as not-found — the former string-matching fallback silently
// turned operational failures into "index missing" outcomes.
func TestIsIndexNotFound_TypedClassificationOnly(t *testing.T) {
	t.Parallel()

	if isIndexNotFound(nil) {
		t.Fatal("nil is not not-found")
	}
	if isIndexNotFound(errors.New("connection refused")) {
		t.Fatal("plain transport error must not classify as not-found")
	}
	// Text containing every historical string-match trigger, but not a
	// typed Meilisearch error: MUST NOT classify.
	if isIndexNotFound(errors.New("gateway said 404 not found index_not_found for another tenant")) {
		t.Fatal("string-matching fallback must be gone — text containing 404/not found/index_not_found is not a typed classification")
	}
	if isIndexNotFound(fmt.Errorf("wrapped: %w", errors.New("dial tcp 404 not found"))) {
		t.Fatal("wrapped non-meili error must not classify as not-found")
	}

	if !isIndexNotFound(typedMeiliError(t, `{"StatusCode":404,"MeilisearchApiError":{"Code":"index_not_found","Message":"Index `+"`x`"+` not found."}}`)) {
		t.Fatal("typed 404 must classify as not-found")
	}
	// Typed error with the authoritative code, non-404 status: still
	// not-found (code wins; some surfaces report it with a different
	// transport status).
	if !isIndexNotFound(typedMeiliError(t, `{"StatusCode":502,"MeilisearchApiError":{"Code":"index_not_found"}}`)) {
		t.Fatal("typed index_not_found code must classify as not-found regardless of status")
	}
	if isIndexNotFound(typedMeiliError(t, `{"StatusCode":400,"MeilisearchApiError":{"Code":"invalid_request"}}`)) {
		t.Fatal("other typed errors must not classify as not-found")
	}

	if isIndexAlreadyExists(errors.New("already exists in text but not typed")) {
		t.Fatal("already-exists must be typed classification only")
	}
	if !isIndexAlreadyExists(typedMeiliError(t, `{"StatusCode":409,"MeilisearchApiError":{"Code":"index_already_exists","Message":"Index `+"`x`"+` already exists."}}`)) {
		t.Fatal("typed index_already_exists must classify")
	}
}

// TestWaitTask_FailedTaskCarriesTypedCode proves waitTask surfaces the
// task's authoritative error code through TaskFailedError (consumed by the
// concurrent-create adoption path).
func TestWaitTask_FailedTaskCarriesTypedCode(t *testing.T) {
	t.Parallel()

	svc := &fakeService{indexes: map[string]*fakeIndex{}, failedTask: failedTask(t, "index_already_exists")}
	err := waitTask(context.Background(), svc, 7, time.Millisecond)
	if err == nil {
		t.Fatal("failed task must error")
	}
	if !isTaskIndexAlreadyExists(err) {
		t.Fatalf("failed task error must carry index_already_exists, got %v", err)
	}

	svcOK := &fakeService{indexes: map[string]*fakeIndex{}}
	if err := waitTask(context.Background(), svcOK, 1, time.Millisecond); err != nil {
		t.Fatalf("succeeded task must not error, got %v", err)
	}
	other := &fakeService{indexes: map[string]*fakeIndex{}, failedTask: failedTask(t, "invalid_index_primary_key")}
	if isTaskIndexAlreadyExists(waitErr(t, other)) {
		t.Fatal("unrelated task failure must not classify as already-exists")
	}
}

func waitErr(t *testing.T, svc *fakeService) error {
	t.Helper()
	err := waitTask(context.Background(), svc, 7, time.Millisecond)
	if err == nil {
		t.Fatal("expected task failure error")
	}
	return err
}

// TestReconcile_CreateTaskConflictAdoptsExistingIndex reproduces the live
// race shape (verified against Meilisearch 1.x): the create POST returns
// 202, but the task FAILS with index_already_exists because a concurrent
// reconciler created the index first. The loser must reload the winner's
// index, verify uid + primary key, and CONTINUE settings reconciliation
// instead of failing the entry.
func TestReconcile_CreateTaskConflictAdoptsExistingIndex(t *testing.T) {
	t.Parallel()

	// The index does not exist yet (GetIndex 404s); the concurrent winner
	// lands at create time with drifted settings.
	svc := &fakeService{
		indexes: map[string]*fakeIndex{"test_driver": {}},
		raceWinner: &fakeIndex{exists: true, primaryKey: "id", settings: meilisearch.Settings{
			SearchableAttributes: []string{"*"},
		}},
		failedTask: failedTask(t, "index_already_exists"),
	}

	report, err := Reconcile(context.Background(), svc, ReconcileOptions{OnlyUIDs: []string{"test_driver"}, Apply: true}, testManifests)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	e := report.Indexes[0]
	if e.Err != nil {
		t.Fatalf("concurrent-create conflict must be adopted, got error: %v", e.Err)
	}
	// We did NOT create it: status must reflect settings reconciliation
	// only (drift applied onto the winner's index), never StatusCreated.
	if e.Status != StatusApplied {
		t.Fatalf("status = %q, want applied (adoption continues settings reconciliation)", e.Status)
	}
	if len(e.Drift) == 0 || !strings.Contains(e.Drift[0], "created concurrently") {
		t.Fatalf("adoption must be recorded first in the audit drift lines, got %v", e.Drift)
	}
	idx := svc.indexes["test_driver"]
	if len(idx.updated) != 1 {
		t.Fatalf("settings must still be reconciled, updates = %d", len(idx.updated))
	}
	want := matchedSettings(t, "test_driver")
	got := idx.updated[0]
	if strings.Join(got.SearchableAttributes, ",") != strings.Join(want.SearchableAttributes, ",") {
		t.Fatalf("applied settings = %+v", got)
	}
}

// TestReconcile_CreateRejectedOutrightAdoptsTypedAlreadyExists covers the
// defensive shape: a server variant that rejects the create POST outright
// with a typed index_already_exists error.
func TestReconcile_CreateRejectedOutrightAdoptsTypedAlreadyExists(t *testing.T) {
	t.Parallel()

	svc := &fakeService{
		indexes:    map[string]*fakeIndex{"test_driver": {}},
		raceWinner: &fakeIndex{exists: true, primaryKey: "id", settings: matchedSettings(t, "test_driver")},
		createErr:  typedMeiliError(t, `{"StatusCode":409,"MeilisearchApiError":{"Code":"index_already_exists","Message":"Index `+"`test_driver`"+` already exists."}}`),
	}

	report, err := Reconcile(context.Background(), svc, ReconcileOptions{OnlyUIDs: []string{"test_driver"}, Apply: true}, testManifests)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	e := report.Indexes[0]
	if e.Err != nil {
		t.Fatalf("typed already-exists rejection must be adopted, got %v", e.Err)
	}
	// The winner's index is already in sync: adoption lands on ok.
	if e.Status != StatusOK {
		t.Fatalf("status = %q, want ok", e.Status)
	}
	if len(e.Drift) != 1 || !strings.Contains(e.Drift[0], "created concurrently") {
		t.Fatalf("adoption must be recorded, got %v", e.Drift)
	}
}

// TestReconcile_AdoptedIndexWithWrongPrimaryKeyFails pins the safety
// boundary of adoption: an index that exists with a DIFFERENT primary key
// is a hard operational error (Meilisearch primary keys are immutable) —
// never adopted, never clobbered with a settings write.
func TestReconcile_AdoptedIndexWithWrongPrimaryKeyFails(t *testing.T) {
	t.Parallel()

	svc := &fakeService{
		indexes:    map[string]*fakeIndex{"test_driver": {}},
		raceWinner: &fakeIndex{exists: true, primaryKey: "documentId"},
		failedTask: failedTask(t, "index_already_exists"),
	}

	report, err := Reconcile(context.Background(), svc, ReconcileOptions{OnlyUIDs: []string{"test_driver"}, Apply: true}, testManifests)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	e := report.Indexes[0]
	if e.Err == nil {
		t.Fatal("primary-key mismatch on adoption must fail the entry")
	}
	if !strings.Contains(e.Err.Error(), "primary key") {
		t.Fatalf("error must name the primary-key mismatch, got %v", e.Err)
	}
	if !report.HasFailures() {
		t.Fatal("HasFailures must be true")
	}
	if len(svc.indexes["test_driver"].updated) != 0 {
		t.Fatal("a mismatched index must never receive settings writes")
	}
}

// TestReconcile_ConcurrentAppliesOnSameMissingIndex is the synchronized
// fake race: two apply runs race to create the same missing index. The fake
// emulates Meilisearch's serialized per-index task queue under a mutex —
// exactly one create succeeds, the loser's task fails with the typed
// index_already_exists code. Both runs must finish without errors, exactly
// one reports created, and the final fake state must carry the manifest
// settings (whoever lost continued into settings reconciliation).
func TestReconcile_ConcurrentAppliesOnSameMissingIndex(t *testing.T) {
	t.Parallel()

	svc := newRacyService()

	const runs = 2
	var wg sync.WaitGroup
	reports := make([]Report, runs)
	errs := make([]error, runs)
	for i := 0; i < runs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reports[i], errs[i] = Reconcile(context.Background(), svc,
				ReconcileOptions{OnlyUIDs: []string{"test_driver"}, Apply: true, TaskWaitInterval: time.Millisecond}, testManifests)
		}(i)
	}
	wg.Wait()

	for i := 0; i < runs; i++ {
		if errs[i] != nil {
			t.Fatalf("run %d: %v", i, errs[i])
		}
		e := reports[i].Indexes[0]
		if e.Err != nil {
			t.Fatalf("run %d: entry error: %v", i, e.Err)
		}
		if reports[i].HasFailures() {
			t.Fatalf("run %d: report must not flag failures", i)
		}
		switch e.Status {
		case StatusCreated, StatusApplied, StatusOK:
		default:
			t.Fatalf("run %d: status %q must be one of created/applied/ok", i, e.Status)
		}
	}

	created := 0
	for i := 0; i < runs; i++ {
		if reports[i].Indexes[0].Status == StatusCreated {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("exactly one run may report created, got %d", created)
	}

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if !svc.exists {
		t.Fatal("index must exist after the race")
	}
	want := matchedSettings(t, "test_driver")
	if strings.Join(svc.settings.SearchableAttributes, ",") != strings.Join(want.SearchableAttributes, ",") {
		t.Fatalf("final settings must match the manifest, got %+v", svc.settings)
	}
}

// racyService is a mutex-guarded fake emulating Meilisearch's serialized
// per-index task queue closely enough to reproduce the create race: the
// FIRST create for an index succeeds; every later create returns a task
// that FAILS with the typed index_already_exists code. Settings reads and
// writes are also serialized, like real task execution.
type racyService struct {
	meilisearch.ServiceManager // embed: only used methods are implemented

	mu       sync.Mutex
	exists   bool
	created  bool
	primary  string
	settings meilisearch.Settings
	updated  int
}

func newRacyService() *racyService {
	return &racyService{primary: "id"}
}

func (r *racyService) Index(string) meilisearch.IndexManager { return racyIndex{r: r} }

func (r *racyService) GetIndexWithContext(_ context.Context, uid string) (*meilisearch.IndexResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if uid != "test_driver" || !r.exists {
		return nil, &meilisearch.Error{StatusCode: 404}
	}
	return &meilisearch.IndexResult{UID: uid, PrimaryKey: r.primary}, nil
}

func (r *racyService) CreateIndexWithContext(_ context.Context, config *meilisearch.IndexConfig) (*meilisearch.TaskInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if config.Uid != "test_driver" {
		return nil, fmt.Errorf("unexpected uid %q", config.Uid)
	}
	if r.created {
		// Loser: POST accepted (202/taskUid), the task will fail with the
		// typed code — the live Meilisearch 1.x behavior.
		return &meilisearch.TaskInfo{TaskUID: 99, Status: meilisearch.TaskStatusEnqueued}, nil
	}
	r.created = true
	r.exists = true
	r.primary = config.PrimaryKey
	return &meilisearch.TaskInfo{TaskUID: 1, Status: meilisearch.TaskStatusEnqueued}, nil
}

func (r *racyService) WaitForTaskWithContext(_ context.Context, taskUID int64, _ time.Duration) (*meilisearch.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if taskUID == 99 {
		var task meilisearch.Task
		if err := json.Unmarshal([]byte(`{"status":"failed","uid":99,"error":{"code":"index_already_exists","message":"Index `+"`test_driver`"+` already exists."}}`), &task); err != nil {
			return nil, err
		}
		return &task, nil
	}
	return &meilisearch.Task{Status: meilisearch.TaskStatusSucceeded}, nil
}

// racyIndex is the IndexManager view over racyService state.
type racyIndex struct {
	meilisearch.IndexManager // embed: only used methods are implemented
	r                        *racyService
}

func (i racyIndex) GetSettingsWithContext(_ context.Context) (*meilisearch.Settings, error) {
	i.r.mu.Lock()
	defer i.r.mu.Unlock()
	if !i.r.exists {
		return nil, &meilisearch.Error{StatusCode: 404}
	}
	s := i.r.settings
	return &s, nil
}

func (i racyIndex) UpdateSettingsWithContext(_ context.Context, request *meilisearch.Settings) (*meilisearch.TaskInfo, error) {
	i.r.mu.Lock()
	defer i.r.mu.Unlock()
	i.r.updated++
	i.r.settings = *request
	return &meilisearch.TaskInfo{TaskUID: 2, Status: meilisearch.TaskStatusEnqueued}, nil
}
