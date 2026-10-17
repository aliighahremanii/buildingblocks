// Package settingsreconcile is the shared Meilisearch INDEX-SETTINGS
// reconcile engine (D155, extracted from the hosts-owned searchops
// reconciler under the Rule of 2 — the operational search manifest class
// (searchops, 25 indexes) and the report-semantic manifest class
// (reportops, 14 Reporting indexes) are its two consumers). Behavior and
// semantics are IDENTICAL to the searchops reconciler it replaces — this
// is an extraction, never a contract change:
//
//   - The reconciler is the ONLY writer of index settings (D006/D102):
//     neither the API nor any worker ever mutates searchable/filterable/
//     sortable attributes or pagination bounds at startup or per request.
//   - Exactly the FOUR declared settings groups are managed per manifest:
//     searchableAttributes, filterableAttributes (also the facet surface),
//     sortableAttributes, and pagination.maxTotalHits. Ranking rules,
//     displayed attributes, synonyms, stop words, and every other setting
//     are NEVER read with intent to enforce and NEVER written — operator
//     tuning outside the declared surface survives a reconcile.
//   - Every manifest group is an EXPLICIT NON-NIL array (the engine
//     rejects nil groups at Reconcile time): an empty array is the
//     deliberate decision "none" (default-deny — e.g. a report index with
//     no approved free-text attribute), while nil would mean "unset" and
//     the SDK would silently OMIT the group from the settings update,
//     leaving Meilisearch's platform default (`["*"]` searchable) in
//     place. Rendering preserves that distinction: an empty group is sent
//     as `[]`, converging the index onto the explicit "none" decision.
//   - Comparison semantics (D105, verified against Meilisearch 1.12):
//     searchableAttributes is ORDER-SENSITIVE (rank order is semantic);
//     filterable/sortable compare as SETS (Meilisearch canonicalizes them
//     alphabetically, so order-insensitivity is required for the
//     post-apply dry-run closure to be reachable); pagination is exact.
//   - Typed classification ONLY (D115 discipline): not-found and
//     already-exists classification reads the authoritative
//     *meilisearch.Error API code / failed-task error payload — never
//     rendered error text.
//   - Concurrent creation races are adopted ONLY on the typed
//     index_already_exists conflict (both the POST-rejection and the
//     failed-task shapes), after reloading and re-verifying identity
//     (uid + primary key). A primary-key mismatch is a hard human-decision
//     error (Meilisearch primary keys are immutable).
//
// The package is deliberately dependency-light (stdlib + the meilisearch
// SDK + the sibling meilisearch building block's typed task-error type):
// it must not import modules or hosts packages.
package settingsreconcile

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	meilisdk "github.com/meilisearch/meilisearch-go"

	meili "github.com/aliighahremanii/buildingblocks/infrastructure/meilisearch"
)

// Index status values reported by Reconcile.
const (
	StatusOK       = "ok"       // settings already match the manifest
	StatusCreated  = "created"  // index was missing and (on apply) created
	StatusMissing  = "missing"  // index missing, dry-run only
	StatusDrift    = "drift"    // settings differ from the manifest, dry-run only
	StatusApplied  = "applied"  // settings updated to the manifest
	StatusNotFound = "notfound" // a requested uid has no manifest in the caller's inventory
)

// Manifest is the source-controlled settings manifest for one index. The
// three attribute groups are REQUIRED to be non-nil (see the package
// comment): nil is "unset" and is rejected by Validate; an empty non-nil
// array is the explicit decision "none".
type Manifest struct {
	// UID is the Meilisearch index uid. It is owned by the surface that
	// writes the index (a module projection worker or the Reporting
	// ingestion writer) and must never be renamed without a matching
	// writer change.
	UID string

	// PrimaryKey is the document primary key ("id" for every manifest
	// class this engine serves).
	PrimaryKey string

	// SearchableAttributes restricts free-text `q` matching
	// (ORDER-SENSITIVE: rank order is semantic).
	SearchableAttributes []string

	// FilterableAttributes declares the typed-equality filter surface; in
	// Meilisearch these are also the facet-able attributes.
	FilterableAttributes []string

	// SortableAttributes declares the typed sort surface.
	SortableAttributes []string

	// MaxTotalHits is the pagination.maxTotalHits bound.
	MaxTotalHits int64
}

// Validate enforces the manifest contract: identity fields are present,
// the pagination bound is positive, and every attribute group is an
// EXPLICIT non-nil array (nil = "unset" is forbidden — an empty array is
// the deliberate "none" decision; see the package comment).
func (m Manifest) Validate() error {
	if m.UID == "" {
		return errors.New("settingsreconcile: manifest with empty UID")
	}
	if m.PrimaryKey == "" {
		return fmt.Errorf("settingsreconcile: manifest %s: empty primary key", m.UID)
	}
	if m.MaxTotalHits <= 0 {
		return fmt.Errorf("settingsreconcile: manifest %s: maxTotalHits must be positive, got %d", m.UID, m.MaxTotalHits)
	}
	if m.SearchableAttributes == nil {
		return fmt.Errorf("settingsreconcile: manifest %s: nil searchableAttributes — declare an explicit (possibly empty) list", m.UID)
	}
	if m.FilterableAttributes == nil {
		return fmt.Errorf("settingsreconcile: manifest %s: nil filterableAttributes — declare an explicit (possibly empty) list", m.UID)
	}
	if m.SortableAttributes == nil {
		return fmt.Errorf("settingsreconcile: manifest %s: nil sortableAttributes — declare an explicit (possibly empty) list", m.UID)
	}
	return nil
}

// Settings renders the manifest as a Meilisearch BULK settings update. Only
// the four declared groups are included, so a settings update can never
// silently reset ranking rules, display lists, or synonyms. Copies are
// non-nil even for EMPTY groups (make+copy, never append-to-nil) so the
// "none" decision stays explicit in memory.
//
// SDK TRANSPORT CAVEAT: the SDK's Settings struct tags every attribute
// group `omitempty`, so an EMPTY group is OMITTED from the bulk PATCH body
// — it cannot converge an index whose stored value is, e.g., the platform
// default `["*"]`. Reconcile therefore sends each EMPTY group through its
// granular sub-route update (a PUT of the bare `[]`, live-verified accepted
// by Meilisearch as "none") in addition to this bulk update; non-empty
// groups ride the bulk update exactly as before.
func (m Manifest) Settings() meilisdk.Settings {
	return meilisdk.Settings{
		SearchableAttributes: copyAttrs(m.SearchableAttributes),
		FilterableAttributes: copyAttrs(m.FilterableAttributes),
		SortableAttributes:   copyAttrs(m.SortableAttributes),
		Pagination:           &meilisdk.Pagination{MaxTotalHits: m.MaxTotalHits},
	}
}

// copyAttrs copies an attribute group non-nil (empty stays empty-but-set).
func copyAttrs(attrs []string) []string {
	out := make([]string, len(attrs))
	copy(out, attrs)
	return out
}

// ReconcileOptions controls one reconcile run.
type ReconcileOptions struct {
	// OnlyUIDs limits the run to the given index uids. Empty means every
	// manifest in the caller's inventory. Unknown uids are reported as
	// StatusNotFound entries.
	OnlyUIDs []string

	// Apply performs the changes. false is a dry run: drift is reported,
	// nothing is mutated.
	Apply bool

	// TaskWaitInterval is the polling interval while waiting for Meilisearch
	// async tasks (index creation, settings updates). Zero means 100ms.
	TaskWaitInterval time.Duration
}

// IndexReport is the per-index outcome of one reconcile run.
type IndexReport struct {
	UID    string
	Status string
	// Drift lists the human-readable differences found (one line per
	// settings group), for both dry-run reporting and the applied audit log.
	Drift []string
	Err   error
}

// Report is the full outcome of one reconcile run.
type Report struct {
	Indexes []IndexReport
}

// HasFailures reports whether any index errored.
func (r Report) HasFailures() bool {
	for _, i := range r.Indexes {
		if i.Err != nil {
			return true
		}
	}
	return false
}

// HasDrift reports whether any index is missing or has settings drift.
func (r Report) HasDrift() bool {
	for _, i := range r.Indexes {
		if i.Status == StatusDrift || i.Status == StatusMissing {
			return true
		}
	}
	return false
}

// Reconcile brings the Meilisearch index inventory into agreement with the
// caller's source-controlled manifests (the operational searchops class,
// the reportops class, or a composition of both). It is the ONLY writer of
// these settings: neither the API nor the workers ever mutate index
// settings at startup or per request (D006, D102); operators run this
// explicitly via `admin meilisearch-reconcile`.
//
// Per index it: (1) creates the index with the manifest primary key when
// missing, (2) reads current settings, (3) compares the four declared
// groups (searchableAttributes order-sensitively, filterable/sortable as
// canonicalized sets, pagination exactly), and (4) on Apply, writes the
// manifest settings and waits for the async task. Groups not declared in a
// manifest (ranking rules, displayed attributes, synonyms, stop words,
// ...) are never read with intent to enforce and never written, so a
// reconcile can never clobber operator-tuned settings outside the declared
// surface.
//
// All manifests are validated (and checked for duplicate uids) BEFORE the
// server is touched: a malformed manifest inventory is a caller defect and
// must fail the whole run, never half-reconcile.
func Reconcile(ctx context.Context, svc meilisdk.ServiceManager, opts ReconcileOptions, manifests []Manifest) (Report, error) {
	if svc == nil {
		return Report{}, errors.New("settingsreconcile.Reconcile: nil Meilisearch client")
	}
	seen := make(map[string]struct{}, len(manifests))
	for _, m := range manifests {
		if err := m.Validate(); err != nil {
			return Report{}, err
		}
		if _, dup := seen[m.UID]; dup {
			return Report{}, fmt.Errorf("settingsreconcile.Reconcile: duplicate manifest uid %s — the inventory must be disjoint", m.UID)
		}
		seen[m.UID] = struct{}{}
	}
	interval := opts.TaskWaitInterval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}

	selected := manifests
	var unknownUIDs []string
	if len(opts.OnlyUIDs) > 0 {
		wanted := append([]string(nil), opts.OnlyUIDs...)
		sort.Strings(wanted)
		byUID := make(map[string]Manifest, len(manifests))
		for _, m := range manifests {
			byUID[m.UID] = m
		}
		selected = nil
		for _, uid := range wanted {
			manifest, ok := byUID[uid]
			if !ok {
				// Unknown --index uid: report it as notfound here and never
				// hand a synthetic manifest to reconcileIndex — the index
				// reconciler must faithfully reconcile ANY manifest it is
				// given (the live race tests drive it with a disposable
				// non-inventory uid), so unknown-uid classification stays
				// in THIS selection layer.
				unknownUIDs = append(unknownUIDs, uid)
				continue
			}
			selected = append(selected, manifest)
		}
	}

	report := Report{Indexes: make([]IndexReport, 0, len(selected)+len(unknownUIDs))}
	for _, uid := range unknownUIDs {
		report.Indexes = append(report.Indexes, IndexReport{UID: uid, Status: StatusNotFound})
	}
	for _, manifest := range selected {
		entry, err := reconcileIndex(ctx, svc, manifest, opts, interval)
		if err != nil {
			entry.Err = err
		}
		report.Indexes = append(report.Indexes, entry)
	}
	return report, nil
}

// reconcileIndex converges ONE index onto its manifest. The manifest may be
// any legitimate Manifest — membership in an inventory is the caller's
// (Reconcile's) concern, not this function's.
func reconcileIndex(ctx context.Context, svc meilisdk.ServiceManager, manifest Manifest, opts ReconcileOptions, interval time.Duration) (IndexReport, error) {
	entry := IndexReport{UID: manifest.UID}

	// (1) Existence. Missing index: create (apply) or report (dry run).
	if _, err := svc.GetIndexWithContext(ctx, manifest.UID); err != nil {
		if !isIndexNotFound(err) {
			return entry, fmt.Errorf("get index %s: %w", manifest.UID, err)
		}
		if !opts.Apply {
			entry.Status = StatusMissing
			entry.Drift = []string{"index does not exist"}
			return entry, nil
		}
		created, err := ensureIndex(ctx, svc, manifest, interval)
		if err != nil {
			return entry, err
		}
		if created {
			entry.Status = StatusCreated
			entry.Drift = []string{"index created with primary key " + manifest.PrimaryKey}
		} else {
			// A concurrent reconciler created the index between our
			// existence check and our creation task. We verified its
			// identity (uid + primary key) and adopted it; record the
			// adoption because the report is the audit artifact of an
			// --apply run.
			entry.Drift = []string{
				"index created concurrently by another reconciler; verified primary key " + manifest.PrimaryKey + " and adopted",
			}
		}
	}

	// (2) Read current settings.
	idx := svc.Index(manifest.UID)
	current, err := idx.GetSettingsWithContext(ctx)
	if err != nil {
		return entry, fmt.Errorf("get settings %s: %w", manifest.UID, err)
	}

	// (3) Compare the four declared groups (searchableAttributes is
	// order-sensitive; filterable/sortable compare as sets because
	// Meilisearch canonicalizes them; pagination is an exact scalar).
	drift := compareSettings(manifest, current)
	if len(drift) == 0 {
		if entry.Status == "" {
			entry.Status = StatusOK
		}
		return entry, nil
	}
	entry.Drift = append(entry.Drift, drift...)

	// (4) Apply or report.
	if !opts.Apply {
		entry.Status = StatusDrift
		return entry, nil
	}
	settings := manifest.Settings()
	task, err := idx.UpdateSettingsWithContext(ctx, &settings)
	if err != nil {
		return entry, fmt.Errorf("update settings %s: %w", manifest.UID, err)
	}
	if err := waitTask(ctx, svc, task.TaskUID, interval); err != nil {
		return entry, fmt.Errorf("wait settings task %s: %w", manifest.UID, err)
	}
	// The SDK's bulk Settings PATCH OMITS empty groups (`omitempty`), so an
	// explicit "none" decision cannot ride it — converge each EMPTY group
	// through its granular sub-route (PUT of the bare `[]`, accepted by
	// Meilisearch as "none" — live-verified). Non-empty groups already
	// converged via the bulk update above.
	if err := applyEmptyGroups(ctx, svc, idx, manifest, interval); err != nil {
		return entry, err
	}
	if entry.Status == StatusCreated {
		entry.Status = StatusCreated // created AND configured
	} else {
		entry.Status = StatusApplied
	}
	return entry, nil
}

// applyEmptyGroups converges the manifest's EMPTY attribute groups (the
// explicit default-deny "none" decision) onto the index. The SDK's Settings
// struct tags every group `omitempty`, so the bulk PATCH silently drops an
// empty slice and the stored value (e.g. the platform default `["*"]`)
// would survive the apply; the granular per-group updates PUT the bare
// array, which the server stores as "none". Meilisearch executes per-index
// tasks FIFO, and every task is waited to its terminal state.
func applyEmptyGroups(ctx context.Context, svc meilisdk.ServiceManager, idx meilisdk.IndexManager, manifest Manifest, interval time.Duration) error {
	if len(manifest.SearchableAttributes) == 0 {
		empty := []string{}
		task, err := idx.UpdateSearchableAttributesWithContext(ctx, &empty)
		if err != nil {
			return fmt.Errorf("update searchableAttributes %s: %w", manifest.UID, err)
		}
		if err := waitTask(ctx, svc, task.TaskUID, interval); err != nil {
			return fmt.Errorf("wait searchableAttributes task %s: %w", manifest.UID, err)
		}
	}
	if len(manifest.FilterableAttributes) == 0 {
		empty := []interface{}{}
		task, err := idx.UpdateFilterableAttributesWithContext(ctx, &empty)
		if err != nil {
			return fmt.Errorf("update filterableAttributes %s: %w", manifest.UID, err)
		}
		if err := waitTask(ctx, svc, task.TaskUID, interval); err != nil {
			return fmt.Errorf("wait filterableAttributes task %s: %w", manifest.UID, err)
		}
	}
	if len(manifest.SortableAttributes) == 0 {
		empty := []string{}
		task, err := idx.UpdateSortableAttributesWithContext(ctx, &empty)
		if err != nil {
			return fmt.Errorf("update sortableAttributes %s: %w", manifest.UID, err)
		}
		if err := waitTask(ctx, svc, task.TaskUID, interval); err != nil {
			return fmt.Errorf("wait sortableAttributes task %s: %w", manifest.UID, err)
		}
	}
	return nil
}

// compareSettings returns one human-readable drift line per declared
// settings group that differs. Order sensitivity follows Meilisearch's
// storage semantics (verified against Meilisearch 1.12, E2E-1):
//
//   - searchableAttributes: ORDER-SENSITIVE — the rank order is semantic
//     (Meilisearch preserves it and uses it for field priority), so a
//     same-members reorder is drift.
//   - filterableAttributes / sortableAttributes: ORDER-INSENSITIVE —
//     Meilisearch stores these as sets and canonicalizes them
//     (alphabetically) regardless of submission order, so an
//     order-sensitive comparison would report permanent drift after every
//     apply and the runbook's post-apply dry-run closure (exit 0) would be
//     unreachable for any non-alphabetical manifest.
//   - pagination.maxTotalHits: exact scalar comparison.
//
// The drift line still renders the raw stored list (have) and the manifest
// list (want) in their own orders, so an operator reviewing a genuine
// membership difference sees exactly what Meilisearch returns.
func compareSettings(manifest Manifest, current *meilisdk.Settings) []string {
	var drift []string
	if d := compareAttrGroup("searchableAttributes", manifest.SearchableAttributes, current.SearchableAttributes); d != "" {
		drift = append(drift, d)
	}
	if d := compareAttrSet("filterableAttributes", manifest.FilterableAttributes, current.FilterableAttributes); d != "" {
		drift = append(drift, d)
	}
	if d := compareAttrSet("sortableAttributes", manifest.SortableAttributes, current.SortableAttributes); d != "" {
		drift = append(drift, d)
	}
	currentMax := int64(0)
	if current.Pagination != nil {
		currentMax = current.Pagination.MaxTotalHits
	}
	if currentMax != manifest.MaxTotalHits {
		drift = append(drift, fmt.Sprintf("pagination.maxTotalHits: %d -> %d", currentMax, manifest.MaxTotalHits))
	}
	return drift
}

// compareAttrGroup reports a difference line when want and have differ as
// ordered lists (position carries meaning), treating nil and empty as
// equivalent (the stored side may be either; the manifest side is
// validated non-nil).
func compareAttrGroup(name string, want, have []string) string {
	if equalAttrs(want, have) {
		return ""
	}
	return fmt.Sprintf("%s: [%s] -> [%s]", name, strings.Join(have, ", "), strings.Join(want, ", "))
}

// compareAttrSet reports a difference line when want and have differ as
// SETS: same length and same member multiset, any order. Meilisearch
// canonicalizes filterable and sortable attributes, so only membership
// matters for convergence. nil and empty remain equivalent, and the
// untouched-index default ["*"] still never equals a manifest list —
// exactly the drift we want to surface.
func compareAttrSet(name string, want, have []string) string {
	if equalAttrSets(want, have) {
		return ""
	}
	return fmt.Sprintf("%s: [%s] -> [%s]", name, strings.Join(have, ", "), strings.Join(want, ", "))
}

func equalAttrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// equalAttrSets reports set equality (order-insensitive). Duplicates in
// either list break equality via the length/sorted-pair comparison, so a
// manifest that (incorrectly) declared an attribute twice still surfaces as
// drift instead of silently matching the deduplicated stored set.
func equalAttrSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as := append([]string(nil), a...)
	bs := append([]string(nil), b...)
	sort.Strings(as)
	sort.Strings(bs)
	for i := range as {
		if as[i] != bs[i] {
			return false
		}
	}
	return true
}

// waitTask blocks until the async Meilisearch task reaches a terminal
// state; a terminally FAILED task surfaces as the typed TaskFailedError
// (the sibling meilisearch building block's shared type) so its
// authoritative error code stays classifiable.
func waitTask(ctx context.Context, svc meilisdk.ServiceManager, taskUID int64, interval time.Duration) error {
	task, err := svc.WaitForTaskWithContext(ctx, taskUID, interval)
	if err != nil {
		return err
	}
	if task != nil && task.Status == meilisdk.TaskStatusFailed {
		return &meili.TaskFailedError{TaskUID: taskUID, Code: task.Error.Code, Message: task.Error.Message}
	}
	return nil
}

// isIndexNotFound reports whether err is the authoritative Meilisearch
// index_not_found response. Typed *meilisearch.Error classification ONLY:
// any other error — including one whose text mentions 404 or "not found" —
// is NOT a not-found and propagates as an operational failure.
func isIndexNotFound(err error) bool {
	var merr *meilisdk.Error
	if !errors.As(err, &merr) {
		return false
	}
	return merr.StatusCode == http.StatusNotFound ||
		merr.MeilisearchApiError.Code == meili.CodeIndexNotFound
}

// isIndexAlreadyExists reports whether err is an authoritative Meilisearch
// index_already_exists rejection (a server that refuses the create POST
// outright instead of failing the enqueued task).
func isIndexAlreadyExists(err error) bool {
	return meili.IsIndexAlreadyExists(err)
}

// isTaskIndexAlreadyExists reports whether err is a failed async task whose
// authoritative error code is index_already_exists.
func isTaskIndexAlreadyExists(err error) bool {
	return meili.TaskFailedIndexAlreadyExists(err)
}

// ensureIndex creates the manifest's index, tolerating a concurrent
// reconciler that wins the creation race. Meilisearch serializes tasks per
// index: when two --apply runs create the same missing index, the loser's
// creation task FAILS with the authoritative index_already_exists code (the
// create POST itself returns 202; some server variants reject the POST
// outright with the same code — both shapes are handled). On that conflict,
// and ONLY on the typed conflict, the existing index is reloaded and its
// identity verified: same uid and same primary key. A primary-key mismatch
// is a hard error — Meilisearch primary keys are immutable, so the index can
// never be converged onto the manifest by writing settings; it needs a human
// decision. ensureIndex reports whether THIS run created the index.
func ensureIndex(ctx context.Context, svc meilisdk.ServiceManager, manifest Manifest, interval time.Duration) (bool, error) {
	if task, err := svc.CreateIndexWithContext(ctx, &meilisdk.IndexConfig{
		Uid:        manifest.UID,
		PrimaryKey: manifest.PrimaryKey,
	}); err != nil {
		if !isIndexAlreadyExists(err) {
			return false, fmt.Errorf("create index %s: %w", manifest.UID, err)
		}
		// Typed already-exists rejection of the POST itself: adopt below.
	} else if werr := waitTask(ctx, svc, task.TaskUID, interval); werr != nil {
		if !isTaskIndexAlreadyExists(werr) {
			return false, fmt.Errorf("wait create index %s: %w", manifest.UID, werr)
		}
		// Creation task failed with index_already_exists — a concurrent
		// reconciler won the race: adopt below.
	} else {
		return true, nil
	}

	existing, err := svc.GetIndexWithContext(ctx, manifest.UID)
	if err != nil {
		return false, fmt.Errorf("reload concurrently created index %s: %w", manifest.UID, err)
	}
	if existing.UID != manifest.UID {
		return false, fmt.Errorf("reload concurrently created index %s: server returned uid %q", manifest.UID, existing.UID)
	}
	if existing.PrimaryKey != manifest.PrimaryKey {
		return false, fmt.Errorf(
			"index %s already exists with primary key %q but the manifest declares %q — Meilisearch primary keys are immutable; resolve the mismatch manually (delete the index only if its documents are reproducible)",
			manifest.UID, existing.PrimaryKey, manifest.PrimaryKey)
	}
	return false, nil
}
