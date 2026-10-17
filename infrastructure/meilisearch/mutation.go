// mutation.go — the shared Meilisearch DOCUMENT-MUTATION machinery
// (D111 + D115 + D113), extracted from the operations module's projection
// sink under the Rule of 2 (the Reporting module's document writer is the
// second consumer). Behavior and semantics are IDENTICAL to the
// operations machinery it replaces — this is an extraction, never a
// contract change.
//
//   - D111 (terminal task wait): every document mutation (add/update/
//     delete) WAITS for its async Meilisearch task to reach a terminal
//     state before returning. Enqueue alone is not success — a caller's
//     durable advance (projection checkpoint, ingestion completion) may
//     happen only after the mutation is durably applied, and a terminally
//     failed task must surface as an error, never as a silently lost write.
//   - D115 (ensure-before-mutation): the index is created explicitly
//     (primary key "id") and terminally verified BEFORE the first document
//     write, so mutations never depend on the server's implicit
//     auto-create semantics. Cross-process creation races are adopted on
//     the TYPED index_already_exists conflict (both the POST-rejection and
//     the failed-task shapes) after reloading and re-verifying identity.
//     A primary-key mismatch is a hard human-decision error (primary keys
//     are immutable). In-process coordination is per-UID: a SUCCESSFUL
//     ensure latches once per process; a failure unlatches for retry.
//     Index SETTINGS are NEVER touched here — they are owned by
//     `admin meilisearch-reconcile`.
//   - D113 (explicit primary key): the primary key is ALWAYS the explicit
//     "id" field. Meilisearch's key inference fails on documents with
//     several id-suffixed fields (bolId, bolRowId, countryID, ...) and the
//     whole task silently fails.
//   - D118: every HTTP round trip is bounded by the shared OpenClient
//     30s per-request timeout; the terminal-task wait itself is per poll
//     round, never bounded by the HTTP timeout policy.
//
// The typed classification helpers classify from the AUTHORITATIVE
// *meilisdk.Error API code (or a failed task's error payload) — never
// from rendered error strings, which would misclassify transport noise
// that happens to contain "404" or "not found".
package meilisearch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	meilisdk "github.com/meilisearch/meilisearch-go"
)

// Meilisearch API error codes used for typed classification
// (docs.meilisearch.com/errors). They are read from the authoritative
// *meilisdk.Error envelope or a failed task's error payload.
const (
	CodeIndexNotFound      = "index_not_found"
	CodeIndexAlreadyExists = "index_already_exists"
	// CodeDocumentNotFound is the authoritative document_not_found
	// envelope a deleting mutation's existence guard treats as a no-op.
	CodeDocumentNotFound = "document_not_found"
)

// DocPrimaryKey is the explicit primary key every document written through
// this machinery carries ("id" = the document id, D113). Meilisearch's key
// inference fails on documents with several id-suffixed fields and the
// whole task silently fails — always pass it explicitly.
const DocPrimaryKey = "id"

// taskWaitInterval is the polling interval while waiting for a Meilisearch
// async task to reach a terminal state (D111's synchronous terminal wait —
// one round per event batch write). Halved from the meilisearch-reconcile
// verb's 100ms default to cut per-event projection latency; correctness
// never depends on the interval, only on reaching the terminal state
// itself.
const taskWaitInterval = 50 * time.Millisecond

// DocumentMutator carries the shared D111/D115/D113 document-mutation
// machinery over one Meilisearch service. Compose ONE value per process
// per service and SHARE it across every index writer (the embedded
// per-process ensure latch is per-UID: one map covering every index) —
// passing several instances to sibling writers would create redundant
// latch maps, never a correctness hazard (the ensure round is idempotent),
// but also never a benefit.
//
// It is inert until first used — a nil service must never reach a
// dereference; callers follow the D049 optional-infrastructure rule and
// only compose (or surface) the writer when the service is configured.
type DocumentMutator struct {
	meili meilisdk.ServiceManager

	// ensureMu guards ensured: per-UID in-process coordination for
	// ensure-before-mutation (D115). The first caller for a UID performs
	// the create/verify round trip; concurrent callers share its result; a
	// SUCCESSFUL ensure latches so each index pays at most one ensure per
	// process, while a FAILED ensure never latches (the retry re-ensures
	// from scratch).
	ensureMu sync.Mutex
	ensured  map[string]*ensureResult
}

// ensureResult is the shared outcome of one ensure call. err is written
// before done is closed; readers synchronize on the channel close.
type ensureResult struct {
	done chan struct{}
	err  error
}

// NewDocumentMutator composes the machinery over the shared Meilisearch
// service (opened through OpenClient — the D118 bounded client).
func NewDocumentMutator(meili meilisdk.ServiceManager) *DocumentMutator {
	return &DocumentMutator{meili: meili}
}

func docOpts() *meilisdk.DocumentOptions {
	pk := DocPrimaryKey
	return &meilisdk.DocumentOptions{PrimaryKey: &pk}
}

// AddDocuments ENSURES the index exists (D115), enqueues a document add,
// and waits for the async Meilisearch task to reach terminal success
// (D111). The primary key is ALWAYS the explicit "id" field (D113).
func (m *DocumentMutator) AddDocuments(ctx context.Context, indexUID string, docs []map[string]any) error {
	if err := m.EnsureIndex(ctx, indexUID); err != nil {
		return err
	}
	info, err := m.meili.Index(indexUID).AddDocumentsWithContext(ctx, docs, docOpts())
	if err != nil {
		return err
	}
	return m.WaitTask(ctx, info)
}

// UpdateDocuments ENSURES the index exists (D115), enqueues a document
// update, and waits for the async Meilisearch task to reach terminal
// success (D111). Primary key is the explicit "id" field (D113).
func (m *DocumentMutator) UpdateDocuments(ctx context.Context, indexUID string, docs []map[string]any) error {
	if err := m.EnsureIndex(ctx, indexUID); err != nil {
		return err
	}
	info, err := m.meili.Index(indexUID).UpdateDocumentsWithContext(ctx, docs, docOpts())
	if err != nil {
		return err
	}
	return m.WaitTask(ctx, info)
}

// DeleteDocuments ENSURES the index exists (D115), enqueues a document
// delete, and waits for the async Meilisearch task to reach terminal
// success (D111).
func (m *DocumentMutator) DeleteDocuments(ctx context.Context, indexUID string, ids []string) error {
	if err := m.EnsureIndex(ctx, indexUID); err != nil {
		return err
	}
	info, err := m.meili.Index(indexUID).DeleteDocumentsWithContext(ctx, ids, nil)
	if err != nil {
		return err
	}
	return m.WaitTask(ctx, info)
}

// GetDocument reads one document into dest. It performs NO ensure and NO
// mutation — it is the raw typed read the D112 existence guards run BEFORE
// any ensure, so a guard miss has zero side effects (neither a document nor
// an index is created).
func (m *DocumentMutator) GetDocument(ctx context.Context, indexUID, docID string, dest any) error {
	return m.meili.Index(indexUID).GetDocumentWithContext(ctx, docID, nil, dest)
}

// DeleteDocumentsGuarded deletes documents under the D112 existence-guard
// contract: the guard read runs BEFORE any ensure, and a missing document
// (typed document_not_found) OR a missing index (typed index_not_found — an
// absent index cannot contain the document) is a NO-OP with ZERO side
// effects: no delete task is enqueued, and crucially the index is NOT
// created — a deleting mutation must never be the reason an index springs
// into existence. Any other guard-read error is an operational failure and
// propagates (it must never be mistaken for "document absent"). When the
// document exists, the delete proceeds through the normal D115/D111 path
// (ensure-before-mutation + terminal task wait).
func (m *DocumentMutator) DeleteDocumentsGuarded(ctx context.Context, indexUID string, ids []string) error {
	var probe struct {
		ID string `json:"id"`
	}
	for _, id := range ids {
		err := m.GetDocument(ctx, indexUID, id, &probe)
		if err != nil {
			if IsDocumentNotFound(err) || IsIndexNotFound(err) {
				return nil // documented no-op: deleting an absent document (or from an absent index)
			}
			return fmt.Errorf("guard read %s/%s: %w", indexUID, id, err)
		}
	}
	return m.DeleteDocuments(ctx, indexUID, ids)
}

// EnsureIndex guarantees the index exists with primary key "id" before a
// document mutation, coordinated per UID (D115). In-process: the first
// caller performs the work and SUCCESS latches (one ensure round per index
// per process); concurrent callers wait for the in-flight result instead
// of stampeding; a failure unlatches so the retry re-ensures.
// Cross-process: a concurrent creator (admin meilisearch-reconcile, a
// worker replica) that wins the creation race is ADOPTED on the typed
// index_already_exists conflict after reloading and verifying the index
// identity. Settings are never touched here — admin
// meilisearch-reconcile owns searchable/filterable/sortable/facet
// settings; ensure only removes the document-mutation dependency on
// implicit server-side index auto-create.
func (m *DocumentMutator) EnsureIndex(ctx context.Context, indexUID string) error {
	m.ensureMu.Lock()
	if m.ensured == nil {
		m.ensured = make(map[string]*ensureResult)
	}
	if res, ok := m.ensured[indexUID]; ok {
		m.ensureMu.Unlock()
		select {
		case <-res.done:
			return res.err
		case <-ctx.Done():
			return fmt.Errorf("ensure index %s: %w", indexUID, ctx.Err())
		}
	}
	res := &ensureResult{done: make(chan struct{})}
	m.ensured[indexUID] = res
	m.ensureMu.Unlock()

	err := m.ensureIndexOnce(ctx, indexUID)

	m.ensureMu.Lock()
	res.err = err
	if err != nil {
		// Never latch a failure: the retry path (redelivery or a later
		// event) must re-attempt the full get/create/verify round.
		delete(m.ensured, indexUID)
	}
	m.ensureMu.Unlock()
	close(res.done)
	return err
}

// ensureIndexOnce performs one idempotent ensure round for indexUID:
// (1) if the index exists, verify its identity (uid + primary key) and
// adopt it — this is also the reconciled-deployment fast path; (2) if the
// typed get says index_not_found, create the index with the D113 primary
// key and wait for TERMINAL task success (strict D111 handling: only an
// explicit succeeded create is success); (3) adopt a cross-process
// creation race loser's outcome — the create POST rejected with the typed
// index_already_exists envelope, or the creation task terminally failed
// with the same typed code (per-index FIFO task serialization) — by
// reloading and verifying. Every other outcome propagates as an error.
func (m *DocumentMutator) ensureIndexOnce(ctx context.Context, indexUID string) error {
	existing, err := m.meili.GetIndexWithContext(ctx, indexUID)
	if err == nil {
		return verifyEnsuredIndex(indexUID, existing)
	}
	if !IsIndexNotFound(err) {
		return fmt.Errorf("get index %s: %w", indexUID, err)
	}

	task, err := m.meili.CreateIndexWithContext(ctx, &meilisdk.IndexConfig{
		Uid:        indexUID,
		PrimaryKey: DocPrimaryKey,
	})
	if err != nil {
		if !IsIndexAlreadyExists(err) {
			return fmt.Errorf("create index %s: %w", indexUID, err)
		}
		// Typed already-exists rejection of the create POST itself: another
		// process created the index between our get and our create — adopt.
	} else if werr := m.WaitTask(ctx, task); werr != nil {
		if !TaskFailedIndexAlreadyExists(werr) {
			return fmt.Errorf("wait create index %s: %w", indexUID, werr)
		}
		// The creation task terminally failed with index_already_exists:
		// another process's creation task won the per-index FIFO race —
		// adopt.
	} else {
		return nil // created by this process and terminally verified
	}

	adopted, err := m.meili.GetIndexWithContext(ctx, indexUID)
	if err != nil {
		return fmt.Errorf("reload concurrently created index %s: %w", indexUID, err)
	}
	return verifyEnsuredIndex(indexUID, adopted)
}

// verifyEnsuredIndex checks that an existing/adopted index is the one this
// machinery writes: same uid and the D113 document primary key. A
// primary-key mismatch is a HARD error — Meilisearch primary keys are
// immutable, so no settings write can converge a wrongly-keyed index; it
// needs a human decision (delete only if the documents are reproducible
// from a rebuild).
func verifyEnsuredIndex(indexUID string, idx *meilisdk.IndexResult) error {
	if idx == nil {
		return fmt.Errorf("verify index %s: server returned no index", indexUID)
	}
	if idx.UID != indexUID {
		return fmt.Errorf("verify index %s: server returned uid %q", indexUID, idx.UID)
	}
	if idx.PrimaryKey != DocPrimaryKey {
		return fmt.Errorf(
			"index %s exists with primary key %q but the documents are keyed %q — Meilisearch primary keys are immutable and index settings are owned by admin meilisearch-reconcile; resolve the mismatch manually (delete the index only if its documents are reproducible from a rebuild)",
			indexUID, idx.PrimaryKey, DocPrimaryKey)
	}
	return nil
}

// TaskFailedError is the typed failure of an async Meilisearch task. It
// carries the authoritative API error code so callers can classify
// failures (e.g. a concurrent creator winning an index-creation race) from
// typed fields, never from rendered error text (D115).
type TaskFailedError struct {
	TaskUID int64
	Code    string
	Message string
}

func (e *TaskFailedError) Error() string {
	return fmt.Sprintf("meilisearch task %d failed: %s (%s)", e.TaskUID, e.Message, e.Code)
}

// WaitTask blocks until the async Meilisearch task reaches a terminal
// state and converts everything except explicit terminal success into an
// error. Success is EXACTLY a non-nil task with TaskStatusSucceeded
// (final-review fix of D111): the SDK's wait returns any
// non-enqueued/processing status as done, so a canceled task (documents
// never applied), a nil task response, and any unexpected status (e.g.
// "unknown") must all propagate as errors instead of silently passing.
// Enqueue alone is never success either — a nil TaskInfo ticket cannot be
// waited on at all. A terminally FAILED task surfaces as the typed
// TaskFailedError so its authoritative code stays classifiable.
//
// A wait error is AMBIGUOUS by nature: the task may still complete
// server-side afterwards. Meilisearch executes tasks FIFO per index, so a
// task that eventually succeeds still applies in enqueue order — an
// ambiguous write cannot overtake later tasks enqueued after it. The error
// still propagates regardless (DLQ + rebuild is the documented recovery).
func (m *DocumentMutator) WaitTask(ctx context.Context, info *meilisdk.TaskInfo) error {
	if info == nil {
		return fmt.Errorf("meilisearch enqueue returned no task ticket: success cannot be verified")
	}
	task, err := m.meili.WaitForTaskWithContext(ctx, info.TaskUID, taskWaitInterval)
	if err != nil {
		return fmt.Errorf("wait meilisearch task %d: %w", info.TaskUID, err)
	}
	if task == nil {
		return fmt.Errorf("meilisearch task %d: wait returned no task: success cannot be verified", info.TaskUID)
	}
	switch task.Status {
	case meilisdk.TaskStatusSucceeded:
		return nil
	case meilisdk.TaskStatusFailed:
		return &TaskFailedError{TaskUID: info.TaskUID, Code: task.Error.Code, Message: task.Error.Message}
	case meilisdk.TaskStatusCanceled:
		if task.Error.Code != "" || task.Error.Message != "" {
			return fmt.Errorf("meilisearch task %d canceled: %s (%s)", info.TaskUID, task.Error.Message, task.Error.Code)
		}
		return fmt.Errorf("meilisearch task %d canceled", info.TaskUID)
	default:
		return fmt.Errorf("meilisearch task %d resolved with unexpected status %q", info.TaskUID, string(task.Status))
	}
}

// IsDocumentNotFound classifies the authoritative Meilisearch
// document_not_found envelope (typed API error code — never rendered error
// text, which would misclassify transport noise).
func IsDocumentNotFound(err error) bool {
	var merr *meilisdk.Error
	if !errors.As(err, &merr) {
		return false
	}
	return merr.MeilisearchApiError.Code == CodeDocumentNotFound
}

// IsIndexNotFound classifies the authoritative Meilisearch index_not_found
// envelope — the response a guard read receives when the index itself does
// not exist (the ephemeral/unreconciled-stack case, CI run #3058). Typed
// API error code only: anything else — including transport noise whose
// text mentions 404 — propagates as an operational failure.
func IsIndexNotFound(err error) bool {
	var merr *meilisdk.Error
	if !errors.As(err, &merr) {
		return false
	}
	return merr.MeilisearchApiError.Code == CodeIndexNotFound
}

// IsIndexAlreadyExists reports whether err is an authoritative Meilisearch
// index_already_exists rejection (a server that refuses the create POST
// outright instead of failing the enqueued task).
func IsIndexAlreadyExists(err error) bool {
	var merr *meilisdk.Error
	if !errors.As(err, &merr) {
		return false
	}
	return merr.MeilisearchApiError.Code == CodeIndexAlreadyExists
}

// TaskFailedIndexAlreadyExists reports whether err is a failed async task
// whose authoritative error code is index_already_exists — the shape a
// per-index-FIFO creation race produces when a concurrent creator's task
// applied first.
func TaskFailedIndexAlreadyExists(err error) bool {
	var tf *TaskFailedError
	return errors.As(err, &tf) && tf.Code == CodeIndexAlreadyExists
}
