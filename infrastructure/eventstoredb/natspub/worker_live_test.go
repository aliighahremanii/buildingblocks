package natspub_test

// worker_live_test.go — live end-to-end coverage of the publisher worker
// loop (the coverage the two long-standing placeholders in worker_test.go
// referenced), gated on KURRENTDB_URL + NATS_URL + POSTGRES_DSN
// (auto-skip otherwise, per repo test policy; FAILS instead when the run
// declares integration tests required — BUILDINGBLOCKS_REQUIRE_INTEGRATION).
//
// It is an EXTERNAL test package (natspub_test) because it wires the
// REAL natsadapter validator and publisher; natsadapter imports natspub,
// so an internal test file would create an import cycle. Default backoff
// delays are used: the 700ms observation windows below stay inside the
// first attempt's 1s initial backoff, keeping the log-dedupe assertions
// deterministic.
//
// It proves through real services, in this order:
//
//  1. the happy path: validation passes → the KurrentDB $all subscription
//     opens → appended test events are republished to the test-owned
//     JetStream stream with Nats-Msg-Id = KurrentDB EventID → the durable
//     position advances past the appended events;
//  2. the production-hypothesis failure path (D114): when the stream
//     subjects are "<prefix>.*" the worker parks in the validation gate —
//     no KurrentDB subscription is opened, nothing is published, no
//     position row is created — and the failure is logged once with
//     actionable guidance.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
	"github.com/nats-io/nats.go"

	kurrenttest "github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/kurrenttest"
	natspub "github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/natspub"
	natsadapter "github.com/aliighahremanii/buildingblocks/infrastructure/nats/natsadapter"
	pgnatspub "github.com/aliighahremanii/buildingblocks/infrastructure/postgres/natspub"
	"github.com/aliighahremanii/buildingblocks/infrastructure/postgres/postgrestest"
	"github.com/aliighahremanii/buildingblocks/infrastructure/testenv"
)

// syncedBuffer guards the log buffer: the worker goroutine writes while
// the test reads (a plain bytes.Buffer would race under -race).
type syncedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncedBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncedBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func captureLogs(t *testing.T) *syncedBuffer {
	t.Helper()
	buf := &syncedBuffer{}
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Writer(buf), &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(old) })
	return buf
}

// liveEnv loads .env once (mirrors kurrenttest) so a developer with the
// dev compose stack can run these tests without exporting variables.
// Discovery is the root-safe, testable helper in dotenv_discovery_test.go
// (the CI run #3058 regression: the previous inline walker derived the
// parent directory with dir[:strings.LastIndex(dir, "/")] and panicked with
// slice bounds [:-1] once the walk reached the filesystem root, which every
// CI live test did — the integration image runs at /src and .env is
// excluded by .dockerignore).
var liveEnvOnce sync.Once

func liveLoadDotEnv() {
	liveEnvOnce.Do(func() {
		dir, err := os.Getwd()
		if err != nil {
			return
		}
		path := findDotEnvFrom(dir)
		if path == "" {
			return
		}
		_ = applyDotEnvFile(path)
	})
}

// openLiveNATSConn connects to NATS_URL with a bounded retry dial and
// skips when unset/unreachable (the CI compose starts NATS without a
// healthcheck; retry absorbs boot). When integration tests are REQUIRED
// (BUILDINGBLOCKS_REQUIRE_INTEGRATION) both conditions FAIL instead.
func openLiveNATSConn(t *testing.T) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()

	if os.Getenv("NATS_URL") == "" {
		testenv.SkipOrFail(t, "live natspub worker test skipped: NATS_URL not set")
	}
	var nc *nats.Conn
	deadline := time.Now().Add(15 * time.Second)
	for {
		var err error
		nc, err = nats.Connect(os.Getenv("NATS_URL"),
			nats.Name("yardflow-natspub-worker-live-test"), nats.Timeout(2*time.Second))
		if err == nil {
			t.Cleanup(func() { nc.Close() })
			js, jsErr := nc.JetStream()
			if jsErr != nil {
				t.Fatalf("jetstream context: %v", jsErr)
			}
			return nc, js
		}
		if time.Now().After(deadline) {
			testenv.SkipOrFail(t, "live natspub worker test skipped: NATS unreachable: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// TestLiveWorkerPublishesAndAdvancesPosition is the happy-path E2E: real
// $all subscription, real JetStream publish, real Postgres position.
func TestLiveWorkerPublishesAndAdvancesPosition(t *testing.T) {
	liveLoadDotEnv()
	if os.Getenv("KURRENTDB_URL") == "" || os.Getenv("POSTGRES_DSN") == "" {
		testenv.SkipOrFail(t, "live natspub worker test skipped: KURRENTDB_URL/POSTGRES_DSN not set")
	}

	client := kurrenttest.OpenClient(t)
	pool := postgrestest.OpenPool(t)
	_, js := openLiveNATSConn(t)

	run := uuid.NewString()[:8]
	prefix := "natstest" + run + "-"            // KurrentDB stream prefix + subject token
	pfx := "yflive-" + run                      // NATS subject prefix
	streamName := pfx + "-events"               // test-owned JetStream stream
	publisherName := "test-natspub-live-" + run // test-owned position row

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Test-owned NATS stream covering the family (the documented correct
	// configuration). Deleted on cleanup — only the TEST mutates streams.
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{pfx + ".>"},
		Storage:  nats.MemoryStorage,
	}); err != nil {
		t.Fatalf("add test stream: %v", err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(streamName) })
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			"DELETE FROM es_nats_published_positions WHERE publisher_name = $1", publisherName)
	})

	// Append two events to a KurrentDB stream matching the prefix. The
	// worker's server-side filter yields only this family.
	aggStream := prefix + uuid.NewString()
	eventIDs := []uuid.UUID{uuid.New(), uuid.New()}
	if _, err := client.AppendToStream(ctx, aggStream,
		kurrentdb.AppendToStreamOptions{StreamState: kurrentdb.NoStream{}},
		kurrentdb.EventData{EventID: eventIDs[0], EventType: prefix[:len(prefix)-1] + ".registered", ContentType: kurrentdb.ContentTypeJson, Data: []byte(`{"live":1}`)},
		kurrentdb.EventData{EventID: eventIDs[1], EventType: prefix[:len(prefix)-1] + ".renamed", ContentType: kurrentdb.ContentTypeJson, Data: []byte(`{"live":2}`)},
	); err != nil {
		t.Fatalf("append: %v", err)
	}

	positions := pgnatspub.New(pool)
	validator := natsadapter.NewStreamValidator(js, natsadapter.StreamValidatorConfig{
		StreamName:     streamName,
		SubjectPrefix:  pfx,
		StreamPrefixes: []string{prefix},
		URL:            os.Getenv("NATS_URL"),
		Timeout:        3 * time.Second,
	})
	publisher := natsadapter.New(js, 3*time.Second)
	w := natspub.New(client, positions, publisher, validator, natspub.WorkerConfig{
		Name:          publisherName,
		Prefixes:      []string{prefix},
		SubjectPrefix: pfx,
		BatchSize:     1,
	})

	runCtx, runCancel := context.WithCancel(ctx)
	errc := make(chan error, 1)
	go func() { errc <- w.Run(runCtx) }()

	// Poll until the durable position reaches the SECOND event's
	// authoritative commit — the exact final boundary, not merely "advanced".
	// With BatchSize 1 the worker saves the position after EVERY event
	// (D114-R1 durable boundary), so the row passes through the transient
	// intermediate value commits[0] while event 2 is still being
	// published/saved; sampling that intermediate and asserting the final
	// value was an intermittent harness race. Overshooting the final commit
	// is impossible in this test (no other event exists in the filtered
	// family) and fails immediately — a position PAST commits[1] would
	// prove a genuine publication/filtering/accounting defect.
	commits := recordedCommits(t, client, ctx, aggStream)
	if len(commits) != 2 {
		t.Fatalf("recorded commits = %d, want 2", len(commits))
	}
	deadline := time.Now().Add(30 * time.Second)
	var pos uint64
	for time.Now().Before(deadline) {
		pos, _ = positions.Get(ctx, publisherName)
		if pos == commits[1] {
			break
		}
		if pos > commits[1] {
			t.Fatalf("saved position advanced to %d, past the expected final commit %d — an unexpected event was published/saved through the filter", pos, commits[1])
		}
		select {
		case err := <-errc:
			t.Fatalf("worker exited early: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	if pos != commits[1] {
		t.Fatalf("position never converged to the second event's commit %d (last seen %d); events were not durably published", commits[1], pos)
	}

	for i, seq := range []uint64{1, 2} {
		msg, err := js.GetMsg(streamName, seq)
		if err != nil {
			t.Fatalf("get msg %d: %v", seq, err)
		}
		if !strings.HasPrefix(msg.Subject, pfx+"."+prefix[:len(prefix)-1]+".") {
			t.Fatalf("msg %d subject %q lacks expected family", seq, msg.Subject)
		}
		if got := msg.Header.Get("Nats-Msg-Id"); got != eventIDs[i].String() {
			t.Fatalf("msg %d Nats-Msg-Id = %q, want %q", seq, got, eventIDs[i].String())
		}
	}

	// D114-R1 strengthening: the saved position must equal the SECOND
	// event's AUTHORITATIVE commit position read back from KurrentDB —
	// not merely "advanced" — and the accumulated events_published
	// counter must be exactly 2 (exact-delta accounting).
	if pos != commits[1] {
		t.Fatalf("saved position = %d, want the second event's authoritative commit %d", pos, commits[1])
	}
	assertPositionRow(t, pool, ctx, publisherName, commits[1], 2)

	runCancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("worker returned %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not stop after cancel")
	}
}

// TestLiveWorkerValidationGateBlocksOnStarSubjects is the live pin of the
// D114 production hypothesis through the REAL worker loop: with the stream
// configured "prefix.*" the worker must never open the KurrentDB
// subscription, publish nothing, and create no position row.
func TestLiveWorkerValidationGateBlocksOnStarSubjects(t *testing.T) {
	liveLoadDotEnv()
	if os.Getenv("KURRENTDB_URL") == "" || os.Getenv("POSTGRES_DSN") == "" {
		testenv.SkipOrFail(t, "live natspub worker test skipped: KURRENTDB_URL/POSTGRES_DSN not set")
	}

	client := kurrenttest.OpenClient(t)
	pool := postgrestest.OpenPool(t)
	_, js := openLiveNATSConn(t)

	run := uuid.NewString()[:8]
	prefix := "natstest" + run + "-"
	pfx := "yflive-" + run
	streamName := pfx + "-events"
	publisherName := "test-natspub-gate-" + run

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// The MISCONFIGURED stream: subjects "<prefix>.*" — the formerly
	// documented guidance that matches none of the published subjects.
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{pfx + ".*"},
		Storage:  nats.MemoryStorage,
	}); err != nil {
		t.Fatalf("add test stream: %v", err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(streamName) })
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			"DELETE FROM es_nats_published_positions WHERE publisher_name = $1", publisherName)
	})

	// Append an event that WOULD match the worker filter — proving the
	// gate (not an empty stream) is what blocks publication.
	aggStream := prefix + uuid.NewString()
	if _, err := client.AppendToStream(ctx, aggStream,
		kurrentdb.AppendToStreamOptions{StreamState: kurrentdb.NoStream{}},
		kurrentdb.EventData{EventID: uuid.New(), EventType: prefix[:len(prefix)-1] + ".registered",
			ContentType: kurrentdb.ContentTypeJson, Data: []byte(`{"live":"gate"}`)},
	); err != nil {
		t.Fatalf("append: %v", err)
	}

	positions := pgnatspub.New(pool)
	validator := natsadapter.NewStreamValidator(js, natsadapter.StreamValidatorConfig{
		StreamName:     streamName,
		SubjectPrefix:  pfx,
		StreamPrefixes: []string{prefix},
		URL:            os.Getenv("NATS_URL"),
		Timeout:        3 * time.Second,
	})
	publisher := natsadapter.New(js, 3*time.Second)
	w := natspub.New(client, positions, publisher, validator, natspub.WorkerConfig{
		Name:          publisherName,
		Prefixes:      []string{prefix},
		SubjectPrefix: pfx,
		BatchSize:     1,
	})

	// Capture logs so the actionable one-time guidance is asserted too.
	buf := captureLogs(t)

	runCtx, runCancel := context.WithCancel(ctx)
	errc := make(chan error, 1)
	go func() { errc <- w.Run(runCtx) }()

	// Let several validation attempts happen, then stop the worker.
	time.Sleep(700 * time.Millisecond)
	runCancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("worker returned %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not stop after cancel")
	}

	// Nothing published, no position row, exactly one full actionable log.
	info, err := js.StreamInfo(streamName)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("stream has %d messages; the gate must block publication", info.State.Msgs)
	}
	var rowCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM es_nats_published_positions WHERE publisher_name = $1",
		publisherName).Scan(&rowCount); err != nil {
		t.Fatalf("count positions: %v", err)
	}
	if rowCount != 0 {
		t.Fatal("position row created although validation never passed")
	}
	if got := strings.Count(buf.String(), "JetStream validation failed"); got != 1 {
		t.Fatalf("full validation-failure logs = %d, want exactly 1:\n%s", got, buf.String())
	}
	if !strings.Contains(buf.String(), "kind=subject_mismatch") {
		t.Fatalf("failure log must carry kind=subject_mismatch:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), pfx+".>") {
		t.Fatalf("failure log must carry the prefix.> guidance:\n%s", buf.String())
	}
}

// recordedCommits reads the aggregate stream back from KurrentDB and
// returns the AUTHORITATIVE $all commit position of each recorded event,
// in stream order. The saved publisher position must equal the last
// published event's commit exactly (D114-R1).
func recordedCommits(t *testing.T, client *kurrentdb.Client, ctx context.Context, aggStream string) []uint64 {
	t.Helper()

	rd, err := client.ReadStream(ctx, aggStream, kurrentdb.ReadStreamOptions{
		Direction: kurrentdb.Forwards,
		From:      kurrentdb.Start{},
	}, 100)
	if err != nil {
		t.Fatalf("ReadStream %s: %v", aggStream, err)
	}
	defer rd.Close()

	var commits []uint64
	for {
		we, err := rd.Recv()
		if err != nil {
			break
		}
		if we.Event != nil {
			commits = append(commits, we.Event.Position.Commit)
		}
	}
	return commits
}

// assertPositionRow asserts the durable position row exactly: position
// and the accumulated events_published counter.
func assertPositionRow(t *testing.T, pool *pgxpool.Pool, ctx context.Context, publisherName string, wantPos uint64, wantPublished uint64) {
	t.Helper()

	var gotPos, gotPublished uint64
	err := pool.QueryRow(ctx,
		"SELECT position, events_published FROM es_nats_published_positions WHERE publisher_name = $1",
		publisherName).Scan(&gotPos, &gotPublished)
	if err != nil {
		t.Fatalf("read position row: %v", err)
	}
	if gotPos != wantPos {
		t.Fatalf("durable position = %d, want %d", gotPos, wantPos)
	}
	if gotPublished != wantPublished {
		t.Fatalf("accumulated events_published = %d, want exactly %d", gotPublished, wantPublished)
	}
}

// flakyPositionStore wraps the real Postgres store and fails the first
// failFirst Save calls (then delegates). It records every Save attempt
// for assertions. Get always delegates — resume positions always come
// from the durable row.
type flakyPositionStore struct {
	inner     natspub.PositionStore
	mu        sync.Mutex
	failFirst int
	attempts  []error // one entry per Save: nil = succeeded
}

func (f *flakyPositionStore) Get(ctx context.Context, name string) (uint64, error) {
	return f.inner.Get(ctx, name)
}

func (f *flakyPositionStore) Save(ctx context.Context, name string, position uint64, eventsPublished uint64) error {
	f.mu.Lock()
	if f.failFirst > 0 {
		f.failFirst--
		err := errors.New("injected save failure")
		f.attempts = append(f.attempts, err)
		f.mu.Unlock()
		return err
	}
	f.mu.Unlock()
	err := f.inner.Save(ctx, name, position, eventsPublished)
	f.mu.Lock()
	f.attempts = append(f.attempts, err)
	f.mu.Unlock()
	return err
}

func (f *flakyPositionStore) failedSaves() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, err := range f.attempts {
		if err != nil {
			n++
		}
	}
	return n
}

// TestLiveWorkerSaveFailurePreservesPositionAndCountsExactly proves the
// D114-R1 durable-boundary contract end to end with real infrastructure:
// a failing position save ENDS the attempt (never superseded), the
// durable position stays at the last successful save, retries replay
// from it, JetStream dedups the republished overlap, and the
// events_published counter accumulates EXACTLY the number of unique
// published events (failed saves account nothing).
func TestLiveWorkerSaveFailurePreservesPositionAndCountsExactly(t *testing.T) {
	liveLoadDotEnv()
	if os.Getenv("KURRENTDB_URL") == "" || os.Getenv("POSTGRES_DSN") == "" {
		testenv.SkipOrFail(t, "live natspub worker test skipped: KURRENTDB_URL/POSTGRES_DSN not set")
	}

	client := kurrenttest.OpenClient(t)
	pool := postgrestest.OpenPool(t)
	_, js := openLiveNATSConn(t)

	run := uuid.NewString()[:8]
	prefix := "natstest" + run + "-"
	pfx := "yflive-" + run
	streamName := pfx + "-events"
	publisherName := "test-natspub-savefail-" + run

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{pfx + ".>"},
		Storage:  nats.MemoryStorage,
	}); err != nil {
		t.Fatalf("add test stream: %v", err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(streamName) })
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			"DELETE FROM es_nats_published_positions WHERE publisher_name = $1", publisherName)
	})

	aggStream := prefix + uuid.NewString()
	if _, err := client.AppendToStream(ctx, aggStream,
		kurrentdb.AppendToStreamOptions{StreamState: kurrentdb.NoStream{}},
		kurrentdb.EventData{EventID: uuid.New(), EventType: prefix[:len(prefix)-1] + ".registered", ContentType: kurrentdb.ContentTypeJson, Data: []byte(`{"savefail":1}`)},
		kurrentdb.EventData{EventID: uuid.New(), EventType: prefix[:len(prefix)-1] + ".renamed", ContentType: kurrentdb.ContentTypeJson, Data: []byte(`{"savefail":2}`)},
	); err != nil {
		t.Fatalf("append: %v", err)
	}
	commits := recordedCommits(t, client, ctx, aggStream)
	if len(commits) != 2 {
		t.Fatalf("recorded commits = %d, want 2", len(commits))
	}

	store := &flakyPositionStore{inner: pgnatspub.New(pool), failFirst: 2}
	validator := natsadapter.NewStreamValidator(js, natsadapter.StreamValidatorConfig{
		StreamName:     streamName,
		SubjectPrefix:  pfx,
		StreamPrefixes: []string{prefix},
		URL:            os.Getenv("NATS_URL"),
		Timeout:        3 * time.Second,
	})
	publisher := natsadapter.New(js, 3*time.Second)
	w := natspub.New(client, store, publisher, validator, natspub.WorkerConfig{
		Name:          publisherName,
		Prefixes:      []string{prefix},
		SubjectPrefix: pfx,
		BatchSize:     1, // every published event hits the save boundary
	})

	runCtx, runCancel := context.WithCancel(ctx)
	errc := make(chan error, 1)
	go func() { errc <- w.Run(runCtx) }()

	// Wait until both injected failures happened and the position row
	// reached the second event's authoritative commit through retries.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if pos, _ := store.Get(ctx, publisherName); pos == commits[1] {
			break
		}
		select {
		case err := <-errc:
			t.Fatalf("worker exited early: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	if pos, _ := store.Get(ctx, publisherName); pos != commits[1] {
		t.Fatalf("durable position = %d, want %d (recovery must converge)", pos, commits[1])
	}

	if got := store.failedSaves(); got != 2 {
		t.Fatalf("injected failed saves observed = %d, want exactly 2", got)
	}

	// Dedup: the republished overlap must not double-store.
	info, err := js.StreamInfo(streamName)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != 2 {
		t.Fatalf("stream messages = %d, want exactly 2 (replay dedup)", info.State.Msgs)
	}

	// Exact-delta accounting through the failure/retry cycles.
	assertPositionRow(t, pool, ctx, publisherName, commits[1], 2)

	runCancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("worker returned %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not stop after cancel")
	}
}

// TestLiveWorkerPublishFailureHeadOfLineAndRecovery proves the controlled
// publish-failure scenario end to end (D114-R1 / review finding 3):
// with the stream deleted AFTER validation had passed, the next publish
// fails with the classified no-responders error, the attempt ends with
// the durable position PRESERVED at the last saved event (head-of-line —
// nothing is skipped), the worker re-validates on retry (stream now
// missing → validation gate), and once the stream is recreated the
// worker recovers, republishes the blocked tail (deduped where already
// stored), and converges to the blocked event's authoritative commit
// with exact accounting.
func TestLiveWorkerPublishFailureHeadOfLineAndRecovery(t *testing.T) {
	liveLoadDotEnv()
	if os.Getenv("KURRENTDB_URL") == "" || os.Getenv("POSTGRES_DSN") == "" {
		testenv.SkipOrFail(t, "live natspub worker test skipped: KURRENTDB_URL/POSTGRES_DSN not set")
	}

	client := kurrenttest.OpenClient(t)
	pool := postgrestest.OpenPool(t)
	_, js := openLiveNATSConn(t)

	run := uuid.NewString()[:8]
	prefix := "natstest" + run + "-"
	pfx := "yflive-" + run
	streamName := pfx + "-events"
	publisherName := "test-natspub-hol-" + run

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{pfx + ".>"},
		Storage:  nats.MemoryStorage,
	}); err != nil {
		t.Fatalf("add test stream: %v", err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(streamName) })
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			"DELETE FROM es_nats_published_positions WHERE publisher_name = $1", publisherName)
	})

	positions := pgnatspub.New(pool)
	validator := natsadapter.NewStreamValidator(js, natsadapter.StreamValidatorConfig{
		StreamName:     streamName,
		SubjectPrefix:  pfx,
		StreamPrefixes: []string{prefix},
		URL:            os.Getenv("NATS_URL"),
		Timeout:        3 * time.Second,
	})
	publisher := natsadapter.New(js, 3*time.Second)
	w := natspub.New(client, positions, publisher, validator, natspub.WorkerConfig{
		Name:          publisherName,
		Prefixes:      []string{prefix},
		SubjectPrefix: pfx,
		BatchSize:     1,
	})

	buf := captureLogs(t)

	runCtx, runCancel := context.WithCancel(ctx)
	errc := make(chan error, 1)
	go func() { errc <- w.Run(runCtx) }()

	// Phase 1: publish event 1 normally and wait for its durable save.
	aggStream := prefix + uuid.NewString()
	ev1 := uuid.New()
	if _, err := client.AppendToStream(ctx, aggStream,
		kurrentdb.AppendToStreamOptions{StreamState: kurrentdb.NoStream{}},
		kurrentdb.EventData{EventID: ev1, EventType: prefix[:len(prefix)-1] + ".registered", ContentType: kurrentdb.ContentTypeJson, Data: []byte(`{"hol":1}`)},
	); err != nil {
		t.Fatalf("append 1: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	var pos uint64
	for time.Now().Before(deadline) {
		pos, _ = positions.Get(ctx, publisherName)
		if pos > 0 {
			break
		}
		select {
		case err := <-errc:
			t.Fatalf("worker exited early: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	if pos == 0 {
		t.Fatal("event 1 was never published+saved")
	}
	commits := recordedCommits(t, client, ctx, aggStream)
	if pos != commits[0] {
		t.Fatalf("position after event 1 = %d, want its commit %d", pos, commits[0])
	}

	// Phase 2: break the sink AFTER validation passed — delete the
	// stream — and append event 2. Its publish must fail (no responders),
	// ending the attempt with the position PRESERVED (head-of-line).
	if err := js.DeleteStream(streamName); err != nil {
		t.Fatalf("delete stream: %v", err)
	}
	ev2 := uuid.New()
	if _, err := client.AppendToStream(ctx, aggStream,
		kurrentdb.AppendToStreamOptions{StreamState: kurrentdb.StreamRevision{Value: 0}},
		kurrentdb.EventData{EventID: ev2, EventType: prefix[:len(prefix)-1] + ".renamed", ContentType: kurrentdb.ContentTypeJson, Data: []byte(`{"hol":2}`)},
	); err != nil {
		t.Fatalf("append 2: %v", err)
	}
	commits = recordedCommits(t, client, ctx, aggStream)
	if len(commits) != 2 {
		t.Fatalf("recorded commits = %d, want 2", len(commits))
	}

	// Head-of-line: observe the position FROZEN at event 1's commit for
	// a window well past the publish-failure + validation cycles.
	holdDeadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(holdDeadline) {
		if got, _ := positions.Get(ctx, publisherName); got != commits[0] {
			t.Fatalf("position advanced to %d while the sink was broken; head-of-line violated (want frozen at %d)", got, commits[0])
		}
		time.Sleep(200 * time.Millisecond)
	}

	// The failure was classified and logged with guidance.
	if !strings.Contains(buf.String(), "kind=no_responders") {
		t.Fatalf("publish failure must be logged with kind=no_responders:\n%s", buf.String())
	}

	// Phase 3: repair the sink (recreate the stream out-of-band — the
	// publisher never does this itself) and prove recovery converges to
	// the blocked event's authoritative commit.
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{pfx + ".>"},
		Storage:  nats.MemoryStorage,
	}); err != nil {
		t.Fatalf("re-add test stream: %v", err)
	}

	deadline = time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if got, _ := positions.Get(ctx, publisherName); got == commits[1] {
			break
		}
		select {
		case err := <-errc:
			t.Fatalf("worker exited early during recovery: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	if got, _ := positions.Get(ctx, publisherName); got != commits[1] {
		t.Fatalf("position after recovery = %d, want the second event's authoritative commit %d", got, commits[1])
	}

	// Exact resume: the worker resumed from the SAVED position (ev1's
	// commit), so the recreated stream receives ONLY the blocked event —
	// ev1 is not republished (its publication is already durably
	// accounted; the operator's deletion destroyed that copy, which is
	// stream-deletion semantics, not publisher loss).
	info, err := js.StreamInfo(streamName)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("stream messages = %d, want exactly 1 (only the blocked event; ev1 is past the saved position)", info.State.Msgs)
	}
	msg, err := js.GetMsg(streamName, 1)
	if err != nil {
		t.Fatalf("get msg 1: %v", err)
	}
	if got := msg.Header.Get("Nats-Msg-Id"); got != ev2.String() {
		t.Fatalf("recovered message Nats-Msg-Id = %q, want the blocked event %q", got, ev2.String())
	}
	// Both events were published and accounted exactly.
	assertPositionRow(t, pool, ctx, publisherName, commits[1], 2)

	runCancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("worker returned %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not stop after cancel")
	}
}

// TestLiveWorkerTerminalFlushPersistsShutdownTail is the live pin of the
// C05-E2E shutdown defect (D131): events published but not yet batch-saved
// (the batch boundary is unreachable) MUST still reach
// es_nats_published_positions when Run is gracefully cancelled — the
// terminal flush runs on a bounded context DETACHED from the cancelled run
// context. Before the fix the flush went through the cancelled ctx, the
// Postgres save returned context.Canceled, Run swallowed the error, and the
// row stayed EMPTY — the restart replayed from position 0 and duplicates
// escaped the JetStream dedup window.
func TestLiveWorkerTerminalFlushPersistsShutdownTail(t *testing.T) {
	liveLoadDotEnv()
	if os.Getenv("KURRENTDB_URL") == "" || os.Getenv("POSTGRES_DSN") == "" {
		testenv.SkipOrFail(t, "live natspub worker test skipped: KURRENTDB_URL/POSTGRES_DSN not set")
	}

	client := kurrenttest.OpenClient(t)
	pool := postgrestest.OpenPool(t)
	_, js := openLiveNATSConn(t)

	run := uuid.NewString()[:8]
	prefix := "natstest" + run + "-"
	pfx := "yflive-" + run
	streamName := pfx + "-events"
	publisherName := "test-natspub-shutdowntail-" + run

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{pfx + ".>"},
		Storage:  nats.MemoryStorage,
	}); err != nil {
		t.Fatalf("add test stream: %v", err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(streamName) })
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			"DELETE FROM es_nats_published_positions WHERE publisher_name = $1", publisherName)
	})

	// Two events, BatchSize large enough that NO batch save can fire:
	// the only possible persistence is the terminal flush — exactly the
	// defect scenario (everything published, nothing durably saved).
	aggStream := prefix + uuid.NewString()
	if _, err := client.AppendToStream(ctx, aggStream,
		kurrentdb.AppendToStreamOptions{StreamState: kurrentdb.NoStream{}},
		kurrentdb.EventData{EventID: uuid.New(), EventType: prefix[:len(prefix)-1] + ".registered", ContentType: kurrentdb.ContentTypeJson, Data: []byte(`{"tail":1}`)},
		kurrentdb.EventData{EventID: uuid.New(), EventType: prefix[:len(prefix)-1] + ".renamed", ContentType: kurrentdb.ContentTypeJson, Data: []byte(`{"tail":2}`)},
	); err != nil {
		t.Fatalf("append: %v", err)
	}
	commits := recordedCommits(t, client, ctx, aggStream)
	if len(commits) != 2 {
		t.Fatalf("recorded commits = %d, want 2", len(commits))
	}

	positions := pgnatspub.New(pool)
	validator := natsadapter.NewStreamValidator(js, natsadapter.StreamValidatorConfig{
		StreamName:     streamName,
		SubjectPrefix:  pfx,
		StreamPrefixes: []string{prefix},
		URL:            os.Getenv("NATS_URL"),
		Timeout:        3 * time.Second,
	})
	publisher := natsadapter.New(js, 3*time.Second)
	w := natspub.New(client, positions, publisher, validator, natspub.WorkerConfig{
		Name:          publisherName,
		Prefixes:      []string{prefix},
		SubjectPrefix: pfx,
		BatchSize:     50, // unreachable with two events — no batch save
	})

	runCtx, runCancel := context.WithCancel(ctx)
	errc := make(chan error, 1)
	go func() { errc <- w.Run(runCtx) }()

	// Wait until BOTH events are durably in JetStream (the adapter waits
	// for the ack, so a stored message means the worker's PublishAsync
	// returned), then let the worker settle back into Recv so the
	// cancellation deterministically lands on the terminal path.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if info, err := js.StreamInfo(streamName); err == nil && info.State.Msgs == 2 {
			break
		}
		select {
		case err := <-errc:
			t.Fatalf("worker exited early: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	if info, err := js.StreamInfo(streamName); err != nil || info.State.Msgs != 2 {
		t.Fatalf("jetstream never durably stored both events (msgs=%d err=%v)", info.State.Msgs, err)
	}
	time.Sleep(500 * time.Millisecond) // worker is parked in Recv; no batch boundary can fire

	// Defect precondition: everything published, NOTHING durably saved
	// (no batch boundary was reached — the position row does not exist).
	var rowCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM es_nats_published_positions WHERE publisher_name = $1",
		publisherName).Scan(&rowCount); err != nil {
		t.Fatalf("count positions: %v", err)
	}
	if rowCount != 0 {
		t.Fatalf("position row exists before shutdown although no batch boundary fired — test setup is wrong")
	}

	// Graceful shutdown: cancel the run context.
	runCancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("worker returned %v, want context.Canceled", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("worker did not stop after cancel")
	}

	// THE defect assertion: the terminal flush ran on the detached
	// bounded context and persisted the shutdown tail. Before the fix
	// this row stayed EMPTY — the flush went through the cancelled run
	// context, failed with context.Canceled, and Run swallowed it.
	assertPositionRow(t, pool, ctx, publisherName, commits[1], 2)
}

// TestLiveWorkerSubjectCorrectionRecovery proves the out-of-band subject
// correction recovery path (D114): the stream exists with wrong subjects
// ("<prefix>.*") → the worker parks in the validation gate → an operator
// fixes the stream's subjects to "<prefix>.>" while the worker runs → the
// worker's next validation attempt passes → the subscription opens and the
// blocked tail publishes on unchanged correct subjects.
func TestLiveWorkerSubjectCorrectionRecovery(t *testing.T) {
	liveLoadDotEnv()
	if os.Getenv("KURRENTDB_URL") == "" || os.Getenv("POSTGRES_DSN") == "" {
		testenv.SkipOrFail(t, "live natspub worker test skipped: KURRENTDB_URL/POSTGRES_DSN not set")
	}

	client := kurrenttest.OpenClient(t)
	pool := postgrestest.OpenPool(t)
	_, js := openLiveNATSConn(t)

	run := uuid.NewString()[:8]
	prefix := "natstest" + run + "-"
	pfx := "yflive-" + run
	streamName := pfx + "-events"
	publisherName := "test-natspub-correct-" + run

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Phase 0: stream with MISCONFIGURED subjects ("<prefix>.*"). The
	// validation gate blocks because "*" matches exactly one token.
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{pfx + ".*"},
		Storage:  nats.MemoryStorage,
	}); err != nil {
		t.Fatalf("add test stream with star subjects: %v", err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(streamName) })
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			"DELETE FROM es_nats_published_positions WHERE publisher_name = $1", publisherName)
	})

	// Phase 0: append events to KurrentDB while the worker is NOT running
	// yet — the blocked validation gate must not lose them.
	aggStream := prefix + uuid.NewString()
	ev1 := uuid.New()
	ev2 := uuid.New()
	if _, err := client.AppendToStream(ctx, aggStream,
		kurrentdb.AppendToStreamOptions{StreamState: kurrentdb.NoStream{}},
		kurrentdb.EventData{EventID: ev1, EventType: prefix[:len(prefix)-1] + ".registered", ContentType: kurrentdb.ContentTypeJson, Data: []byte(`{"correct":1}`)},
		kurrentdb.EventData{EventID: ev2, EventType: prefix[:len(prefix)-1] + ".renamed", ContentType: kurrentdb.ContentTypeJson, Data: []byte(`{"correct":2}`)},
	); err != nil {
		t.Fatalf("append: %v", err)
	}
	commits := recordedCommits(t, client, ctx, aggStream)
	if len(commits) != 2 {
		t.Fatalf("recorded commits = %d, want 2", len(commits))
	}

	positions := pgnatspub.New(pool)
	validator := natsadapter.NewStreamValidator(js, natsadapter.StreamValidatorConfig{
		StreamName:     streamName,
		SubjectPrefix:  pfx,
		StreamPrefixes: []string{prefix},
		URL:            os.Getenv("NATS_URL"),
		Timeout:        3 * time.Second,
	})
	publisher := natsadapter.New(js, 3*time.Second)
	w := natspub.New(client, positions, publisher, validator, natspub.WorkerConfig{
		Name:          publisherName,
		Prefixes:      []string{prefix},
		SubjectPrefix: pfx,
		BatchSize:     1,
	})

	buf := captureLogs(t)

	runCtx, runCancel := context.WithCancel(ctx)
	errc := make(chan error, 1)
	go func() { errc <- w.Run(runCtx) }()

	// Phase 1: while the worker is running, observe the validation gate
	// blocking — no position row, no publishes, actionable log once.
	time.Sleep(600 * time.Millisecond)

	// The subject_mismatch log must have appeared (exactly one full).
	if !strings.Contains(buf.String(), "kind=subject_mismatch") {
		t.Fatalf("worker must log subject_mismatch while stream subjects are wrong:\n%s", buf.String())
	}
	fullFailures := strings.Count(buf.String(), "JetStream validation failed")
	if fullFailures != 1 {
		t.Fatalf("full validation-failure logs = %d, want exactly 1 (deduplicated):\n%s", fullFailures, buf.String())
	}
	if !strings.Contains(buf.String(), pfx+".>") {
		t.Fatalf("failure log must carry the <prefix>.> guidance:\n%s", buf.String())
	}

	// No position row — validation never passed.
	var rowCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM es_nats_published_positions WHERE publisher_name = $1", publisherName).Scan(&rowCount); err != nil {
		t.Fatalf("count positions: %v", err)
	}
	if rowCount != 0 {
		t.Fatal("position row created while validation was blocking")
	}

	// No messages published.
	info, err := js.StreamInfo(streamName)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("stream has %d messages while the validation gate was blocking", info.State.Msgs)
	}

	// Phase 2: correct the stream subjects out-of-band (simulating the
	// operator action documented in the runbook: `nats stream update`).
	// We delete and recreate because the nats.go UpdateStream signature
	// s not trivially callable hrough jetstreamContext in all versions.
	if err := js.DeleteStream(streamName); err != nil {
		t.Fatalf("delete stream for correction: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{pfx + ".>"},
		Storage:  nats.MemoryStorage,
	}); err != nil {
		t.Fatalf("re-add stream with correct subjects: %v", err)
	}

	// Phase 3: the worker's next validation cycle finds the corrected
	// stream, opens the $all subscription, and publishes the tail.
	// Poll until the durable position reaches the SECOND event's exact
	// commit — with BatchSize 1 the row passes through the transient
	// intermediate commits[0] while event 2 is still being published/saved
	// (D114-R1 per-event durable boundary), and sampling that intermediate
	// value (the old "first nonzero" break) was an intermittent harness
	// race. Overshoot is impossible here (no other event exists in the
	// filtered family) and fails immediately as a real defect signal.
	deadline := time.Now().Add(60 * time.Second)
	var latestPos uint64
	for time.Now().Before(deadline) {
		latestPos, _ = positions.Get(ctx, publisherName)
		if latestPos == commits[1] {
			break
		}
		if latestPos > commits[1] {
			t.Fatalf("durable position advanced to %d, past the expected final commit %d — an unexpected event was published/saved through the filter", latestPos, commits[1])
		}
		select {
		case err := <-errc:
			t.Fatalf("worker exited early during recovery: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	if latestPos != commits[1] {
		t.Fatalf("position never converged to the second event's commit %d (last seen %d) after subject correction", commits[1], latestPos)
	}

	// Both events in the corrected stream.
	info, err = js.StreamInfo(streamName)
	if err != nil {
		t.Fatalf("stream info after correction: %v", err)
	}
	if info.State.Msgs != 2 {
		t.Fatalf("corrected stream messages = %d, want exactly 2 (both events published)", info.State.Msgs)
	}

	// Correct subjects and Msg-Id.
	msg1, err := js.GetMsg(streamName, 1)
	if err != nil {
		t.Fatalf("get msg 1: %v", err)
	}
	if !strings.HasPrefix(msg1.Subject, pfx+"."+prefix[:len(prefix)-1]+".") {
		t.Fatalf("msg 1 subject %q does not begin with correct family", msg1.Subject)
	}
	if got := msg1.Header.Get("Nats-Msg-Id"); got != ev1.String() {
		t.Fatalf("msg 1 Nats-Msg-Id = %q, want %q", got, ev1.String())
	}
	msg2, err := js.GetMsg(streamName, 2)
	if err != nil {
		t.Fatalf("get msg 2: %v", err)
	}
	if got := msg2.Header.Get("Nats-Msg-Id"); got != ev2.String() {
		t.Fatalf("msg 2 Nats-Msg-Id = %q, want %q", got, ev2.String())
	}

	// Exact accounting.
	assertPositionRow(t, pool, ctx, publisherName, commits[1], 2)

	runCancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("worker returned %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not stop after cancel")
	}
}
