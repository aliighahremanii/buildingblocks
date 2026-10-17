package kurrentexport

// exporter_test.go — unit coverage for the bounded as-of exporter (AR-510
// / R02 Step E, Q4-B/R3-F2) WITHOUT a live server, through the AllReader /
// PageStream seams:
//
//   - CaptureAsOf: the backwards single-event head capture; the empty-$all
//     degenerate bound (0,0); the fail-closed nil reader; reader errors;
//   - Run: the inclusive as-of bound (the event AT C processed, the first
//     event beyond never scanned — even while writes continue past the
//     capture); the exclusive resume boundary; the client-side prefix
//     filter (foreign/system streams advance the position without a
//     batch); multi-page scans (a full page continues from its last
//     position, a short page ends the scan); 0..N batch emission through
//     the SAME natspub transform; whole-source-event redrive (a mid-batch
//     sink failure keeps the boundary at the previous fully-applied event);
//     progress saves at batch boundaries, silent-tail page boundaries, and
//     the final save; progress/sink failure aborting the scan with the
//     durable boundary preserved; fail-closed nil reader/sink/transform;
//     the degenerate (0,0) bound scanning nothing.
//
// Live $all coverage (real KurrentDB positions, real pages) is in
// exporter_live_test.go (KURRENTDB_URL-gated).

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"

	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/natspub"
)

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

// fakePage is one scripted page: a fixed event slice, then EOF.
type fakePage struct {
	events []*kurrentdb.ResolvedEvent
	i      int
	closed bool
}

func (p *fakePage) Recv() (*kurrentdb.ResolvedEvent, error) {
	if p.i >= len(p.events) {
		return nil, io.EOF
	}
	ev := p.events[p.i]
	p.i++
	return ev, nil
}

func (p *fakePage) Close() { p.closed = true }

// rec builds a recorded event at a synthetic $all position.
func rec(id string, streamID string, commit, prepare uint64, eventType string) *kurrentdb.RecordedEvent {
	return &kurrentdb.RecordedEvent{
		EventID:   uuid.MustParse(id),
		EventType: eventType,
		StreamID:  streamID,
		Position:  kurrentdb.Position{Commit: commit, Prepare: prepare},
		Data:      []byte(`{}`),
	}
}

// resolved wraps an event in a ResolvedEvent.
func resolved(ev *kurrentdb.RecordedEvent) *kurrentdb.ResolvedEvent {
	return &kurrentdb.ResolvedEvent{Event: ev}
}

// fakeReader scripts pages per read call: each ReadAll invocation pops
// the next page from pages (when non-empty), or returns the fallthrough
// page (normally an EOF page — the empty log tail). The From position is
// recorded for assertion, and injectable errors are surfaced.
type fakeReader struct {
	pages     []*fakePage
	calls     []kurrentdb.ReadAllOptions
	openErr   error
	openErrOn int // 1-based read call that fails (0 = never)
	// backwards requests (the capture) get the last page instead.
	capturePage []*kurrentdb.ResolvedEvent
}

func (f *fakeReader) ReadAll(_ context.Context, opts kurrentdb.ReadAllOptions, _ uint64) (PageStream, error) {
	f.calls = append(f.calls, opts)
	if f.openErr != nil && (f.openErrOn == 0 || len(f.calls) == f.openErrOn) {
		return nil, f.openErr
	}
	if opts.Direction == kurrentdb.Backwards {
		return &fakePage{events: f.capturePage}, nil
	}
	if len(f.pages) == 0 {
		return &fakePage{}, nil // empty tail
	}
	p := f.pages[0]
	f.pages = f.pages[1:]
	return p, nil
}

// sinkRecorder records every applied message and can fail on a msgID.
type sinkRecorder struct {
	applied []natspub.BatchMessage
	failOn  string
	err     error
}

func (s *sinkRecorder) sink(_ context.Context, m natspub.BatchMessage) error {
	if s.failOn != "" && m.MsgID == s.failOn {
		return s.err
	}
	s.applied = append(s.applied, m)
	return nil
}

// progressRecorder records every durable save and can fail after n saves.
type progressRecorder struct {
	saves     [][4]uint64 // (prepare, commit, scanned, emitted)
	failAfter int
	err       error
}

func (p *progressRecorder) progress(_ context.Context, prepare, commit, scanned, emitted uint64) error {
	if p.failAfter > 0 && len(p.saves) >= p.failAfter {
		return p.err
	}
	p.saves = append(p.saves, [4]uint64{prepare, commit, scanned, emitted})
	return nil
}

// testTransform derives 2 deterministic messages per event of type
// "source.batch", none for "source.silent", and an error otherwise.
func testTransform() natspub.EventTransform {
	return natspub.EventTransform(func(ev *kurrentdb.RecordedEvent) ([]natspub.BatchMessage, error) {
		switch ev.EventType {
		case "source.batch", "source.silent":
			if ev.EventType == "source.silent" {
				return nil, nil
			}
			out := make([]natspub.BatchMessage, 0, 2)
			for i := 0; i < 2; i++ {
				out = append(out, natspub.BatchMessage{
					Subject: fmt.Sprintf("yardflow.reporting.contracts.test.v1"),
					Data:    []byte(fmt.Sprintf(`{"i":%d,"ev":%q}`, i, ev.EventID)),
					MsgID:   ev.EventID.String() + ":test:" + fmt.Sprint(i),
				})
			}
			return out, nil
		default:
			return nil, fmt.Errorf("unknown event type %q", ev.EventType)
		}
	})
}

// matchingEvents builds n matching source events at consecutive
// positions starting at (commit*10, prepare*10)-ish synthetic positions.
func matchingEvents(prefix string, n int) []*kurrentdb.RecordedEvent {
	out := make([]*kurrentdb.RecordedEvent, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("9f2b0000-0000-4000-8000-%012d", i+1)
		out = append(out, rec(id, fmt.Sprintf("%s-%012d", prefix, i+1), uint64(100+i), uint64(100+i), "source.batch"))
	}
	return out
}

// ---------------------------------------------------------------------------
// CaptureAsOf
// ---------------------------------------------------------------------------

func TestCaptureAsOfReturnsHeadPosition(t *testing.T) {
	head := rec("9f2b0000-0000-4000-8000-0000000000ff", "order-1", 4242, 4240, "order.registered")
	reader := &fakeReader{capturePage: []*kurrentdb.ResolvedEvent{resolved(head)}}
	asOf, err := CaptureAsOf(context.Background(), reader)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if asOf.Commit != 4242 || asOf.Prepare != 4240 {
		t.Fatalf("as-of = (%d,%d), want the head (4242,4240)", asOf.Commit, asOf.Prepare)
	}
	// The capture is ONE backwards read of ONE event from the end.
	if len(reader.calls) != 1 {
		t.Fatalf("ReadAll calls = %d, want 1", len(reader.calls))
	}
	if reader.calls[0].Direction != kurrentdb.Backwards {
		t.Fatal("capture must read BACKWARDS")
	}
	if _, ok := reader.calls[0].From.(kurrentdb.End); !ok {
		t.Fatalf("capture must read from End, got %T", reader.calls[0].From)
	}
}

func TestCaptureAsOfEmptyAllIsDegenerateZero(t *testing.T) {
	reader := &fakeReader{capturePage: nil}
	asOf, err := CaptureAsOf(context.Background(), reader)
	if err != nil {
		t.Fatalf("capture empty $all: %v", err)
	}
	if !asOf.IsZero() {
		t.Fatalf("empty $all as-of = %+v, want the degenerate (0,0)", asOf)
	}
}

func TestCaptureAsOfFailsClosedAndSurfacesErrors(t *testing.T) {
	if _, err := CaptureAsOf(context.Background(), nil); err == nil {
		t.Fatal("a nil reader must fail closed, never fabricate a bound")
	}
	reader := &fakeReader{openErr: errors.New("connection refused")}
	if _, err := CaptureAsOf(context.Background(), reader); err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("reader error must surface, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Run — bounds, resume, filtering, paging
// ---------------------------------------------------------------------------

// pageOf wraps events into a scripted page.
func pageOf(evs ...*kurrentdb.RecordedEvent) *fakePage {
	out := make([]*kurrentdb.ResolvedEvent, 0, len(evs))
	for _, e := range evs {
		out = append(out, resolved(e))
	}
	return &fakePage{events: out}
}

func TestRunScansInclusiveBoundAndFiltersClientSide(t *testing.T) {
	// Log: foreign event, matching event A, matching event B (AT the bound),
	// and a matching event BEYOND the bound (appended after the capture —
	// never scanned).
	a := rec("9f2b0000-0000-4000-8000-00000000000a", "order-a", 100, 100, "source.batch")
	b := rec("9f2b0000-0000-4000-8000-00000000000b", "order-b", 110, 110, "source.batch")
	c := rec("9f2b0000-0000-4000-8000-00000000000c", "order-c", 120, 120, "source.batch")
	foreign := rec("9f2b0000-0000-4000-8000-00000000000f", "foreign-x", 90, 90, "other.thing")
	reader := &fakeReader{pages: []*fakePage{pageOf(foreign, a, b, c)}}
	sink := &sinkRecorder{}

	res, err := Run(context.Background(), reader, Config{
		Name:      "unit-export",
		Prefixes:  []string{"order-"},
		Transform: testTransform(),
	}, Options{AsOf: AsOf{Prepare: 110, Commit: 110}}, sink.sink, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	// Only A and B match and are ≤ C; the beyond-bound event is never
	// derived or applied.
	if res.EventsScanned != 2 {
		t.Fatalf("events scanned = %d, want 2", res.EventsScanned)
	}
	if res.MessagesEmitted != 4 {
		t.Fatalf("messages emitted = %d, want 4 (2 per event)", res.MessagesEmitted)
	}
	if len(sink.applied) != 4 {
		t.Fatalf("applied = %d, want 4", len(sink.applied))
	}
	for _, m := range sink.applied {
		if strings.Contains(m.MsgID, "00000000000c") {
			t.Fatalf("the event beyond the bound must never be applied: %s", m.MsgID)
		}
	}
	// The resume boundary is the LAST processed position (the event AT
	// the bound) — the beyond-bound event does not move it.
	if res.Last.Commit != 110 || res.Last.Prepare != 110 {
		t.Fatalf("last = (%d,%d), want the at-bound (110,110)", res.Last.Prepare, res.Last.Commit)
	}
	// The foreign event advanced the position without a batch but the
	// boundary still ends at the last MATCHING... no: Last is the last
	// PROCESSED position within the bound — B (at the bound) is last.
	if res.AsOf.Commit != 110 {
		t.Fatalf("result as-of = %+v, want the passed bound verbatim", res.AsOf)
	}
}

func TestRunResumesExclusivelyFromSavedProgress(t *testing.T) {
	// Progress boundary at (100,100): the scan must request the page from
	// kurrentdb.Position{100,100} — and, because the REAL server returns
	// the event AT that position (live-verified inclusive read), the scan
	// must SKIP the boundary event (the exclusive-resume contract).
	// The scripted page mirrors the real server: a AT the boundary, then b.
	a := rec("9f2b0000-0000-4000-8000-00000000000a", "order-a", 100, 100, "source.batch")
	b := rec("9f2b0000-0000-4000-8000-00000000000b", "order-b", 110, 110, "source.batch")
	reader := &fakeReader{pages: []*fakePage{pageOf(a, b)}}
	sink := &sinkRecorder{}
	res, err := Run(context.Background(), reader, Config{
		Name: "unit-export", Prefixes: []string{"order-"}, Transform: testTransform(),
	}, Options{
		From: AsOf{Prepare: 100, Commit: 100},
		AsOf: AsOf{Prepare: 110, Commit: 110},
	}, sink.sink, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(reader.calls) == 0 {
		t.Fatal("no read calls recorded")
	}
	from, ok := reader.calls[0].From.(kurrentdb.Position)
	if !ok {
		t.Fatalf("From = %T, want kurrentdb.Position for a non-zero resume", reader.calls[0].From)
	}
	if from.Commit != 100 || from.Prepare != 100 {
		t.Fatalf("From position = (%d,%d), want the saved progress (100,100)", from.Commit, from.Prepare)
	}
	// The boundary event a was already processed and durably saved — it
	// must NOT be re-derived or re-applied (the whole point of the
	// exclusive resume). Only b (strictly after the boundary) processes.
	if res.EventsScanned != 1 {
		t.Fatalf("events scanned = %d, want 1 (the boundary event skipped)", res.EventsScanned)
	}
	if len(sink.applied) != 2 {
		t.Fatalf("applied = %d, want 2 (only b's batch)", len(sink.applied))
	}
	for _, m := range sink.applied {
		if strings.Contains(m.MsgID, "00000000000a") {
			t.Fatalf("the boundary event was re-applied: %s", m.MsgID)
		}
	}
}

func TestRunInclusiveReadCompensationCountsBoundaryEventForPageFullness(t *testing.T) {
	// A full page whose FIRST event is the boundary event (the real
	// server's inclusive read): the event is skipped but MUST count
	// toward `read` — otherwise the page would look short and end the
	// scan early with events still ≤ C unprocessed. The page is full
	// (read == ReadPageSize), so the scan continues to the next page.
	a := rec("9f2b0000-0000-4000-8000-00000000000a", "order-a", 100, 100, "source.batch")
	b := rec("9f2b0000-0000-4000-8000-00000000000b", "order-b", 110, 110, "source.batch")
	c := rec("9f2b0000-0000-4000-8000-00000000000c", "order-c", 120, 120, "source.batch")
	reader := &fakeReader{pages: []*fakePage{
		pageOf(a, b), // full page: a at the boundary (skipped), b processes
		pageOf(c),    // short page → done
	}}
	sink := &sinkRecorder{}
	res, err := Run(context.Background(), reader, Config{
		Name: "unit-export", Prefixes: []string{"order-"}, Transform: testTransform(), ReadPageSize: 2,
	}, Options{
		From: AsOf{Prepare: 100, Commit: 100},
		AsOf: AsOf{Prepare: 120, Commit: 120},
	}, sink.sink, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.EventsScanned != 2 || res.MessagesEmitted != 4 {
		t.Fatalf("res = %+v, want 2 events / 4 messages (b and c; the boundary event skipped)", res)
	}
	if len(reader.calls) != 2 {
		t.Fatalf("read calls = %d, want 2 (the boundary-leading page is FULL, the scan must continue)", len(reader.calls))
	}
	for _, m := range sink.applied {
		if strings.Contains(m.MsgID, "00000000000a") {
			t.Fatalf("the boundary event was re-applied: %s", m.MsgID)
		}
	}
	if from := reader.calls[1].From.(kurrentdb.Position); from.Commit != 110 {
		t.Fatalf("page 2 From = %d, want 110 (past the skipped boundary, at b)", from.Commit)
	}
}

func TestRunRejectsPageSizeBelowTwo(t *testing.T) {
	// ReadPageSize 1 would stall against the inclusive server read: a
	// boundary-only page can never advance the scan. Fail closed.
	reader := &fakeReader{}
	sink := func(context.Context, natspub.BatchMessage) error { return nil }
	_, err := Run(context.Background(), reader, Config{
		Name: "unit-export", Prefixes: []string{"order-"}, Transform: testTransform(), ReadPageSize: 1,
	}, Options{AsOf: AsOf{Prepare: 1, Commit: 1}}, sink, nil)
	if err == nil || !strings.Contains(err.Error(), "ReadPageSize") {
		t.Fatalf("ReadPageSize 1 must fail closed, got %v", err)
	}
}

func TestRunZeroResumeReadsFromStart(t *testing.T) {
	a := rec("9f2b0000-0000-4000-8000-00000000000a", "order-a", 100, 100, "source.batch")
	reader := &fakeReader{pages: []*fakePage{pageOf(a)}}
	_, err := Run(context.Background(), reader, Config{
		Name: "unit-export", Prefixes: []string{"order-"}, Transform: testTransform(),
	}, Options{AsOf: AsOf{Prepare: 100, Commit: 100}}, func(context.Context, natspub.BatchMessage) error { return nil }, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if _, ok := reader.calls[0].From.(kurrentdb.Start); !ok {
		t.Fatalf("From = %T, want Start for the (0,0) resume", reader.calls[0].From)
	}
}

func TestRunDegenerateZeroBoundScansNothing(t *testing.T) {
	// An empty store's captured head is (0,0): every real event is
	// strictly beyond it, so the scan processes nothing.
	a := rec("9f2b0000-0000-4000-8000-00000000000a", "order-a", 100, 100, "source.batch")
	reader := &fakeReader{pages: []*fakePage{pageOf(a)}}
	sink := &sinkRecorder{}
	res, err := Run(context.Background(), reader, Config{
		Name: "unit-export", Prefixes: []string{"order-"}, Transform: testTransform(),
	}, Options{AsOf: AsOf{}}, sink.sink, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.EventsScanned != 0 || res.MessagesEmitted != 0 || len(sink.applied) != 0 {
		t.Fatalf("degenerate bound processed something: res=%+v applied=%d", res, len(sink.applied))
	}
	if !res.Last.IsZero() {
		t.Fatalf("last = %+v, want the untouched (0,0) boundary", res.Last)
	}
}

func TestRunPagesContinueFromLastReadPosition(t *testing.T) {
	// 5 matching events over pages of 2: pages [a,b], [c,d], [e]; each
	// full page's read must resume EXCLUSIVELY from the previous page's
	// last read position.
	evs := matchingEvents("order", 5)
	reader := &fakeReader{pages: []*fakePage{
		pageOf(evs[0], evs[1]),
		pageOf(evs[2], evs[3]),
		pageOf(evs[4]),
	}}
	sink := &sinkRecorder{}
	res, err := Run(context.Background(), reader, Config{
		Name: "unit-export", Prefixes: []string{"order-"}, Transform: testTransform(), ReadPageSize: 2,
	}, Options{AsOf: AsOf{Prepare: 104, Commit: 104}}, sink.sink, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.EventsScanned != 5 || res.MessagesEmitted != 10 {
		t.Fatalf("res = %+v, want 5 events / 10 messages", res)
	}
	if len(reader.calls) != 3 {
		t.Fatalf("read calls = %d, want 3 pages", len(reader.calls))
	}
	// Page 2 resumes after page 1's LAST READ event (position 101); page
	// 3 after (103).
	if from := reader.calls[1].From.(kurrentdb.Position); from.Commit != 101 {
		t.Fatalf("page 2 From = %d, want 101", from.Commit)
	}
	if from := reader.calls[2].From.(kurrentdb.Position); from.Commit != 103 {
		t.Fatalf("page 3 From = %d, want 103", from.Commit)
	}
	if res.Last.Commit != 104 {
		t.Fatalf("last = %d, want 104", res.Last.Commit)
	}
}

func TestRunSilentTailPageAdvancesBoundaryWithoutSavesChurn(t *testing.T) {
	// A page of foreign events: the position advances past them; the
	// next page's read resumes after them (the silent-tail discipline).
	f1 := rec("9f2b0000-0000-4000-8000-00000000000f", "foreign-1", 90, 90, "other.thing")
	f2 := rec("9f2b0000-0000-4000-8000-00000000000e", "foreign-2", 91, 91, "other.thing")
	a := rec("9f2b0000-0000-4000-8000-00000000000a", "order-a", 100, 100, "source.batch")
	reader := &fakeReader{pages: []*fakePage{
		pageOf(f1, f2), // full page (ReadPageSize 2), no matches
		pageOf(a),      // short page → done
	}}
	sink := &sinkRecorder{}
	progress := &progressRecorder{}
	res, err := Run(context.Background(), reader, Config{
		Name: "unit-export", Prefixes: []string{"order-"}, Transform: testTransform(), ReadPageSize: 2,
	}, Options{AsOf: AsOf{Prepare: 100, Commit: 100}}, sink.sink, progress.progress)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	// Page 2 resumes after the silent tail.
	if from := reader.calls[1].From.(kurrentdb.Position); from.Commit != 91 {
		t.Fatalf("page 2 From = %d, want 91 (past the silent tail)", from.Commit)
	}
	if res.EventsScanned != 1 || res.Last.Commit != 100 {
		t.Fatalf("res = %+v, want 1 event scanned to (100)", res)
	}
	// Saves: the silent-tail page boundary (91, 0, 0) and the final
	// (100, 1, 2). Exactly two — no churn.
	if len(progress.saves) != 2 {
		t.Fatalf("saves = %d (%+v), want 2", len(progress.saves), progress.saves)
	}
	if progress.saves[0][0] != 91 || progress.saves[0][2] != 0 {
		t.Fatalf("silent-tail save = %+v, want position 91 with zero counters", progress.saves[0])
	}
	if progress.saves[1][0] != 100 || progress.saves[1][2] != 1 || progress.saves[1][3] != 2 {
		t.Fatalf("final save = %+v, want (100, 1 event, 2 messages)", progress.saves[1])
	}
}

// ---------------------------------------------------------------------------
// Run — progress durability and failure recovery
// ---------------------------------------------------------------------------

func TestRunSavesProgressAtBatchBoundariesAndFinally(t *testing.T) {
	// 4 matching events, BatchSize 2: saves at 2, 4 — the final save is
	// already covered by the batch save at 4 (no churn).
	evs := matchingEvents("order", 4)
	reader := &fakeReader{pages: []*fakePage{pageOf(evs...)}}
	sink := &sinkRecorder{}
	progress := &progressRecorder{}
	res, err := Run(context.Background(), reader, Config{
		Name: "unit-export", Prefixes: []string{"order-"}, Transform: testTransform(), BatchSize: 2,
	}, Options{AsOf: AsOf{Prepare: 103, Commit: 103}}, sink.sink, progress.progress)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(progress.saves) != 2 {
		t.Fatalf("saves = %d (%+v), want 2 (batch boundaries only)", len(progress.saves), progress.saves)
	}
	if progress.saves[0][2] != 2 || progress.saves[1][2] != 4 {
		t.Fatalf("saves = %+v, want counters 2 then 4", progress.saves)
	}
	if res.Last.Commit != 103 {
		t.Fatalf("last = %d, want 103", res.Last.Commit)
	}
}

func TestRunProgressFailureStopsScanAtLastSavedBoundary(t *testing.T) {
	// 4 matching events, BatchSize 2, the progress store fails on the
	// SECOND save: the scan aborts after 4 events were APPLIED but the
	// durable boundary covers only the first save (2 events). The resume
	// re-drives the unsaved tail (idempotent sink + deterministic batch).
	evs := matchingEvents("order", 4)
	reader := &fakeReader{pages: []*fakePage{pageOf(evs...)}}
	sink := &sinkRecorder{}
	progress := &progressRecorder{failAfter: 1, err: errors.New("pg down")}
	_, err := Run(context.Background(), reader, Config{
		Name: "unit-export", Prefixes: []string{"order-"}, Transform: testTransform(), BatchSize: 2,
	}, Options{AsOf: AsOf{Prepare: 103, Commit: 103}}, sink.sink, progress.progress)
	if err == nil || !strings.Contains(err.Error(), "pg down") {
		t.Fatalf("progress failure must abort the scan, got %v", err)
	}
	if len(sink.applied) != 8 {
		t.Fatalf("applied = %d, want 8 (the 4 events were applied before the save failed)", len(sink.applied))
	}
	if len(progress.saves) != 1 || progress.saves[0][2] != 2 {
		t.Fatalf("saves = %+v, want exactly the first boundary (2 events)", progress.saves)
	}

	// RECOVERY: the restart resumes From the last durable save (the
	// second event's position, counter 2) — the scripted page replays
	// events 3 and 4 only.
	reader2 := &fakeReader{pages: []*fakePage{pageOf(evs[2], evs[3])}}
	sink2 := &sinkRecorder{}
	progress2 := &progressRecorder{}
	res, err := Run(context.Background(), reader2, Config{
		Name: "unit-export", Prefixes: []string{"order-"}, Transform: testTransform(), BatchSize: 2,
	}, Options{
		From: AsOf{Prepare: progress.saves[0][0], Commit: progress.saves[0][1]},
		AsOf: AsOf{Prepare: 103, Commit: 103},
	}, sink2.sink, progress2.progress)
	if err != nil {
		t.Fatalf("recovery run: %v", err)
	}
	if res.EventsScanned != 2 || res.MessagesEmitted != 4 {
		t.Fatalf("recovery res = %+v, want the 2 unsaved events", res)
	}
}

func TestRunSinkFailureMidBatchKeepsWholeEventRedriveable(t *testing.T) {
	// Each event derives 2 messages; the sink fails on the SECOND message
	// of the FIRST event: the scan aborts with NOTHING applied and the
	// boundary untouched — the resume re-derives the whole batch
	// (whole-source-event redrive).
	evs := matchingEvents("order", 2)
	reader := &fakeReader{pages: []*fakePage{pageOf(evs...)}}
	sink := &sinkRecorder{failOn: evs[0].EventID.String() + ":test:1", err: errors.New("engine nak")}
	progress := &progressRecorder{}
	_, err := Run(context.Background(), reader, Config{
		Name: "unit-export", Prefixes: []string{"order-"}, Transform: testTransform(), BatchSize: 10,
	}, Options{AsOf: AsOf{Prepare: 101, Commit: 101}}, sink.sink, progress.progress)
	if err == nil || !strings.Contains(err.Error(), "engine nak") {
		t.Fatalf("sink failure must abort the scan, got %v", err)
	}
	if len(sink.applied) != 1 {
		t.Fatalf("applied = %d, want 1 (the batch aborted on the 2nd message)", len(sink.applied))
	}
	if len(progress.saves) != 0 {
		t.Fatalf("saves = %d, want 0 (the whole source event stays redriveable)", len(progress.saves))
	}

	// RECOVERY: re-run from the same boundary with the failure cleared —
	// the whole batch re-derives and both events complete.
	sink2 := &sinkRecorder{}
	reader2 := &fakeReader{pages: []*fakePage{pageOf(evs...)}}
	res, err := Run(context.Background(), reader2, Config{
		Name: "unit-export", Prefixes: []string{"order-"}, Transform: testTransform(), BatchSize: 10,
	}, Options{AsOf: AsOf{Prepare: 101, Commit: 101}}, sink2.sink, nil)
	if err != nil {
		t.Fatalf("recovery run: %v", err)
	}
	if len(sink2.applied) != 4 {
		t.Fatalf("recovery applied = %d, want 4 (the whole batches re-derived)", len(sink2.applied))
	}
	if res.EventsScanned != 2 {
		t.Fatalf("recovery events = %d, want 2", res.EventsScanned)
	}
}

func TestRunTransformErrorAbortsAndNeverSkips(t *testing.T) {
	// An event the transform cannot decode (a relay defect): the scan
	// aborts with the position preserved — the event is never skipped.
	bad := rec("9f2b0000-0000-4000-8000-00000000000a", "order-a", 100, 100, "source.unknown")
	reader := &fakeReader{pages: []*fakePage{pageOf(bad)}}
	sink := &sinkRecorder{}
	progress := &progressRecorder{}
	res, err := Run(context.Background(), reader, Config{
		Name: "unit-export", Prefixes: []string{"order-"}, Transform: testTransform(),
	}, Options{AsOf: AsOf{Prepare: 100, Commit: 100}}, sink.sink, progress.progress)
	if err == nil {
		t.Fatal("a transform error must abort the scan")
	}
	if res.EventsScanned != 0 || len(sink.applied) != 0 || len(progress.saves) != 0 {
		t.Fatalf("transform error must leave everything untouched: res=%+v applied=%d saves=%d",
			res, len(sink.applied), len(progress.saves))
	}
}

func TestRunFailsClosedOnNilDependencies(t *testing.T) {
	cfg := Config{Name: "unit-export", Prefixes: []string{"order-"}, Transform: testTransform()}
	sink := func(context.Context, natspub.BatchMessage) error { return nil }
	base := Options{AsOf: AsOf{Prepare: 1, Commit: 1}}

	if _, err := Run(context.Background(), nil, cfg, base, sink, nil); err == nil {
		t.Fatal("nil reader must fail closed")
	}
	if _, err := Run(context.Background(), &fakeReader{}, cfg, base, nil, nil); err == nil {
		t.Fatal("nil sink must fail closed")
	}
	noTransform := Config{Name: "unit-export", Prefixes: []string{"order-"}}
	if _, err := Run(context.Background(), &fakeReader{}, noTransform, base, sink, nil); err == nil {
		t.Fatal("nil transform must fail closed (bind the producer relay's transform)")
	}
}

func TestRunContextCancellationSurfaces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	evs := matchingEvents("order", 1)
	reader := &fakeReader{pages: []*fakePage{pageOf(evs...)}}
	sink := &sinkRecorder{}
	if _, err := Run(ctx, reader, Config{
		Name: "unit-export", Prefixes: []string{"order-"}, Transform: testTransform(),
	}, Options{AsOf: AsOf{Prepare: 100, Commit: 100}}, sink.sink, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled run err = %v, want context.Canceled", err)
	}
}

func TestRunReaderOpenErrorSurfaces(t *testing.T) {
	reader := &fakeReader{openErr: errors.New("boom"), openErrOn: 1}
	sink := func(context.Context, natspub.BatchMessage) error { return nil }
	if _, err := Run(context.Background(), reader, Config{
		Name: "unit-export", Prefixes: []string{"order-"}, Transform: testTransform(),
	}, Options{AsOf: AsOf{Prepare: 1, Commit: 1}}, sink, nil); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("reader open error must surface, got %v", err)
	}
}
