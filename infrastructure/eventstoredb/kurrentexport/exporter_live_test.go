package kurrentexport

// exporter_live_test.go — LIVE KurrentDB coverage for the bounded as-of
// exporter (AR-510 / R02 Step E, Q4-B/R3-F2) through kurrenttest.OpenClient
// (KURRENTDB_URL-gated; auto-skip when unset/unreachable):
//
//   - CaptureAsOf returns the REAL $all head (a later capture never moves
//     backwards; appending events moves it forward);
//   - Run scans the real $all forward: the client-side prefix filter
//     exports only the test's unique stream prefix; the INCLUSIVE bound
//     processes the event at C and never anything appended after the
//     capture; the EXCLUSIVE resume boundary re-scans nothing already
//     covered; multi-page scans (a small ReadPageSize over many events)
//     converge; progress saves fire at batch boundaries and the run's
//     final save lands on the last scanned position.
//
// The test appends only to its own unique test streams (the shared $all
// discipline — unique identifiers, no cleanup of foreign state).

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"

	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/kurrenttest"
	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/natspub"
)

// appendLive appends one event to the stream and returns the position it
// landed at plus its event id.
func appendLive(t *testing.T, client *kurrentdb.Client, stream string, seq int) (kurrentdb.Position, string) {
	t.Helper()
	id := uuid.New()
	res, err := client.AppendToStream(context.Background(), stream,
		kurrentdb.AppendToStreamOptions{StreamState: kurrentdb.Any{}},
		kurrentdb.EventData{
			EventID:     id,
			EventType:   "test.source",
			ContentType: kurrentdb.ContentTypeJson,
			Data:        []byte(`{}`),
			Metadata:    []byte(`{}`),
		})
	_ = seq
	if err != nil {
		t.Fatalf("append %s: %v", stream, err)
	}
	return kurrentdb.Position{Commit: res.CommitPosition, Prepare: res.PreparePosition}, id.String()
}

// liveTransform derives one message per event carrying the event id (the
// live relay's identity discipline).
func liveTransform() natspub.EventTransform {
	return natspub.EventTransform(func(ev *kurrentdb.RecordedEvent) ([]natspub.BatchMessage, error) {
		return []natspub.BatchMessage{{
			Subject: "yardflow.reporting.contracts.test.v1",
			Data:    []byte(`{}`),
			MsgID:   ev.EventID.String() + ":test:0",
		}}, nil
	})
}

// liveSink records applied messages by message id.
type liveSink struct {
	applied map[string]bool
}

func (s *liveSink) sink(_ context.Context, m natspub.BatchMessage) error {
	s.applied[m.MsgID] = true
	return nil
}

func TestLiveCaptureAsOfIsTheRealHead(t *testing.T) {
	client := kurrenttest.OpenClient(t)
	ctx := context.Background()
	reader := ClientReader{Client: client}

	stream := kurrenttest.UniqueStreamName(t, "kurrentexport-capture")

	before, err := CaptureAsOf(ctx, reader)
	if err != nil {
		t.Fatalf("capture before: %v", err)
	}
	pos, _ := appendLive(t, client, stream, 1)
	after, err := CaptureAsOf(ctx, reader)
	if err != nil {
		t.Fatalf("capture after: %v", err)
	}
	if after.Commit < pos.Commit {
		t.Fatalf("captured head (%d,%d) is behind the appended event at commit %d", after.Commit, after.Prepare, pos.Commit)
	}
	if after.Commit < before.Commit {
		t.Fatalf("captured head moved backwards: before=%d after=%d (the log only grows)", before.Commit, after.Commit)
	}
}

func TestLiveRunExportsPrefixFilteredInclusiveOfCapture(t *testing.T) {
	client := kurrenttest.OpenClient(t)
	ctx := context.Background()
	reader := ClientReader{Client: client}

	// The matching prefix: every stream id starting with this unique
	// token; a foreign stream proves the client-side filter.
	base := kurrenttest.UniqueStreamName(t, "kurrentexport-incl")
	prefix := base + "-"
	matching := []string{base + "-a", base + "-b"}

	// Bounded scan range: everything is appended between two captures, so
	// the scan reads only this test's own events even on a busy shared
	// $all (the production export scans the whole log from its saved
	// progress; the mechanics under test are identical over any range).
	start, err := CaptureAsOf(ctx, reader)
	if err != nil {
		t.Fatalf("capture start: %v", err)
	}
	var ids []string
	for i, s := range []string{matching[0], matching[1], matching[0]} {
		_, id := appendLive(t, client, s, i)
		ids = append(ids, id)
	}
	foreign := kurrenttest.UniqueStreamName(t, "kurrentexport-foreign")
	appendLive(t, client, foreign, 99)

	c, err := CaptureAsOf(ctx, reader)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	// An event appended AFTER the capture is beyond C — never exported.
	_, beyondID := appendLive(t, client, matching[1], 100)

	sink := &liveSink{applied: map[string]bool{}}
	res, err := Run(ctx, reader, Config{
		Name:      "live-export-inclusive",
		Prefixes:  []string{prefix},
		Transform: liveTransform(),
	}, Options{From: start, AsOf: c}, sink.sink, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if res.EventsScanned != 3 {
		t.Fatalf("events scanned = %d, want 3 (prefix-filtered, inclusive of the capture)", res.EventsScanned)
	}
	if res.MessagesEmitted != 3 {
		t.Fatalf("messages emitted = %d, want 3", res.MessagesEmitted)
	}
	for _, id := range ids {
		if !sink.applied[id+":test:0"] {
			t.Fatalf("pre-capture event %s was not exported", id)
		}
	}
	if sink.applied[beyondID+":test:0"] {
		t.Fatal("the event appended AFTER the capture must never be exported (inclusive as-of)")
	}
	if res.Last.Commit > c.Commit {
		t.Fatalf("last boundary %d is beyond the bound %d", res.Last.Commit, c.Commit)
	}
}

func TestLiveRunExclusiveResumeAndPaging(t *testing.T) {
	client := kurrenttest.OpenClient(t)
	ctx := context.Background()
	reader := ClientReader{Client: client}

	base := kurrenttest.UniqueStreamName(t, "kurrentexport-resume")
	prefix := base + "-"
	matching := base + "-m"
	// Bounded scan range (see the inclusive test): append everything
	// between two captures so the pages cover only this test's events.
	start, err := CaptureAsOf(ctx, reader)
	if err != nil {
		t.Fatalf("capture start: %v", err)
	}
	var positions []kurrentdb.Position
	for i := 0; i < 7; i++ {
		pos, _ := appendLive(t, client, matching, i)
		positions = append(positions, pos)
	}
	// Foreign noise events interleaved after the matching batch (still
	// before the capture) — they advance $all without a batch.
	for i := 0; i < 3; i++ {
		appendLive(t, client, kurrenttest.UniqueStreamName(t, "kurrentexport-noise"), i)
	}
	c, err := CaptureAsOf(ctx, reader)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}

	// First run: pages of 2 over the test's range, BatchSize 3.
	sink := &liveSink{applied: map[string]bool{}}
	type save struct {
		prepare, commit, scanned, emitted uint64
	}
	var saves []save
	res, err := Run(ctx, reader, Config{
		Name: "live-export-resume", Prefixes: []string{prefix}, Transform: liveTransform(),
		BatchSize: 3, ReadPageSize: 2,
	}, Options{From: start, AsOf: c}, sink.sink, func(_ context.Context, prepare, commit, scanned, emitted uint64) error {
		saves = append(saves, save{prepare, commit, scanned, emitted})
		return nil
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.EventsScanned != 7 || res.MessagesEmitted != 7 {
		t.Fatalf("res = %+v, want 7 matching events", res)
	}
	if len(saves) == 0 {
		t.Fatal("no progress saves recorded")
	}
	last := saves[len(saves)-1]
	if last.scanned != 7 || last.emitted != 7 {
		t.Fatalf("final save = %+v, want counters (7,7)", last)
	}
	if last.commit > c.Commit {
		t.Fatalf("final save commit %d is beyond the bound %d", last.commit, c.Commit)
	}

	// Second run: EXCLUSIVE resume from the first run's final boundary —
	// nothing new was appended ≤ C, so nothing is re-exported.
	sink2 := &liveSink{applied: map[string]bool{}}
	res2, err := Run(ctx, reader, Config{
		Name: "live-export-resume2", Prefixes: []string{prefix}, Transform: liveTransform(),
		BatchSize: 3, ReadPageSize: 2,
	}, Options{From: res.Last, AsOf: c}, sink2.sink, nil)
	if err != nil {
		t.Fatalf("resume run: %v", err)
	}
	if res2.EventsScanned != 0 || len(sink2.applied) != 0 {
		t.Fatalf("resume re-exported %d events (scanned=%d) — the boundary must be EXCLUSIVE", len(sink2.applied), res2.EventsScanned)
	}

	// Third run: resume from the 3rd matching event's position — only the
	// 4 events strictly after it export.
	sink3 := &liveSink{applied: map[string]bool{}}
	res3, err := Run(ctx, reader, Config{
		Name: "live-export-resume3", Prefixes: []string{prefix}, Transform: liveTransform(),
		BatchSize: 3, ReadPageSize: 2,
	}, Options{From: AsOf{Prepare: positions[2].Prepare, Commit: positions[2].Commit}, AsOf: c}, sink3.sink, nil)
	if err != nil {
		t.Fatalf("mid resume run: %v", err)
	}
	if res3.EventsScanned != 4 {
		t.Fatalf("mid-resume scanned = %d, want 4 (events strictly after the 3rd)", res3.EventsScanned)
	}
}
