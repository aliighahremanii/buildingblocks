// Package kurrentexport is the bounded AS-OF exporter building block
// (AR-510 / R02 Step E, Q4-B/R3-F2): the KurrentDB half of the
// producer-side bootstrap export.
//
// One producer export scans KurrentDB's $all stream FORWARD from an
// EXCLUSIVE resume position (the export's saved durable progress) up to
// an INCLUSIVE as-of bound C (a captured $all head position — the
// guarantee that every source event committed at position ≤ C is
// reflected in the export content), derives the producer's 0..N contract
// batch per matching source event through the SAME natspub transform the
// producer's live relay uses (natspub.DeriveBatch — byte-identical
// batches between the live relay and the export are what makes the
// sources' overlap messageId-dedupable, R4-F2), and hands every derived
// message to a caller-supplied sink (the Reporting binding feeds the SAME
// ingestion engine as live transport with Sequence 0).
//
// BOUNDED CLIENT-SIDE prefix-filtered scan: the scan is not a live
// subscription — it is a sequence of fixed-count ReadAll pages with the
// producer's stream prefixes applied CLIENT-SIDE. The bound makes it
// finite: the scan always terminates at C even while writes continue past
// it (events appended after the capture are never scanned), and each
// server round trip is bounded by the page size.
//
// Crash safety: the scan persists its resume boundary through the
// onProgress callback at batch boundaries (every BatchSize matched
// events), at page boundaries the silent tail moved (zero-message
// positions), and once at the END of a successful run — a crash between
// the run's return and the caller's completion write must resume from
// the run's final boundary, not re-scan the tail. A failed progress save
// ends the scan; a failed sink call ends the scan (the un-saved tail
// re-derives identically on resume — determinism — and the sink's
// idempotence absorbs the re-drive).
package kurrentexport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"

	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/natspub"
)

// AsOf is a KurrentDB $all transaction-log position (prepare, commit) —
// the as-of bound C or a resume boundary.
type AsOf struct {
	Prepare uint64
	Commit  uint64
}

// IsZero reports the degenerate position (0,0) — the beginning of $all
// for a resume boundary, or an EMPTY $all head capture (an empty store's
// head is (0,0): no event exists, so trivially every event ≤ it — none —
// is reflected).
func (a AsOf) IsZero() bool { return a.Prepare == 0 && a.Commit == 0 }

// PageStream is one bounded page of $all read results; *kurrentdb.ReadStream
// satisfies it. It lives here (where it is consumed) so the scanner is
// unit-testable without a live server.
type PageStream interface {
	Recv() (*kurrentdb.ResolvedEvent, error)
	Close()
}

// AllReader opens bounded $all pages; *kurrentdb.Client adapts through
// ClientReader. The seam makes both the capture and the scan unit-testable.
type AllReader interface {
	ReadAll(ctx context.Context, opts kurrentdb.ReadAllOptions, count uint64) (PageStream, error)
}

// ClientReader adapts *kurrentdb.Client to AllReader (the production
// reader; tests substitute fakes).
type ClientReader struct{ Client *kurrentdb.Client }

// ReadAll opens one bounded forwards/backwards page of $all.
func (r ClientReader) ReadAll(ctx context.Context, opts kurrentdb.ReadAllOptions, count uint64) (PageStream, error) {
	return r.Client.ReadAll(ctx, opts, count)
}

// Compile-time: the production adapter satisfies the seam.
var _ AllReader = ClientReader{}

// CaptureAsOf captures the CURRENT $all head position as the as-of bound
// C: one BACKWARDS read of a single event. An empty $all captures the
// degenerate bound (0,0) — the caller declares it once and the export
// with bound (0,0) scans nothing (as-of semantics of an empty store).
//
// The capture is a committed-head FACT: every event at position ≤ C was
// already committed when the capture returned, so a forward scan to C
// inclusive can never miss an event the capture observed.
func CaptureAsOf(ctx context.Context, reader AllReader) (AsOf, error) {
	if reader == nil {
		return AsOf{}, fmt.Errorf("kurrentexport capture as-of: no reader configured (fail closed)")
	}
	rd, err := reader.ReadAll(ctx, kurrentdb.ReadAllOptions{
		Direction: kurrentdb.Backwards,
		From:      kurrentdb.End{},
	}, 1)
	if err != nil {
		return AsOf{}, fmt.Errorf("kurrentexport capture as-of: %w", err)
	}
	defer rd.Close()
	res, err := rd.Recv()
	if errors.Is(err, io.EOF) {
		// Empty $all: the degenerate bound.
		return AsOf{}, nil
	}
	if err != nil {
		return AsOf{}, fmt.Errorf("kurrentexport capture as-of head: %w", err)
	}
	if res == nil || res.Event == nil {
		return AsOf{}, nil
	}
	return AsOf{Prepare: res.Event.Position.Prepare, Commit: res.Event.Position.Commit}, nil
}

// Config binds ONE producer's relay transform to the scanner.
type Config struct {
	// Name identifies the export in errors (e.g. "bootstrap-operations").
	Name string
	// Prefixes are the producer's canonical stream-id prefixes — the
	// CLIENT-SIDE filter: only events in these streams are exported. An
	// EMPTY prefix set exports NOTHING (fail-safe default-deny, the same
	// posture as the live relay's server-side filter).
	Prefixes []string
	// Transform derives the 0..N contract batch per matching source
	// event — the SAME natspub.EventTransform the producer's live relay
	// runs. REQUIRED (an unbound export is a setup defect, D049
	// fail-closed); raw-republish mode is a live-relay concept only.
	Transform natspub.EventTransform
	// BatchSize bounds the matched events between durable progress saves.
	// Zero → 50 (the natspub default the live relay uses).
	BatchSize int
	// ReadPageSize bounds each $all read page (the server returns at
	// most this many events per round trip). Zero → 1000. Values below 2
	// are refused (fail closed): the inclusive $all read returns the
	// event AT the resume position, so a boundary-only page of size 1
	// can never advance the scan.
	ReadPageSize int
}

func (c Config) withDefaults() Config {
	if c.BatchSize <= 0 {
		c.BatchSize = 50
	}
	if c.ReadPageSize <= 0 {
		c.ReadPageSize = 1000
	}
	return c
}

// Options parameterize one bounded scan.
type Options struct {
	// From is the EXCLUSIVE resume position (the export's saved durable
	// progress). (0,0) starts at the beginning of $all. KurrentDB's
	// forwards $all read FROM a position INCLUDES the event at that
	// position (live-verified); Run compensates by skipping the boundary
	// event, so the exclusive contract holds against the real server.
	From AsOf
	// AsOf is the INCLUSIVE as-of bound C: every source event at position
	// ≤ C is processed; the first event strictly beyond C ends the scan.
	// This is the DURABLE DECLARED bound — pass it VERBATIM on every
	// resume; a run must never re-capture a moving head.
	AsOf AsOf
}

// MessageSink applies ONE derived contract message (the Reporting binding
// feeds the SAME ingestion engine as live transport — Sequence 0). An
// error stops the scan: the durable progress stays at the last
// successful save and the resume re-drives the unsaved tail (the sink's
// idempotence and the transform's determinism make the overlap harmless).
type MessageSink func(ctx context.Context, msg natspub.BatchMessage) error

// ProgressFunc durably persists the scan's resume boundary (the EXCLUSIVE
// position) and the running observability counters. Called at batch
// boundaries, at silent-tail page boundaries, and once at the END of a
// successful scan; the counters are ABSOLUTE running totals (the store's
// progress columns are SET, not accumulated). An error stops the scan
// with the progress at the last successful save. Nil disables durable
// saves (in-memory scans/tests only).
type ProgressFunc func(ctx context.Context, prepare, commit uint64, eventsScanned, messagesEmitted uint64) error

// Result reports one scan run's outcome. Last is the last processed
// position within the bound (the resume boundary: matched AND scanned-past
// events both advance it).
type Result struct {
	// AsOf is the bound the run used (passed through verbatim).
	AsOf AsOf
	// Last is the last processed position — the EXCLUSIVE resume
	// boundary after this run (opts.From when nothing was processed).
	Last AsOf
	// EventsScanned counts the prefix-matching source events processed.
	EventsScanned uint64
	// MessagesEmitted counts the derived contract messages handed to the
	// sink.
	MessagesEmitted uint64
}

// Run executes one bounded client-side prefix-filtered as-of scan. It
// returns the final Result; on error the durable progress stands at the
// last successful save (the caller's restart passes it back as From).
//
// Whole-source-event redrive (the natspub invariant): the boundary
// advances past a source event only after EVERY message of its batch was
// applied through the sink. A sink failure mid-batch stops the scan with
// the boundary at the previous fully-applied event — the resume re-derives
// the whole batch identically.
func Run(ctx context.Context, reader AllReader, cfg Config, opts Options, sink MessageSink, onProgress ProgressFunc) (Result, error) {
	cfg = cfg.withDefaults()
	res := Result{AsOf: opts.AsOf, Last: opts.From}
	if reader == nil {
		return res, fmt.Errorf("kurrentexport %s: no reader configured (fail closed)", cfg.Name)
	}
	if sink == nil {
		return res, fmt.Errorf("kurrentexport %s: no sink configured (fail closed)", cfg.Name)
	}
	if cfg.Transform == nil {
		return res, fmt.Errorf("kurrentexport %s: no transform configured (fail closed — bind the producer relay's transform)", cfg.Name)
	}
	if cfg.ReadPageSize < 2 {
		return res, fmt.Errorf("kurrentexport %s: ReadPageSize %d < 2 (fail closed — the inclusive $all read returns the event AT the resume boundary, so a boundary-only page of size 1 can never advance the scan; every page of size >= 2 containing the boundary event also contains a beyond-boundary event)", cfg.Name, cfg.ReadPageSize)
	}

	saved := opts.From // the last durably saved boundary
	from := opts.From
	for {
		if err := ctx.Err(); err != nil {
			return res, fmt.Errorf("kurrentexport %s: %w", cfg.Name, err)
		}
		rd, err := reader.ReadAll(ctx, kurrentdb.ReadAllOptions{
			Direction: kurrentdb.Forwards,
			From:      positionOrStart(from),
		}, uint64(cfg.ReadPageSize))
		if err != nil {
			return res, fmt.Errorf("kurrentexport %s: open page from (%d,%d): %w", cfg.Name, from.Prepare, from.Commit, err)
		}

		var (
			read      int  // events read this page (matching or not)
			pastBound bool // the first event strictly beyond C ended the scan
			lastPos   = from
		)
		for {
			r, rerr := rd.Recv()
			if rerr != nil {
				rd.Close()
				if errors.Is(rerr, io.EOF) {
					// Normal page end: a count-bounded $all read closes
					// its stream when the page is delivered OR the log
					// end is reached — the read count distinguishes them
					// below, EOF does not.
					break
				}
				return res, fmt.Errorf("kurrentexport %s: read page: %w", cfg.Name, rerr)
			}
			if r == nil || r.Event == nil {
				continue
			}
			ev := r.Event
			// INCLUSIVE-READ COMPENSATION (live-server semantics,
			// verified against KurrentDB): a forwards $all read FROM a
			// position RETURNS the event AT that position. The scan's
			// From boundary is EXCLUSIVE (the event at it was already
			// processed and durably saved by a prior run/page), so the
			// boundary event is skipped — it still counts toward `read`
			// (the server counted it against the page size; not counting
			// it would make a boundary-only page look short and end the
			// scan early). (Commit, Prepare) is unique per event in
			// $all, so the equality is an identity check.
			if !from.IsZero() && ev.Position.Commit == from.Commit && ev.Position.Prepare == from.Prepare {
				read++
				continue
			}
			if beyondBound(ev.Position, opts.AsOf) {
				// INCLUSIVE C: the event AT C was processed; the first
				// event strictly beyond ends the scan. Its position is
				// NOT the boundary (it stays unprocessed).
				pastBound = true
				break
			}
			read++
			lastPos = AsOf{Prepare: ev.Position.Prepare, Commit: ev.Position.Commit}
			res.Last = lastPos
			if !prefixMatches(cfg.Prefixes, ev.StreamID) {
				// Client-side prefix filter: the position advances past
				// the event without deriving a batch.
				continue
			}
			batch, derr := natspub.DeriveBatch(natspub.WorkerConfig{
				Name:      cfg.Name,
				Prefixes:  cfg.Prefixes,
				Transform: cfg.Transform,
			}, ev)
			if derr != nil {
				rd.Close()
				return res, fmt.Errorf("kurrentexport %s: derive batch: %w", cfg.Name, derr)
			}
			// Apply EVERY message of the batch before the boundary may
			// advance past this source event (whole-source-event
			// redrive).
			for _, m := range batch {
				if serr := sink(ctx, m); serr != nil {
					rd.Close()
					return res, fmt.Errorf("kurrentexport %s: apply message %s: %w", cfg.Name, m.MsgID, serr)
				}
			}
			res.EventsScanned++
			res.MessagesEmitted += uint64(len(batch))
			if int(res.EventsScanned)%cfg.BatchSize == 0 {
				if perr := saveProgress(ctx, onProgress, cfg, res); perr != nil {
					rd.Close()
					return res, perr
				}
				saved = res.Last
			}
		}

		if pastBound || read < cfg.ReadPageSize {
			// The scan is complete: the bound was reached (pastBound), or
			// the page came up SHORT — the server returned fewer events
			// than requested, which is the log end (the log only grows
			// and C was a committed head, so a short page means
			// everything ≤ C was processed).
			break
		}

		// Full page: continue from its last read position. Silent-tail
		// save (the natspub discipline): a page of foreign/system
		// events moved the position without a batch-boundary save —
		// persist the moved boundary so the durable cursor does not
		// trail the silent tail (re-scanning silent events is harmless
		// but wasteful).
		if res.Last != saved {
			if perr := saveProgress(ctx, onProgress, cfg, res); perr != nil {
				return res, perr
			}
			saved = res.Last
		}
		from = res.Last
	}

	// Final durable boundary (a crash between this run's return and the
	// caller's completion write must resume from the run's final
	// boundary, not re-scan the tail). A no-op when the last save already
	// covered it.
	if res.Last != saved {
		if perr := saveProgress(ctx, onProgress, cfg, res); perr != nil {
			return res, perr
		}
	}
	return res, nil
}

// saveProgress persists the current boundary through onProgress (nil
// onProgress is an in-memory no-op).
func saveProgress(ctx context.Context, onProgress ProgressFunc, cfg Config, res Result) error {
	if onProgress == nil {
		return nil
	}
	if err := onProgress(ctx, res.Last.Prepare, res.Last.Commit, res.EventsScanned, res.MessagesEmitted); err != nil {
		return fmt.Errorf("kurrentexport %s: progress save: %w", cfg.Name, err)
	}
	return nil
}

// beyondBound reports whether p is STRICTLY beyond the as-of bound in
// $all tuple order (commit, then prepare) — the INCLUSIVE bound processes
// the event at C itself. Events sharing a commit position (one
// multi-stream commit) are ordered by prepare position.
func beyondBound(p kurrentdb.Position, bound AsOf) bool {
	if p.Commit != bound.Commit {
		return p.Commit > bound.Commit
	}
	return p.Prepare > bound.Prepare
}

// positionOrStart maps the resume boundary to a read position: (0,0) is
// the unambiguous Start of $all; any other position resumes exclusively
// after it.
func positionOrStart(from AsOf) kurrentdb.AllPosition {
	if from.IsZero() {
		return kurrentdb.Start{}
	}
	return kurrentdb.Position{Commit: from.Commit, Prepare: from.Prepare}
}

// prefixMatches is the CLIENT-SIDE stream-prefix filter — the same
// semantics as the live relay's server-side stream filter: an event
// matches when its stream id carries one of the configured prefixes.
func prefixMatches(prefixes []string, streamID string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(streamID, p) {
			return true
		}
	}
	return false
}
