// Package eventstoredb — clientmanager.go provides the host-owned
// reconnectable KurrentDB client generation manager (D107-R2).
//
// Why it exists: the pinned client (KurrentDB-Client-Go v1.4.1)
// PERMANENTLY closes itself when ONE full node-discovery sequence
// exhausts its attempt budget while the server is unreachable (see
// client_pinned_behavior_test.go for the deterministic pin and
// docs/decisions.md D107 for the investigation). A volume-backed
// KurrentDB restart keeps the TCP port refusing for longer than that
// budget (~1s against immediate RST), so the shared client poisons, every
// RPC fails fast with ErrorCodeConnectionClosed forever, and — because
// workers, pollers, and stores hold the client pointer for the process
// lifetime — the whole worker process loses its event store until it is
// restarted. E2E proved this violates the required same-process restart
// recovery.
//
// The fix is a coordinated CLIENT GENERATION SWAP owned by the worker
// host (D107-R2): the manager holds the current client generation; a
// monitor detects the terminal close; the supervisor cancels the module
// runners, swaps to a freshly opened client (singleflight + minimum swap
// interval + retry backoff), and restarts the runners against the new
// generation. Because every module runner constructs its stores and
// projection workers inside RunWorker (the host inventory's runWorker
// closures call module.New(deps.EventStore, ...) per invocation), a
// restart of the runners re-wires EVERY consumer — projections, pollers,
// and the NATS publisher — to the fresh client without touching any
// module signature.
//
// Explicitly out of scope (unchanged behavior): the API process holds its
// own client for request-scoped writes and does not participate in the
// swap; cmd/admin and cmd/replay are offline tooling.
package eventstoredb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

const (
	// defaultProbeInterval is the base interval between terminal-close
	// probes of the current client generation during healthy operation.
	// Each probe is one unary ReadStream against a nonexistent stream —
	// negligible load.
	defaultProbeInterval = 10 * time.Second

	// maxProbeInterval caps the adaptive probe backoff applied while a
	// terminal close keeps being detected (server still down), damping
	// the swap cycle: probe → poison → swap → probe ...
	maxProbeInterval = 60 * time.Second

	// defaultMinSwapInterval is the minimum time between completed swaps.
	// It prevents hot client-cycling even if terminal detection fires
	// repeatedly; a swap triggered earlier waits out the remainder.
	defaultMinSwapInterval = 5 * time.Second

	// probeTimeout bounds each probe RPC's OBSERVED wait (D135). The
	// probe runs on a goroutine — the pinned client's connection
	// acquisition is not context-cancellable, so the wait bound, not the
	// RPC deadline, is what keeps the monitor loop alive.
	probeTimeout = 5 * time.Second

	// probeStream is a stream guaranteed not to exist; reading it is a
	// cheap unary round-trip whose error (stream-not-found) proves the
	// client is alive and the server answered.
	probeStream = "yardflow-client-manager-probe"
)

// IsTerminalClose reports whether err is the pinned client's PERMANENT
// connection-closed failure — the only condition that justifies a client
// generation swap. Every other transport/unavailable error is transient
// from the manager's point of view: the client may still recover on its
// own (and normally does — D107's live restart experiment), so no swap is
// triggered for it.
func IsTerminalClose(err error) bool {
	var esErr *kurrentdb.Error
	if !errors.As(err, &esErr) {
		return false
	}
	return esErr.IsErrorCode(kurrentdb.ErrorCodeConnectionClosed)
}

// ClientManager owns the KurrentDB client generation for one host
// process. It is safe for concurrent use.
//
// Ownership: exactly one manager per worker host (created in
// hosts.NewWorker). Consumers never call Swap directly — the host
// supervisor does, and only after the monitor signalled a terminal close.
//
// Retirement policy (D135): a generation that was ever handed out is
// RETIRED BY DROPPING THE REFERENCE and is NEVER closed. A retired
// generation is, by the swap's only production trigger, terminally
// poisoned — the pinned SDK's connection state machine has exited with
// its close flag set while its UNBUFFERED channel stays open, so any
// concurrent RPC that passed the flag check just before is parked on a
// channel send no receiver will ever complete. Calling Close on such a
// client closes the channel under the parked senders and panics them
// ("send on closed channel" — the AR-210 E2E worker crash). Parked
// senders and the generation's already-dead SDK state are inert and are
// reclaimed at process exit — the same bounded-leak class as D107's
// parked probe. Only NEVER-PUBLISHED clients (failed or abandoned dials)
// are closed: nothing else references them and no RPC was ever issued on
// them, so closing them is provably safe.
type ClientManager struct {
	cfg Config

	// dial opens a new client. Injectable for tests; production uses
	// OpenClient (lazy: it does not contact the server).
	dial func(context.Context, Config) (*kurrentdb.Client, error)

	// closeClient releases an UNPUBLISHED client (a dial that failed or
	// whose swap was cancelled). Injectable for tests; production uses
	// the client's Close. Handed-out generations are NEVER closed (D135)
	// — see the struct comment.
	closeClient func(*kurrentdb.Client)

	// check probes one client generation and reports whether it is
	// terminally closed. Injectable for tests; production runs one cheap
	// unary RPC and classifies with IsTerminalClose.
	check func(ctx context.Context, c *kurrentdb.Client) bool

	minSwapInterval time.Duration

	// probeTimeout bounds each terminal probe's OBSERVED wait (D135).
	// The probe RPC itself runs on a goroutine — the pinned client's
	// connection acquisition is NOT context-cancellable, so a probe can
	// park inside getConnectionHandle during a wedged discovery; the
	// bound keeps the monitor loop (and therefore terminal detection and
	// the swap) alive. Injectable for tests.
	probeTimeout time.Duration

	mu                sync.Mutex
	current           *kurrentdb.Client
	swapping          chan struct{} // non-nil while a swap is in flight
	lastSwapErr       error
	lastSwap          time.Time
	swaps             int
	terminal          chan struct{} // closed when terminal close detected
	terminalFired     bool
	parkedProbeWarned bool
}

// ClientManagerOption customizes a ClientManager (tests).
type ClientManagerOption func(*ClientManager)

// WithDialFunc overrides how new clients are opened (tests).
func WithDialFunc(fn func(context.Context, Config) (*kurrentdb.Client, error)) ClientManagerOption {
	return func(m *ClientManager) { m.dial = fn }
}

// WithCheckFunc overrides terminal-close detection (tests). The default
// check performs one probe RPC; an override MUST NOT perform blocking
// work beyond the provided context.
func WithCheckFunc(fn func(ctx context.Context, c *kurrentdb.Client) bool) ClientManagerOption {
	return func(m *ClientManager) { m.check = fn }
}

// WithMinSwapInterval overrides the minimum completed-swap spacing (tests).
func WithMinSwapInterval(d time.Duration) ClientManagerOption {
	return func(m *ClientManager) { m.minSwapInterval = d }
}

// WithCloseFunc overrides how UNPUBLISHED clients are released
// (tests). Handed-out generations are never closed (D135).
func WithCloseFunc(fn func(*kurrentdb.Client)) ClientManagerOption {
	return func(m *ClientManager) { m.closeClient = fn }
}

// WithProbeTimeout overrides the observed wait bound per terminal probe
// (tests). The default is probeTimeout.
func WithProbeTimeout(d time.Duration) ClientManagerOption {
	return func(m *ClientManager) {
		if d > 0 {
			m.probeTimeout = d
		}
	}
}

// NewClientManager creates a manager that will serve `seed` as the first
// client generation. seed may be nil (KurrentDB disabled): the manager
// then never probes, never signals, and Swap returns the nil dial result.
func NewClientManager(cfg Config, seed *kurrentdb.Client, opts ...ClientManagerOption) *ClientManager {
	m := &ClientManager{
		cfg:             cfg,
		dial:            OpenClient,
		check:           probeTerminal,
		closeClient:     func(c *kurrentdb.Client) { c.Close() },
		minSwapInterval: defaultMinSwapInterval,
		probeTimeout:    probeTimeout,
		current:         seed,
		terminal:        make(chan struct{}),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Client returns the current client generation. Callers that keep the
// pointer for their process lifetime do so at the host's direction: the
// host restarts its runners after a swap so long-lived consumers are
// reconstructed against the new generation.
func (m *ClientManager) Client() *kurrentdb.Client {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current
}

// Swaps returns the number of completed generation swaps (observability).
func (m *ClientManager) Swaps() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.swaps
}

// TerminalCh returns a channel that is closed when the current generation
// was detected terminally closed. A successful Swap replaces it with a
// fresh open channel for the new generation.
func (m *ClientManager) TerminalCh() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.terminal
}

// SignalTerminal marks the current generation terminally closed. It is
// idempotent per generation (the monitor may observe the terminal state
// on several consecutive probes) and is a no-op when no client is
// configured.
func (m *ClientManager) SignalTerminal() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil || m.terminalFired {
		return
	}
	m.terminalFired = true
	close(m.terminal)
}

// Swap replaces the current client generation with a freshly opened one.
// The old generation is RETIRED, NOT CLOSED (D135): by the swap's only
// production trigger it is terminally poisoned, which means the pinned
// SDK's connection state machine has already exited with its close flag
// set while its UNBUFFERED channel stays open — every concurrent RPC
// that passed the flag check just before is parked on a channel send no
// receiver will ever complete, and closing the client would close that
// channel under the parked senders and panic them ("send on closed
// channel" — the AR-210 E2E worker crash). The retired generation's
// already-dead SDK state and its parked senders are inert and reclaimed
// at process exit. SINGLEFLIGHT: concurrent callers
// coalesce onto one dial and all receive the same generation (or the same
// error). BACKOFF: a completed swap enforces minSwapInterval before the
// next dial; Swap waits out the remainder, bounded by ctx.
//
// Cancellation contract (D107-R4): if ctx is already cancelled when the
// dial returns, the fresh client is NOT published — it is closed and the
// swap fails with ctx.Err() (the production dial is lazy and ignores its
// context, so a caller that gave up mid-dial can still be handed a live
// client). Best-effort by necessity: a cancellation racing the post-dial
// check loses and the client publishes, which is harmless (it is a valid
// generation). Likewise, a dial that returns a client TOGETHER with an
// error has that client closed before the error surfaces — a non-nil
// client is never leaked on either failure path. These UNPUBLISHED
// clients are the ONLY ones the manager ever closes: nothing else
// references them and no RPC was ever issued on them, so closing them
// is provably safe.
func (m *ClientManager) Swap(ctx context.Context) (*kurrentdb.Client, error) {
	m.mu.Lock()
	if m.swapping != nil {
		inflight := m.swapping
		m.mu.Unlock()
		select {
		case <-inflight:
			m.mu.Lock()
			c, err := m.current, m.lastSwapErr
			m.mu.Unlock()
			return c, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	m.swapping = make(chan struct{})
	m.mu.Unlock()

	finish := func(c *kurrentdb.Client, err error) (*kurrentdb.Client, error) {
		m.mu.Lock()
		inflight := m.swapping
		m.swapping = nil
		m.lastSwapErr = err
		m.mu.Unlock()
		close(inflight) // wake coalesced callers
		return c, err
	}

	// Backoff spacing between completed swaps (cancellable).
	m.mu.Lock()
	nextAllowed := m.lastSwap.Add(m.minSwapInterval)
	m.mu.Unlock()
	if wait := time.Until(nextAllowed); wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return finish(nil, ctx.Err())
		}
	}

	fresh, err := m.dial(ctx, m.cfg)
	if err != nil {
		// A misbehaving dial may hand back a partially-built client
		// together with the error. The client was never published, so
		// nothing else references it — close it to prevent a leak.
		if fresh != nil {
			m.closeClient(fresh)
		}
		return finish(nil, fmt.Errorf("clientmanager: open client: %w", err))
	}

	// Re-check ctx AFTER the dial (D107-R4 review HIGH): OpenClient
	// ignores its context, so a cancelled caller can still receive a live
	// client here. Publishing it would make a generation current whose
	// swap already failed from the caller's perspective. Close the
	// unpublished client and fail the swap; the caller (the supervisor's
	// retry loop) dials again on a live context.
	if cerr := ctx.Err(); cerr != nil {
		m.closeClient(fresh)
		return finish(nil, cerr)
	}

	m.mu.Lock()
	old := m.current
	m.current = fresh
	m.lastSwap = time.Now()
	m.swaps++
	m.terminal = make(chan struct{})
	m.terminalFired = false
	m.mu.Unlock()

	if old != nil && old != fresh {
		// D135: retire, do NOT close. The retired generation is
		// terminally poisoned: the SDK's state machine has exited with
		// the close flag set while its unbuffered channel stays open,
		// and closing that channel under the parked senders (any RPC
		// that passed the flag check just before the poison) panics
		// them — the AR-210 E2E "send on closed channel" worker crash.
		// The parked senders and the dead generation's SDK state are
		// inert and reclaimed at process exit.
		slog.Info("eventstoredb: retired client generation (left unclosed; parked callers possible)",
			"swaps", m.Swaps())
	}
	slog.Info("eventstoredb: client generation swapped",
		"swaps", m.Swaps(), "url", m.cfg.URL)
	// Complete the singleflight: reset swapping and wake coalesced
	// callers (they observe the new generation and a nil error).
	return finish(fresh, nil)
}

// RetireCurrent drops the reference to the current generation (host
// shutdown). The generation is NOT closed (D135): a worker-shutdown that
// races an outage can hold a terminally-poisoned generation whose SDK
// channel carries parked senders, and closing it would panic them. The
// process exits immediately after the host closes, reclaiming the
// generation's (already-dead) SDK state; there is no persistent resource
// to release. Swap-retired generations were already dropped by Swap.
func (m *ClientManager) RetireCurrent() {
	m.mu.Lock()
	m.current = nil
	m.mu.Unlock()
}

// Monitor probes the current generation every interval (base, doubling up
// to maxProbeInterval while terminal closes persist, resetting on a
// healthy probe) and signals TerminalCh on a terminal close. It returns
// when ctx is cancelled. It is a no-op pass when no client is configured.
//
// The adaptive backoff matters: during an outage that outlasts the
// client's discovery budget, each new generation gets poisoned by the
// first probe that fully exhausts discovery, so without backoff the
// manager would cycle generations at probe speed. With backoff the cycle
// is bounded and gentle, and the moment the server answers, the current
// generation is healthy and the cycle stops on its own.
//
// Bounded probe (D135): each check runs on a goroutine and the loop waits
// at most probeTimeout for its result. The pinned client's connection
// acquisition is NOT context-cancellable, so a probe that fires while the
// state machine is mid-discovery can park inside getConnectionHandle
// forever; if the loop called the check synchronously, ONE such parked
// probe would wedge terminal detection permanently (no swap, no
// recovery). An abandoned probe is inconclusive (never terminal on its
// own): the next tick re-probes, and once the poison flag is set the
// fast-failing probe detects the terminal close. A parked probe's late
// result lands in a buffered channel and is discarded. Parked probes are
// bounded by the probe interval and inert (their generation is retired,
// never closed) — reclaimed at process exit.
func (m *ClientManager) Monitor(ctx context.Context, base time.Duration) {
	if base <= 0 {
		base = defaultProbeInterval
	}
	interval := base
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		c := m.Client()
		if c == nil {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, m.probeTimeout)
		terminal, abandoned := m.boundedCheck(pctx, c)
		cancel()
		if abandoned {
			if ctx.Err() == nil {
				m.warnParkedProbe()
			}
			// Inconclusive: the next tick re-probes. The interval stays
			// as-is; backoff only applies to CONFIRMED terminal probes.
			continue
		}
		if terminal {
			m.SignalTerminal()
			if interval < maxProbeInterval {
				interval *= 2
				if interval > maxProbeInterval {
					interval = maxProbeInterval
				}
				ticker.Reset(interval)
			}
			continue
		}
		if interval != base {
			interval = base
			ticker.Reset(base)
		}
	}
}

// boundedCheck runs one terminal-close check with an OBSERVED wait bound:
// the check executes on a goroutine and the caller waits at most for ctx's
// deadline. It reports (result, false) when the check answered in time and
// (false, true) when the probe was abandoned (timed out or the monitor
// stopped). The abandoned probe's late result is discarded via the
// buffered channel.
func (m *ClientManager) boundedCheck(ctx context.Context, c *kurrentdb.Client) (terminal, abandoned bool) {
	res := make(chan bool, 1)
	go func() { res <- m.check(ctx, c) }()
	select {
	case terminal := <-res:
		m.mu.Lock()
		m.parkedProbeWarned = false
		m.mu.Unlock()
		return terminal, false
	case <-ctx.Done():
		return false, true
	}
}

// warnParkedProbe logs the abandoned-probe condition once per streak (a
// probe result arriving resets the streak) — the probe interval already
// bounds how many probes can pile up parked.
func (m *ClientManager) warnParkedProbe() {
	m.mu.Lock()
	warned := m.parkedProbeWarned
	m.parkedProbeWarned = true
	m.mu.Unlock()
	if warned {
		return
	}
	slog.Warn("eventstoredb: terminal-close probe parked (pinned client connection acquisition is not cancellable); abandoning and re-probing next tick")
}

// probeTerminal is the production terminal-close check: one cheap unary
// ReadStream against a stream that does not exist. A live client returns
// stream-not-found (non-terminal); a poisoned client returns
// ErrorCodeConnectionClosed (terminal).
func probeTerminal(ctx context.Context, c *kurrentdb.Client) bool {
	rd, err := c.ReadStream(ctx, probeStream, kurrentdb.ReadStreamOptions{
		Direction: kurrentdb.Forwards,
		From:      kurrentdb.Start{},
	}, 1)
	if err != nil {
		return IsTerminalClose(err)
	}
	rd.Close()
	return false
}
