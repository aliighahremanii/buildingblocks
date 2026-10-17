// Package metrics defines the Prometheus counters, gauges, and
// histograms that instrument the event-sourcing system. The endpoint
// is exposed at /metrics by both the API process (which owns the HTTP
// server) and the worker process (which scrapes itself via a small
// dedicated listener when not co-located with the API).
//
// Every metric here is declared with a stable name that operators and
// dashboards depend on; do NOT rename without coordinating an alerting
// migration. The Help text doubles as the alerting runbook's
// "what does this metric mean" reference.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Namespace prefixes every metric name so yardflow.* does not collide with
// other services scraped into the same Prometheus instance.
const Namespace = "yardflow"

// App-level labels that every metric carries.
const (
	// LabelAggregateType is the aggregate's type string ("country",
	// "province", "location", "item", "note"). Used to break down
	// per-aggregate metrics.
	LabelAggregateType = "aggregate_type"
	// LabelProjection is the projection's checkpoint key
	// (e.g. "sample1-postgres"). Used to break down per-projection metrics.
	LabelProjection = "projection"
	// LabelOutcome is "success" or "failure" (or more granular values
	// like "conflict" for appends). Used so a single counter can carry
	// both outcomes without doubling the metric cardinality.
	LabelOutcome = "outcome"
	// LabelEventType is the KurrentDB event-type string
	// ("country.registered", "item.processed", etc.).
	LabelEventType = "event_type"
)

var (
	// EventAppends counts KurrentDB AppendToStream calls, partitioned
	// by aggregate type and outcome (success / conflict / failure).
	// Alert on rate(conflict) > 0 for sustained concurrency-contention;
	// alert on rate(failure) > 0 for infrastructure problems.
	EventAppends = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "eventstore",
			Name:      "appends_total",
			Help:      "Number of KurrentDB AppendToStream calls by aggregate type and outcome (success|conflict|failure).",
		},
		[]string{LabelAggregateType, LabelOutcome},
	)

	// ConcurrencyConflicts counts optimistic-concurrency rejections
	// (HTTP 409 surfaced to the client). Equal to the "conflict"
	// outcome slice of EventAppends but broken out because it is the
	// primary alerting signal for "two clients raced on the same
	// aggregate".
	ConcurrencyConflicts = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "eventstore",
			Name:      "concurrency_conflicts_total",
			Help:      "Number of optimistic-concurrency conflicts (HTTP 409) by aggregate type.",
		},
		[]string{LabelAggregateType},
	)

	// SnapshotWrites counts aggregate snapshots written to the snapshot
	// store by aggregate type. Combined with EventAppends (success), the
	// ratio tells the operator how many appends land on a snapshot
	// boundary. If snapshot writes are rare relative to appends, the
	// hardcoded SnapshotEvery threshold (currently 50) is too high; if
	// they match appends 1:1, the threshold is too low.
	SnapshotWrites = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "eventstore",
			Name:      "snapshot_writes_total",
			Help:      "Number of aggregate snapshots written by aggregate type.",
		},
		[]string{LabelAggregateType},
	)

	// SnapshotSkipsBecauseNoNewEvents counts SaveAggregateWithSnapshot
	// calls that short-circuited because the events slice was empty
	// (Save was a no-op). A high rate relative to EventAppends indicates
	// handlers are calling the save path without producing events, which
	// is wasteful but not incorrect.
	SnapshotSkipsBecauseNoNewEvents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "eventstore",
			Name:      "snapshot_skips_no_events_total",
			Help:      "Number of SaveAggregateWithSnapshot calls that skipped the snapshot because the events slice was empty.",
		},
		[]string{LabelAggregateType},
	)

	// AggregateReplayDuration records the time spent in
	// eventstore.LoadAggregate (snapshot load + stream/tail read +
	// rehydrate closure). Long durations indicate either a missing
	// snapshot or a slow KurrentDB read; alert on p99 > 100ms for a
	// single aggregate.
	AggregateReplayDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "eventstore",
			Name:      "replay_duration_seconds",
			Help:      "Time spent loading and rehydrating an aggregate from KurrentDB by aggregate type.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 12), // 1ms → 4s
		},
		[]string{LabelAggregateType},
	)

	// ProjectionEvents counts events the projection worker has
	// processed, partitioned by projection name and outcome
	// (success|dlq|failure). The "dlq" outcome means the event
	// exhausted retries and was routed to the dead-letter queue;
	// alert on rate(dlq) > 0.
	ProjectionEvents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "projection",
			Name:      "events_total",
			Help:      "Number of projection events processed by projection name and outcome (success|dlq|failure).",
		},
		[]string{LabelProjection, LabelOutcome},
	)

	// ProjectionCheckpointPosition exposes the last CONFIRMED-PERSISTED
	// KurrentDB $all commit position per projection: the gauge advances
	// only after the checkpoint save (or atomic DLQ+checkpoint flush) has
	// committed, so it always mirrors durable state. Combined with the
	// global $all tail position (exported separately by a sidecar or by
	// scraping KurrentDB's /stats), this yields projection lag =
	// tail - checkpoint.
	ProjectionCheckpointPosition = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "projection",
			Name:      "checkpoint_position",
			Help:      "Last-flushed KurrentDB $all commit position by projection name.",
		},
		[]string{LabelProjection},
	)

	// ProjectionSubscriptionUp is 1 while a projection worker's KurrentDB
	// subscription is live (lifecycle state running) and 0 otherwise
	// (idle, connecting, down, stopped). Combined with
	// ProjectionReconnects it is the projection-resilience alerting
	// signal: subscription_up=0 for a required projection means the
	// worker host's /ready is (or should be) failing. Alert on
	// yardflow_projection_subscription_up{...} == 0 for required
	// projections.
	ProjectionSubscriptionUp = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "projection",
			Name:      "subscription_up",
			Help:      "1 while the projection worker's KurrentDB subscription is live; 0 when idle, connecting, down, or stopped.",
		},
		[]string{LabelProjection},
	)

	// ProjectionReconnects counts ended LIVE subscriptions: every
	// subscription that was established (reached the running state) and
	// then dropped. Failed establishments are deliberately NOT counted —
	// watch ProjectionSubscriptionUp == 0 for those. A sustained
	// reconnect rate means the subscription is flapping — investigate
	// KurrentDB health or poison-event backpressure.
	ProjectionReconnects = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "projection",
			Name:      "subscription_reconnects_total",
			Help:      "Number of dropped live subscriptions by projection name (established subscriptions that ended; failed establishments are not counted); sustained rate means flapping.",
		},
		[]string{LabelProjection},
	)

	// ProjectionDLQDepth is the current size of each projection's
	// dead-letter queue. Alert on > 0 — every DLQ entry is an event
	// the projection has explicitly decided to skip and requires
	// operator action.
	ProjectionDLQDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "projection",
			Name:      "dlq_depth",
			Help:      "Number of entries in each projection's dead-letter queue. Alert on > 0.",
		},
		[]string{LabelProjection},
	)

	// NATSPublishes counts events the NATS publisher has shipped to
	// JetStream by outcome (success|failure). Equal in shape to
	// ProjectionEvents but for the system's outbox.
	NATSPublishes = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "nats",
			Name:      "publishes_total",
			Help:      "Number of KurrentDB events republished to NATS JetStream by outcome (success|failure).",
		},
		[]string{LabelOutcome},
	)

	// NATSPosition is the publisher's last-flushed $all commit
	// position. Combined with ProjectionCheckpointPosition and the
	// global $all tail, this tells the operator whether the outbox is
	// keeping up with the event store.
	NATSPosition = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "nats",
			Name:      "position",
			Help:      "Last-flushed KurrentDB $all commit position published to NATS JetStream.",
		},
	)

	// ReportingIngestionMessages counts consumed Reporting contract
	// messages by the disposition outcome of the crash-safe ingestion
	// state machine (AR-510 / R02 Step C). "ack" is a terminal outcome
	// (completed, terminal no-mutation, or an atomic poison/foreign DLQ
	// disposition — every ack is preceded by the terminal Postgres
	// commit, R3-F3); "nak" is an incomplete retryable outcome; "dlq" is
	// a poison or foreign-subject disposition into the Reporting-owned
	// DLQ. Alert on rate(dlq) > 0 — every DLQ row needs operator action.
	ReportingIngestionMessages = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "reporting_ingestion",
			Name:      "messages_total",
			Help:      "Number of Reporting contract messages consumed by disposition outcome (ack|nak|dlq). Every ack is preceded by the terminal Postgres commit; dlq counts poison/foreign dispositions.",
		},
		[]string{LabelOutcome},
	)

	// ReportingIngestionCheckpoint is the last JetStream stream sequence
	// the ingestion worker has acknowledged. Delivery is ordered per
	// durable consumer and every message reaches a terminal Postgres
	// disposition before its ack (R3-F3), so the gauge mirrors the
	// Reporting-owned checkpoint boundary in reporting_checkpoints —
	// combined with the stream's last sequence it yields ingestion lag.
	ReportingIngestionCheckpoint = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "reporting_ingestion",
			Name:      "checkpoint_sequence",
			Help:      "Last JetStream stream sequence terminally dispositioned (acked) by the Reporting ingestion worker; mirrors the reporting_checkpoints boundary.",
		},
	)

	// ReportingIngestionDLQDepth is the current size of the
	// Reporting-owned DLQ (reporting_dlq), refreshed after every
	// disposition. Alert on > 0.
	ReportingIngestionDLQDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "reporting_ingestion",
			Name:      "dlq_depth",
			Help:      "Number of rows in the Reporting-owned dead-letter queue (reporting_dlq). Alert on > 0.",
		},
	)
)
