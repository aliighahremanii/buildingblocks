// Package events defines the application-level publisher port for domain
// events. The domain records events; the application publishes them.
//
// The registered publisher is NoOpPublisher (logs at debug, swallows errors).
// The actual publication mechanism today is the KurrentDB $all subscription
// consumed by projection workers; this publisher seam exists for future
// in-process publication paths.
package events

import (
	"context"
	"log/slog"

	domainevents "github.com/aliighahremanii/buildingblocks/domain/events"
)

// Publisher forwards domain events to whatever downstream sink is configured
// (nothing in phase 1; outbox in phase 2).
type Publisher interface {
	Publish(ctx context.Context, events []domainevents.Event) error
}

// NoOpPublisher is the phase-1 default. It logs the events at debug level and
// returns nil, so commands succeed without any outbox infrastructure.
type NoOpPublisher struct{}

// NewNoOpPublisher returns a NoOpPublisher.
func NewNoOpPublisher() NoOpPublisher { return NoOpPublisher{} }

// Publish logs each event at debug level and returns nil.
func (NoOpPublisher) Publish(_ context.Context, events []domainevents.Event) error {
	for _, e := range events {
		slog.Debug("domain event",
			"event", e.EventName(),
			"aggregateType", e.AggregateType(),
			"aggregateId", e.AggregateID(),
			"occurredAt", e.OccurredAt().Format("2006-01-02T15:04:05.000Z07:00"),
		)
	}
	return nil
}
