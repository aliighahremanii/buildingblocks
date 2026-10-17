package commands

import (
	"context"

	publevents "github.com/aliighahremanii/buildingblocks/application/events"
	domainevents "github.com/aliighahremanii/buildingblocks/domain/events"
)

// EventSource is implemented by a command Result that carries an aggregate (or
// any value) whose recorded domain events should be published after a
// successful Save.
//
// Each vertical-slice command Result embeds or wraps the saved aggregate and
// implements this interface so the middleware can extract events generically
// without reflection.
type EventSource interface {
	PullDomainEvents() []domainevents.Event
}

// PublishDomainEvents is a command middleware that, after the inner handler
// succeeds, pulls domain events from the result (if it implements EventSource)
// and publishes them via the configured Publisher.
//
// In phase 1 the publisher is a NoOp, so this is a transparent pass-through
// that validates the plumbing. In phase 2 the publisher will be an outbox
// writer composed inside commands.Transaction so events are persisted
// atomically with the aggregate state.
func PublishDomainEvents[C any, R any](publisher publevents.Publisher) Middleware[C, R] {
	return func(next Handler[C, R]) Handler[C, R] {
		return HandlerFunc[C, R](func(ctx context.Context, command C) (R, error) {
			result, err := next.Handle(ctx, command)
			if err != nil {
				return result, err
			}

			if source, ok := any(result).(EventSource); ok {
				events := source.PullDomainEvents()
				if len(events) > 0 {
					if pubErr := publisher.Publish(ctx, events); pubErr != nil {
						return result, pubErr
					}
				}
			}

			return result, nil
		})
	}
}
