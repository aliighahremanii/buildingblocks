package events_test

import (
	"context"
	"testing"
	"time"

	publevents "github.com/aliighahremanii/buildingblocks/application/events"
	domainevents "github.com/aliighahremanii/buildingblocks/domain/events"
)

type fakeEvent struct{ domainevents.Base }

func TestNoOpPublisherReturnsNil(t *testing.T) {
	t.Parallel()

	p := publevents.NewNoOpPublisher()

	// Non-empty event slice.
	err := p.Publish(context.Background(), []domainevents.Event{
		fakeEvent{domainevents.NewBase("country.registered", "uuid", "country", time.Now())},
	})
	if err != nil {
		t.Fatalf("NoOpPublisher.Publish returned error: %v", err)
	}

	// Empty / nil slice must also be fine.
	if err := p.Publish(context.Background(), nil); err != nil {
		t.Fatalf("NoOpPublisher.Publish(nil) returned error: %v", err)
	}
}
