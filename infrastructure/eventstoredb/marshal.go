package eventstoredb

import (
	"encoding/json"
	"fmt"

	domainevents "github.com/aliighahremanii/buildingblocks/domain/events"

	"github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

func StreamName(aggregateType, aggregateID string) string {
	return fmt.Sprintf("%s-%s", aggregateType, aggregateID)
}

func ToEventData(e domainevents.Event) (kurrentdb.EventData, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return kurrentdb.EventData{}, fmt.Errorf("marshal event %s: %w", e.EventName(), err)
	}
	return kurrentdb.EventData{
		EventID:     uuid.New(),
		EventType:   e.EventName(),
		ContentType: kurrentdb.ContentTypeJson,
		Data:        data,
	}, nil
}

type EventMetadata struct {
	CorrelationID string `json:"correlationId,omitempty"`
	CausationID   string `json:"causationId,omitempty"`
	UserID        string `json:"userId,omitempty"`
}

func ToEventDataWithMetadata(e domainevents.Event, meta EventMetadata) (kurrentdb.EventData, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return kurrentdb.EventData{}, fmt.Errorf("marshal event %s: %w", e.EventName(), err)
	}
	var metaData []byte
	if meta != (EventMetadata{}) {
		metaData, err = json.Marshal(meta)
		if err != nil {
			return kurrentdb.EventData{}, fmt.Errorf("marshal metadata: %w", err)
		}
	}
	return kurrentdb.EventData{
		EventID:     uuid.New(),
		EventType:   e.EventName(),
		ContentType: kurrentdb.ContentTypeJson,
		Data:        data,
		Metadata:    metaData,
	}, nil
}
