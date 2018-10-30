package stream

import (
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// ID is a unique identifier for a stream.
type ID = uuid.UUID

// NewStreamID creates a new stream identifier.
func NewStreamID() ID {
	streamID, err := uuid.NewUUID()
	if err != nil {
		log.Panicf("Cannot create new stream ID: %s", err)
	}

	return streamID
}
