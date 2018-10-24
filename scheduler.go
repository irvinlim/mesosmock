package main

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/recordio"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"log"
	"net/http"
	"time"
)

type StreamID = uuid.UUID

type streamState struct {
	streamID      StreamID
	frameworkInfo *mesos.FrameworkInfo

	flusher http.Flusher
	writer  *recordio.Writer
}

func newStreamID() StreamID {
	streamID, err := uuid.NewUUID()
	if err != nil {
		log.Panicf("Cannot create new stream ID: %#v", err)
	}

	return streamID
}

type subscription struct {
	streamID      StreamID
	frameworkInfo mesos.FrameworkInfo
}

// Create maps for each stream (framework connection).
var streamStates = make(map[StreamID]streamState)
var streamClose = make(map[StreamID]chan bool)

// Store local states.
var subscriptions = make(map[mesos.FrameworkID]subscription)
var offers = make(map[mesos.OfferID]mesos.Offer)

// Scheduler returns a http.Handler for providing the Mesos Scheduler HTTP API:
// https://mesos.apache.org/documentation/latest/scheduler-http-api/
func Scheduler(opts *Options) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := &scheduler.Call{}
		err := json.NewDecoder(r.Body).Decode(&call)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Failed to parse body into JSON: %s", err)
			return
		}

		err = callMux(opts, call, w)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Failed to validate scheduler::Call: %#v", err)
			return
		}
	})
}

func callMux(opts *Options, call *scheduler.Call, w http.ResponseWriter) error {
	callTypeHandlers := map[scheduler.Call_Type]func(*Options, *scheduler.Call, http.ResponseWriter) error{
		scheduler.Call_SUBSCRIBE: subscribe,
	}

	if call.Type == scheduler.Call_UNKNOWN {
		return fmt.Errorf("expecting 'type' to be present")
	}

	// Invoke handler for different call types
	handler := callTypeHandlers[call.Type]
	if handler == nil {
		return fmt.Errorf("handler not implemented")
	}

	return handler(opts, call, w)
}

func subscribe(opts *Options, call *scheduler.Call, w http.ResponseWriter) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		panic("expected http.ResponseWriter to be an http.Flusher")
	}

	writer := recordio.NewWriter(w)
	streamID := newStreamID()

	id := call.FrameworkID
	info := call.Subscribe.FrameworkInfo
	log.Printf("Received subscription request for HTTP framework '%s'", info.Name)

	if id != nil {
		if id.Value != info.ID.Value {
			return fmt.Errorf("'framework_id' differs from 'subscribe.framework_info.id'")
		}

		// Check if framework already has an existing subscription, and close it.
		// See https://mesos.apache.org/documentation/latest/scheduler-http-api/#disconnections
		if subscription, exists := subscriptions[*info.ID]; exists {
			if closed, exists := streamClose[subscription.streamID]; exists {
				closed <- true
			}
		}
	}

	// Initialise framework
	log.Printf("Adding framework %s", info.ID.Value)
	streamStates[streamID] = streamState{
		streamID:      streamID,
		frameworkInfo: info,
		flusher:       flusher,
		writer:        writer,
	}

	log.Printf("Subscribing framework '%s'", info.Name)
	subscriptions[*info.ID] = subscription{
		streamID:      streamID,
		frameworkInfo: *info,
	}

	streamClose[streamID] = make(chan bool)

	// Mock event producers, as if this is the master of a real Mesos cluster
	go sendResourceOffers(streamID)

	log.Printf("Added framework %s", info.ID.Value)

	// Send headers
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Mesos-Stream-Id", streamID.String())
	w.WriteHeader(http.StatusOK)

	// Create SUBSCRIBED event
	heartbeat := float64(15)
	event := &scheduler.Event{
		Type: scheduler.Event_SUBSCRIBED,
		Subscribed: &scheduler.Event_Subscribed{
			FrameworkID:              info.ID,
			HeartbeatIntervalSeconds: &heartbeat,
		},
	}
	sendEvent(streamID, event)

	// TODO: Handle disconnections and unsubscribe stream.

	<-streamClose[streamID]

	// Send failover error.
	failover := &scheduler.Event{
		Type: scheduler.Event_ERROR,
		Error: &scheduler.Event_Error{
			Message: "Framework failed over",
		},
	}
	sendEvent(streamID, failover)

	// Clean up stream once closed.
	delete(streamStates, streamID)
	close(streamClose[streamID])
	delete(streamClose, streamID)

	return nil
}

func sendResourceOffers(streamID StreamID) {
	for {
		state, exists := streamStates[streamID]
		if !exists {
			break
		}

		var offersToSend []mesos.Offer
		for i := 0; i < 1; i++ {
			offerID := mesos.OfferID{Value: uuid.New().String()}
			offer := mesos.Offer{
				ID:          offerID,
				AgentID:     mesos.AgentID{Value: uuid.New().String()},
				FrameworkID: *state.frameworkInfo.ID,
			}

			offers[offerID] = offer
			offersToSend = append(offersToSend, offer)
		}

		event := &scheduler.Event{
			Type: scheduler.Event_OFFERS,
			Offers: &scheduler.Event_Offers{
				Offers: offersToSend,
			},
		}

		log.Printf("Sending 2 offers to framework %s (%s)", state.frameworkInfo.ID.Value, state.frameworkInfo.Name)
		sendEvent(streamID, event)
		time.Sleep(5 * time.Second)
	}
}

func sendEvent(streamID StreamID, event *scheduler.Event) {
	frame, err := event.MarshalJSON()
	if err != nil {
		log.Panicf("Cannot marshal JSON for %s event: %#v", event.Type.String(), err)
	}

	if state, exists := streamStates[streamID]; exists {
		state.writer.WriteFrame(frame)
		state.flusher.Flush()
	}
}
