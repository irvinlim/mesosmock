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

	write chan<- []byte
}

func newStreamID() StreamID {
	streamID, err := uuid.NewUUID()
	if err != nil {
		log.Panicf("Cannot create new stream ID: %#v", err)
	}

	return streamID
}

type subscription struct {
	streamID StreamID
	closed   chan struct{}
}

// Create maps for each stream (framework connection).
var streamStates = make(map[StreamID]streamState)

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

		err = callMux(opts, call, w, r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Failed to validate scheduler::Call: %#v", err)
			return
		}
	})
}

func callMux(opts *Options, call *scheduler.Call, w http.ResponseWriter, r *http.Request) error {
	callTypeHandlers := map[scheduler.Call_Type]func(*Options, *scheduler.Call, http.ResponseWriter, *http.Request) error{
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

	return handler(opts, call, w, r)
}

func subscribe(opts *Options, call *scheduler.Call, w http.ResponseWriter, r *http.Request) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		panic("expected http.ResponseWriter to be an http.Flusher")
	}

	writer := recordio.NewWriter(w)
	streamID := newStreamID()

	id := call.FrameworkID
	info := call.Subscribe.FrameworkInfo
	log.Printf("Received subscription request for HTTP framework '%s'", info.Name)

	var closeOld chan struct{}
	if id != nil {
		if id.Value != info.ID.Value {
			return fmt.Errorf("'framework_id' differs from 'subscribe.framework_info.id'")
		}

		// Check if framework already has an existing subscription, and close it.
		// See https://mesos.apache.org/documentation/latest/scheduler-http-api/#disconnections
		if subscription, exists := subscriptions[*info.ID]; exists {
			closeOld = subscription.closed
		}
	}

	// Initialise subscription
	log.Printf("Adding framework %s", info.ID.Value)
	write := make(chan []byte)
	streamStates[streamID] = streamState{
		streamID:      streamID,
		frameworkInfo: info,
		write:         write,
	}

	log.Printf("Subscribing framework '%s'", info.Name)
	sub := subscription{
		streamID: streamID,
		closed:   make(chan struct{}),
	}
	subscriptions[*info.ID] = sub

	if closeOld != nil {
		log.Printf("Disconnecting old subscription")
		closeOld <- struct{}{}
	}

	// Event consumer, write to HTTP output buffer
	go func() {
		for {
			if _, exists := streamStates[streamID]; !exists {
				break
			}

			frame := <-write
			writer.WriteFrame(frame)
			flusher.Flush()
		}
	}()

	// Mock event producers, as if this is the master of a real Mesos cluster
	go sendHeartbeat(streamID)
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

	// Block until subscription is closed in either direction.
	select {

	case <-sub.closed:
		// Subscription was closed by another subscription
		log.Printf("Ignoring disconnection for framework %s (%s) as it has already reconnected", info.ID.Value,
			info.Name)
		failover := &scheduler.Event{
			Type: scheduler.Event_ERROR,
			Error: &scheduler.Event_Error{
				Message: "Framework failed over",
			},
		}
		sendEvent(streamID, failover)

	case <-r.Context().Done():
		// Subscription was closed by disconnected scheduler connection
		// TODO: Handle deactivation of frameworks and failover timeouts.
		log.Printf("Disconnecting framework %s (%s)", info.ID.Value, info.Name)
		delete(subscriptions, *info.ID)
	}

	// Clean up stream once closed.
	delete(streamStates, streamID)

	return nil
}

func sendHeartbeat(streamID StreamID) {
	for {
		if _, exists := streamStates[streamID]; !exists {
			break
		}

		event := &scheduler.Event{
			Type: scheduler.Event_HEARTBEAT,
		}
		sendEvent(streamID, event)

		time.Sleep(15 * time.Second)
	}
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

		log.Printf("Sending %d offers to framework %s (%s)", len(offersToSend), state.frameworkInfo.ID.Value,
			state.frameworkInfo.Name)
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
		state.write <- frame
	}
}
