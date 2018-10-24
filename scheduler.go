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
	subscribed    bool
	frameworkInfo *mesos.FrameworkInfo
}

func newStreamID() StreamID {
	streamID, err := uuid.NewUUID()
	if err != nil {
		log.Panicf("Cannot create new stream ID: %#v", err)
	}

	return streamID
}

// Create maps for each stream (framework connection).
var streamStates = make(map[StreamID]streamState)
var streamChannels = make(map[StreamID]chan []byte)

// Store local state.
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

	info := call.Subscribe.FrameworkInfo
	frameworkID := info.ID.Value
	frameworkName := info.Name
	log.Printf("Received subscription request for HTTP framework '%s'", frameworkName)

	// Initialise framework
	log.Printf("Adding framework %s", frameworkID)
	streamStates[streamID] = streamState{
		streamID:      streamID,
		subscribed:    true,
		frameworkInfo: info,
	}

	// Initialise channels
	log.Printf("Subscribing framework '%s'", frameworkName)
	streamChannels[streamID] = make(chan []byte)
	closed := make(chan bool)

	go func() {
		for {
			// Stop goroutine once connection has closed.
			state := streamStates[streamID]
			if !state.subscribed {
				closed <- true
			}

			frame := <-streamChannels[streamID]
			writer.WriteFrame(frame)
			flusher.Flush()
		}
	}()

	// Mock event producers, as if this is the master of a real Mesos cluster
	go sendResourceOffers(streamID)

	log.Printf("Added framework %s", frameworkID)

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

	<-closed
	return nil
}

func sendResourceOffers(streamID StreamID) {
	for {
		state := streamStates[streamID]

		// Stop goroutine once framework has ended.
		if !state.subscribed {
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

	streamChannels[streamID] <- frame
}
