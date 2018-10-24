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

type frameworkState struct {
	subscribed bool
	streamID   string
}

var frameworkStates = make(map[string]frameworkState)
var frameworkChans = make(map[string]chan []byte)
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
	streamID, err := uuid.NewUUID()
	if err != nil {
		log.Panicf("Cannot create stream ID for SUBSCRIBE call: %#v", err)
	}

	frameworkID := call.Subscribe.FrameworkInfo.ID.Value
	frameworkName := call.Subscribe.FrameworkInfo.Name
	log.Printf("Received subscription request for HTTP framework '%s'", frameworkName)

	// Initialise framework
	log.Printf("Adding framework %s", frameworkID)
	frameworkStates[frameworkID] = frameworkState{
		subscribed: true,
		streamID:   streamID.String(),
	}

	// Initialise channels
	log.Printf("Subscribing framework '%s'", frameworkName)
	frameworkChans[frameworkID] = make(chan []byte)
	closed := make(chan bool)

	go func() {
		for {
			// Stop goroutine once framework has ended.
			state := frameworkStates[frameworkID]
			if !state.subscribed {
				closed <- true
			}

			frame := <-frameworkChans[frameworkID]
			writer.WriteFrame(frame)
			flusher.Flush()
		}
	}()

	// Mock event producers, as if this is the master of a real Mesos cluster
	go sendResourceOffers(frameworkID)

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
			FrameworkID:              call.Subscribe.FrameworkInfo.ID,
			HeartbeatIntervalSeconds: &heartbeat,
		},
	}
	sendEvent(frameworkID, event)

	<-closed
	return nil
}

func sendResourceOffers(frameworkID string) {
	for {
		state := frameworkStates[frameworkID]

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
				FrameworkID: mesos.FrameworkID{Value: frameworkID},
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

		sendEvent(frameworkID, event)
		time.Sleep(5 * time.Second)
	}
}

func sendEvent(frameworkID string, event *scheduler.Event) {
	frame, err := event.MarshalJSON()
	if err != nil {
		log.Panicf("Cannot marshal JSON for %s event: %#v", event.Type.String(), err)
	}

	frameworkChans[frameworkID] <- frame
}
