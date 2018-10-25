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

type schedulerReq struct {
	opts  *Options
	state *MasterState
	call  *scheduler.Call

	responseWriter http.ResponseWriter
	request        *http.Request
}

func newStreamID() StreamID {
	streamID, err := uuid.NewUUID()
	if err != nil {
		log.Panicf("Cannot create new stream ID: %s", err)
	}

	return streamID
}

type subscription struct {
	streamID    StreamID
	frameworkID mesos.FrameworkID

	write  chan<- []byte
	closed chan struct{}
}

var streams = make(map[StreamID]subscription)
var subscriptions = make(map[mesos.FrameworkID]subscription)

// Scheduler returns a http.Handler for providing the Mesos Scheduler HTTP API:
// https://mesos.apache.org/documentation/latest/scheduler-http-api/
func Scheduler(opts *Options, state *MasterState) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := schedulerReq{
			opts:           opts,
			state:          state,
			call:           &scheduler.Call{},
			responseWriter: w,
			request:        r,
		}

		err := json.NewDecoder(r.Body).Decode(&req.call)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Failed to parse body into JSON: %s", err)
			return
		}

		err = schedulerCallMux(req)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Failed to validate scheduler::Call: %s", err)
			return
		}
	})
}

func schedulerCallMux(req schedulerReq) error {
	callTypeHandlers := map[scheduler.Call_Type]func(*scheduler.Call, *MasterState, schedulerReq) error{
		scheduler.Call_SUBSCRIBE: subscribe,
		scheduler.Call_DECLINE:   decline,
	}

	if req.call.Type == scheduler.Call_UNKNOWN {
		return fmt.Errorf("expecting 'type' to be present")
	}

	// Invoke handler for different call types
	handler := callTypeHandlers[req.call.Type]
	if handler == nil {
		return fmt.Errorf("handler for '%s' call not implemented", req.call.Type.Enum().String())
	}

	return handler(req.call, req.state, req)
}

func subscribe(call *scheduler.Call, state *MasterState, req schedulerReq) error {
	r := req.request
	w := req.responseWriter

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

	// Initialise framework
	if _, exists := state.Frameworks[*info.ID]; !exists {
		log.Printf("Adding framework %s", info.ID.Value)
		state.NewFramework(info)
	}

	// Subscribe framework
	log.Printf("Subscribing framework '%s'", info.Name)
	write := make(chan []byte)
	sub := subscription{
		streamID:    streamID,
		frameworkID: *info.ID,
		closed:      make(chan struct{}),
		write:       write,
	}
	subscriptions[*info.ID] = sub
	streams[streamID] = sub

	if closeOld != nil {
		log.Printf("Disconnecting old subscription")
		closeOld <- struct{}{}
	}

	// Event consumer, write to HTTP output buffer
	go func() {
		for {
			frame, ok := <-write
			if !ok {
				break
			}

			writer.WriteFrame(frame)
			flusher.Flush()
		}
	}()

	// Mock event producers, as if this is the master of a real Mesos cluster
	go sendHeartbeat(streamID)
	go sendResourceOffers(state, streamID)

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

		// Handle disconnected framework
		state.DisconnectFramework(*info.ID)

		// Send failover event
		failover := &scheduler.Event{
			Type: scheduler.Event_ERROR,
			Error: &scheduler.Event_Error{
				Message: "Framework failed over",
			},
		}
		frame, _ := failover.MarshalJSON()
		writer.WriteFrame(frame)

	case <-r.Context().Done():
		// Subscription was closed by disconnected scheduler connection
		// TODO: Handle deactivation of frameworks and failover timeouts.
		log.Printf("Disconnecting framework %s (%s)", info.ID.Value, info.Name)
		state.DisconnectFramework(*info.ID)

		delete(subscriptions, *info.ID)
	}

	// Clean up stream once closed.
	close(sub.write)
	close(sub.closed)
	delete(streams, streamID)

	return nil
}

func decline(call *scheduler.Call, state *MasterState, req schedulerReq) error {
	framework := state.Frameworks[*call.FrameworkID]
	info := framework.FrameworkInfo

	log.Printf("Processing DECLINE call for offers: %s for framework %s (%s)", call.Decline.OfferIDs,
		info.ID.Value, info.Name)

	for _, offerID := range call.Decline.OfferIDs {
		state.RemoveOffer(*info.ID, offerID)
		log.Printf("Removing offer %s", offerID.Value)
	}

	return nil
}

func sendHeartbeat(streamID StreamID) {
	for {
		if _, exists := streams[streamID]; !exists {
			break
		}

		event := &scheduler.Event{
			Type: scheduler.Event_HEARTBEAT,
		}
		sendEvent(streamID, event)

		time.Sleep(15 * time.Second)
	}
}

func sendResourceOffers(state *MasterState, streamID StreamID) {
	for {
		stream, exists := streams[streamID]
		if !exists {
			break
		}

		framework := state.Frameworks[stream.frameworkID]

		var offersToSend []mesos.Offer

		for _, agentID := range state.AgentIDs {
			if offer := state.NewOffer(*framework.FrameworkInfo.ID, agentID); offer != nil {
				offersToSend = append(offersToSend, *offer)
			}
		}

		if len(offersToSend) > 0 {
			event := &scheduler.Event{
				Type: scheduler.Event_OFFERS,
				Offers: &scheduler.Event_Offers{
					Offers: offersToSend,
				},
			}

			log.Printf("Sending %d offers to framework %s (%s)", len(offersToSend), framework.FrameworkInfo.ID.Value,
				framework.FrameworkInfo.Name)
			sendEvent(streamID, event)
		}

		// Attempt to send resource offers for all agents every second.
		time.Sleep(1 * time.Second)
	}
}

func sendEvent(streamID StreamID, event *scheduler.Event) {
	frame, err := event.MarshalJSON()
	if err != nil {
		log.Panicf("Cannot marshal JSON for %s event: %s", event.Type.String(), err)
	}

	if stream, exists := streams[streamID]; exists {
		stream.write <- frame
	}
}
