package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/irvinlim/mesosmock/internal/pkg/stream"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

type schedulerSubscription struct {
	streamID    stream.ID
	frameworkID mesos.FrameworkID

	write  chan<- []byte
	closed chan struct{}
}

var schedulerSubscriptions = make(map[mesos.FrameworkID]schedulerSubscription)

// Scheduler returns a http.Handler for providing the Mesos Scheduler HTTP API:
// https://mesos.apache.org/documentation/latest/scheduler-http-api/
func Scheduler(state *MasterState) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := &scheduler.Call{}

		err := json.NewDecoder(r.Body).Decode(&call)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Failed to parse body into JSON: %s", err)
			return
		}

		err = schedulerCallMux(call, state, w, r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Failed to validate scheduler::Call: %s", err)
			return
		}
	})
}

func schedulerCallMux(call *scheduler.Call, state *MasterState, w http.ResponseWriter, r *http.Request) error {
	if call.Type == scheduler.Call_UNKNOWN {
		return fmt.Errorf("expecting 'type' to be present")
	}

	// Handle SUBSCRIBE calls differently
	if call.Type == scheduler.Call_SUBSCRIBE {
		return schedulerSubscribe(call, state, w, r)
	}

	// Invoke handler for different call types
	callTypeHandlers := map[scheduler.Call_Type]func(*scheduler.Call, *MasterState) (*scheduler.Response, error){
		scheduler.Call_DECLINE: decline,
	}

	handler := callTypeHandlers[call.Type]
	if handler == nil {
		return fmt.Errorf("handler for '%s' call not implemented", call.Type.Enum().String())
	}

	res, err := handler(call, state)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	// Handle empty responses
	if res == nil {
		return nil
	}

	// Convert response to JSON body
	body, err := res.MarshalJSON()
	if err != nil {
		log.Panicf("Cannot marshal JSON for master response: %s", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Write(body)

	return nil
}

func schedulerSubscribe(call *scheduler.Call, state *MasterState, w http.ResponseWriter, r *http.Request) error {
	streamID := stream.NewStreamID()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Mesos-Stream-Id", streamID.String())
	w.WriteHeader(http.StatusOK)

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
		if subscription, exists := schedulerSubscriptions[*info.ID]; exists {
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
	sub := schedulerSubscription{
		streamID:    streamID,
		frameworkID: *info.ID,
		closed:      make(chan struct{}),
		write:       write,
	}
	schedulerSubscriptions[*info.ID] = sub

	if closeOld != nil {
		log.Printf("Disconnecting old subscription")
		closeOld <- struct{}{}
	}

	log.Printf("Added framework %s", info.ID.Value)

	ctx := r.Context()
	writer := stream.NewWriter(w).WithContext(ctx)

	// Event consumer, write to HTTP output buffer
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case frame, ok := <-write:
				if !ok {
					return
				}

				writer.WriteFrame(frame)
			}
		}
	}()

	// Mock event producers, as if this is the master of a real Mesos cluster
	go sub.sendHeartbeat(streamID)
	go sub.sendResourceOffers(state, streamID)

	// Create SUBSCRIBED event
	heartbeat := float64(15)
	event := &scheduler.Event{
		Type: scheduler.Event_SUBSCRIBED,
		Subscribed: &scheduler.Event_Subscribed{
			FrameworkID:              info.ID,
			HeartbeatIntervalSeconds: &heartbeat,
		},
	}
	sub.sendEvent(streamID, event)

	// Block until subscription is closed either by the current client, or by another request.
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

		delete(schedulerSubscriptions, *info.ID)
	}

	close(sub.closed)

	return nil
}

func decline(call *scheduler.Call, state *MasterState) (*scheduler.Response, error) {
	framework := state.Frameworks[*call.FrameworkID]
	info := framework.FrameworkInfo

	log.Printf("Processing DECLINE call for offers: %s for framework %s (%s)", call.Decline.OfferIDs,
		info.ID.Value, info.Name)

	for _, offerID := range call.Decline.OfferIDs {
		state.RemoveOffer(*info.ID, offerID)
		log.Printf("Removing offer %s", offerID.Value)
	}

	return nil, nil
}

func (s schedulerSubscription) sendHeartbeat(streamID stream.ID) {
	for {
		event := &scheduler.Event{
			Type: scheduler.Event_HEARTBEAT,
		}
		s.sendEvent(streamID, event)

		time.Sleep(15 * time.Second)
	}
}

func (s schedulerSubscription) sendResourceOffers(state *MasterState, streamID stream.ID) {
	for {
		framework := state.Frameworks[s.frameworkID]

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
			s.sendEvent(streamID, event)
		}

		// Attempt to send resource offers for all agents every second.
		time.Sleep(1 * time.Second)
	}
}

func (s schedulerSubscription) sendEvent(streamID stream.ID, event *scheduler.Event) {
	frame, err := event.MarshalJSON()
	if err != nil {
		log.Panicf("Cannot marshal JSON for %s event: %s", event.Type.String(), err)
	}
	s.write <- frame
}
