package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/irvinlim/mesosmock/pkg/config"
	"github.com/irvinlim/mesosmock/pkg/emulation"
	"github.com/irvinlim/mesosmock/pkg/state"
	"github.com/irvinlim/mesosmock/pkg/stream"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	log "github.com/sirupsen/logrus"
)

type schedulerSubscription struct {
	streamID    stream.ID
	frameworkID mesos.FrameworkID

	taskEmulation *emulation.TaskEmulation

	write  chan<- []byte
	closed chan struct{}
}

var schedulerSubscriptions = make(map[mesos.FrameworkID]schedulerSubscription)

// Scheduler returns a http.Handler for providing the Mesos Scheduler HTTP API:
// https://mesos.apache.org/documentation/latest/scheduler-http-api/
func Scheduler(opts *config.Options, st *state.MasterState) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := &scheduler.Call{}

		err := json.NewDecoder(r.Body).Decode(&call)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Failed to parse body into JSON: %s", err)
			return
		}

		err = schedulerCallMux(opts, call, st, w, r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Failed to validate scheduler::Call: %s", err)
			return
		}
	})
}

func schedulerCallMux(opts *config.Options, call *scheduler.Call, st *state.MasterState, w http.ResponseWriter, r *http.Request) error {
	if call.Type == scheduler.Call_UNKNOWN {
		return fmt.Errorf("expecting 'type' to be present")
	}

	// Handle SUBSCRIBE calls differently
	if call.Type == scheduler.Call_SUBSCRIBE {
		return schedulerSubscribe(opts, call, st, w, r)
	}

	// Invoke handler for different call types
	callTypeHandlers := map[scheduler.Call_Type]func(*scheduler.Call, *state.MasterState) (*scheduler.Response, error){
		scheduler.Call_DECLINE: decline,
		scheduler.Call_ACCEPT:  accept,
	}

	handler := callTypeHandlers[call.Type]
	if handler == nil {
		return fmt.Errorf("handler for '%s' call not implemented", call.Type.Enum().String())
	}

	res, err := handler(call, st)
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

func schedulerSubscribe(opts *config.Options, call *scheduler.Call, st *state.MasterState, w http.ResponseWriter, r *http.Request) error {
	var closeOld chan struct{}
	streamID := stream.NewStreamID()

	// Validate SUBSCRIBE call
	if call.Subscribe == nil || call.Subscribe.FrameworkInfo == nil {
		return fmt.Errorf("missing required fields: subscribe.framework_info")
	}

	info := call.Subscribe.FrameworkInfo
	if info.User == "" || info.Name == "" {
		return fmt.Errorf("missing required fields: subscribe.framework_info.user, subscribe.framework_info.name")
	}

	log.Infof("Received subscription request for HTTP framework '%s'", info.Name)

	// Generate a random framework ID if not specified
	if info.ID == nil {
		newID := &mesos.FrameworkID{Value: fmt.Sprintf("%s-0000", uuid.New().String())}
		info.ID = newID
		if call.FrameworkID == nil {
			call.FrameworkID = newID
		}
	}

	id := call.FrameworkID
	if id == nil || id.Value != info.ID.Value {
		return fmt.Errorf("'framework_id' differs from 'subscribe.framework_info.id'")
	}

	// Check if framework already has an existing subscription, and close it.
	// See https://mesos.apache.org/documentation/latest/scheduler-http-api/#disconnections
	if subscription, exists := schedulerSubscriptions[*info.ID]; exists {
		closeOld = subscription.closed
	}

	// Initialise framework
	if _, exists := st.GetFramework(*info.ID); !exists {
		log.Infof("Adding framework %s", info.ID.Value)
		st.NewFramework(info)
	}

	// Subscribe framework
	log.Infof("Subscribing framework '%s'", info.Name)
	write := make(chan []byte)
	sub := schedulerSubscription{
		streamID:      streamID,
		frameworkID:   *info.ID,
		taskEmulation: emulation.NewTaskEmulation(opts.Emulation, st),
		closed:        make(chan struct{}),
		write:         write,
	}
	schedulerSubscriptions[*info.ID] = sub

	if closeOld != nil {
		log.Infof("Disconnecting old subscription")
		closeOld <- struct{}{}
	}

	log.Infof("Added framework %s", info.ID.Value)

	// Begin writing to HTTP output buffer
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Mesos-Stream-Id", streamID.String())
	w.WriteHeader(http.StatusOK)

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

	// Create SUBSCRIBED event
	heartbeat := float64(15)
	event := &scheduler.Event{
		Type: scheduler.Event_SUBSCRIBED,
		Subscribed: &scheduler.Event_Subscribed{
			FrameworkID:              info.ID,
			HeartbeatIntervalSeconds: &heartbeat,
		},
	}
	sub.sendEvent(event)

	// Emulate task updates
	go sub.taskEmulation.EmulateTasks(ctx, *info.ID)

	// Mock event producers, as if this is the master of a real Mesos cluster
	go sub.sendHeartbeat(ctx)
	go sub.sendResourceOffers(ctx, st)
	go sub.sendStatusUpdates(ctx, st)

	// Block until subscription is closed either by the current client, or by another request.
	select {
	case <-sub.closed:
		// Subscription was closed by another subscription
		log.Infof("Ignoring disconnection for framework %s (%s) as it has already reconnected", info.ID.Value,
			info.Name)

		// Handle disconnected framework
		st.DisconnectFramework(*info.ID)

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
		log.Infof("Disconnecting framework %s (%s)", info.ID.Value, info.Name)
		st.DisconnectFramework(*info.ID)

		delete(schedulerSubscriptions, *info.ID)
	}

	close(sub.closed)

	return nil
}

func decline(call *scheduler.Call, st *state.MasterState) (*scheduler.Response, error) {
	framework, exists := st.GetFramework(*call.FrameworkID)
	if !exists {
		return nil, fmt.Errorf("framework does not exist: %s", call.FrameworkID.Value)
	}

	info := framework.FrameworkInfo

	log.Debugf("Processing DECLINE call for offers: %s for framework %s (%s)", call.Decline.OfferIDs,
		info.ID.Value, info.Name)

	for _, offerID := range call.Decline.OfferIDs {
		// Refuse seconds defaults to 5 seconds:
		// https://github.com/apache/mesos/blob/5b8f632e75d3c20be172c1678c04f77ae18cda1a/include/mesos/mesos.proto#L2577
		refuseDuration := time.Duration(5 * time.Second)
		if call.Decline.Filters.RefuseSeconds != nil {
			refuseDuration = time.Duration(*call.Decline.Filters.RefuseSeconds * float64(time.Second))
		}

		st.RemoveOffer(*info.ID, offerID, refuseDuration)
		log.Debugf("Removing offer %s", offerID.Value)
	}

	return nil, nil
}

func accept(call *scheduler.Call, st *state.MasterState) (*scheduler.Response, error) {
	framework, exists := st.GetFramework(*call.FrameworkID)
	if !exists {
		return nil, fmt.Errorf("framework does not exist: %s", call.FrameworkID.Value)
	}

	info := framework.FrameworkInfo

	subscription, exists := schedulerSubscriptions[*info.ID]
	if !exists {
		return nil, fmt.Errorf("framework is not subscribed: %s", call.FrameworkID.Value)
	}

	log.Debugf("Processing ACCEPT call for offers: %s for framework %s (%s)", call.Accept.OfferIDs,
		info.ID.Value, info.Name)

	for _, offerID := range call.Accept.OfferIDs {
		// Refuse seconds defaults to 5 seconds:
		// https://github.com/apache/mesos/blob/5b8f632e75d3c20be172c1678c04f77ae18cda1a/include/mesos/mesos.proto#L2577
		refuseDuration := time.Duration(5 * time.Second)
		if call.Accept.Filters.RefuseSeconds != nil {
			refuseDuration = time.Duration(*call.Accept.Filters.RefuseSeconds * float64(time.Second))
		}

		st.RemoveOffer(*info.ID, offerID, refuseDuration)
		log.Debugf("Removing offer %s", offerID.Value)
	}

	// Only LAUNCH is supported at the moment
	for _, operation := range call.Accept.Operations {
		if operation.Type != mesos.Offer_Operation_LAUNCH {
			return nil, fmt.Errorf("operation '%s' is not yet supported by mesosmock", operation.Type.String())
		}
	}

	for _, operation := range call.Accept.Operations {
		for _, taskInfo := range operation.Launch.TaskInfos {
			task := st.NewTask(*info.ID, taskInfo)

			log.Debugf("Launching task %s", taskInfo.TaskID.String())
			subscription.taskEmulation.CreateTask <- task
		}
	}

	return nil, nil
}

func (s schedulerSubscription) sendHeartbeat(ctx context.Context) {
	event := &scheduler.Event{Type: scheduler.Event_HEARTBEAT}

	for {
		s.sendEvent(event)

		select {
		case <-ctx.Done():
			return
		case <-time.After(15 * time.Second):
		}
	}
}

func (s schedulerSubscription) sendResourceOffers(ctx context.Context, st *state.MasterState) {
	for {
		framework, exists := st.GetFramework(s.frameworkID)
		if !exists {
			return
		}

		var offersToSend []mesos.Offer

		for _, agentID := range st.GetAgentIDs() {
			if offer := st.NewOffer(*framework.FrameworkInfo.ID, agentID); offer != nil {
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

			log.Debugf("Sending %d offers to framework %s (%s)", len(offersToSend),
				framework.FrameworkInfo.ID.Value, framework.FrameworkInfo.Name)
			s.sendEvent(event)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
	}
}

func (s schedulerSubscription) sendStatusUpdates(ctx context.Context, st *state.MasterState) {
	for {
		select {
		case <-ctx.Done():
			return
		case status := <-s.taskEmulation.GetStatus:
			framework, exists := st.GetFramework(s.frameworkID)
			if !exists {
				return
			}

			event := &scheduler.Event{
				Type: scheduler.Event_UPDATE,
				Update: &scheduler.Event_Update{
					Status: status,
				},
			}

			log.Debugf("Sending %s update for task '%s' to framework %s (%s)", status.State, status.TaskID,
				framework.FrameworkInfo.ID.Value, framework.FrameworkInfo.Name)
			s.sendEvent(event)
		}
	}
}

func (s schedulerSubscription) sendEvent(event *scheduler.Event) {
	log.Tracef("Sending %s event to framework %s", event.Type, s.frameworkID.Value)

	frame, err := event.MarshalJSON()
	if err != nil {
		log.Panicf("Cannot marshal JSON for %s event: %s", event.Type.String(), err)
	}
	s.write <- frame
}
