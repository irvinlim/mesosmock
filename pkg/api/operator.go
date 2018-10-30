package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/irvinlim/mesosmock/pkg/state"
	"github.com/irvinlim/mesosmock/pkg/stream"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/master"
	log "github.com/sirupsen/logrus"
)

type operatorSubscription struct {
	streamID   stream.ID
	writeFrame chan<- []byte
}

func Operator(st *state.MasterState) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := &master.Call{}
		err := json.NewDecoder(r.Body).Decode(&call)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Failed to parse body into JSON: %s", err)
			return
		}

		if err := operatorCallMux(st, call, w, r); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Failed to validate master::Call: %s", err)
		}
	})
}

func operatorCallMux(st *state.MasterState, call *master.Call, w http.ResponseWriter, r *http.Request) error {
	if call.Type == master.Call_UNKNOWN {
		return fmt.Errorf("expecting 'type' to be present")
	}

	log.Infof("Processing call %s", call.Type.Enum().String())

	// Handle SUBSCRIBE calls differently
	if call.Type == master.Call_SUBSCRIBE {
		return operatorSubscribe(st, call, w, r)
	}

	// Invoke handler for different call types
	callTypeHandlers := map[master.Call_Type]func(*master.Call, *state.MasterState) *master.Response{
		master.Call_GET_STATE:  getState,
		master.Call_GET_AGENTS: getAgents,
		master.Call_GET_TASKS:  getTasks,
	}

	handler := callTypeHandlers[call.Type]
	if handler == nil {
		return fmt.Errorf("handler for '%s' call not implemented", call.Type.Enum().String())
	}

	res := handler(call, st)
	body, err := res.MarshalJSON()
	if err != nil {
		log.Panicf("Cannot marshal JSON for master response: %s", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	w.Write(body)

	return nil
}

func operatorSubscribe(st *state.MasterState, call *master.Call, w http.ResponseWriter, r *http.Request) error {
	streamID := stream.NewStreamID()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Create subscription
	readFrame := make(chan []byte)
	sub := operatorSubscription{
		streamID:   streamID,
		writeFrame: readFrame,
	}

	// Add subscription
	log.Infof("Added subscriber %s from the list of active subscribers", streamID)

	ctx := r.Context()
	writer := stream.NewWriter(w).WithContext(ctx)

	// Event consumer, write to HTTP output buffer
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case frame, ok := <-readFrame:
				if !ok {
					return
				}

				writer.WriteFrame(frame)
			}
		}
	}()

	// Create SUBSCRIBED event
	heartbeat := float64(15)
	event := &master.Event{
		Type: master.Event_SUBSCRIBED,
		Subscribed: &master.Event_Subscribed{
			GetState:                 getState(call, st).GetState,
			HeartbeatIntervalSeconds: &heartbeat,
		},
	}
	sub.sendEvent(event)

	// Mock event producers, as if this is the master of a real Mesos cluster
	go sub.sendHeartbeat(ctx)

	// Automatically cancels all downstream Contexts if request is cancelled
	<-ctx.Done()

	log.Infof("Removed subscriber %s from the list of active subscribers", streamID)

	return nil
}

func (s operatorSubscription) sendHeartbeat(ctx context.Context) {
	event := &master.Event{Type: master.Event_HEARTBEAT}
	for {
		s.sendEvent(event)

		select {
		case <-ctx.Done():
			return
		case <-time.After(15 * time.Second):
		}
	}
}

func (s operatorSubscription) sendEvent(event *master.Event) {
	frame, err := event.MarshalJSON()
	if err != nil {
		log.Panicf("Cannot marshal JSON for %s event: %s", event.Type.String(), err)
	}
	s.writeFrame <- frame
}

func getState(call *master.Call, st *state.MasterState) *master.Response {
	res := &master.Response{
		Type: master.Response_GET_STATE,
		GetState: &master.Response_GetState{
			GetAgents: getAgents(call, st).GetAgents,
			GetTasks:  getTasks(call, st).GetTasks,
		},
	}

	return res
}

func getAgents(call *master.Call, st *state.MasterState) *master.Response {
	res := &master.Response{
		Type: master.Response_GET_AGENTS,
		GetAgents: &master.Response_GetAgents{
			Agents: st.GetAgents(),
		},
	}

	return res
}

func getTasks(call *master.Call, st *state.MasterState) *master.Response {
	var tasks []mesos.Task
	for _, task := range st.GetTasks() {
		tasks = append(tasks, task)
	}

	if len(tasks) == 0 {
		tasks = []mesos.Task{}
	}

	res := &master.Response{
		Type: master.Response_GET_TASKS,
		GetTasks: &master.Response_GetTasks{
			Tasks:            tasks,
			CompletedTasks:   []mesos.Task{},
			OrphanTasks:      []mesos.Task{},
			PendingTasks:     []mesos.Task{},
			UnreachableTasks: []mesos.Task{},
		},
	}

	return res
}
