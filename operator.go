package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/irvinlim/mesosmock/internal/pkg/stream"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/master"
	"log"
	"net/http"
	"strconv"
	"time"
)

type operatorSubscription struct {
	streamID   stream.ID
	writeFrame chan<- []byte
}

func Operator(state *MasterState) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := &master.Call{}
		err := json.NewDecoder(r.Body).Decode(&call)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Failed to parse body into JSON: %s", err)
			return
		}

		if err := operatorCallMux(state, call, w, r); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Failed to validate master::Call: %s", err)
		}
	})
}

func operatorCallMux(state *MasterState, call *master.Call, w http.ResponseWriter, r *http.Request) error {
	if call.Type == master.Call_UNKNOWN {
		return fmt.Errorf("expecting 'type' to be present")
	}

	log.Printf("Processing call %s", call.Type.Enum().String())

	// Handle SUBSCRIBE calls differently
	if call.Type == master.Call_SUBSCRIBE {
		return operatorSubscribe(w, r)
	}

	// Invoke handler for different call types
	callTypeHandlers := map[master.Call_Type]func(*master.Call, *MasterState) *master.Response{
		master.Call_GET_AGENTS: getAgents,
		master.Call_GET_TASKS:  getTasks,
	}

	handler := callTypeHandlers[call.Type]
	if handler == nil {
		return fmt.Errorf("handler for '%s' call not implemented", call.Type.Enum().String())
	}

	res := handler(call, state)
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

func operatorSubscribe(w http.ResponseWriter, r *http.Request) error {
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
	log.Printf("Added subscriber %s from the list of active subscribers", streamID)

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
			HeartbeatIntervalSeconds: &heartbeat,
		},
	}
	sub.sendEvent(event)

	// Mock event producers, as if this is the master of a real Mesos cluster
	go sub.sendHeartbeat(ctx)

	// Automatically cancels all downstream Contexts if request is cancelled
	<-ctx.Done()

	log.Printf("Removed subscriber %s from the list of active subscribers", streamID)

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

func getAgents(call *master.Call, state *MasterState) *master.Response {
	var agents []master.Response_GetAgents_Agent

	for i, agentID := range state.AgentIDs {
		port := int32(5051)
		pid := fmt.Sprintf("slave(1)@%s:%d", *state.MasterInfo.Address.IP, port)

		agent := master.Response_GetAgents_Agent{
			AgentInfo: mesos.AgentInfo{
				ID:       &agentID,
				Port:     &port,
				Hostname: fmt.Sprintf("mesos-slave-%d", i),
			},
			PID:    &pid,
			Active: true,
		}
		agents = append(agents, agent)
	}

	res := &master.Response{
		Type: master.Response_GET_AGENTS,
		GetAgents: &master.Response_GetAgents{
			Agents: agents,
		},
	}

	return res
}

func getTasks(call *master.Call, state *MasterState) *master.Response {
	var tasks []mesos.Task
	for _, task := range state.GetTasks() {
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
