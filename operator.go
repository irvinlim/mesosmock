package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/master"
)

func Operator(state *MasterState) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := &master.Call{}
		err := json.NewDecoder(r.Body).Decode(&call)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Failed to parse body into JSON: %s", err)
			return
		}

		res, err := operatorCallMux(state, call)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Failed to validate master::Call: %#v", err)
			return
		}

		body, err := res.MarshalJSON()
		if err != nil {
			log.Panicf("Cannot marshal JSON for master response: %#v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	})
}

func operatorCallMux(state *MasterState, call *master.Call) (*master.Response, error) {
	callTypeHandlers := map[master.Call_Type]func(*master.Call, *MasterState) (*master.Response, error){
		master.Call_GET_AGENTS: getAgents,
	}

	if call.Type == master.Call_UNKNOWN {
		return nil, fmt.Errorf("expecting 'type' to be present")
	}

	// Invoke handler for different call types
	handler := callTypeHandlers[call.Type]
	if handler == nil {
		return nil, fmt.Errorf("handler not implemented")
	}

	return handler(call, state)
}

func getAgents(call *master.Call, state *MasterState) (*master.Response, error) {
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

	return res, nil
}
