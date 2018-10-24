package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/mesos/mesos-go/api/v1/lib/recordio"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

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

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Mesos-Stream-Id", streamID.String())
	w.WriteHeader(http.StatusOK)

	heartbeat := float64(15)
	event := &scheduler.Event{
		Type: scheduler.Event_SUBSCRIBED,
		Subscribed: &scheduler.Event_Subscribed{
			FrameworkID:              call.Subscribe.FrameworkInfo.ID,
			HeartbeatIntervalSeconds: &heartbeat,
		},
	}

	res, err := event.MarshalJSON()
	if err != nil {
		log.Panicf("Cannot marshal JSON for SUBSCRIBE call: %#v", err)
	}

	for {
		writer.WriteFrame(res)
		flusher.Flush()
		time.Sleep(5 * time.Second)
	}
}
