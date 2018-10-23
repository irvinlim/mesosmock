package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/mesos/mesos-go/api/v1/lib/recordio"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

// Scheduler returns a Handler for providing the Mesos Scheduler HTTP API:
// https://mesos.apache.org/documentation/latest/scheduler-http-api/
func Scheduler(opts *Options) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := new(bytes.Buffer)

		call := &scheduler.Call{}
		err := json.NewDecoder(r.Body).Decode(&call)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			buf.WriteString(fmt.Sprintf("Failed to parse body into JSON: %s", err))
			w.Write(buf.Bytes())
			return
		}

		callTypeHandlers := map[scheduler.Call_Type]func(*scheduler.Call, http.ResponseWriter){
			scheduler.Call_SUBSCRIBE: subscribe,
		}

		if call.Type == scheduler.Call_UNKNOWN {
			w.WriteHeader(http.StatusBadRequest)
			buf.WriteString(fmt.Sprintf("Failed to validate scheduler::Call: Expecting 'type' to be present"))
			w.Write(buf.Bytes())
			return
		}

		// Invoke handler for different call types
		handler := callTypeHandlers[call.Type]
		if handler == nil {
			w.WriteHeader(http.StatusBadRequest)
			buf.WriteString(fmt.Sprintf("Failed to validate scheduler::Call: Handler not implemented"))
			w.Write(buf.Bytes())
			return
		}

		handler(call, w)
	})
}

func subscribe(call *scheduler.Call, w http.ResponseWriter) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		panic("expected http.ResponseWriter to be an http.Flusher")
	}

	writer := recordio.NewWriter(w)
	streamID, err := uuid.NewUUID()
	if err != nil {
		log.Panicf("Cannot create stream ID for SUBSCRIBE call: %#v", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.Header().Set("Mesos-Stream-Id", streamID.String())
	w.WriteHeader(http.StatusOK)

	heartbeat := float64(15)
	event := &scheduler.Event{
		Type: scheduler.Event_SUBSCRIBED,
		Subscribed: &scheduler.Event_Subscribed{
			FrameworkID:              call.FrameworkID,
			HeartbeatIntervalSeconds: &heartbeat,
		},
	}

	res, err := event.MarshalJSON()
	if err != nil {
		log.Panicf("Cannot marshal JSON for SUBSCRIBE call: %#v", err)
		return
	}

	for {
		writer.WriteFrame(res)
		flusher.Flush()
		time.Sleep(5 * time.Second)
	}
}
