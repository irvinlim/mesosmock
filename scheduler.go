package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

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

}
