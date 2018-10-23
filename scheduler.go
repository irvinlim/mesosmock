package main

import (
	"bytes"
	"fmt"
	"net/http"
)

// Scheduler returns a Handler for providing the Mesos Scheduler HTTP API:
// https://mesos.apache.org/documentation/latest/scheduler-http-api/
func Scheduler(opts *Options) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := new(bytes.Buffer)
		defer func() {
			w.Write(buf.Bytes())
		}()

		if r.Method != "POST" {
			w.Header().Set("Allow", "POST")
			w.WriteHeader(http.StatusMethodNotAllowed)
			buf.WriteString(fmt.Sprintf("Expecting one of { 'POST' }, but received '%s'", r.Method))
			return
		}

		contentType := r.Header.Get("Content-Type")
		if contentType == "" {
			w.WriteHeader(http.StatusBadRequest)
			buf.WriteString("Expecting 'Content-Type' to be present")
			return
		}

		switch contentType {
		case "application/json",
			"application/x-protobuf":
		default:
			w.WriteHeader(http.StatusUnsupportedMediaType)
			buf.WriteString("Expecting 'Content-Type' of application/json or application/x-protobuf")
			return
		}

		// Protobuf API is not supported in mesosmock.
		if contentType == "application/x-protobuf" {
			w.WriteHeader(http.StatusUnsupportedMediaType)
			buf.WriteString("Protobuf API is not supported in mesosmock")
			return
		}

		w.Write(bytes.NewBufferString("Hello " + opts.ListenAddr).Bytes())
	})
}
