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

		buf.WriteString(fmt.Sprintf("Hello %s", opts.ListenAddr))
	})
}
