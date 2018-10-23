package main

import (
	"log"
	"net/http"
	"os"
)

// APIServer creates a Mesos master server for serving HTTP requests.
type APIServer struct {
	server     *http.Server
	listenAddr string
}

// NewAPIServer creates a new API server for serving Mesos master requests.
func NewAPIServer(opts *Options) (*APIServer, error) {
	httpLogger := log.New(os.Stdout, "http: ", log.LstdFlags)

	router := http.NewServeMux()
	router.Handle("/api/v1/scheduler", Scheduler(opts))
	router.Handle("/master/api/v1/scheduler", Scheduler(opts))

	server := http.Server{
		Addr:     opts.ListenAddr,
		Handler:  logging(httpLogger)(router),
		ErrorLog: httpLogger,
	}

	apiServer := &APIServer{
		server:     &server,
		listenAddr: opts.ListenAddr,
	}

	return apiServer, nil
}

// ListenAndServe starts the API server on the configured address.
func (s APIServer) ListenAndServe() error {
	return s.server.ListenAndServe()
}

func logging(logger *log.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				logger.Println(r.Method, r.URL.Path, r.RemoteAddr, r.UserAgent())
			}()

			next.ServeHTTP(w, r)
		})
	}
}
