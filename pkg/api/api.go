package api

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"

	"github.com/irvinlim/mesosmock/pkg/config"
	"github.com/irvinlim/mesosmock/pkg/state"
	logger "github.com/sirupsen/logrus"
)

// Server creates a Mesos master server for serving HTTP requests.
type Server struct {
	server     *http.Server
	listenAddr string
}

// NewServer creates a new API server for serving Mesos master requests.
func NewServer(o *config.Options, s *state.MasterState) *Server {
	httpLogger := logger.New()
	logW := log.New(httpLogger.Writer(), "", 0)

	router := http.NewServeMux()
	router.Handle("/api/v1", Operator(s))
	router.Handle("/master/api/v1", Operator(s))
	router.Handle("/api/v1/operator", Operator(s))
	router.Handle("/master/api/v1/operator", Operator(s))
	router.Handle("/api/v1/scheduler", Scheduler(o, s))
	router.Handle("/master/api/v1/scheduler", Scheduler(o, s))

	server := http.Server{
		Addr:     o.GetAddress(),
		Handler:  logging(logW)(profiling(validateReq(router))),
		ErrorLog: logW,
	}

	Server := &Server{
		server:     &server,
		listenAddr: o.GetAddress(),
	}

	return Server
}

// ListenAndServe starts the API server on the configured address.
func (s Server) ListenAndServe() error {
	listener, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		log.Fatal(err)
	}

	logger.Infof("Starting server on %s...\n", listener.Addr())
	return http.Serve(listener, s.server.Handler)
}

func profiling(next http.Handler) http.Handler {
	// Expose pprof endpoints
	router := http.NewServeMux()
	router.HandleFunc("/debug/pprof/", pprof.Index)

	router.Handle("/", next)
	return router
}

func logging(logger *log.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Don't defer logging since responses could be chunked/streamed.
			logger.Println(r.Method, r.URL.Path, r.RemoteAddr, r.UserAgent())
			next.ServeHTTP(w, r)
		})
	}
}

func validateReq(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.Header().Set("Allow", "POST")
			w.WriteHeader(http.StatusMethodNotAllowed)
			fmt.Fprintf(w, "Expecting one of { 'POST' }, but received '%s'", r.Method)
			return
		}

		contentType := r.Header.Get("Content-Type")
		if contentType == "" {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Expecting 'Content-Type' to be present")
			return
		}

		switch contentType {
		case
			"application/json",
			"application/x-protobuf":
		default:
			w.WriteHeader(http.StatusUnsupportedMediaType)
			fmt.Fprint(w, "Expecting 'Content-Type' of application/json or application/x-protobuf")
			return
		}

		// Protobuf API is not supported in mesosmock.
		if contentType == "application/x-protobuf" {
			w.WriteHeader(http.StatusUnsupportedMediaType)
			fmt.Fprint(w, "Protobuf API is not supported in mesosmock")
			return
		}

		// Otherwise, pass request to be handled by the next middleware.
		next.ServeHTTP(w, r)
	})
}
