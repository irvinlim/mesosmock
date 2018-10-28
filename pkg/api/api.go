package api

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/irvinlim/mesosmock/pkg/config"
	"github.com/irvinlim/mesosmock/pkg/state"
)

// Server creates a Mesos master server for serving HTTP requests.
type Server struct {
	server     *http.Server
	listenAddr string
}

// NewServer creates a new API server for serving Mesos master requests.
func NewServer(o *config.Options, s *state.MasterState) *Server {
	httpLogger := log.New(os.Stdout, "http: ", log.LstdFlags)

	router := http.NewServeMux()
	router.Handle("/api/v1", Operator(s))
	router.Handle("/master/api/v1", Operator(s))
	router.Handle("/api/v1/operator", Operator(s))
	router.Handle("/master/api/v1/operator", Operator(s))
	router.Handle("/api/v1/scheduler", Scheduler(s))
	router.Handle("/master/api/v1/scheduler", Scheduler(s))

	server := http.Server{
		Addr:     o.GetAddress(),
		Handler:  logging(httpLogger)(validateReq()(router)),
		ErrorLog: httpLogger,
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

	log.Printf("Starting server on %s...\n", listener.Addr())
	return http.Serve(listener, nil)
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

func validateReq() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
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
}
