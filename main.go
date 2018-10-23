package main

import (
	"flag"
	"log"
	"net/http"
	"os"
)

func main() {
	httpLogger := log.New(os.Stdout, "http: ", log.LstdFlags)

	flagSet := flag.NewFlagSet("mesosmock", flag.ExitOnError)
	config := flagSet.String("config", "", "path to config.json")
	flagSet.String("listenAddr", "5050", "address:port or port to listen on for HTTP requests")

	flagSet.Parse(os.Args[1:])

	opts, err := ConfigOptions(*config, flagSet)
	if err != nil {
		log.Fatalf("ERROR: Could not load config %s: %v", *config, err)
	}

	router := http.NewServeMux()
	router.Handle("/api/v1/scheduler", Scheduler(opts))
	router.Handle("/master/api/v1/scheduler", Scheduler(opts))

	server := http.Server{
		Addr:     opts.ListenAddr,
		Handler:  logging(httpLogger)(router),
		ErrorLog: httpLogger,
	}

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Could not listen on %s: %v\n", opts.ListenAddr, err)
	}
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
