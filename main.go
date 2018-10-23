package main

import (
	"flag"
	"log"
	"os"
)

func main() {
	flagSet := flag.NewFlagSet("mesosmock", flag.ExitOnError)
	config := flagSet.String("config", "", "path to config.json")
	flagSet.String("listenAddr", "5050", "address:port or port to listen on for HTTP requests")

	flagSet.Parse(os.Args[1:])

	opts, err := ConfigOptions(*config, flagSet)
	if err != nil {
		log.Fatalf("ERROR: Could not load config %s: %v", *config, err)
	}

	server, err := NewAPIServer(opts)

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Could not listen on %s: %v\n", opts.ListenAddr, err)
	}
}
